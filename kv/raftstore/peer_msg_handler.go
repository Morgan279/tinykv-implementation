package raftstore

import (
	"fmt"
	"github.com/Connor1996/badger"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/meta"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
	"reflect"
	"time"

	"github.com/Connor1996/badger/y"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/message"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/runner"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/snap"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/util"
	"github.com/pingcap-incubator/tinykv/log"
	"github.com/pingcap-incubator/tinykv/proto/pkg/metapb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/raft_cmdpb"
	rspb "github.com/pingcap-incubator/tinykv/proto/pkg/raft_serverpb"
	"github.com/pingcap-incubator/tinykv/scheduler/pkg/btree"
	"github.com/pingcap/errors"
)

type PeerTick int

const (
	PeerTickRaft               PeerTick = 0
	PeerTickRaftLogGC          PeerTick = 1
	PeerTickSplitRegionCheck   PeerTick = 2
	PeerTickSchedulerHeartbeat PeerTick = 3
)

type peerMsgHandler struct {
	*peer
	ctx *GlobalContext
}

func newPeerMsgHandler(peer *peer, ctx *GlobalContext) *peerMsgHandler {
	return &peerMsgHandler{
		peer: peer,
		ctx:  ctx,
	}
}

func (d *peerMsgHandler) HandleRaftReady() {
	if d.stopped {
		return
	}
	// Your Code Here (2B).
	if !d.RaftGroup.HasReady() {
		return
	}

	rd := d.RaftGroup.Ready()
	result, err := d.peerStorage.SaveReadyState(&rd)
	if err != nil {
		return
	}
	if result != nil && !reflect.DeepEqual(result.PrevRegion, result.Region) {
		d.updateStoreMetaRegion(result.Region)
	}

	if len(rd.Messages) > 0 {
		d.Send(d.ctx.trans, rd.Messages)
	}

	if len(rd.CommittedEntries) > 0 {
		for _, e := range rd.CommittedEntries {
			kvWB := new(engine_util.WriteBatch)
			d.applyEntry(&e, kvWB)
			if d.stopped {
				wb := new(engine_util.WriteBatch)
				wb.DeleteMeta(meta.ApplyStateKey(d.regionId))
				if err := wb.WriteToDB(d.peerStorage.Engines.Kv); err != nil {
					panic(err)
				}
				return
			}
		}
	}
	d.RaftGroup.Advance(rd)
}

func (d *peerMsgHandler) applyEntry(entry *eraftpb.Entry, kvWB *engine_util.WriteBatch) {
	if entry.EntryType == eraftpb.EntryType_EntryConfChange {
		d.applyConfChange(entry, kvWB)
		return
	}

	cmd := &raft_cmdpb.RaftCmdRequest{}
	if err := cmd.Unmarshal(entry.Data); err != nil {
		panic(err)
	}

	switch cmd.AdminRequest {
	default:
		d.applyAdminReq(entry, kvWB, cmd.AdminRequest, cmd)
	case nil:
		d.applyNormalReqs(entry, cmd.Header, kvWB, cmd.Requests)
	}
}

func (d *peerMsgHandler) applyConfChange(entry *eraftpb.Entry, kvWB *engine_util.WriteBatch) {
	cc := &eraftpb.ConfChange{}
	if err := cc.Unmarshal(entry.Data); err != nil {
		panic(err)
	}
	msg := &raft_cmdpb.RaftCmdRequest{}
	err := msg.Unmarshal(cc.Context)
	if err != nil {
		panic(err)
	}
	region := d.Region()
	err = util.CheckRegionEpoch(msg, region, true)
	if errEpochNotMatching, ok := err.(*util.ErrEpochNotMatch); ok {
		d.response(entry, ErrResp(errEpochNotMatching), nil)
		return
	}
	switch cc.ChangeType {
	case eraftpb.ConfChangeType_AddNode:
		n := searchRegionPeer(region, cc.NodeId)
		if n == len(region.Peers) {
			peer := msg.AdminRequest.ChangePeer.Peer
			region.Peers = append(region.Peers, peer)
			region.RegionEpoch.ConfVer++
			meta.WriteRegionState(kvWB, region, rspb.PeerState_Normal)
			d.updateStoreMetaRegion(region)
			d.insertPeerCache(peer)
		}
	case eraftpb.ConfChangeType_RemoveNode:
		if cc.NodeId == d.Meta.Id {
			d.destroyPeer()
			return
		}
		n := searchRegionPeer(region, cc.NodeId)
		if n < len(region.Peers) {
			region.Peers = append(region.Peers[:n], region.Peers[n+1:]...)
			region.RegionEpoch.ConfVer++
			meta.WriteRegionState(kvWB, region, rspb.PeerState_Normal)
			d.updateStoreMetaRegion(region)
			d.removePeerCache(cc.NodeId)
		}
	}
	d.RaftGroup.ApplyConfChange(*cc)
	d.response(entry, &raft_cmdpb.RaftCmdResponse{
		Header: &raft_cmdpb.RaftResponseHeader{},
		AdminResponse: &raft_cmdpb.AdminResponse{
			CmdType:    raft_cmdpb.AdminCmdType_ChangePeer,
			ChangePeer: &raft_cmdpb.ChangePeerResponse{},
		},
	}, nil)
	if d.IsLeader() {
		d.HeartbeatScheduler(d.ctx.schedulerTaskSender)
	}
}

func searchRegionPeer(region *metapb.Region, id uint64) int {
	for i, peer := range region.Peers {
		if peer.Id == id {
			return i
		}
	}
	return len(region.Peers)
}

func (d *peerMsgHandler) updateStoreMetaRegion(region *metapb.Region) {
	storeMeta := d.ctx.storeMeta
	storeMeta.Lock()
	storeMeta.regions[region.Id] = region
	storeMeta.Unlock()
}

func (d *peerMsgHandler) applyAdminReq(entry *eraftpb.Entry, kvWB *engine_util.WriteBatch, req *raft_cmdpb.AdminRequest, cmd *raft_cmdpb.RaftCmdRequest) {
	switch req.CmdType {
	case raft_cmdpb.AdminCmdType_CompactLog:
		cl := req.GetCompactLog()
		applySt := d.peerStorage.applyState
		if cl.CompactIndex < applySt.TruncatedState.Index {
			return
		}
		applySt.TruncatedState.Index = cl.CompactIndex
		applySt.TruncatedState.Term = cl.CompactTerm
		kvWB.SetMeta(meta.ApplyStateKey(d.regionId), applySt)
		d.ScheduleCompactLog(cl.CompactIndex)
	case raft_cmdpb.AdminCmdType_Split:
		region := d.Region()
		err := util.CheckRegionEpoch(cmd, region, true)
		if errEpochNotMatching, ok := err.(*util.ErrEpochNotMatch); ok {
			d.response(entry, ErrResp(errEpochNotMatching), nil)
			return
		}
		split := req.GetSplit()
		err = util.CheckKeyInRegion(split.SplitKey, region)
		if err != nil {
			d.response(entry, ErrResp(err), nil)
			return
		}
		storeMeta := d.ctx.storeMeta
		storeMeta.Lock()
		storeMeta.regionRanges.Delete(&regionItem{region: region})
		region.RegionEpoch.Version++
		peers := make([]*metapb.Peer, 0)
		for i, peer := range region.Peers {
			peers = append(peers, &metapb.Peer{Id: split.NewPeerIds[i], StoreId: peer.StoreId})
		}
		newRegion := &metapb.Region{
			Id:       split.NewRegionId,
			StartKey: split.SplitKey,
			EndKey:   region.EndKey,
			Peers:    peers,
			RegionEpoch: &metapb.RegionEpoch{
				ConfVer: 1,
				Version: 1,
			},
		}
		storeMeta.regions[newRegion.Id] = newRegion
		region.EndKey = split.SplitKey
		storeMeta.regionRanges.ReplaceOrInsert(&regionItem{region: region})
		storeMeta.regionRanges.ReplaceOrInsert(&regionItem{region: newRegion})
		storeMeta.Unlock()
		meta.WriteRegionState(kvWB, region, rspb.PeerState_Normal)
		meta.WriteRegionState(kvWB, newRegion, rspb.PeerState_Normal)
		d.SizeDiffHint = 0
		d.ApproximateSize = new(uint64)
		peer, err := createPeer(d.ctx.store.Id, d.ctx.cfg, d.ctx.regionTaskSender, d.ctx.engine, newRegion)
		if err != nil {
			panic(err)
		}
		d.ctx.router.register(peer)
		d.ctx.router.send(newRegion.Id, message.Msg{Type: message.MsgTypeStart})
		d.response(entry, &raft_cmdpb.RaftCmdResponse{
			Header: &raft_cmdpb.RaftResponseHeader{},
			AdminResponse: &raft_cmdpb.AdminResponse{
				CmdType: raft_cmdpb.AdminCmdType_Split,
				Split:   &raft_cmdpb.SplitResponse{Regions: []*metapb.Region{region, newRegion}},
			},
		}, nil)
		if d.IsLeader() {
			d.HeartbeatScheduler(d.ctx.schedulerTaskSender)
		}
	}
}

func (d *peerMsgHandler) applyNormalReqs(entry *eraftpb.Entry, header *raft_cmdpb.RaftRequestHeader, kvWB *engine_util.WriteBatch, reqs []*raft_cmdpb.Request) {
	log.Info(fmt.Sprintf("receive entry[%d] apply request", entry.Index))
	if len(reqs) == 0 {
		d.persistApplyIndex(entry.Index, kvWB)
		log.Info(fmt.Sprintf("entry[%d] is a no op entry, igonre", entry.Index))
		return
	}

	for _, req := range reqs {
		if err := d.checkRequestKey(req); err != nil {
			log.Info(fmt.Sprintf("entry[%d] is not in the region, dropped", entry.Index))
			return
		}
	}

	resp := &raft_cmdpb.RaftCmdResponse{
		Header:    &raft_cmdpb.RaftResponseHeader{},
		Responses: make([]*raft_cmdpb.Response, 0, len(reqs)),
	}
	//types of reqs are all the same
	switch reqs[0].CmdType {
	case raft_cmdpb.CmdType_Get:
		for _, req := range reqs {
			v, _ := engine_util.GetCF(d.peerStorage.Engines.Kv, req.Get.GetCf(), req.Get.GetKey())
			resp.Responses = append(resp.Responses, &raft_cmdpb.Response{
				CmdType: raft_cmdpb.CmdType_Get,
				Get:     &raft_cmdpb.GetResponse{Value: v},
			})
		}
		log.Info(fmt.Sprintf("apply entry %+v, Get response: %+v", entry.Index, resp))
	case raft_cmdpb.CmdType_Put:
		for _, req := range reqs {
			kvWB.SetCF(req.Put.GetCf(), req.Put.GetKey(), req.Put.GetValue())
			resp.Responses = append(resp.Responses, &raft_cmdpb.Response{
				CmdType: raft_cmdpb.CmdType_Put,
				Put:     &raft_cmdpb.PutResponse{},
			})
		}
		log.Info(fmt.Sprintf("apply entry %+v, Put response: %+v", entry.Index, resp))
	case raft_cmdpb.CmdType_Delete:
		for _, req := range reqs {
			kvWB.DeleteCF(req.Delete.GetCf(), req.Delete.GetKey())
			resp.Responses = append(resp.Responses, &raft_cmdpb.Response{
				CmdType: raft_cmdpb.CmdType_Delete,
				Delete:  &raft_cmdpb.DeleteResponse{},
			})
		}
	case raft_cmdpb.CmdType_Snap:
		if header.RegionEpoch.Version != d.Region().RegionEpoch.Version {
			d.response(entry, ErrResp(&util.ErrEpochNotMatch{}), nil)
			return
		}
		for _, _ = range reqs {
			resp.Responses = append(resp.Responses, &raft_cmdpb.Response{
				CmdType: raft_cmdpb.CmdType_Snap,
				Snap:    &raft_cmdpb.SnapResponse{Region: d.Region()},
			})
		}
		d.persistApplyIndex(entry.Index, kvWB)
		d.response(entry, resp, d.peerStorage.Engines.Kv.NewTransaction(false))
		return

	default:
		panic("unknown cmd type")
	}

	d.persistApplyIndex(entry.Index, kvWB)
	d.response(entry, resp, nil)
}

func (d *peerMsgHandler) persistApplyIndex(index uint64, kvWB *engine_util.WriteBatch) {
	d.peerStorage.applyState.AppliedIndex = index
	kvWB.SetMeta(meta.ApplyStateKey(d.regionId), d.peerStorage.applyState)
	kvWB.WriteToDB(d.peerStorage.Engines.Kv)
}

func (d *peerMsgHandler) staleCheck(entry *eraftpb.Entry) bool {
	if len(d.proposals) == 0 {
		return true
	}

	p := d.proposals[0]
	//log.Info(fmt.Sprintf("stale check: entry[index = %d term = %d] p[index = %d term = %d]", entry.Index, entry.Term, p.index, p.term))
	for p.index < entry.Index {
		log.Info(fmt.Sprintf("current entry index is %d, drop proposal[%d]", entry.Index, p.index))
		p.cb.Done(ErrResp(&util.ErrStaleCommand{}))
		d.proposals = d.proposals[1:]
		if len(d.proposals) == 0 {
			return true
		}
		p = d.proposals[0]
	}

	if p.index == entry.Index {
		if p.term != entry.Term {
			log.Info(fmt.Sprintf("current term is %d, drop proposal[index = %d term = %d]", entry.Term, p.index, p.term))
			NotifyStaleReq(entry.Term, p.cb)
			d.proposals = d.proposals[1:]
			return true
		} else {
			return false
		}
	}
	return true
	//panic("p.index can not greater than entry.index")
	//p := d.proposals[0]
	//if entry.Term == p.term && entry.Index < p.index {
	//	return true
	//}
	//
	//if entry.Term == p.term && entry.Index == p.index {
	//	return false
	//} else {
	//	p.cb.Done(ErrRespStaleCommand(p.term))
	//	d.proposals = d.proposals[1:]
	//	return true
	//}
}

func (d *peerMsgHandler) response(entry *eraftpb.Entry, resp *raft_cmdpb.RaftCmdResponse, txn *badger.Txn) {
	if d.staleCheck(entry) {
		log.Info(fmt.Sprintf("try to response a stale entry[index = %d, term = %d], applied but not response to client", entry.Index, entry.Term))
		return
	}
	p := d.proposals[0]
	p.cb.Txn = txn
	p.cb.Done(resp)
	d.proposals = d.proposals[1:]
	log.Info(fmt.Sprintf("response proposal[%d]", p.index))
}

func (d *peerMsgHandler) HandleMsg(msg message.Msg) {
	switch msg.Type {
	case message.MsgTypeRaftMessage:
		raftMsg := msg.Data.(*rspb.RaftMessage)
		if err := d.onRaftMsg(raftMsg); err != nil {
			log.Errorf("%s handle raft message error %v", d.Tag, err)
		}
	case message.MsgTypeRaftCmd:
		raftCMD := msg.Data.(*message.MsgRaftCmd)
		d.proposeRaftCommand(raftCMD.Request, raftCMD.Callback)
	case message.MsgTypeTick:
		d.onTick()
	case message.MsgTypeSplitRegion:
		split := msg.Data.(*message.MsgSplitRegion)
		log.Infof("%s on split with %v", d.Tag, split.SplitKey)
		d.onPrepareSplitRegion(split.RegionEpoch, split.SplitKey, split.Callback)
	case message.MsgTypeRegionApproximateSize:
		d.onApproximateRegionSize(msg.Data.(uint64))
	case message.MsgTypeGcSnap:
		gcSnap := msg.Data.(*message.MsgGCSnap)
		d.onGCSnap(gcSnap.Snaps)
	case message.MsgTypeStart:
		d.startTicker()
	}
}

func (d *peerMsgHandler) preProposeRaftCommand(req *raft_cmdpb.RaftCmdRequest) error {
	// Check store_id, make sure that the msg is dispatched to the right place.
	if err := util.CheckStoreID(req, d.storeID()); err != nil {
		return err
	}

	// Check whether the store has the right peer to handle the request.
	regionID := d.regionId
	leaderID := d.LeaderId()
	if !d.IsLeader() {
		leader := d.getPeerFromCache(leaderID)
		return &util.ErrNotLeader{RegionId: regionID, Leader: leader}
	}
	// peer_id must be the same as peer's.
	if err := util.CheckPeerID(req, d.PeerId()); err != nil {
		return err
	}
	// Check whether the term is stale.
	if err := util.CheckTerm(req, d.Term()); err != nil {
		return err
	}
	err := util.CheckRegionEpoch(req, d.Region(), true)
	if errEpochNotMatching, ok := err.(*util.ErrEpochNotMatch); ok {
		// Attach the region which might be split from the current region. But it doesn't
		// matter if the region is not split from the current region. If the region meta
		// received by the TiKV driver is newer than the meta cached in the driver, the meta is
		// updated.
		siblingRegion := d.findSiblingRegion()
		if siblingRegion != nil {
			errEpochNotMatching.Regions = append(errEpochNotMatching.Regions, siblingRegion)
		}
		return errEpochNotMatching
	}
	return err
}

func (d *peerMsgHandler) proposeRaftCommand(msg *raft_cmdpb.RaftCmdRequest, cb *message.Callback) {
	err := d.preProposeRaftCommand(msg)
	if err != nil {
		cb.Done(ErrResp(err))
		return
	}
	// Your Code Here (2B).
	data, err := msg.Marshal()
	if err != nil {
		panic(err)
	}

	switch msg.AdminRequest {
	default:
		log.Info(fmt.Sprintf("propose a admin req: %+v", msg.AdminRequest))
		d.proposeAdminReq(msg.AdminRequest, data, cb)
	case nil:
		log.Info(fmt.Sprintf("propose normal reqs: %+v", msg.Requests))
		d.proposeNormalReqs(msg.Requests, data, cb)
	}
}

func (d *peerMsgHandler) proposeNormalReqs(reqs []*raft_cmdpb.Request, data []byte, cb *message.Callback) {
	if len(reqs) == 0 {
		log.Fatal("receive an empty request")
		//cb.Done(ErrResp(errors.Errorf("can not propose empty request")))
		//return
	}

	for _, req := range reqs {
		if err := d.checkRequestKey(req); err != nil {
			cb.Done(ErrResp(err))
			return
		}
	}

	d.propose(cb)
	log.Info(fmt.Sprintf("add a normal request proposal(index = %d), there are %d proposals at all now", d.proposals[len(d.proposals)-1].index, len(d.proposals)))
	if err := d.RaftGroup.Propose(data); err != nil {
		panic(err)
		//cb.Done(ErrResp(err))
	}
}

func (d *peerMsgHandler) proposeAdminReq(req *raft_cmdpb.AdminRequest, data []byte, cb *message.Callback) {
	switch req.CmdType {
	case raft_cmdpb.AdminCmdType_ChangePeer:
		d.handleProposeChangePeer(req, data, cb)
	case raft_cmdpb.AdminCmdType_CompactLog:
		d.handleProposeCompactLog(req, data, cb)
	case raft_cmdpb.AdminCmdType_TransferLeader:
		d.handleProposeTransferLeader(req, data, cb)
	case raft_cmdpb.AdminCmdType_Split:
		d.handleProposeSplit(req, data, cb)
	default:
		cb.Done(ErrResp(errors.Errorf(fmt.Sprintf("invalid command type: %+v", req.CmdType))))
	}
}

func (d *peerMsgHandler) handleProposeChangePeer(req *raft_cmdpb.AdminRequest, data []byte, cb *message.Callback) {
	if d.RaftGroup.Raft.PendingConfIndex > d.peerStorage.AppliedIndex() {
		cb.Done(ErrResp(errors.Errorf("there is a pending conf change, try again later")))
		return
	}
	d.propose(cb)
	d.RaftGroup.ProposeConfChange(eraftpb.ConfChange{
		ChangeType: req.ChangePeer.ChangeType,
		NodeId:     req.ChangePeer.Peer.Id,
		Context:    data,
	})
}

func (d *peerMsgHandler) handleProposeCompactLog(req *raft_cmdpb.AdminRequest, data []byte, cb *message.Callback) {
	d.RaftGroup.Propose(data)
}

func (d *peerMsgHandler) handleProposeTransferLeader(req *raft_cmdpb.AdminRequest, data []byte, cb *message.Callback) {
	d.RaftGroup.TransferLeader(req.TransferLeader.Peer.Id)
	cb.Done(&raft_cmdpb.RaftCmdResponse{
		Header: &raft_cmdpb.RaftResponseHeader{},
		AdminResponse: &raft_cmdpb.AdminResponse{
			CmdType:        raft_cmdpb.AdminCmdType_TransferLeader,
			TransferLeader: &raft_cmdpb.TransferLeaderResponse{},
		},
	})
}

func (d *peerMsgHandler) handleProposeSplit(req *raft_cmdpb.AdminRequest, data []byte, cb *message.Callback) {
	if err := util.CheckKeyInRegion(req.Split.SplitKey, d.Region()); err != nil {
		cb.Done(ErrResp(err))
		return
	}
	d.propose(cb)
	d.RaftGroup.Propose(data)
}

func (d *peerMsgHandler) checkRequestKey(req *raft_cmdpb.Request) error {
	switch req.CmdType {
	case raft_cmdpb.CmdType_Get:
		return util.CheckKeyInRegion(req.Get.Key, d.Region())
	case raft_cmdpb.CmdType_Put:
		return util.CheckKeyInRegion(req.Put.Key, d.Region())
	case raft_cmdpb.CmdType_Delete:
		return util.CheckKeyInRegion(req.Delete.Key, d.Region())
	case raft_cmdpb.CmdType_Snap:
		return nil
	}
	return errors.Errorf(fmt.Sprintf("invalid command type: %+v", req.CmdType))
}

func (d *peerMsgHandler) propose(cb *message.Callback) {
	d.proposals = append(d.proposals, &proposal{index: d.nextProposalIndex(), term: d.Term(), cb: cb})
}

func (d *peerMsgHandler) onTick() {
	if d.stopped {
		return
	}
	d.ticker.tickClock()
	if d.ticker.isOnTick(PeerTickRaft) {
		d.onRaftBaseTick()
	}
	if d.ticker.isOnTick(PeerTickRaftLogGC) {
		d.onRaftGCLogTick()
	}
	if d.ticker.isOnTick(PeerTickSchedulerHeartbeat) {
		d.onSchedulerHeartbeatTick()
	}
	if d.ticker.isOnTick(PeerTickSplitRegionCheck) {
		d.onSplitRegionCheckTick()
	}
	d.ctx.tickDriverSender <- d.regionId
}

func (d *peerMsgHandler) startTicker() {
	d.ticker = newTicker(d.regionId, d.ctx.cfg)
	d.ctx.tickDriverSender <- d.regionId
	d.ticker.schedule(PeerTickRaft)
	d.ticker.schedule(PeerTickRaftLogGC)
	d.ticker.schedule(PeerTickSplitRegionCheck)
	d.ticker.schedule(PeerTickSchedulerHeartbeat)
}

func (d *peerMsgHandler) onRaftBaseTick() {
	d.RaftGroup.Tick()
	d.ticker.schedule(PeerTickRaft)
}

func (d *peerMsgHandler) ScheduleCompactLog(truncatedIndex uint64) {
	raftLogGCTask := &runner.RaftLogGCTask{
		RaftEngine: d.ctx.engine.Raft,
		RegionID:   d.regionId,
		StartIdx:   d.LastCompactedIdx,
		EndIdx:     truncatedIndex + 1,
	}
	d.LastCompactedIdx = raftLogGCTask.EndIdx
	d.ctx.raftLogGCTaskSender <- raftLogGCTask
}

func (d *peerMsgHandler) onRaftMsg(msg *rspb.RaftMessage) error {
	log.Debugf("%s handle raft message %s from %d to %d",
		d.Tag, msg.GetMessage().GetMsgType(), msg.GetFromPeer().GetId(), msg.GetToPeer().GetId())
	if !d.validateRaftMessage(msg) {
		return nil
	}
	if d.stopped {
		return nil
	}
	if msg.GetIsTombstone() {
		// we receive a message tells us to remove self.
		d.handleGCPeerMsg(msg)
		return nil
	}
	if d.checkMessage(msg) {
		return nil
	}
	key, err := d.checkSnapshot(msg)
	if err != nil {
		return err
	}
	if key != nil {
		// If the snapshot file is not used again, then it's OK to
		// delete them here. If the snapshot file will be reused when
		// receiving, then it will fail to pass the check again, so
		// missing snapshot files should not be noticed.
		s, err1 := d.ctx.snapMgr.GetSnapshotForApplying(*key)
		if err1 != nil {
			return err1
		}
		d.ctx.snapMgr.DeleteSnapshot(*key, s, false)
		return nil
	}
	d.insertPeerCache(msg.GetFromPeer())
	err = d.RaftGroup.Step(*msg.GetMessage())
	if err != nil {
		return err
	}
	if d.AnyNewPeerCatchUp(msg.FromPeer.Id) {
		d.HeartbeatScheduler(d.ctx.schedulerTaskSender)
	}
	return nil
}

// return false means the message is invalid, and can be ignored.
func (d *peerMsgHandler) validateRaftMessage(msg *rspb.RaftMessage) bool {
	regionID := msg.GetRegionId()
	from := msg.GetFromPeer()
	to := msg.GetToPeer()
	log.Debugf("[region %d] handle raft message %s from %d to %d", regionID, msg, from.GetId(), to.GetId())
	if to.GetStoreId() != d.storeID() {
		log.Warnf("[region %d] store not match, to store id %d, mine %d, ignore it",
			regionID, to.GetStoreId(), d.storeID())
		return false
	}
	if msg.RegionEpoch == nil {
		log.Errorf("[region %d] missing epoch in raft message, ignore it", regionID)
		return false
	}
	return true
}

/// Checks if the message is sent to the correct peer.
///
/// Returns true means that the message can be dropped silently.
func (d *peerMsgHandler) checkMessage(msg *rspb.RaftMessage) bool {
	fromEpoch := msg.GetRegionEpoch()
	isVoteMsg := util.IsVoteMessage(msg.Message)
	fromStoreID := msg.FromPeer.GetStoreId()

	// Let's consider following cases with three nodes [1, 2, 3] and 1 is leader:
	// a. 1 removes 2, 2 may still send MsgAppendResponse to 1.
	//  We should ignore this stale message and let 2 remove itself after
	//  applying the ConfChange log.
	// b. 2 is isolated, 1 removes 2. When 2 rejoins the cluster, 2 will
	//  send stale MsgRequestVote to 1 and 3, at this time, we should tell 2 to gc itself.
	// c. 2 is isolated but can communicate with 3. 1 removes 3.
	//  2 will send stale MsgRequestVote to 3, 3 should ignore this message.
	// d. 2 is isolated but can communicate with 3. 1 removes 2, then adds 4, remove 3.
	//  2 will send stale MsgRequestVote to 3, 3 should tell 2 to gc itself.
	// e. 2 is isolated. 1 adds 4, 5, 6, removes 3, 1. Now assume 4 is leader.
	//  After 2 rejoins the cluster, 2 may send stale MsgRequestVote to 1 and 3,
	//  1 and 3 will ignore this message. Later 4 will send messages to 2 and 2 will
	//  rejoin the raft group again.
	// f. 2 is isolated. 1 adds 4, 5, 6, removes 3, 1. Now assume 4 is leader, and 4 removes 2.
	//  unlike case e, 2 will be stale forever.
	// TODO: for case f, if 2 is stale for a long time, 2 will communicate with scheduler and scheduler will
	// tell 2 is stale, so 2 can remove itself.
	region := d.Region()
	if util.IsEpochStale(fromEpoch, region.RegionEpoch) && util.FindPeer(region, fromStoreID) == nil {
		// The message is stale and not in current region.
		handleStaleMsg(d.ctx.trans, msg, region.RegionEpoch, isVoteMsg)
		return true
	}
	target := msg.GetToPeer()
	if target.Id < d.PeerId() {
		log.Infof("%s target peer ID %d is less than %d, msg maybe stale", d.Tag, target.Id, d.PeerId())
		return true
	} else if target.Id > d.PeerId() {
		if d.MaybeDestroy() {
			log.Infof("%s is stale as received a larger peer %s, destroying", d.Tag, target)
			d.destroyPeer()
			d.ctx.router.sendStore(message.NewMsg(message.MsgTypeStoreRaftMessage, msg))
		}
		return true
	}
	return false
}

func handleStaleMsg(trans Transport, msg *rspb.RaftMessage, curEpoch *metapb.RegionEpoch,
	needGC bool) {
	regionID := msg.RegionId
	fromPeer := msg.FromPeer
	toPeer := msg.ToPeer
	msgType := msg.Message.GetMsgType()

	if !needGC {
		log.Infof("[region %d] raft message %s is stale, current %v ignore it",
			regionID, msgType, curEpoch)
		return
	}
	gcMsg := &rspb.RaftMessage{
		RegionId:    regionID,
		FromPeer:    toPeer,
		ToPeer:      fromPeer,
		RegionEpoch: curEpoch,
		IsTombstone: true,
	}
	if err := trans.Send(gcMsg); err != nil {
		log.Errorf("[region %d] send message failed %v", regionID, err)
	}
}

func (d *peerMsgHandler) handleGCPeerMsg(msg *rspb.RaftMessage) {
	fromEpoch := msg.RegionEpoch
	if !util.IsEpochStale(d.Region().RegionEpoch, fromEpoch) {
		return
	}
	if !util.PeerEqual(d.Meta, msg.ToPeer) {
		log.Infof("%s receive stale gc msg, ignore", d.Tag)
		return
	}
	log.Infof("%s peer %s receives gc message, trying to remove", d.Tag, msg.ToPeer)
	if d.MaybeDestroy() {
		d.destroyPeer()
	}
}

// Returns `None` if the `msg` doesn't contain a snapshot or it contains a snapshot which
// doesn't conflict with any other snapshots or regions. Otherwise a `snap.SnapKey` is returned.
func (d *peerMsgHandler) checkSnapshot(msg *rspb.RaftMessage) (*snap.SnapKey, error) {
	if msg.Message.Snapshot == nil {
		return nil, nil
	}
	regionID := msg.RegionId
	snapshot := msg.Message.Snapshot
	key := snap.SnapKeyFromRegionSnap(regionID, snapshot)
	snapData := new(rspb.RaftSnapshotData)
	err := snapData.Unmarshal(snapshot.Data)
	if err != nil {
		return nil, err
	}
	snapRegion := snapData.Region
	peerID := msg.ToPeer.Id
	var contains bool
	for _, peer := range snapRegion.Peers {
		if peer.Id == peerID {
			contains = true
			break
		}
	}
	if !contains {
		log.Infof("%s %s doesn't contains peer %d, skip", d.Tag, snapRegion, peerID)
		return &key, nil
	}
	meta := d.ctx.storeMeta
	meta.Lock()
	defer meta.Unlock()
	if !util.RegionEqual(meta.regions[d.regionId], d.Region()) {
		if !d.isInitialized() {
			log.Infof("%s stale delegate detected, skip", d.Tag)
			return &key, nil
		} else {
			panic(fmt.Sprintf("%s meta corrupted %s != %s", d.Tag, meta.regions[d.regionId], d.Region()))
		}
	}

	existRegions := meta.getOverlapRegions(snapRegion)
	for _, existRegion := range existRegions {
		if existRegion.GetId() == snapRegion.GetId() {
			continue
		}
		log.Infof("%s region overlapped %s %s", d.Tag, existRegion, snapRegion)
		return &key, nil
	}

	// check if snapshot file exists.
	_, err = d.ctx.snapMgr.GetSnapshotForApplying(key)
	if err != nil {
		return nil, err
	}
	return nil, nil
}

func (d *peerMsgHandler) destroyPeer() {
	log.Infof("%s starts destroy", d.Tag)
	regionID := d.regionId
	// We can't destroy a peer which is applying snapshot.
	meta := d.ctx.storeMeta
	meta.Lock()
	defer meta.Unlock()
	isInitialized := d.isInitialized()
	if err := d.Destroy(d.ctx.engine, false); err != nil {
		// If not panic here, the peer will be recreated in the next restart,
		// then it will be gc again. But if some overlap region is created
		// before restarting, the gc action will delete the overlap region's
		// data too.
		panic(fmt.Sprintf("%s destroy peer %v", d.Tag, err))
	}
	d.ctx.router.close(regionID)
	d.stopped = true
	if isInitialized && meta.regionRanges.Delete(&regionItem{region: d.Region()}) == nil {
		panic(d.Tag + " meta corruption detected")
	}
	if _, ok := meta.regions[regionID]; !ok {
		panic(d.Tag + " meta corruption detected")
	}
	delete(meta.regions, regionID)
}

func (d *peerMsgHandler) findSiblingRegion() (result *metapb.Region) {
	meta := d.ctx.storeMeta
	meta.RLock()
	defer meta.RUnlock()
	item := &regionItem{region: d.Region()}
	meta.regionRanges.AscendGreaterOrEqual(item, func(i btree.Item) bool {
		result = i.(*regionItem).region
		return true
	})
	return
}

func (d *peerMsgHandler) onRaftGCLogTick() {
	d.ticker.schedule(PeerTickRaftLogGC)
	if !d.IsLeader() {
		return
	}

	appliedIdx := d.peerStorage.AppliedIndex()
	firstIdx, _ := d.peerStorage.FirstIndex()
	var compactIdx uint64
	if appliedIdx > firstIdx && appliedIdx-firstIdx >= d.ctx.cfg.RaftLogGcCountLimit {
		compactIdx = appliedIdx
	} else {
		return
	}

	y.Assert(compactIdx > 0)
	compactIdx -= 1
	if compactIdx < firstIdx {
		// In case compact_idx == first_idx before subtraction.
		return
	}

	term, err := d.RaftGroup.Raft.RaftLog.Term(compactIdx)
	if err != nil {
		log.Fatalf("appliedIdx: %d, firstIdx: %d, compactIdx: %d", appliedIdx, firstIdx, compactIdx)
		panic(err)
	}

	// Create a compact log request and notify directly.
	regionID := d.regionId
	request := newCompactLogRequest(regionID, d.Meta, compactIdx, term)
	d.proposeRaftCommand(request, nil)
}

func (d *peerMsgHandler) onSplitRegionCheckTick() {
	d.ticker.schedule(PeerTickSplitRegionCheck)
	// To avoid frequent scan, we only add new scan tasks if all previous tasks
	// have finished.
	if len(d.ctx.splitCheckTaskSender) > 0 {
		return
	}

	if !d.IsLeader() {
		return
	}
	if d.ApproximateSize != nil && d.SizeDiffHint < d.ctx.cfg.RegionSplitSize/8 {
		return
	}
	d.ctx.splitCheckTaskSender <- &runner.SplitCheckTask{
		Region: d.Region(),
	}
	d.SizeDiffHint = 0
}

func (d *peerMsgHandler) onPrepareSplitRegion(regionEpoch *metapb.RegionEpoch, splitKey []byte, cb *message.Callback) {
	if err := d.validateSplitRegion(regionEpoch, splitKey); err != nil {
		cb.Done(ErrResp(err))
		return
	}
	region := d.Region()
	d.ctx.schedulerTaskSender <- &runner.SchedulerAskSplitTask{
		Region:   region,
		SplitKey: splitKey,
		Peer:     d.Meta,
		Callback: cb,
	}
}

func (d *peerMsgHandler) validateSplitRegion(epoch *metapb.RegionEpoch, splitKey []byte) error {
	if len(splitKey) == 0 {
		err := errors.Errorf("%s split key should not be empty", d.Tag)
		log.Error(err)
		return err
	}

	if !d.IsLeader() {
		// region on this store is no longer leader, skipped.
		log.Infof("%s not leader, skip", d.Tag)
		return &util.ErrNotLeader{
			RegionId: d.regionId,
			Leader:   d.getPeerFromCache(d.LeaderId()),
		}
	}

	region := d.Region()
	latestEpoch := region.GetRegionEpoch()

	// This is a little difference for `check_region_epoch` in region split case.
	// Here we just need to check `version` because `conf_ver` will be update
	// to the latest value of the peer, and then send to Scheduler.
	if latestEpoch.Version != epoch.Version {
		log.Infof("%s epoch changed, retry later, prev_epoch: %s, epoch %s",
			d.Tag, latestEpoch, epoch)
		return &util.ErrEpochNotMatch{
			Message: fmt.Sprintf("%s epoch changed %s != %s, retry later", d.Tag, latestEpoch, epoch),
			Regions: []*metapb.Region{region},
		}
	}
	return nil
}

func (d *peerMsgHandler) onApproximateRegionSize(size uint64) {
	d.ApproximateSize = &size
}

func (d *peerMsgHandler) onSchedulerHeartbeatTick() {
	d.ticker.schedule(PeerTickSchedulerHeartbeat)

	if !d.IsLeader() {
		return
	}
	d.HeartbeatScheduler(d.ctx.schedulerTaskSender)
}

func (d *peerMsgHandler) onGCSnap(snaps []snap.SnapKeyWithSending) {
	compactedIdx := d.peerStorage.truncatedIndex()
	compactedTerm := d.peerStorage.truncatedTerm()
	for _, snapKeyWithSending := range snaps {
		key := snapKeyWithSending.SnapKey
		if snapKeyWithSending.IsSending {
			snap, err := d.ctx.snapMgr.GetSnapshotForSending(key)
			if err != nil {
				log.Errorf("%s failed to load snapshot for %s %v", d.Tag, key, err)
				continue
			}
			if key.Term < compactedTerm || key.Index < compactedIdx {
				log.Infof("%s snap file %s has been compacted, delete", d.Tag, key)
				d.ctx.snapMgr.DeleteSnapshot(key, snap, false)
			} else if fi, err1 := snap.Meta(); err1 == nil {
				modTime := fi.ModTime()
				if time.Since(modTime) > 4*time.Hour {
					log.Infof("%s snap file %s has been expired, delete", d.Tag, key)
					d.ctx.snapMgr.DeleteSnapshot(key, snap, false)
				}
			}
		} else if key.Term <= compactedTerm &&
			(key.Index < compactedIdx || key.Index == compactedIdx) {
			log.Infof("%s snap file %s has been applied, delete", d.Tag, key)
			a, err := d.ctx.snapMgr.GetSnapshotForApplying(key)
			if err != nil {
				log.Errorf("%s failed to load snapshot for %s %v", d.Tag, key, err)
				continue
			}
			d.ctx.snapMgr.DeleteSnapshot(key, a, false)
		}
	}
}

func newAdminRequest(regionID uint64, peer *metapb.Peer) *raft_cmdpb.RaftCmdRequest {
	return &raft_cmdpb.RaftCmdRequest{
		Header: &raft_cmdpb.RaftRequestHeader{
			RegionId: regionID,
			Peer:     peer,
		},
	}
}

func newCompactLogRequest(regionID uint64, peer *metapb.Peer, compactIndex, compactTerm uint64) *raft_cmdpb.RaftCmdRequest {
	req := newAdminRequest(regionID, peer)
	req.AdminRequest = &raft_cmdpb.AdminRequest{
		CmdType: raft_cmdpb.AdminCmdType_CompactLog,
		CompactLog: &raft_cmdpb.CompactLogRequest{
			CompactIndex: compactIndex,
			CompactTerm:  compactTerm,
		},
	}
	return req
}
