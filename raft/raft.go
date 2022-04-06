// Copyright 2015 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package raft

import (
	"errors"
	"fmt"
	"github.com/pingcap/log"
	"go.uber.org/zap"
	"math/rand"
	"sort"
	"time"

	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
)

// None is a placeholder node ID used when there is no leader.
const None uint64 = 0

// StateType represents the role of a node in a cluster.
type StateType uint64

const (
	StateFollower StateType = iota
	StateCandidate
	StateLeader
)

var stmap = [...]string{
	"StateFollower",
	"StateCandidate",
	"StateLeader",
}

func (st StateType) String() string {
	return stmap[uint64(st)]
}

// ErrProposalDropped is returned when the proposal is ignored by some cases,
// so that the proposer can be notified and fail fast.
var ErrProposalDropped = errors.New("raft proposal dropped")

// Config contains the parameters to start a raft.
type Config struct {
	// ID is the identity of the local raft. ID cannot be 0.
	ID uint64

	// peers contains the IDs of all nodes (including self) in the raft cluster. It
	// should only be set when starting a new raft cluster. Restarting raft from
	// previous configuration will panic if peers is set. peer is private and only
	// used for testing right now.
	peers []uint64

	// ElectionTick is the number of Node.Tick invocations that must pass between
	// elections. That is, if a follower does not receive any message from the
	// leader of current term before ElectionTick has elapsed, it will become
	// candidate and start an election. ElectionTick must be greater than
	// HeartbeatTick. We suggest ElectionTick = 10 * HeartbeatTick to avoid
	// unnecessary leader switching.
	ElectionTick int
	// HeartbeatTick is the number of Node.Tick invocations that must pass between
	// heartbeats. That is, a leader sends heartbeat messages to maintain its
	// leadership every HeartbeatTick ticks.
	HeartbeatTick int

	// Storage is the storage for raft. raft generates entries and states to be
	// stored in storage. raft reads the persisted entries and states out of
	// Storage when it needs. raft reads out the previous state and configuration
	// out of storage when restarting.
	Storage Storage
	// Applied is the last applied index. It should only be set when restarting
	// raft. raft will not return entries to the application smaller or equal to
	// Applied. If Applied is unset when restarting, raft might return previous
	// applied entries. This is a very application dependent configuration.
	Applied uint64
}

func (c *Config) validate() error {
	if c.ID == None {
		return errors.New("cannot use none as id")
	}

	if c.HeartbeatTick <= 0 {
		return errors.New("heartbeat tick must be greater than 0")
	}

	if c.ElectionTick <= c.HeartbeatTick {
		return errors.New("election tick must be greater than heartbeat tick")
	}

	if c.Storage == nil {
		return errors.New("storage cannot be nil")
	}

	return nil
}

// Progress represents a follower’s progress in the view of the leader. Leader maintains
// progresses of all followers, and sends entries to the follower based on its progress.
type Progress struct {
	Match, Next uint64
}

type Raft struct {
	id uint64

	Term uint64
	Vote uint64

	// the log
	RaftLog *RaftLog

	// log replication progress of each peers
	Prs map[uint64]*Progress

	// this peer's role
	State StateType

	// votes records
	votes map[uint64]bool

	// msgs need to send
	msgs []pb.Message

	// the leader id
	Lead uint64

	// heartbeat interval, should send
	heartbeatTimeout int
	// baseline of election interval
	electionTimeout int
	// actual election interval
	randomizedElectionTimeout int

	// number of ticks since it reached last heartbeatTimeout.
	// only leader keeps heartbeatElapsed.
	heartbeatElapsed int
	// Ticks since it reached last electionTimeout when it is leader or candidate.
	// Number of ticks since it reached last electionTimeout or received a
	// valid message from current leader when it is a follower.
	electionElapsed int

	// leadTransferee is id of the leader transfer target when its value is not zero.
	// Follow the procedure defined in section 3.10 of Raft phd thesis.
	// (https://web.stanford.edu/~ouster/cgi-bin/papers/OngaroPhD.pdf)
	// (Used in 3A leader transfer)
	leadTransferee uint64

	// Only one conf change may be pending (in the log, but not yet
	// applied) at a time. This is enforced via PendingConfIndex, which
	// is set to a value >= the log index of the latest pending
	// configuration change (if any). Config changes are only allowed to
	// be proposed if the leader's applied index is greater than this
	// value.
	// (Used in 3A conf change)
	PendingConfIndex uint64
}

// newRaft return a raft peer with the given config
func newRaft(c *Config) *Raft {
	if err := c.validate(); err != nil {
		panic(err.Error())
	}
	// Your Code Here (2A).
	hs, cs, err := c.Storage.InitialState()
	if err != nil {
		panic(err)
	}
	peers := c.peers
	if len(cs.Nodes) > 0 {
		if len(peers) > 0 {
			panic("initial peers are conflicted")
		}
		peers = cs.Nodes
	}

	r := &Raft{
		id:               c.ID,
		Lead:             None,
		electionTimeout:  c.ElectionTick,
		heartbeatTimeout: c.HeartbeatTick,
	}
	r.becomeFollower(r.Term, None)

	raftLog := newLog(c.Storage)
	if raftLog.committed >= c.Applied && c.Applied > raftLog.applied {
		raftLog.applied = c.Applied
	}

	if !IsEmptyHardState(hs) {
		raftLog.committed = hs.Commit
		r.Term = hs.Term
		r.Vote = hs.Vote
	}
	r.RaftLog = raftLog
	log.Debug(fmt.Sprintf("next empty: %d", raftLog.NextEmpty()))
	prs := make(map[uint64]*Progress)
	for _, peer := range peers {
		prs[peer] = &Progress{Match: raftLog.NextEmpty() - 1, Next: raftLog.NextEmpty()}
	}
	r.Prs = prs

	return r
}

// sendAppend sends an Append RPC with new entries (if any) and the
// current commit index to the given peer. Returns true if a message was sent.
func (r *Raft) sendAppend(to uint64) bool {
	// Your Code Here (2A).
	pr := r.Prs[to]
	log.Debug(fmt.Sprintf("pr[%d].next = %d r.firstIndex=%d r.lastIndex = %d", to, pr.Next, r.RaftLog.FirstIndex(), r.RaftLog.LastIndex()))
	if pr != nil && pr.Next < r.RaftLog.NextEmpty() {
		nextEs := make([]*pb.Entry, 0, r.RaftLog.NextEmpty()-pr.Next)
		for _, e := range r.RaftLog.GetEsSlice(pr.Next, r.RaftLog.NextEmpty()) {
			localE := e
			nextEs = append(nextEs, &localE)
		}
		firstIndex := r.RaftLog.FirstIndex()

		appendMsg := pb.Message{MsgType: pb.MessageType_MsgAppend, To: to, Commit: r.RaftLog.committed, Entries: nextEs, Index: firstIndex - 1, LogTerm: 0}
		if pr.Next > firstIndex {
			prevEntry := r.RaftLog.GetEntry(pr.Next - 1)
			appendMsg.Index = prevEntry.Index
			appendMsg.LogTerm = prevEntry.Term
		}
		r.send(appendMsg)
		return true
	}

	return false
}

// sendHeartbeat sends a heartbeat RPC to the given peer.
func (r *Raft) sendHeartbeat(to uint64) {
	// Your Code Here (2A).
	m := pb.Message{
		MsgType: pb.MessageType_MsgHeartbeat,
		To:      to,
		Commit:  min(r.Prs[to].Match, r.RaftLog.committed),
	}
	r.send(m)
}

// tick advances the internal logical clock by a single tick.
func (r *Raft) tick() {
	// Your Code Here (2A).
	switch r.State {
	case StateFollower, StateCandidate:
		r.tickElection()
	case StateLeader:
		r.tickHeartbeat()
	}
}

// tickElection is run by followers and candidates after r.electionTimeout.
func (r *Raft) tickElection() {
	r.electionElapsed++
	if r.electionElapsed >= r.randomizedElectionTimeout {
		r.Lead = None
		r.electionElapsed = 0
		r.Step(pb.Message{MsgType: pb.MessageType_MsgHup, From: r.id, Term: r.Term})
	}
}

func (r *Raft) tickHeartbeat() {
	r.heartbeatElapsed++
	r.electionElapsed++

	if r.electionElapsed >= r.electionTimeout {
		r.electionElapsed = 0
		if r.State == StateLeader && r.leadTransferee != None {
			r.leadTransferee = None
		}
	}

	if r.State != StateLeader {
		return
	}

	if r.heartbeatElapsed >= r.heartbeatTimeout {
		r.heartbeatElapsed = 0
		r.Step(pb.Message{MsgType: pb.MessageType_MsgBeat, From: r.id, Term: r.Term})
	}
}

// becomeFollower transform this peer's state to Follower
func (r *Raft) becomeFollower(term, lead uint64) {
	// Your Code Here (2A).
	r.reset()
	r.Term = term
	r.Lead = lead
	r.State = StateFollower
}

// becomeCandidate transform this peer's state to candidate
func (r *Raft) becomeCandidate() {
	// Your Code Here (2A).
	r.reset()
	r.Term++
	r.Vote = r.id
	r.State = StateCandidate
}

// becomeLeader transform this peer's state to leader
func (r *Raft) becomeLeader() {
	// Your Code Here (2A).
	// NOTE: Leader should propose a noop entry on its term
	log.Info(fmt.Sprintf("%d now is the leader", r.id))
	r.reset()
	r.Lead = r.id
	r.State = StateLeader
	noOpEntry := &pb.Entry{Data: nil, Index: r.RaftLog.NextEmpty(), Term: r.Term}
	r.appendEntries([]*pb.Entry{noOpEntry})
}

func (r *Raft) reset() {
	r.Lead = None
	r.Vote = None
	r.votes = make(map[uint64]bool)
	r.electionElapsed = 0
	r.setNextElectionTimeout()
	r.heartbeatElapsed = 0
	r.leadTransferee = None
	r.PendingConfIndex = 0
}

// Step the entrance of handle message, see `MessageType`
// on `eraftpb.proto` for what msgs should be handled
func (r *Raft) Step(m pb.Message) error {
	// Your Code Here (2A).
	if m.MsgType != pb.MessageType_MsgBeat && m.MsgType != pb.MessageType_MsgHeartbeat {
		log.Debug(fmt.Sprintf("node %d[term = %d] send %v to node %d[%s term = %d]", m.From, m.Term, m.MsgType, m.To, r.State, r.Term))
	}

	if m.Term >= r.Term || m.From == m.To || m.MsgType == pb.MessageType_MsgHup || m.MsgType == pb.MessageType_MsgPropose || m.MsgType == pb.MessageType_MsgTransferLeader {
		//[m.From == m.To]: The tests doesn’t set term for the local messages
		//or it is stale message
		switch r.State {
		case StateFollower:
			if err := r.stepFollower(m); err != nil {
				return err
			}
		case StateCandidate:
			if err := r.stepCandidate(m); err != nil {
				return err
			}
		case StateLeader:
			if err := r.stepLeader(m); err != nil {
				return err
			}
		}
	}

	return nil
}

func (r *Raft) stepFollower(m pb.Message) error {
	if m.Term > r.Term {
		r.Term = m.Term
		r.reset()
	}

	switch m.MsgType {
	case pb.MessageType_MsgPropose:
		return r.handleNonLeaderPropose(m)
	case pb.MessageType_MsgHup:
		r.handleMsgHup()
	case pb.MessageType_MsgAppend:
		r.handleHeartbeat(m)
		r.handleAppendEntries(m)
	case pb.MessageType_MsgHeartbeat:
		r.handleHeartbeat(m)
	case pb.MessageType_MsgRequestVote:
		r.handleRequestVote(m)
	case pb.MessageType_MsgSnapshot:
		r.handleHeartbeat(m)
		r.handleSnapshot(m)
	case pb.MessageType_MsgTransferLeader:
		r.handleTransferLeader(m)
	case pb.MessageType_MsgTimeoutNow:
		r.handleMsgTimeoutNow(m)
	}

	return nil
}

// stepCandidate handle candidate's message
func (r *Raft) stepCandidate(m pb.Message) error {
	if m.Term > r.Term {
		r.becomeFollower(m.Term, None)
		return r.stepFollower(m)
	}

	switch m.MsgType {
	case pb.MessageType_MsgPropose:
		return r.handleNonLeaderPropose(m)
	case pb.MessageType_MsgHup:
		r.handleMsgHup()
	case pb.MessageType_MsgAppend:
		r.becomeFollower(m.Term, m.From)
		r.handleAppendEntries(m)
	case pb.MessageType_MsgHeartbeat:
		r.becomeFollower(m.Term, m.From)
		r.handleHeartbeat(m)
	case pb.MessageType_MsgSnapshot:
		r.becomeFollower(m.Term, m.From)
		r.handleSnapshot(m)
	case pb.MessageType_MsgRequestVote:
		r.handleRequestVote(m)
	case pb.MessageType_MsgRequestVoteResponse:
		r.handleRequestVoteResponse(m)
	case pb.MessageType_MsgTransferLeader:
		r.handleTransferLeader(m)
	}
	return nil
}

// stepLeader handle leader's message
func (r *Raft) stepLeader(m pb.Message) error {
	if m.Term > r.Term {
		r.becomeFollower(m.Term, None)
		return r.stepFollower(m)
	}

	switch m.MsgType {
	case pb.MessageType_MsgBeat:
		r.handleBeat()
	case pb.MessageType_MsgPropose:
		return r.handlePropose(m)
	case pb.MessageType_MsgAppendResponse:
		r.handleAppendResponse(m)
	case pb.MessageType_MsgHeartbeatResponse:
		r.handleHeartbeatResponse(m)
	case pb.MessageType_MsgSnapshot:
		r.handleSnapshot(m)
	case pb.MessageType_MsgRequestVote:
		r.handleRequestVote(m)
	case pb.MessageType_MsgTransferLeader:
		r.handleTransferLeader(m)
	}
	return nil
}

func (r *Raft) handleRequestVote(m pb.Message) {
	grant := r.RaftLog.IsUpToDate(m.Index, m.LogTerm) && (r.Vote == None || r.Vote == m.From)
	if grant {
		r.Term = m.Term
		r.electionElapsed = 0
		r.Vote = m.From
	}
	r.send(pb.Message{MsgType: pb.MessageType_MsgRequestVoteResponse, To: m.From, Reject: !grant})
}

func (r *Raft) handleRequestVoteResponse(m pb.Message) {
	r.votes[m.From] = !m.Reject
	grantNum, rejectNum := r.countVotes()
	if grantNum >= r.majority() && r.State == StateCandidate {
		r.becomeLeader()
		r.broadcastAppend()
	} else if rejectNum >= r.majority() {
		r.becomeFollower(r.Term, None)
	}
}

func (r *Raft) handleBeat() {
	// 'MessageType_MsgBeat' is a local message that signals the leader to send a heartbeat
	// of the 'MessageType_MsgHeartbeat' type to its followers.
	for id := range r.Prs {
		if id != r.id {
			r.sendHeartbeat(id)
		}
	}
}

func (r *Raft) handlePropose(m pb.Message) error {
	//cmd := &raft_cmdpb.RaftCmdRequest{}
	//if err := cmd.Unmarshal(m.Entries[0].Data); err != nil {
	//	panic(err)
	//}
	//log.Debug(fmt.Sprintf("%d receive a proposal: %+v", r.id, cmd))
	if _, ok := r.Prs[r.id]; !ok || r.leadTransferee != None {
		return ErrProposalDropped
	}

	for i, e := range m.Entries {
		if e.EntryType == pb.EntryType_EntryConfChange {
			if r.PendingConfIndex > r.RaftLog.applied {
				m.Entries[i] = &pb.Entry{EntryType: pb.EntryType_EntryNormal}
			} else {
				r.PendingConfIndex = r.RaftLog.LastIndex() + uint64(i) + 1
			}
		}
	}

	nextEmpty := r.RaftLog.NextEmpty()
	for i, e := range m.Entries {
		e.Index = nextEmpty + uint64(i)
		e.Term = r.Term
	}
	r.appendEntries(m.Entries)
	r.broadcastAppend()
	return nil
}

func (r *Raft) handleNonLeaderPropose(m pb.Message) error {
	if r.Lead != None {
		m.To = r.Lead
		r.send(m)
		return nil
	} else {
		return ErrProposalDropped
	}
}

func (r *Raft) handleTransferLeader(m pb.Message) {
	if r.State != StateLeader && r.Lead != None {
		m.To = r.Lead
		r.send(m)
		return
	}

	leadTransferee := m.From
	lastLeadTransferee := r.leadTransferee
	if lastLeadTransferee != None {
		if lastLeadTransferee == leadTransferee {
			return
		}
		r.leadTransferee = None
	}
	if leadTransferee == r.id {
		return
	}
	if _, ok := r.Prs[m.From]; !ok {
		return
	}
	r.leadTransferee = leadTransferee
	r.electionElapsed = 0
	if r.Prs[m.From].Match == r.RaftLog.LastIndex() {
		r.send(pb.Message{MsgType: pb.MessageType_MsgTimeoutNow, To: m.From})
	} else {
		r.sendAppend(m.From)
	}
}

func (r *Raft) handleMsgTimeoutNow(m pb.Message) {
	if _, ok := r.Prs[r.id]; ok {
		//campaign immediately
		r.handleMsgHup()
	}
}

// handleAppendEntries handle AppendEntries RPC request
func (r *Raft) handleAppendEntries(m pb.Message) {
	// Your Code Here (2A).

	////reject == true means it is a commit advanced notification
	if m.Reject {
		r.RaftLog.updateCommitted(m.Commit)
		return
	}

	//m.Index here is the prev log index
	//if m.Index < r.RaftLog.committed {
	//	log.Error(fmt.Sprintf("Append only violated: try to Append index %d, but logs already committed to %d %+v", m.Index+1, r.RaftLog.committed, m))
	//	r.send(pb.Message{MsgType: pb.MessageType_MsgAppendResponse, To: m.From, Commit: r.RaftLog.committed, Reject: true})
	//	return
	//}

	log.Debug(fmt.Sprintf("es: %v %v %d %d", r.RaftLog.entries, r.RaftLog.consistencyCheck(m.Index, m.LogTerm), m.Index, m.LogTerm))
	if !r.RaftLog.consistencyCheck(m.Index, m.LogTerm) {
		r.send(pb.Message{MsgType: pb.MessageType_MsgAppendResponse, To: m.From, Reject: true})
		return
	}

	log.Debug("present", zap.Any("xx", r.RaftLog.IsAllPresent(m.Entries)))

	if !r.RaftLog.IsAllPresent(m.Entries) {
		r.RaftLog.EliminateConflict(m.Index)
		r.appendEntries(m.Entries)
		//log.Info(fmt.Sprintf("%d's entries: %+v last index: %d", r.id, r.RaftLog.entries, r.RaftLog.LastIndex()))
	}

	//If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
	if len(m.Entries) == 0 {
		r.RaftLog.updateCommitted(min(m.Commit, m.Index))
	} else {
		r.RaftLog.updateCommitted(m.Commit)
	}
	//log.Info(fmt.Sprintf("%d(leader committed = %d committed = %d) applied: %d", r.id, m.Commit, r.RaftLog.committed, r.RaftLog.applied))
	r.send(pb.Message{MsgType: pb.MessageType_MsgAppendResponse, To: m.From, Index: r.RaftLog.LastIndex()})
}

func (r *Raft) handleAppendResponse(m pb.Message) {
	pr := r.Prs[m.From]
	if pr == nil {
		return
	}

	if m.Reject {
		pr.Next--
		//pr.Next = max(r.RaftLog.FirstIndex(), pr.Next-1)
		r.sendAppend(m.From)
	} else {
		pr.Match = m.Index
		pr.Next = pr.Match + 1
		r.leaderUpdateCommitted()
		if m.From == r.leadTransferee && pr.Match == r.RaftLog.LastIndex() {
			r.send(pb.Message{MsgType: pb.MessageType_MsgTimeoutNow, To: m.From})
		}
	}
}

// handleHeartbeat handle Heartbeat RPC request
func (r *Raft) handleHeartbeat(m pb.Message) {
	// Your Code Here (2A).
	r.electionElapsed = 0
	r.Lead = m.From
	r.RaftLog.updateCommitted(m.Commit)
	if m.MsgType == pb.MessageType_MsgHeartbeat {
		r.send(pb.Message{MsgType: pb.MessageType_MsgHeartbeatResponse, To: m.From})
	}
}

func (r *Raft) handleHeartbeatResponse(m pb.Message) {
	pr := r.Prs[m.From]
	if pr != nil && pr.Match < r.RaftLog.LastIndex() {
		r.sendAppend(m.From)
	}
}

// handleSnapshot handle Snapshot RPC request
func (r *Raft) handleSnapshot(m pb.Message) {
	// Your Code Here (2C).
	meta := m.Snapshot.Metadata
	if meta.Index <= r.RaftLog.committed {
		r.send(pb.Message{MsgType: pb.MessageType_MsgAppendResponse, To: m.From, Index: r.RaftLog.committed})
		return
	}
	r.becomeFollower(m.Term, m.From)
	if len(r.RaftLog.entries) > 0 {
		r.RaftLog.entries = nil
	}
	raftLog := r.RaftLog
	raftLog.applied = meta.Index
	raftLog.committed = meta.Index
	raftLog.stabled = meta.Index
	r.Prs = make(map[uint64]*Progress)
	for _, peer := range meta.ConfState.Nodes {
		r.Prs[peer] = &Progress{Match: raftLog.NextEmpty() - 1, Next: raftLog.NextEmpty()}
	}
	r.RaftLog.pendingSnapshot = m.Snapshot
	r.send(pb.Message{MsgType: pb.MessageType_MsgAppendResponse, To: m.From, Index: r.RaftLog.LastIndex()})
}

// addNode add a new node to raft group
func (r *Raft) addNode(id uint64) {
	// Your Code Here (3A).
	if _, ok := r.Prs[id]; !ok {
		r.Prs[id] = &Progress{Match: r.RaftLog.NextEmpty() - 1, Next: r.RaftLog.NextEmpty()}
	}
	r.PendingConfIndex = None
}

// removeNode remove a node from raft group
func (r *Raft) removeNode(id uint64) {
	// Your Code Here (3A).
	if _, ok := r.Prs[id]; ok {
		delete(r.Prs, id)
		if r.State == StateLeader {
			r.leaderUpdateCommitted()
		}
	}

	if r.State == StateLeader && r.leadTransferee == id {
		r.leadTransferee = None
	}
	r.PendingConfIndex = None
}

func (r *Raft) majority() int {
	return len(r.Prs)/2 + 1
}

func (r *Raft) appendEntries(entities []*pb.Entry) {
	log.Debug(fmt.Sprintf("%d append %+v before: %+v", r.id, entities, r.RaftLog.entries))
	es := make([]pb.Entry, 0, len(entities))
	for _, ent := range entities {
		es = append(es, *ent)
	}
	r.RaftLog.Append(es)
	if _, ok := r.Prs[r.id]; ok {
		r.Prs[r.id].Match = r.RaftLog.LastIndex()
		r.Prs[r.id].Next = r.Prs[r.id].Match + 1
	}
	if r.State == StateLeader {
		r.leaderUpdateCommitted()
	}
	//log.Info(fmt.Sprintf("%d after Append(len = %d) committed: %d", r.id, len(r.RaftLog.entries), r.RaftLog.committed))
	//for _, ent := range entities {
	//	if len(ent.Data) > 0 {
	//		cmd := &raft_cmdpb.RaftCmdRequest{}
	//		if err := cmd.Unmarshal(ent.Data); err != nil {
	//			panic(err)
	//		}
	//		log.Debug(fmt.Sprintf("%d.e[%d] = %+v", r.id, ent.Index, cmd))
	//	}
	//}
}

func (r *Raft) handleMsgHup() {
	//campaign
	r.becomeCandidate()
	if r.majority() == 1 {
		// singleton
		r.becomeLeader()
		return
	}
	//vote for itself
	r.votes[r.id] = true
	voteRequest := pb.Message{MsgType: pb.MessageType_MsgRequestVote, Index: r.RaftLog.LastIndex(), LogTerm: r.RaftLog.lastTerm()}
	r.broadcastToOthers(voteRequest)
}

func (r *Raft) countVotes() (int, int) {
	grantNum := 0
	rejectNum := 0
	for _, vote := range r.votes {
		if !vote {
			rejectNum++
		} else {
			grantNum++
		}
	}
	return grantNum, rejectNum
}

func (r *Raft) broadcastToOthers(m pb.Message) {
	for id := range r.Prs {
		if id != r.id {
			m.To = id
			r.send(m)
		}
	}
}

func (r *Raft) send(m pb.Message) {
	m.From = r.id
	m.Term = r.Term
	r.msgs = append(r.msgs, m)
}

func (r *Raft) setNextElectionTimeout() {
	rand.Seed(time.Now().UnixNano())
	r.randomizedElectionTimeout = r.electionTimeout + rand.Intn(r.electionTimeout)
}

func (r *Raft) leaderUpdateCommitted() {
	//log.Info(fmt.Sprintf("before update leader committed %d", r.RaftLog.committed))
	lmm := r.lastMajorityMatch()
	if r.RaftLog.IsTermMatched(lmm, r.Term) {
		originCommitted := r.RaftLog.committed
		r.RaftLog.updateCommitted(lmm)
		if r.RaftLog.committed > originCommitted {
			//The tests assume that once the leader advances its commit index, it will broadcast the commit index by MessageType_MsgAppend messages.
			//reject == true means it is a committed advanced notification
			r.broadcastToOthers(pb.Message{MsgType: pb.MessageType_MsgAppend, Commit: r.RaftLog.committed, Reject: true})
		}
	}
}

func (r *Raft) lastMajorityMatch() uint64 {
	matchIndexes := make([]uint64, 0, len(r.Prs))
	for _, p := range r.Prs {
		matchIndexes = append(matchIndexes, p.Match)
	}
	sort.Slice(matchIndexes, func(i, j int) bool { return matchIndexes[i] < matchIndexes[j] })
	log.Debug(fmt.Sprintf("matchIndexes: %+v len: %d", matchIndexes, len(r.Prs)))
	return matchIndexes[len(r.Prs)-r.majority()]
}

func (r *Raft) broadcastAppend() {
	if r.majority() == 1 {
		r.RaftLog.updateCommitted(r.RaftLog.LastIndex())
		return
	}

	for id := range r.Prs {
		if id != r.id {
			r.sendAppend(id)
		}
	}
}

func (r *Raft) GetSoftState() *SoftState {
	return &SoftState{Lead: r.Lead, RaftState: r.State}
}

func (r *Raft) GetHardState() pb.HardState {
	return pb.HardState{
		Term:   r.Term,
		Vote:   r.Vote,
		Commit: r.RaftLog.committed,
	}
}
