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
	"fmt"
	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
	"github.com/pingcap/log"
)

// RaftLog manage the log entries, its struct look like:
//
//  snapshot/first.....applied....committed....stabled.....last
//  --------|------------------------------------------------|
//                            log entries
//
// for simplify the RaftLog implement should manage all log entries
// that not truncated
type RaftLog struct {
	// storage contains all stable entries since the last snapshot.
	storage Storage

	// committed is the highest log position that is known to be in
	// stable storage on a quorum of nodes.
	committed uint64

	// applied is the highest log position that the application has
	// been instructed to apply to its state machine.
	// Invariant: applied <= committed
	applied uint64

	// log entries with index <= stabled are persisted to storage.
	// It is used to record the logs that are not persisted by storage yet.
	// Everytime handling `Ready`, the unstabled logs will be included.
	stabled uint64

	// all entries that have not yet compact.
	entries []pb.Entry

	// the incoming unstable snapshot, if any.
	// (Used in 2C)
	pendingSnapshot *pb.Snapshot

	// Your Data Here (2A).
	offset            uint64
	lastIncludedIndex uint64
	lastIncludedTerm  uint64
}

// newLog returns log using the given storage. It recovers the log
// to the state that it just commits and applies the latest snapshot.
func newLog(storage Storage) *RaftLog {
	// Your Code Here (2A).
	l := &RaftLog{
		storage: storage,
	}
	firstIndex, err := storage.FirstIndex()
	if err != nil {
		panic(err)
	}
	lastIndex, err := storage.LastIndex()
	if err != nil {
		panic(err)
	}
	snapTerm, err := storage.Term(firstIndex - 1)
	if err != nil {
		panic(err)
	}

	entries, err := storage.Entries(firstIndex, lastIndex+1)
	if err != nil {
		panic(err)
	}
	l.entries = entries
	l.offset = firstIndex

	l.stabled = lastIndex
	// Initialize our committed and applied pointers to the time of the last compaction.
	l.committed = firstIndex - 1
	l.applied = firstIndex - 1

	l.lastIncludedIndex = firstIndex - 1
	l.lastIncludedTerm = snapTerm

	return l
}

// We need to compact the log entries in some point of time like
// storage compact stabled log entries prevent the log entries
// grow unlimitedly in memory
func (l *RaftLog) maybeCompact() {
	// Your Code Here (2C).
	sFirstIndex, _ := l.storage.FirstIndex()
	if sFirstIndex > l.FirstIndex() && len(l.entries) > 0 {
		entries := l.entries[l.physicalIndex(sFirstIndex):]
		l.entries = make([]pb.Entry, len(entries))
		copy(l.entries, entries)
	}
}

// unstableEntries return all the unstable entries
func (l *RaftLog) unstableEntries() []pb.Entry {
	// Your Code Here (2A).
	if l.IsLogEmpty() {
		return nil
	}

	return l.GetEsSlice(l.stabled+1, l.NextEmpty())
}

// nextEnts returns all the committed but not applied entries
func (l *RaftLog) nextEnts() (ents []pb.Entry) {
	// Your Code Here (2A).
	if l.IsAllApplied() {
		return nil
	}

	return l.GetEsSlice(l.applied+1, l.committed+1)
}

func (l *RaftLog) IsAllApplied() bool {
	return l.committed == l.applied
}

// LastIndex return the last index of the log entries
func (l *RaftLog) LastIndex() uint64 {
	// Your Code Here (2A).
	log.Debug(fmt.Sprintf("lastIncludedIndex %+v %+v", l.lastIncludedIndex, l.pendingSnapshot))
	if len(l.entries) != 0 {
		return LastEntry(l.entries).Index
	}

	if l.pendingSnapshot != nil {
		return l.pendingSnapshot.Metadata.Index
	}

	return l.lastIncludedIndex
}

func (l *RaftLog) FirstIndex() uint64 {
	if len(l.entries) != 0 {
		return l.entries[0].Index
	}
	if l.pendingSnapshot != nil {
		return l.pendingSnapshot.Metadata.Index
	}
	return l.lastIncludedIndex
}

func (l *RaftLog) lastTerm() uint64 {
	t, err := l.Term(l.LastIndex())
	if err != nil {
		log.Error("fetch last term error")
	}
	return t
}

// Term return the term of the entry in the given index
func (l *RaftLog) Term(i uint64) (uint64, error) {
	// Your Code Here (2A).
	if i == l.lastIncludedIndex {
		return l.lastIncludedTerm, nil
	}

	if len(l.entries) == 0 {
		return 0, ErrCompacted
	}

	if i < l.offset {
		return 0, ErrCompacted
	}

	if l.physicalIndex(i) > uint64(len(l.entries)-1) {
		return 0, ErrUnavailable
	}

	return l.entries[l.physicalIndex(i)].Term, nil
}

func (l *RaftLog) IsTermMatched(index, term uint64) bool {
	if t, err := l.Term(index); err == nil {
		return t == term
	}
	return false
}

func (l *RaftLog) consistencyCheck(index, term uint64) bool {
	//log.Info(fmt.Sprintf("consistency check: index %d term %d %d next empty %d", index, term, l.FirstIndex(), l.NextEmpty()))
	log.Debug(fmt.Sprintf("consistency check: %+v es: %+v", term == 0 || l.IsTermMatched(index, term), l.entries))
	if term == 0 || (term == l.lastIncludedTerm && index == l.lastIncludedIndex) {
		return true
	}
	return l.IsTermMatched(index, term)
}

func (l *RaftLog) updateCommitted(leaderCommitted uint64) {
	l.committed = min(max(l.committed, leaderCommitted), l.LastIndex())
}

func (l *RaftLog) IsUpToDate(index, term uint64) bool {
	log.Debug(fmt.Sprintf("index: %d term: %d es: %+v", index, term, l.entries))
	return term > l.lastTerm() || (term == l.lastTerm() && index >= l.LastIndex())
}

func (l *RaftLog) IsAllPresent(es []*pb.Entry) bool {
	if l.IsLogEmpty() {
		return false
	}

	for _, e := range es {
		if !l.IsTermMatched(e.Index, e.Term) {
			return false
		}
	}
	return true
}

func (l *RaftLog) physicalIndex(logicIndex uint64) uint64 {
	return logicIndex - l.offset
}

func (l *RaftLog) Append(es []pb.Entry) {
	l.entries = append(l.entries, es...)
	//log.Debug(fmt.Sprintf("Append entries: %v", l.entries))
	//l.truncateAndAppend(es)
}

func (l *RaftLog) truncateAndAppend(ents []pb.Entry) {
	after := ents[0].Index
	if after == l.LastIndex()+1 {
		l.entries = append(l.entries, ents...)
		return
	}
	// truncate to after and copy to u.entries then append
	log.Info(fmt.Sprintf("truncate the unstable entries before index %d", after))
	if after-1 < l.stabled {
		l.stabled = after - 1
	}
	l.entries = append([]pb.Entry{}, l.entries[:after-l.offset]...)
	l.entries = append(l.entries, ents...)
}

func (l *RaftLog) EliminateConflict(index uint64) {
	if l.IsLogEmpty() {
		return
	}

	if index == l.FirstIndex()-1 {
		l.entries = l.entries[0:0]
		l.stabled = l.LastIndex()
		l.applied = l.LastIndex()
		return
	}

	l.entries = l.entries[:l.physicalIndex(index+1)]
	l.stabled = min(l.stabled, index)
	l.applied = min(l.applied, index)
	log.Debug(fmt.Sprintf("after eliminate: %+v", l.entries))
}

func (l *RaftLog) GetEntry(index uint64) pb.Entry {
	if index < 0 || index > l.NextEmpty() {
		log.Fatal(fmt.Sprintf("indexes %d out of bound [%d, %d]", index, l.FirstIndex(), l.NextEmpty()))
	}
	return l.entries[l.physicalIndex(index)]
}

func (l *RaftLog) GetEsSlice(start, end uint64) []pb.Entry {
	if start < 0 || end > l.NextEmpty() {
		log.Fatal(fmt.Sprintf("indexes [%d, %d] out of bound [%d, %d]", start, end, l.FirstIndex(), l.NextEmpty()))
	}
	return l.entries[l.physicalIndex(start):l.physicalIndex(end)]
}

func (l *RaftLog) IsLogEmpty() bool {
	return len(l.entries) == 0
}

func (l *RaftLog) NextEmpty() uint64 {
	log.Debug(fmt.Sprintf("last index: %d offset %d", l.LastIndex(), l.offset))
	return l.LastIndex() + 1
}

func (l *RaftLog) IsSnapshotStableReady(rdSnapshot *pb.Snapshot) bool {
	return !IsEmptySnap(rdSnapshot) && l.pendingSnapshot != nil && rdSnapshot.Metadata.Index == l.pendingSnapshot.Metadata.Index
}
