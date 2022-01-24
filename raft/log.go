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
	"github.com/pingcap-incubator/tinykv/log"
	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
)

const RaftLogEntryNotFound = -10

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
	// Everytime handling `Ready`, the unstable logs will be included.
	stabled uint64

	// all entries that have not yet compact.
	entries []pb.Entry

	// the incoming unstable snapshot, if any.
	// (Used in 2C)
	pendingSnapshot *pb.Snapshot

	// Your Data Here (2A).
}

// newLog returns log using the given storage. It recovers the log
// to the state that it just commits and applies the latest snapshot.
func newLog(storage Storage) *RaftLog {
	// Your Code Here (2A).
	lastIndex, _ := storage.LastIndex()
	firstIndex, _ := storage.FirstIndex()
	entries, err := storage.Entries(firstIndex, lastIndex + 1)
	if err != nil {
		log.Warnf("fetch entry err: %s, firstIndex:%d, lastIndex:%d", err.Error(), firstIndex, lastIndex)
	}
	log.Debugf("building new log entry from storage(len:%d), lastIndex:%d", len(entries), lastIndex)
	hardState, _, _ := storage.InitialState()
	log := &RaftLog{
		storage: storage,
		committed: hardState.Commit,
		applied: 0,
		stabled: lastIndex,
		entries: entries,
		pendingSnapshot: nil,
	}
	return log
}

// We need to compact the log entries in some point of time like
// storage compact stabled log entries prevent the log entries
// grow unlimitedly in memory
func (l *RaftLog) maybeCompact() {
	// Your Code Here (2C).
	if index := l.getArrayIndex(l.applied); index != RaftLogEntryNotFound && index < len(l.entries){

		l.entries = l.entries[index + 1:]
	}
	// TODO(wendongbo): only when we made a snapshot did we compact log in memory
}

// unstableEntries return all the unstable entries
func (l *RaftLog) unstableEntries() []pb.Entry {
	// Your Code Here (2A).
	//res := make([]pb.Entry, 0)
	i := l.getArrayIndex(l.stabled + 1)
	if i != RaftLogEntryNotFound {
		return l.entries[i:]
	}
	// all entries in memory are persisted
	if length := len(l.entries); length == 0 || l.stabled >= l.entries[length-1].Index{
		return make([]pb.Entry, 0)
	}
	// snapshot
	var err error
	if l.stabled, err = l.storage.LastIndex(); err != nil {
		log.Errorf("storage get last index err:%v", err)
	}
	return l.entries[:]
}

// nextEnts returns all the committed but not applied entries
func (l *RaftLog) nextEnts() (ents []pb.Entry) {
	// Your Code Here (2A).
	begin, end := 0, len(l.entries)
	for ; begin != end && l.entries[begin].Index <= l.applied; begin++ {
		// TODO(wendongbo): use getArrayIndex(binary search) to skip
	}
	for ; begin != end && l.entries[begin].Index <= l.committed; begin++ {
		ents = append(ents, l.entries[begin])
	}
	return ents
}

// LastIndex return the last index of the log entries
func (l *RaftLog) LastIndex() uint64 {
	// Your Code Here (2A).
	if len(l.entries) == 0 {
		if lastIndex, err := l.storage.LastIndex(); err != nil {
			return lastIndex
		}
		return l.committed
	}
	last := len(l.entries) - 1
	return l.entries[last].Index
}

// Term return the term of the entry in the given index
func (l *RaftLog) Term(i uint64) (term uint64, err error) {
	// Your Code Here (2A).
	if i == 0 {
		return 0, nil
	}
	idx := l.getArrayIndex(i)
	if idx != RaftLogEntryNotFound {
		return l.entries[idx].Term, nil
	}
	if term, err = l.storage.Term(i); err != nil {
		if l.pendingSnapshot != nil {
			return l.pendingSnapshot.Metadata.Term, nil
		}
		return 0, fmt.Errorf("log entry not found at index[%d]:%v", i, err)
	}
	return term, err
}

// entsAfterIndex return entries after given index(including entries in storage)
// TODO(wendongbo): read from storage if entries not in memory
func (l *RaftLog) entsAfterIndex(index uint64) []*pb.Entry {
	//log.Debugf("Raft log generates entries after index:%d begin", index)
	res := make([]*pb.Entry, 0)
	if len(l.entries) == 0 || index < l.entries[0].Index {
		firstIdx, _ := l.storage.FirstIndex()
		index = max(index, firstIdx)
		stableEnt, err := l.storage.Entries(index, l.stabled + 1)
		if err != nil {
			log.Warnf("RaftLog get stable entries [%d, %d) err:%v", index, l.stabled, err)
		}
		//log.Debugf("Raft log get stabled entries(len:%d):%v", len(stableEnt), stableEnt)
		unstableEnt := l.unstableEntries()
		for i := range stableEnt {
			res = append(res, &stableEnt[i])
		}
		for i := range unstableEnt {
			res = append(res, &unstableEnt[i])
		}
		return res
	}
	if begin := l.getArrayIndex(index); begin != RaftLogEntryNotFound{
		end := len(l.entries)
		for ; begin < end; begin++ {
			res = append(res, &l.entries[begin])
		}
	}
	//log.Debugf("Raft log generates entries after %d complete, len:%d, theSecHalfEntry:%v", index, len(res), res[len(res)/2:])
	return res
}

// return matchIndex(entry array index, begin with 0)
func (l *RaftLog) findMatchEntry(index, logTerm uint64) (matchIndex int, found bool) {
	if index == 0 {
		// first no op entry index == 1, hence follower expect find match at index 0
		// return (-1, true) for follower append index at [matchIndex + 1]
		return -1, true
	}
	idx := l.getArrayIndex(index)
	if idx != RaftLogEntryNotFound && l.entries[idx].Term == logTerm {
		return idx, true
	}
	return RaftLogEntryNotFound, false
}

// getArrayIndex returns entries' array index on given parameter [index]
// return RaftLogEntryNotFound if entry not found
func (l *RaftLog) getArrayIndex(index uint64) int {
	if index == 0 || len(l.entries) == 0 {
		return RaftLogEntryNotFound
	}
	lo, hi := 0, len(l.entries) - 1
	for ; lo <= hi;  {
		mid := lo + (hi - lo) / 2
		if l.entries[mid].Index < index {
			lo = mid + 1
		} else if l.entries[mid].Index > index {
			hi = mid - 1
		} else {
			return mid
		}
	}
	return RaftLogEntryNotFound
}

func (l *RaftLog) getArrayIndexV0(index uint64) int {
	i, end := 0, len(l.entries)
	for ; i < end; i++ {
		if l.entries[i].Index == index {
			return i
		}
	}
	return RaftLogEntryNotFound
}