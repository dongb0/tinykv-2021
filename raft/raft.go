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
	"github.com/pingcap-incubator/tinykv/kv/raftstore/meta"
	pclog "github.com/pingcap-incubator/tinykv/log"
	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
	"github.com/sirupsen/logrus"
	"log"
	"math/rand"
	"time"
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

	rejectCount int
}

// TODO(wendongbo): replace all log/logrus with pingcap/log
// newRaft return a raft peer with the given config
func newRaft(c *Config) *Raft {
	if err := c.validate(); err != nil {
		panic(err.Error())
	}
	// Your Code Here (2A).

	hardState, confState, _ := c.Storage.InitialState()
	pclog.Debugf("peer[%d] build from hard state:%s", c.ID, hardState.String())
	prs := make(map[uint64]*Progress)
	votes := make(map[uint64]bool)
	for _, v := range c.peers {
		prs[v] = &Progress{ Next: 0, Match: 0 }
		votes[v] = false
	}
	for _, v := range confState.GetNodes() {
		prs[v] = &Progress{ Next: 0, Match: 0 }
		votes[v] = false
	}
	logEntry := newLog(c.Storage)
	logEntry.applied = c.Applied

	node := &Raft{
		id: c.ID,
		Term: hardState.Term,
		Vote: hardState.Vote,
		RaftLog: logEntry,
		Prs: prs,
		State: StateFollower,
		votes: votes,
		msgs: make([]pb.Message, 0),
		Lead: 0,
		heartbeatTimeout: c.HeartbeatTick,
		electionTimeout: c.ElectionTick,
		leadTransferee: 0, // for 3A
		PendingConfIndex: 0,	// for 3A
	}
	node.initPrs()
	return node
}

// sendAppend sends an append RPC with new entries (if any) and the
// current commit index to the given peer. Returns true if a message was sent.
func (r *Raft) sendAppend(to uint64) bool {
	// Your Code Here (2A).

	prevIndex := r.Prs[to].Next - 1
	logTerm, err := r.RaftLog.Term(prevIndex)
	if err != nil {
		pclog.Warnf("sendAppend(to:%d) get term at index %d err:%v", to, prevIndex, err)
	}
	ent := r.RaftLog.entsAfterIndex(prevIndex + 1)
	msg := pb.Message{
		MsgType: pb.MessageType_MsgAppend,
		From: r.id,
		To: to,
		Term: r.Term,
		LogTerm: logTerm,
		Index: prevIndex,
		Entries: ent,
		Commit: r.RaftLog.committed,
	}
	// needs snapshot
	// TODO(wdb): lab 3 may need to change snapshot condition
	// RaftInitLogIndex + 1 < firstIdx means peer has been compacted

	firstIdx, _ := r.RaftLog.storage.FirstIndex()
	if len(ent) != 0 {
		firstIdx = ent[0].Index
	}
	needSnap := r.Prs[to].Next < firstIdx && meta.RaftInitLogIndex + 1 < firstIdx

	pclog.Debugf("leader[%d] term:%d firstIdx:%d, to peer[%d] prevIdx:%d, need Snapshot:%t", r.id, r.Term, firstIdx, to, prevIndex, needSnap)
	if needSnap {
		// TODO(wendongbo): opt: start from index 5 no nedd to send snapshot?
		// but we can not distinct it from change conf from index 0
		pclog.Infof("peer[%d] term:%d generating snapshot", r.id, r.Term)
		if snapshot, err := r.RaftLog.storage.Snapshot(); err != nil {
			pclog.Infof("peer[%d] term:%d generate snapshot err:%v", r.id, r.Term, err.Error())
			return false
		} else {
			msg.MsgType = pb.MessageType_MsgSnapshot
			// TODO(wendongbo): we need entries after Meta.Index
			pclog.Infof("peer[%d] term:%d generates snapshot complete, meta:%v, data:%v", r.id, r.Term, snapshot.Metadata, snapshot.Data)
			msg.Snapshot = &snapshot
			msg.Index = snapshot.Metadata.Index
			msg.LogTerm = snapshot.Metadata.Term
			msg.Entries = r.RaftLog.entsAfterIndex(msg.Index + 1)
		}
	}
	r.msgs = append(r.msgs, msg)
	pclog.Debugf("leader[%d] term:%d sends append msg to peer[%d], firstIdx:%d, entryLength:%d, msg:%v", r.id, r.Term, to, firstIdx, len(r.RaftLog.entries), msg.String())
	return true
}

func (r *Raft) sendVoteRequest(to uint64) {
	logTerm, err := r.RaftLog.Term(r.RaftLog.LastIndex())
	if err != nil {
		pclog.Warnf("sendVoteRequest err: %s", err.Error())
		logTerm = r.Term
	}
	msg := pb.Message{
		MsgType: pb.MessageType_MsgRequestVote,
		From: r.id,
		To: to,
		Term: r.Term,
		LogTerm: logTerm,
		Index: r.RaftLog.LastIndex(),
	}
	r.msgs = append(r.msgs, msg)
}

// sendHeartbeat sends a heartbeat RPC to the given peer.
func (r *Raft) sendHeartbeat(to uint64) {
	// Your Code Here (2A).
	msg := pb.Message{
		MsgType: pb.MessageType_MsgHeartbeat,
		From: r.id,
		To: to,
		Term: r.Term,
		Commit: r.RaftLog.committed,
	}
	r.msgs = append(r.msgs, msg)
}

// tick advances the internal logical clock by a single tick.
func (r *Raft) tick() {
	// Your Code Here (2A).
	switch r.State {
	case StateFollower, StateCandidate:
		r.electionElapsed++
		rand.Seed(time.Now().UnixNano())
		randTimeout := r.electionTimeout + rand.Intn(r.electionTimeout / 2) + rand.Intn(r.electionTimeout)  // two rand numbers for more randomization
		if r.electionElapsed > randTimeout {
			logrus.Warnf("peer[%d] term:%d election timeout occur(curTime:%d>timeout:%d)", r.id, r.Term, r.electionElapsed, randTimeout)
			// TODO(wendongbo): why does candidate have higher conflict rate?
			r.Step(pb.Message{
				From: r.id,
				To: r.id,
				MsgType: pb.MessageType_MsgHup,
			})
		}
	case StateLeader:
		r.heartbeatElapsed++
		if r.heartbeatElapsed >= r.heartbeatTimeout {
			r.broadcastHeartbeat()
		}
	}
}

// becomeFollower transform this peer's state to Follower
func (r *Raft) becomeFollower(term uint64, lead uint64) {
	r.Term = term
	r.Lead = lead
	r.State = StateFollower
	r.Vote = lead

	rand.Seed(time.Now().UnixNano())
	r.electionElapsed = rand.Intn(r.electionTimeout / 5)
}

// becomeCandidate transform this peer's state to candidate
func (r *Raft) becomeCandidate() {
	// Your Code Here (2A).
	for i := range r.votes {	// reset votes
		r.votes[i] = false
	}
	r.State = StateCandidate
	r.Term++

	rand.Seed(time.Now().UnixNano())
	r.electionElapsed = rand.Intn(r.electionTimeout / 2)

	r.rejectCount = 0
	r.Vote = r.id
	r.votes[r.id] = true	// vote for myself
}

// becomeLeader transform this peer's state to leader
func (r *Raft) becomeLeader() {
	// Your Code Here (2A).
	// NOTE: Leader should propose a noop entry on its term
	r.Lead = r.id
	r.State = StateLeader
	// TODO(wendongbo): can we use Step propose to do this? Tests will not handle propose msg(not sure), just have a try
	r.RaftLog.entries = append(r.RaftLog.entries, pb.Entry{ Term: r.Term , Index: r.RaftLog.LastIndex() + 1 })
	r.initPrs()
	pclog.Infof("peer[%d] comes to power at term %d, with index %d(commit:%d, apply:%d, len:%d)", r.id, r.Term, r.RaftLog.LastIndex(), r.RaftLog.committed, r.RaftLog.applied, len(r.RaftLog.entries))
	if len(r.RaftLog.entries) != 0 {
		index := maxInt(r.RaftLog.getArrayIndex(r.RaftLog.LastIndex() - 3), 0)
		pclog.Debugf("last entries:%v", r.RaftLog.entries[index:])
	}
}

// sends heartbeat and append log entries
func (r *Raft) leaderResponsibility(){
	//r.broadcastHeartbeat()	// test require msg number == xxx, so we cannot send heartbeat
	// TODO(wendongbo): if we send entries in heartbeat, then no need broadcastEntries later
	r.broadcastAppendEntries()
}

// Step the entrance of handle message, see `MessageType`
// on `eraftpb.proto` for what msgs should be handled
func (r *Raft) Step(m pb.Message) error {
	// Your Code Here (2A).
	pclog.Infof("peer[%d]-term:%d Step recv:[%s]", r.id, r.Term, m.String())
	switch r.State {
	case StateFollower:
		switch m.MsgType {
		case pb.MessageType_MsgHup:		// raise election
			goto Election
		case pb.MessageType_MsgRequestVote:
			r.handleVoteRequest(m)
		case pb.MessageType_MsgHeartbeat:
			goto HeartBeat
		case pb.MessageType_MsgAppend:
			goto Append
		case pb.MessageType_MsgSnapshot:
			goto Snapshot
		}
	case StateCandidate:
		switch m.MsgType {
		case pb.MessageType_MsgHup:
			goto Election
		case pb.MessageType_MsgRequestVote:
			r.handleVoteRequest(m)
		case pb.MessageType_MsgRequestVoteResponse:
			r.handleVoteResponse(m)
		case pb.MessageType_MsgHeartbeat:
			goto HeartBeat
		case pb.MessageType_MsgAppend:
			if m.Term >= r.Term {
				// TODO(wendongbo): redundant in handleAppend
				r.becomeFollower(m.Term, None)
			}
			goto Append
		case pb.MessageType_MsgSnapshot:
			goto Snapshot
		}
	case StateLeader:
		switch m.MsgType {
		case pb.MessageType_MsgPropose: // append logs to leader's entries
			for _, ent := range m.Entries {
				ent.Term = r.Term
				ent.Index = r.RaftLog.LastIndex() + 1
				r.RaftLog.entries = append(r.RaftLog.entries, *ent)
				r.Prs[r.id].Match = r.RaftLog.LastIndex()
				r.Prs[r.id].Next = r.RaftLog.LastIndex() + 1
				pclog.Debugf("leader[%d] term:%d propose %v At Index %d", r.id, r.Term, ent, ent.Index)
			}
			if len(r.Prs) != 1 {
				r.broadcastAppendEntries()
			} else {
				r.RaftLog.committed = r.RaftLog.LastIndex()
			}
		case pb.MessageType_MsgBeat:
			r.broadcastHeartbeat()
		case pb.MessageType_MsgHeartbeat:
			// leader will not send heart beat to itself,
			// but split leader and new leader may send heart beat to each other after split recover
			goto HeartBeat
		case pb.MessageType_MsgHeartbeatResponse:
			lastIndex := r.RaftLog.LastIndex()
			lastTerm, _ := r.RaftLog.Term(lastIndex)
			pclog.Debugf("leader[%d] term:%d handling heartbeat response, lastIdx:%d, msg:%v, need append:%t", r.id, r.Term, lastIndex, m, (lastIndex != m.Index||lastTerm != m.LogTerm))
			if lastIndex != m.Index || lastTerm != m.LogTerm {
				r.sendAppend(m.From)
			}
		case pb.MessageType_MsgRequestVote:
			r.handleVoteRequest(m)
		case pb.MessageType_MsgRequestVoteResponse:
			r.handleVoteResponse(m)
		case pb.MessageType_MsgAppend:
			goto Append
		case pb.MessageType_MsgAppendResponse:
			// TODO(wendongbo): update code structure
			r.Prs[m.From].Next = m.Index
			if m.Reject {
				r.sendAppend(m.From)
			} else {
				// update match index
				r.Prs[m.From].Match = max(r.Prs[m.From].Match, m.Index)
				r.Prs[m.From].Next = r.Prs[m.From].Match + 1
				newCommit := r.checkCommitAt(r.Prs[m.From].Match)
				uptodate := false
				if newCommit {
					r.broadcastAppendEntries()
				} else if r.Prs[m.From].Match < r.RaftLog.committed {
					r.sendAppend(m.From)
				} else {
					uptodate = true
				}
				pclog.Debugf("leader[%d] knows peer[%d] accept up to [%d], up to date:%t", r.id, m.From, r.Prs[m.From].Match, uptodate)
			}
		case pb.MessageType_MsgSnapshot:
			goto Snapshot
		}
	}
	return nil

HeartBeat:
	r.handleHeartbeat(m)
	return nil
Append:
	r.handleAppendEntries(m)
	return nil
Election:
	r.becomeCandidate()
	r.broadcastVoteRequest()
	if len(r.Prs) == 1 {
		r.becomeLeader()
		r.updateCommit(r.RaftLog.LastIndex())
	}
	return nil
Snapshot:
	r.handleSnapshot(m)
	return nil
}

// TODO(wenodngbo): handleXXX function could use pointer to reduce memory copy
// handleAppendEntries handle AppendEntries RPC request
func (r *Raft) handleAppendEntries(m pb.Message) {
	// Your Code Here (2A).
	// Even receive 2 leaders' msg after split(only one is valid leader)
	// we can safely append any newer entry, log entries will finally converge

	// TODO(wendongbo): do we need to reset election timer here?
	if m.Term >= r.Term {
		r.becomeFollower(m.Term, m.From)
	} else {
		//logrus.Warnf("peer[%d] term:%d recv stale append from id:%d term:%d, reject append",
		//	r.id, r.Term, m.From, m.Term)
		return
	}
	msg := pb.Message{
		MsgType: pb.MessageType_MsgAppendResponse,
		From: r.id,
		To: m.From,
		Term: r.Term,
		Reject: true,
		Index: r.RaftLog.LastIndex(),
	}
	defer func() {
		r.msgs = append(r.msgs, msg)
	}()
	matchIndex, found := r.RaftLog.findMatchEntry(m.Index, m.LogTerm)
	// accept entries
	if found {
		r.followerAppendEntries(matchIndex + 1, &m)
		// set commit index to min(m.committed, index of last message entry)
		var followerCommit uint64 = m.Index
		if len(m.Entries) != 0 {
			followerCommit = min(m.Commit, m.Entries[len(m.Entries) - 1].Index)
		}
		r.updateCommit(followerCommit)
		msg.Reject = false
		msg.Index = r.RaftLog.LastIndex()
		msg.Commit = r.RaftLog.committed // maybe no need to set
	} else {
		// tells leader I don't have entry at m.Index
		msg.Index = m.Index
	}
	pclog.Debugf("peer[%d] term:%d append complete msg:%s, stabled:%d, lastIndex:%d", r.id, r.Term, msg.String(), r.RaftLog.stabled, r.RaftLog.LastIndex())
}

// handleHeartbeat handle Heartbeat RPC request
func (r *Raft) handleHeartbeat(m pb.Message) {
	// Your Code Here (2A).
	pclog.Debugf("peer[%d] handling heartbeat from p[%d] term:%d\n", r.id, m.From, m.Term)

	// ignore stale heartbeat
	if m.Term < r.Term {
		pclog.Warnf("peer[%d] t:%d recv unexpected heartbeat from id:%d term:%d\n", r.id, r.Term, m.From, m.Term)
		// TODO(wendongbo): return response with higher term? should we or new leader return?
		if r.Lead == r.id {	// TODO(wdb): maybe no need to do this?
			r.sendHeartbeat(m.From)
		}
		return
	}
	r.becomeFollower(m.Term, m.From)
	//r.heartbeatElapsed = 0
	r.electionElapsed = 0

	lastIndex := r.RaftLog.LastIndex()
	term, err := r.RaftLog.Term(lastIndex)
	if err != nil {
		pclog.Errorf("handle heartbeat get term at index:%d err:%v", lastIndex, err)
	}
	msg := pb.Message{
		MsgType: pb.MessageType_MsgHeartbeatResponse,
		From: r.id,
		To: m.From,
		Index: lastIndex,
		LogTerm: term,
	}
	r.msgs = append(r.msgs, msg)
}

func (r *Raft) handleVoteRequest(m pb.Message) {
	// TODO(wendongbo): campaign conflict rate is high, how to solve it
	if m.Term > r.Term {	// reset self status to follower
		r.becomeFollower(m.Term, None)
	}
	r.electionElapsed = 0
	msg := pb.Message{
		MsgType: pb.MessageType_MsgRequestVoteResponse,
		From: r.id,
		To: m.From,
		Term: r.Term,
		Reject: true,
	}
	logTerm, err := r.RaftLog.Term(r.RaftLog.LastIndex())
	// TODO(wendongbo): under what condiction, LastIndex entry will not exist?
	// 1. star up
	// 2. anything else?
	if err != nil {	// init state or ...(compact?
		logTerm = r.Term
	}
	if m.Term < r.Term ||  m.LogTerm < logTerm || m.LogTerm == logTerm && m.Index < r.RaftLog.LastIndex() {	// msg from stale peer
		msg.Reject = true
	} else if r.Vote == None || r.Vote == m.From {	// if not vote yet or voted for the same node before, grant vote
		msg.Reject = false
		r.Vote = m.From
	} else {
		logrus.Warnf("unhandle situation in vote request[%s]", m.String())
	}
	pclog.Debugf("peer[%d] term:%d handling vote req:[%s], self last-index:%d, reject:%t\n", r.id, r.Term, m.String(), r.RaftLog.LastIndex(), msg.Reject)
	r.msgs = append(r.msgs, msg)
}

func (r *Raft) handleVoteResponse(m pb.Message) {
	pclog.Debugf("peer[%d] handling vote response:[%s]\n", r.id, m.String())
	if m.Term < r.Term { 	// stale vote response
		return
	}

	if !m.Reject {
		r.votes[m.From] = true
	} else {
		r.rejectCount++
	}
	peerNum := len(r.votes)
	voteCnt := 0
	for _, v := range r.votes {
		if v {
			voteCnt++
		}
	}
	pclog.Debugf("peer[%d] gets %d votes at term %d\n", r.id, voteCnt, r.Term)
	if voteCnt > peerNum / 2 && r.State == StateCandidate { // avoid sending msg twice
		r.becomeLeader()
		r.leaderResponsibility()
	} else if r.rejectCount > peerNum / 2 {
		r.becomeFollower(r.Term, None)
	}
}
// handleSnapshot handle Snapshot RPC request
func (r *Raft) handleSnapshot(m pb.Message) {
	// Your Code Here (2C).
	// handle snapshot only update metadata, apply operation is executed in raw node
	// we just put snapshot in raft log pendingSnapshot field here, wait for ready function to read
	pclog.Infof("peer[%d] term:%d handling snapshot, meta:%v", r.id, r.Term, m.Snapshot)
	//if m.Snapshot == nil {
	//	pclog.Warnf("no snapshot in message:%v", m)
	//	return
	//}
	//if m.Snapshot.Metadata.Index < r.RaftLog.committed {
	//	// TODO(wdb): ignore snapshot update, but still needs to check entries
	//	pclog.Warnf("peer[%d] term:%d recv snapshot at %v, current commit:%d, ignore", r.id, r.Term, m.Snapshot.Metadata, r.RaftLog.committed)
	//	return
	//}
	if m.Term >= r.Term {
		r.becomeFollower(m.Term, m.From)
	}
	if m.Snapshot != nil && m.Snapshot.Metadata.Index > r.RaftLog.committed {
		r.RaftLog.pendingSnapshot = m.Snapshot
		r.RaftLog.applied = m.Snapshot.Metadata.Index
		r.RaftLog.committed = m.Snapshot.Metadata.Index
		if m.Snapshot.Metadata.ConfState.Nodes != nil {
			r.Prs = make(map[uint64]*Progress)
			r.votes = make(map[uint64]bool)
			for _, i := range m.Snapshot.Metadata.ConfState.Nodes {
				r.Prs[i] = &Progress{Next: 0, Match: 0}
				r.votes[i] = false
			}
		}
	}

	// TODO(wendongbo): opt, merge with handleAppend
	msg := pb.Message{
		MsgType: pb.MessageType_MsgAppendResponse,
		From: r.id,
		To: m.From,
		Term: r.Term,
		Reject: false,
		Index: r.RaftLog.committed,
	}
	if len(m.Entries) != 0 {
		matchIndex, found := r.RaftLog.findMatchEntry(m.Index, m.LogTerm)
		if !found {
			matchIndex = -1
		}
		r.followerAppendEntries(matchIndex + 1, &m)
		r.updateCommit(min(m.Commit, m.Entries[len(m.Entries) - 1].Index))
		msg.Index = r.RaftLog.LastIndex()
	}

	r.msgs = append(r.msgs, msg)
}

// addNode add a new node to raft group
func (r *Raft) addNode(id uint64) {
	// Your Code Here (3A).
}

// removeNode remove a node from raft group
func (r *Raft) removeNode(id uint64) {
	// Your Code Here (3A).
}

func (r *Raft) initPrs() {
	for i := range r.Prs {
		if i == r.id {
			r.Prs[i].Match = r.RaftLog.LastIndex()
			r.Prs[i].Next = r.RaftLog.LastIndex() + 1
		} else {
			// TODO(wendongbo): should we start from 0? any optimization?
			r.Prs[i].Match = 0
			r.Prs[i].Next = 1
		}
	}
}

func (r *Raft) broadcastAppendEntries(){
	for to := range r.Prs {
		if to != r.id {
			r.sendAppend(to)
		}
	}
}

func (r *Raft) broadcastHeartbeat(){
	r.heartbeatElapsed = 0
	for to := range r.Prs {
		if to != r.id {
			r.sendHeartbeat(to)
		}
	}
}

func (r *Raft) broadcastVoteRequest() {
	for to := range r.votes {
		if to != r.id {
			log.Printf("p[%d] Term:%d send vote request to peer[%d] index:%d, term:%d", r.id, r.Term, to, r.RaftLog.LastIndex(), r.Term)
			r.sendVoteRequest(to)
		}
	}
}

// checkCommitAt updates leader's committed index
func (r *Raft) checkCommitAt(index uint64) (newCommit bool){
	// TODO(wendongbo): check logTerm; Why does Term matter?
	count := 0
	// TODO(wdn): debug struct
	debugPrs := make(map[uint64]Progress)
	for i, v := range r.Prs {
		if v.Match >= index {
			count++
			debugPrs[i] = *v
		}
	}
	logrus.Infof("leader[%d] term %d checking commit status at index[%d], committed:%d, replica count:%d, Prs:%v",
		r.id, r.Term, index, r.RaftLog.committed, count, debugPrs)
	logTerm, err := r.RaftLog.Term(index)
	if err != nil {
		pclog.Errorf("sendVoteRequest err: %s", err.Error())
	}
	if count > len(r.Prs) / 2 && r.RaftLog.committed < index && logTerm == r.Term{
		r.updateCommit(index)
		pclog.Debugf("leader[%d] term:%d commit entry at index %d, leader last index:%d", r.id, r.Term, index, r.RaftLog.LastIndex())
		return true
	}
	return false
}

func (r *Raft) updateCommit(committed uint64) {
	if committed > r.RaftLog.committed {
		r.RaftLog.committed = min(committed, r.RaftLog.LastIndex())
	}
}

func (r *Raft) followerAppendEntries(matchIndex int, m *pb.Message){
	j, end2 := 0, len(m.Entries)
	// skip match entry
	conflictIndex := matchIndex
	// TODO(wendongbo): use binary search to accelerate
	for end1 := len(r.RaftLog.entries); conflictIndex < end1 && j < end2; conflictIndex, j = conflictIndex+1, j+1 {
		if r.RaftLog.entries[conflictIndex].Index != m.Entries[j].Index || r.RaftLog.entries[conflictIndex].Term != m.Entries[j].Term {
			break
		}
	}
	// resolve follower conflict entries
	if j != len(m.Entries) {
		// if leader overwrite entries, decrease stabled index to [conflictIndex - 1]
		if m.Entries[j].Index <= r.RaftLog.stabled {
			r.RaftLog.stabled = m.Entries[j].Index - 1
		}
		r.RaftLog.entries = r.RaftLog.entries[:conflictIndex]
		for end := len(m.Entries); j < end; j++ {
			r.RaftLog.entries = append(r.RaftLog.entries, *m.Entries[j])
		}
	}
}