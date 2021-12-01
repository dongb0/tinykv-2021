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
	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
	"github.com/sirupsen/logrus"
	"log"
	"math/rand"
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

// newRaft return a raft peer with the given config
func newRaft(c *Config) *Raft {
	if err := c.validate(); err != nil {
		panic(err.Error())
	}
	// Your Code Here (2A).
	// c.Applied?

	prs := make(map[uint64]*Progress)
	votes := make(map[uint64]bool)
	for _, v := range c.peers {
		prs[v] = &Progress{ Next: 0, Match: 0 }
		votes[v] = false
	}
	log := newLog(c.Storage)
	// TODO(wendongbo): use confState?
	hardState, _, _ := c.Storage.InitialState()
	node := &Raft{
		id: c.ID,
		Term: hardState.Term,
		Vote: hardState.Vote,
		RaftLog: log,
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
	nextIndex := r.Prs[to].Next // why do we initialize next to lastIndex + 1
	//logTerm, _ := r.RaftLog.Term(nextIndex)

	ent := r.RaftLog.entsAfterIndex(nextIndex)
	msg := pb.Message{
		MsgType: pb.MessageType_MsgAppend,
		From: r.id,
		To: to,
		Term: r.Term,
		//LogTerm: logTerm,
		Index: nextIndex,
		Entries: ent,
		Commit: r.RaftLog.committed,
	}
	r.msgs = append(r.msgs, msg)
	return false
}

func (r *Raft) sendVoteRequest(to uint64) {
	logTerm, _ := r.RaftLog.Term(r.RaftLog.LastIndex())
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
	}
	r.msgs = append(r.msgs, msg)
}

// tick advances the internal logical clock by a single tick.
func (r *Raft) tick() {
	// Your Code Here (2A).
	r.heartbeatElapsed++
	r.electionElapsed++
	switch r.State {
	case StateFollower, StateCandidate:
		if r.electionElapsed >= r.electionTimeout + rand.Intn(r.electionTimeout * 2){
			// TODO(wendongbo): why does candidate have higher conflict rate?
			// TODO(wendongbo): call Step() instead
			r.becomeCandidate()
			r.broadcastVoteRequest()
		}
	case StateLeader:
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
}

// becomeCandidate transform this peer's state to candidate
func (r *Raft) becomeCandidate() {
	// Your Code Here (2A).
	for i := range r.votes {	// reset votes
		r.votes[i] = false
	}
	r.State = StateCandidate
	r.Term++
	r.electionElapsed = 0

	r.rejectCount = 0
	r.Vote = r.id
	r.votes[r.id] = true	// vote for myself
}

// becomeLeader transform this peer's state to leader
func (r *Raft) becomeLeader() {
	// Your Code Here (2A).
	// NOTE: Leader should propose a noop entry on its term
	r.State = StateLeader
	r.RaftLog.entries = append(r.RaftLog.entries, pb.Entry{ Term: r.Term , Index: r.RaftLog.LastIndex() + 1})
	r.initPrs()
	r.broadcastHeartbeat()
	r.broadcastAppendEntries()
	log.Printf("peer[%d] comes to power at term %d\n", r.id, r.Term)
}

// Step the entrance of handle message, see `MessageType`
// on `eraftpb.proto` for what msgs should be handled
func (r *Raft) Step(m pb.Message) error {
	// Your Code Here (2A).
	log.Printf("peer[%d]-term:%d Step recv:[%s]\n", r.id, r.Term, m.String())
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
		}
	case StateCandidate:
		//if m.Term > r.Term {
		//	err := fmt.Sprintf("peer[%d] term:%d Step recv stale msg from peer[%d] term:%d", r.id, r.Term, m.From, m.Term)
		//	r.becomeFollower(m.Term, None)
		//	log.Println(err)
		//	// should we reset timer here?
		//}
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
				r.becomeFollower(m.Term, None)
			}
			goto Append
		}
	case StateLeader:
		if m.Term > r.Term {
			err := fmt.Sprintf("peer[%d] term:%d Step recv Higher Term msg from peer[%d] term:%d", r.id, r.Term, m.From, m.Term)
			r.becomeFollower(m.Term, None)
			log.Println(err)
			// should we reset timer here?
		}
		switch m.MsgType {
		case pb.MessageType_MsgPropose: // append logs to leader's entries
			for _, ent := range m.Entries {
				r.RaftLog.entries = append(r.RaftLog.entries, *ent)
				r.Prs[r.id].Match = uint64(len(r.RaftLog.entries) - 1)
				r.Prs[r.id].Next = uint64(len(r.RaftLog.entries))
			}
			r.broadcastAppendEntries()
		case pb.MessageType_MsgBeat:
			r.broadcastHeartbeat()
		case pb.MessageType_MsgHeartbeat:
			// normally leader do not send heart beat to itself,
			// but split leader may do this after split recover
			goto HeartBeat
		case pb.MessageType_MsgHeartbeatResponse:
			// TODO(wendongbo): update Prs
			r.Prs[m.From].Next = m.Index
		case pb.MessageType_MsgRequestVote:
			r.handleVoteRequest(m)
		case pb.MessageType_MsgAppend:
			goto Append
		case pb.MessageType_MsgAppendResponse:
			// TODO(wendongbo): update Prs
			if m.Reject {
				r.Prs[m.From].Next = m.Index
				r.sendAppend(m.From)
			} else {
				// update Prs.Match
			}
		}
	}
	return nil

HeartBeat:
	r.handleHeartbeat(m)
	// TODO(wendongbo): should we move this into handle heartbeat
	if m.Term > r.Term && r.State != StateFollower{
		r.becomeFollower(m.Term, m.From)
	}
	return nil
Election:
	r.becomeCandidate()
	r.broadcastVoteRequest()
	if len(r.Prs) == 1 {
		r.becomeLeader()
	}
	return nil
Append:
	// TODO: ignore stale append msg
	r.handleAppendEntries(m)
	return nil
}

// handleAppendEntries handle AppendEntries RPC request
func (r *Raft) handleAppendEntries(m pb.Message) {
	// Your Code Here (2A).
	if m.Term > r.Term { // no logic update Term in follower
		r.becomeFollower(m.Term, None)
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
	if m.Term < r.Term{
		// TODO(wendongbo): update logic
		logrus.Warnf("peer[%d] term:%d recv append from id:%d term:%d [%s], reject append",
			r.id, r.Term, m.From, m.Term, m.String())
		return
	}

	// TODO(wendongbo): get entry at m.Index, m.Term,
	idx := 0
	end := len(r.RaftLog.entries)
	for ; idx < end; idx++ {
		ent := &r.RaftLog.entries[idx]
		if ent.Index == m.Index /* && ent.Term == m.LogTerm*/{
			break
		}
	}

	// accept entries
	// TODO: this will erase all logs if append msg Index not set
	if idx != end {
		r.RaftLog.entries = r.RaftLog.entries[:idx]
		for _, ent := range m.Entries {
			r.RaftLog.entries = append(r.RaftLog.entries, *ent)
		}
		msg.Reject = false
		msg.Index = r.RaftLog.LastIndex()
	}
}

// handleHeartbeat handle Heartbeat RPC request
func (r *Raft) handleHeartbeat(m pb.Message) {
	// Your Code Here (2A).
	log.Printf("peer[%d] handling heartbeat from p[%d] term:%d\n", r.id, m.From, m.Term)
	if m.Term < r.Term || m.From != r.Vote {
		logrus.Warnf("peer[%d] recv unexpected heartbeat from id:%d term:%d\n", r.id, m.From, m.Term)
		return
	}
	r.Lead = m.From
	r.heartbeatElapsed = 0
	r.electionElapsed = 0
	//lastIndex := r.RaftLog.LastIndex()
	//logTerm, _ := r.RaftLog.Term(lastIndex)
	//msg := pb.Message{
	//	MsgType: pb.MessageType_MsgHeartbeatResponse,
	//	From: r.id,
	//	To: m.From,
	//	LogTerm: logTerm,
	//	Index: lastIndex,
	//}
	//r.msgs = append(r.msgs, msg)
}

func (r *Raft) handleVoteRequest(m pb.Message) {
	if m.Term > r.Term {	// reset self status to follower
		r.becomeFollower(m.Term, None)
	}
	msg := pb.Message{
		MsgType: pb.MessageType_MsgRequestVoteResponse,
		From: r.id,
		To: m.From,
		Term: r.Term,
		Reject: true,
	}
	logTerm, _ := r.RaftLog.Term(r.RaftLog.LastIndex())
	if m.Term < r.Term ||  m.LogTerm < logTerm || m.LogTerm == logTerm && m.Index < r.RaftLog.LastIndex() {	// msg from stale peer
		msg.Reject = true
	} else if r.Vote == None || r.Vote == m.From {	// if not vote yet or voted for the same node before, grant vote
		msg.Reject = false
		r.Vote = m.From
	} else {
		logrus.Warnf("unhandle situation in vote request[%s]", m.String())
	}
	log.Printf("peer[%d] term:%d handling vote req:[%s], reject:%t\n", r.id, r.Term, m.String(), msg.Reject)
	r.msgs = append(r.msgs, msg)
}

func (r *Raft) handleVoteResponse(m pb.Message) {
	log.Printf("peer[%d] handleing vote response:[%s]\n", r.id, m.String())
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
	log.Printf("peer[%d] gets %d votes at term %d\n", r.id, voteCnt, r.Term)
	if voteCnt > peerNum / 2 {
		r.becomeLeader()
	} else if r.rejectCount > peerNum / 2 {
		r.becomeFollower(r.Term, None)
	}
}
// handleSnapshot handle Snapshot RPC request
func (r *Raft) handleSnapshot(m pb.Message) {
	// Your Code Here (2C).
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
		//r.Prs[i].Next = r.RaftLog.LastIndex()
		r.Prs[i].Next = r.RaftLog.LastIndex() + 1	// TODO(wendongbo): obviously a bunch of overhead
		r.Prs[i].Match = 1 // TODO(wendongbo): paper said it should be init to 0?
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
	for to := range r.Prs {
		if to != r.id {
			r.sendHeartbeat(to)
		}
	}
}

func (r *Raft) broadcastVoteRequest() {
	for to := range r.votes {
		if to != r.id {
			r.sendVoteRequest(to)
		}
	}
}