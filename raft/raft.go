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
	pclog "github.com/pingcap-incubator/tinykv/log"
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
		return false
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
	r.msgs = append(r.msgs, msg)
	pclog.Debugf("leader[%d] term:%d sends append msg to peer[%d], msg:{%s}", r.id, r.Term, to, msg.String())
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
		curTimeCount := r.electionTimeout + rand.Intn(r.electionTimeout * 2)
		if r.electionElapsed >= curTimeCount{
			logrus.Warnf("peer[%d] term:%d election timeout occur(curTime:%d>timeout:%d)", r.id, r.Term, curTimeCount, r.electionTimeout)
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
	r.Lead = r.id
	r.State = StateLeader
	r.RaftLog.entries = append(r.RaftLog.entries, pb.Entry{ Term: r.Term , Index: r.RaftLog.LastIndex() + 1 })
	r.initPrs()
	pclog.Infof("peer[%d] comes to power at term %d, with index(commit:%d, apply:%d):%d\n", r.id, r.Term, r.RaftLog.committed, r.RaftLog.applied, r.RaftLog.LastIndex())
	if len(r.RaftLog.entries) != 0 {
		index := maxInt(r.RaftLog.getArrayIndex(r.RaftLog.LastIndex()), 0)
		pclog.Debugf("last entries:%v", r.RaftLog.entries[index:])
	}
}

// sends heartbeat and append log entries
func (r *Raft) leaderResponsibility(){
	//r.broadcastHeartbeat()
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
		}
	case StateLeader:
		switch m.MsgType {
		case pb.MessageType_MsgPropose: // append logs to leader's entries
			for _, ent := range m.Entries {
				//initIndex := r.RaftLog.LastIndex()
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
			// normally leader do not send heart beat to itself,
			// but split leader may do this after split recover
			goto HeartBeat
		case pb.MessageType_MsgHeartbeatResponse:
			lastIndex := r.RaftLog.LastIndex()
			if lastIndex > m.Index {
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
}

// TODO(wenodngbo): handleXXX function could use pointer to reduce memory copy
// handleAppendEntries handle AppendEntries RPC request
func (r *Raft) handleAppendEntries(m pb.Message) {
	// Your Code Here (2A).
	// Even receive 2 leaders' msg after split(only one is valid leader)
	// we can safely append any newer entry, log entries will finally converge
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
		end := maxInt(len(m.Entries) - 1, 0)
		endEntryIdx := m.Index
		if len(m.Entries) != 0 {
			endEntryIdx = m.Entries[end].Index
		}
		// set commit index to min(m.committed, index of last message entry)
		r.updateCommit(min(m.Commit, endEntryIdx))

		msg.Reject = false
		msg.Index = r.RaftLog.LastIndex()
		msg.Commit = r.RaftLog.committed // maybe no need to set
	} else {
		msg.Index = m.Index
	}
	pclog.Debugf("peer[%d] term:%d append complete msg:%s", r.id, r.Term, msg.String())
}

// handleHeartbeat handle Heartbeat RPC request
func (r *Raft) handleHeartbeat(m pb.Message) {
	// Your Code Here (2A).

	pclog.Debugf("peer[%d] handling heartbeat from p[%d] term:%d\n", r.id, m.From, m.Term)

	// ignore stale heartbeat
	if m.Term < r.Term {
		pclog.Warnf("peer[%d] t:%d recv unexpected heartbeat from id:%d term:%d\n", r.id, r.Term, m.From, m.Term)
		return
	}
	r.becomeFollower(m.Term, m.From)
	//r.heartbeatElapsed = 0
	r.electionElapsed = 0

	lastIndex := r.RaftLog.LastIndex()
	msg := pb.Message{
		MsgType: pb.MessageType_MsgHeartbeatResponse,
		From: r.id,
		To: m.From,
		Index: lastIndex,
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

// TODO(wendongbo): broadcast new heartbeat or append msg to update followers' commit index
// But performance consideration?
// checkCommitAt updates leader's committed index
func (r *Raft) checkCommitAt(index uint64) (newCommit bool){
	// TODO(wendongbo): check logTerm; Why does Term matter?
	count := 0
	for _, v := range r.Prs {
		if v.Match >= index {
			count++
		}
	}
	logrus.Infof("leader[%d] term %d checking commit status at index[%d], committed:%d, replica count:%d",
		r.id, r.Term, index, r.RaftLog.committed, count)
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
	if r.State != StateFollower || len(m.Entries) == 0 {
		log.Printf("peer[%d] term:%d state:%s failed to append [%s]", r.id, r.Term, r.State, m.String())
		return
	}
	j, end2 := 0, len(m.Entries)
	// skip match entry
	conflictIndex := matchIndex
	for end1 := len(r.RaftLog.entries); conflictIndex < end1 && j < end2; conflictIndex, j = conflictIndex+1, j+1 {
		if r.RaftLog.entries[conflictIndex].Index != m.Entries[j].Index || r.RaftLog.entries[conflictIndex].Term != m.Entries[j].Term {
			break
		}
	}
	// resolve follower conflict entries
	if j != len(m.Entries) {
		// if leader overwrite entries, decrease stabled index to [conflict index - 1]
		if m.Entries[j].Index <= r.RaftLog.stabled {
			r.RaftLog.stabled = m.Entries[j].Index - 1
		}
		r.RaftLog.entries = r.RaftLog.entries[:conflictIndex]
		for end := len(m.Entries); j < end; j++ {
			r.RaftLog.entries = append(r.RaftLog.entries, *m.Entries[j])
		}
	}
}