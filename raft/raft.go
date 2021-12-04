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
	prevIndex := r.Prs[to].Next - 1 // why do we initialize next to lastIndex + 1
	logTerm, _ := r.RaftLog.Term(prevIndex)
	// TODO(wendongbo): if we don't find corresponding term,
	// append definitely will fail, why not just return first/last entry

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
		Commit: r.RaftLog.committed,
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
	r.RaftLog.entries = append(r.RaftLog.entries, pb.Entry{ Term: r.Term , Index: r.RaftLog.LastIndex() + 1 })
	r.initPrs()
	log.Printf("peer[%d] comes to power at term %d\n", r.id, r.Term)
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
				ent.Term = r.Term
				ent.Index = r.RaftLog.LastIndex() + 1
				r.RaftLog.entries = append(r.RaftLog.entries, *ent)
				r.Prs[r.id].Match = r.RaftLog.LastIndex()
				r.Prs[r.id].Next = r.RaftLog.LastIndex() + 1
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
				if r.Prs[m.From].Match < r.RaftLog.committed {
					r.sendAppend(m.From)
				}
				// TODO(wendongbo): this can be optimize, reduce msg sending
				if newCommit {
					r.broadcastAppendEntries()
				}
			}
		}
	}
	return nil

HeartBeat:
	if m.Term >= r.Term/* && r.State != StateFollower */{
		r.becomeFollower(m.Term, m.From)
	}
	r.handleHeartbeat(m)
	// TODO(wendongbo): should we move this into handle heartbeat
	return nil
Append:
	// TODO: ignore stale append msg
	if m.Term >= r.Term {
		r.becomeFollower(m.Term, m.From)
	}
	r.handleAppendEntries(m)
	return nil
Election:
	r.becomeCandidate()
	r.broadcastVoteRequest()
	if len(r.Prs) == 1 {
		r.becomeLeader()
		r.leaderResponsibility()
	}
	return nil
}

// TODO(wenodngbo): handleXXX function could use pointer to reduce memory copy
// handleAppendEntries handle AppendEntries RPC request
func (r *Raft) handleAppendEntries(m pb.Message) {
	// Your Code Here (2A).
	if m.Term > r.Term { // no logic update Term in follower
		r.becomeFollower(m.Term, None)
	}
	//r.becomeFollower(m.Term, m.From)
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
	if m.Term < r.Term {
		// TODO(wendongbo): update logic
		logrus.Warnf("peer[%d] term:%d recv append from id:%d term:%d [%s], reject append",
			r.id, r.Term, m.From, m.Term, m.String())
		return
	}

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
		//r.RaftLog.applyEntry()

		msg.Reject = false
		msg.Index = r.RaftLog.LastIndex()
		msg.Commit = r.RaftLog.committed
	} else {
		//msg.Index = r.RaftLog.findEntryIndexByTerm(m.LogTerm)
		msg.Index = m.Index
	}
}

// handleHeartbeat handle Heartbeat RPC request
func (r *Raft) handleHeartbeat(m pb.Message) {
	// Your Code Here (2A).
	log.Printf("peer[%d] handling heartbeat from p[%d] term:%d\n", r.id, m.From, m.Term)
	if m.Term < r.Term {
		logrus.Warnf("peer[%d] recv unexpected heartbeat from id:%d term:%d\n", r.id, m.From, m.Term)
		return
	}
	r.Lead = m.From // TODO(wendongbo): remove this assignment?
	r.heartbeatElapsed = 0
	r.electionElapsed = 0
	//r.updateCommit(m.Commit)

	lastIndex := r.RaftLog.LastIndex()
	//logTerm, _ := r.RaftLog.Term(lastIndex)
	msg := pb.Message{
		MsgType: pb.MessageType_MsgHeartbeatResponse,
		From: r.id,
		To: m.From,
		//LogTerm: logTerm,
		Index: lastIndex,
	}
	r.msgs = append(r.msgs, msg)
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
	logrus.Infof("leader[%d] term %d checking commit status at index[%d], replica count:%d",
		r.id, r.Term, index, count)
	logTerm, _ := r.RaftLog.Term(index)
	if count > len(r.Prs) / 2 && r.RaftLog.committed < index && logTerm == r.Term{
		r.updateCommit(index)
		logrus.Infof("leader[%d] term:%d commit entry at index %d", r.id, r.Term, index)
		return true
	}
	return false
}

// TODO(wendongbo): update
func (r *Raft) updateCommit(committed uint64) {
	if committed > r.RaftLog.committed {
		r.RaftLog.committed = min(committed, r.RaftLog.LastIndex())
	}
}

// TODO(wendongbo): move into log.go?
func (r *Raft) followerAppendEntries(matchIndex int, m *pb.Message){
	if len(m.Entries) == 0 {
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
		// TODO(wendongbo): should move update stabled out?
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