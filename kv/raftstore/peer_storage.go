package raftstore

import (
	"bytes"
	"fmt"
	"github.com/pingcap-incubator/tinykv/proto/pkg/raft_cmdpb"
	"time"

	"github.com/Connor1996/badger"
	"github.com/Connor1996/badger/y"
	"github.com/golang/protobuf/proto"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/meta"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/runner"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/snap"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/util"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/kv/util/worker"
	"github.com/pingcap-incubator/tinykv/log"
	"github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/metapb"
	rspb "github.com/pingcap-incubator/tinykv/proto/pkg/raft_serverpb"
	"github.com/pingcap-incubator/tinykv/raft"
	"github.com/pingcap/errors"
)

type ApplySnapResult struct {
	// PrevRegion is the region before snapshot applied
	PrevRegion *metapb.Region
	Region     *metapb.Region
}

var _ raft.Storage = new(PeerStorage)

type PeerStorage struct {
	// current region information of the peer
	region *metapb.Region
	// current raft state of the peer
	raftState *rspb.RaftLocalState
	// current apply state of the peer
	applyState *rspb.RaftApplyState

	// current snapshot state
	snapState snap.SnapState
	// regionSched used to schedule task to region worker
	regionSched chan<- worker.Task
	// generate snapshot tried count
	snapTriedCnt int
	// Engine include two badger instance: Raft and Kv
	Engines *engine_util.Engines
	// Tag used for logging
	Tag string
}

// NewPeerStorage get persisted raftState from engines and return a peer storage
func NewPeerStorage(engines *engine_util.Engines, region *metapb.Region, regionSched chan<- worker.Task, tag string) (*PeerStorage, error) {
	log.Debugf("%s creating storage for %s", tag, region.String())
	raftState, err := meta.InitRaftLocalState(engines.Raft, region)
	if err != nil {
		return nil, err
	}
	applyState, err := meta.InitApplyState(engines.Kv, region)
	if err != nil {
		return nil, err
	}
	if raftState.LastIndex < applyState.AppliedIndex {
		log.Errorf("peer storage raftState:%v, applyState:%v", raftState, applyState)
		panic(fmt.Sprintf("%s unexpected raft log index: lastIndex %d < appliedIndex %d",
			tag, raftState.LastIndex, applyState.AppliedIndex))
	}
	return &PeerStorage{
		Engines:     engines,
		region:      region,
		Tag:         tag,
		raftState:   raftState,
		applyState:  applyState,
		regionSched: regionSched,
	}, nil
}

func (ps *PeerStorage) InitialState() (eraftpb.HardState, eraftpb.ConfState, error) {
	raftState := ps.raftState
	if raft.IsEmptyHardState(*raftState.HardState) {
		y.AssertTruef(!ps.isInitialized(),
			"peer for region %s is initialized but local state %+v has empty hard state",
			ps.region, ps.raftState)
		return eraftpb.HardState{}, eraftpb.ConfState{}, nil
	}
	return *raftState.HardState, util.ConfStateFromRegion(ps.region), nil
}

// Entries reads entries between [low, high) from Engines.raft
func (ps *PeerStorage) Entries(low, high uint64) ([]eraftpb.Entry, error) {
	if err := ps.checkRange(low, high); err != nil || low == high {
		return nil, err
	}
	buf := make([]eraftpb.Entry, 0, high-low)
	nextIndex := low
	txn := ps.Engines.Raft.NewTransaction(false)
	defer txn.Discard()
	startKey := meta.RaftLogKey(ps.region.Id, low)
	endKey := meta.RaftLogKey(ps.region.Id, high)
	iter := txn.NewIterator(badger.DefaultIteratorOptions)
	defer iter.Close()
	for iter.Seek(startKey); iter.Valid(); iter.Next() {
		item := iter.Item()
		if bytes.Compare(item.Key(), endKey) >= 0 {
			break
		}
		val, err := item.Value()
		if err != nil {
			return nil, err
		}
		var entry eraftpb.Entry
		if err = entry.Unmarshal(val); err != nil {
			return nil, err
		}
		// May meet gap or has been compacted.
		if entry.Index != nextIndex {
			break
		}
		nextIndex++
		buf = append(buf, entry)
	}
	// If we get the correct number of entries, returns.
	if len(buf) == int(high-low) {
		return buf, nil
	}
	// Here means we don't fetch enough entries.
	log.Errorf("Entries length:%d[want:%d,%d), get:%v", len(buf), low, high, buf)
	return nil, raft.ErrUnavailable
}

func (ps *PeerStorage) Term(idx uint64) (uint64, error) {
	if idx == ps.truncatedIndex() {
		return ps.truncatedTerm(), nil
	}
	if err := ps.checkRange(idx, idx+1); err != nil {
		return 0, err
	}
	if ps.truncatedTerm() == ps.raftState.LastTerm || idx == ps.raftState.LastIndex {
		return ps.raftState.LastTerm, nil
	}
	var entry eraftpb.Entry
	if err := engine_util.GetMeta(ps.Engines.Raft, meta.RaftLogKey(ps.region.Id, idx), &entry); err != nil {
		return 0, err
	}
	return entry.Term, nil
}

func (ps *PeerStorage) LastIndex() (uint64, error) {
	return ps.raftState.LastIndex, nil
}

func (ps *PeerStorage) FirstIndex() (uint64, error) {
	return ps.truncatedIndex() + 1, nil
}

func (ps *PeerStorage) Snapshot() (eraftpb.Snapshot, error) {
	log.Debugf("peer storage Snapshot generation begin")
	var snapshot eraftpb.Snapshot
	if ps.snapState.StateType == snap.SnapState_Generating {
		select {
		case s := <-ps.snapState.Receiver:
			if s != nil {
				snapshot = *s
			}
		default:
			return snapshot, raft.ErrSnapshotTemporarilyUnavailable
		}
		ps.snapState.StateType = snap.SnapState_Relax
		if snapshot.GetMetadata() != nil {
			ps.snapTriedCnt = 0
			if ps.validateSnap(&snapshot) {
				return snapshot, nil
			}
		} else {
			log.Warnf("%s failed to try generating snapshot, times: %d", ps.Tag, ps.snapTriedCnt)
		}
	}

	if ps.snapTriedCnt >= 5 {
		err := errors.Errorf("failed to get snapshot after %d times", ps.snapTriedCnt)
		ps.snapTriedCnt = 0
		return snapshot, err
	}

	log.Infof("%s requesting snapshot", ps.Tag)
	ps.snapTriedCnt++
	ch := make(chan *eraftpb.Snapshot, 1)
	ps.snapState = snap.SnapState{
		StateType: snap.SnapState_Generating,
		Receiver:  ch,
	}
	// schedule snapshot generate task
	ps.regionSched <- &runner.RegionTaskGen{
		RegionId: ps.region.GetId(),
		Notifier: ch,
	}
	return snapshot, raft.ErrSnapshotTemporarilyUnavailable
}

func (ps *PeerStorage) isInitialized() bool {
	return len(ps.region.Peers) > 0
}

func (ps *PeerStorage) Region() *metapb.Region {
	return ps.region
}

func (ps *PeerStorage) SetRegion(region *metapb.Region) {
	ps.region = region
}

func (ps *PeerStorage) checkRange(low, high uint64) error {
	if low > high {
		return errors.Errorf("low %d is greater than high %d", low, high)
	} else if low <= ps.truncatedIndex() {
		return raft.ErrCompacted
	} else if high > ps.raftState.LastIndex+1 {
		return errors.Errorf("entries' high %d is out of bound, lastIndex %d",
			high, ps.raftState.LastIndex)
	}
	return nil
}

func (ps *PeerStorage) truncatedIndex() uint64 {
	return ps.applyState.TruncatedState.Index
}

func (ps *PeerStorage) truncatedTerm() uint64 {
	return ps.applyState.TruncatedState.Term
}

func (ps *PeerStorage) AppliedIndex() uint64 {
	return ps.applyState.AppliedIndex
}

func (ps *PeerStorage) validateSnap(snap *eraftpb.Snapshot) bool {
	idx := snap.GetMetadata().GetIndex()
	if idx < ps.truncatedIndex() {
		log.Infof("%s snapshot is stale, generate again, snapIndex: %d, truncatedIndex: %d", ps.Tag, idx, ps.truncatedIndex())
		return false
	}
	var snapData rspb.RaftSnapshotData
	if err := proto.UnmarshalMerge(snap.GetData(), &snapData); err != nil {
		log.Errorf("%s failed to decode snapshot, it may be corrupted, err: %v", ps.Tag, err)
		return false
	}
	snapEpoch := snapData.GetRegion().GetRegionEpoch()
	latestEpoch := ps.region.GetRegionEpoch()
	if snapEpoch.GetConfVer() < latestEpoch.GetConfVer() {
		log.Infof("%s snapshot epoch is stale, snapEpoch: %s, latestEpoch: %s", ps.Tag, snapEpoch, latestEpoch)
		return false
	}
	return true
}

func (ps *PeerStorage) clearMeta(kvWB, raftWB *engine_util.WriteBatch) error {
	return ClearMeta(ps.Engines, kvWB, raftWB, ps.region.Id, ps.raftState.LastIndex)
}

// Delete all data that is not covered by `new_region`.
func (ps *PeerStorage) clearExtraData(newRegion *metapb.Region) {
	oldStartKey, oldEndKey := ps.region.GetStartKey(), ps.region.GetEndKey()
	newStartKey, newEndKey := newRegion.GetStartKey(), newRegion.GetEndKey()
	if bytes.Compare(oldStartKey, newStartKey) < 0 {
		ps.clearRange(newRegion.Id, oldStartKey, newStartKey)
	}
	if bytes.Compare(newEndKey, oldEndKey) < 0 {
		ps.clearRange(newRegion.Id, newEndKey, oldEndKey)
	}
}

// ClearMeta delete stale metadata like raftState, applyState, regionState and raft log entries
func ClearMeta(engines *engine_util.Engines, kvWB, raftWB *engine_util.WriteBatch, regionID uint64, lastIndex uint64) error {
	start := time.Now()
	kvWB.DeleteMeta(meta.RegionStateKey(regionID))
	kvWB.DeleteMeta(meta.ApplyStateKey(regionID))

	firstIndex := lastIndex + 1
	beginLogKey := meta.RaftLogKey(regionID, 0)
	endLogKey := meta.RaftLogKey(regionID, firstIndex)
	err := engines.Raft.View(func(txn *badger.Txn) error {
		it := txn.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()
		it.Seek(beginLogKey)
		if it.Valid() && bytes.Compare(it.Item().Key(), endLogKey) < 0 {
			logIdx, err1 := meta.RaftLogIndex(it.Item().Key())
			if err1 != nil {
				return err1
			}
			firstIndex = logIdx
		}
		return nil
	})
	if err != nil {
		return err
	}
	for i := firstIndex; i <= lastIndex; i++ {
		raftWB.DeleteMeta(meta.RaftLogKey(regionID, i))
	}
	raftWB.DeleteMeta(meta.RaftStateKey(regionID))
	log.Infof(
		"[region %d] clear peer 1 meta key 1 apply key 1 raft key and %d raft logs, takes %v",
		regionID,
		lastIndex+1-firstIndex,
		time.Since(start),
	)
	return nil
}

// Append the given entries to the raft log and update ps.raftState also delete log entries that will
// never be committed
func (ps *PeerStorage) Append(entries []eraftpb.Entry, raftWB *engine_util.WriteBatch) error {
	// Your Code Here (2B).
	// TODO(wendongbo): append entries here
	err := ps.Engines.Raft.Update(func(txn *badger.Txn) error {
		return nil
	})
	if err != nil {
		return err
	}
	return nil
}

// update state using given index & term
func (ps *PeerStorage) updateRaftLocalState(index, term uint64) bool {
	if index > ps.raftState.LastIndex || term > ps.raftState.LastTerm {
		ps.raftState.LastIndex = index
		ps.raftState.LastTerm = term
		return true
	}
	return false
}

func (ps *PeerStorage) updateApplyState(index, term, apply uint64) bool {
	hasUpdated := false
	if index > ps.applyState.TruncatedState.Index || term > ps.applyState.TruncatedState.Term {
		ps.applyState.TruncatedState.Index = index
		ps.applyState.TruncatedState.Term = term
		hasUpdated = true
	}
	if apply > ps.applyState.AppliedIndex {
		ps.applyState.AppliedIndex = apply
		hasUpdated = true
	}
	return hasUpdated
}

// Apply the peer with given snapshot
func (ps *PeerStorage) ApplySnapshot(snapshot *eraftpb.Snapshot, kvWB *engine_util.WriteBatch, raftWB *engine_util.WriteBatch) (*ApplySnapResult, error) {
	log.Infof("%v begin to apply snapshot", ps.Tag)
	snapData := new(rspb.RaftSnapshotData)
	if err := snapData.Unmarshal(snapshot.Data); err != nil {
		return nil, err
	}
	// TODO(wdb): maybe only use new Region?
	log.Debugf("snapshot unmarshal to RaftSnapshotData:%v", snapData.String()) // no KeyVal?
	log.Debugf("snapshot kv data:%v", snapData.Data)
	// Hint: things need to do here including: update peer storage state like raftState and applyState, etc,
	// and send RegionTaskApply task to region worker through ps.regionSched, also remember call ps.clearMeta
	// and ps.clearExtraData to delete stale data
	// Your Code Here (2C).

	// TODO(wdb): what did we do with snapData? how does it associate with state machine?

	log.Warnf("TODO: applySnapshot is not implemented yet, meta:%v", snapshot.Metadata)
	if ps.updateRaftLocalState(snapshot.Metadata.Index, snapshot.Metadata.Term) {
		log.Debugf("ApplySnapshot update raftState:%v", ps.raftState)
		err := raftWB.SetMeta(meta.RaftStateKey(ps.region.Id), ps.raftState)
		if err != nil {
			log.Error(err)
			return nil, err
		}
	}
	if ps.updateApplyState(snapshot.Metadata.Index, snapshot.Metadata.Term, snapshot.Metadata.Index) {
		log.Debugf("ApplySnapshot update applyState:%v", ps.applyState)
		err := kvWB.SetMeta(meta.ApplyStateKey(ps.region.Id), ps.applyState)
		if err != nil {
			log.Error(err)
			return nil, err
		}
	}
	ch := make(chan bool)
	task := &runner.RegionTaskApply{
		RegionId: ps.region.Id,
		Notifier: ch,
		SnapMeta: snapshot.Metadata,
		StartKey: ps.region.StartKey,
		EndKey: ps.region.EndKey,
	}
	// TODO(wendongbo): when do we update region Start&End Key ?
	log.Debugf("RegionTaskApply begin, region:%v", ps.region)
	ps.regionSched <- task
	<-ch
	log.Debugf("RegionTaskApply complete")
	ps.clearExtraData(ps.region)
	if err := ps.clearMeta(kvWB, raftWB); err != nil {
		return nil, err
	}
	raftWB.MustWriteToDB(ps.Engines.Raft)
	kvWB.MustWriteToDB(ps.Engines.Kv)
	return nil, nil
}

// Save memory states to disk.
// Do not modify ready in this function, this is a requirement to advance the ready object properly later.
func (ps *PeerStorage) SaveReadyState(ready *raft.Ready) (*ApplySnapResult, error) {
	// Hint: you may call `Append()` and `ApplySnapshot()` in this function
	// Your Code Here (2B/2C).
	log.Debugf("peerStorage SaveReadyState")
	res := &ApplySnapResult{
		Region: ps.Region(),
		PrevRegion: ps.Region(),
	}
	if ready.Snapshot.Data != nil || ready.Snapshot.Metadata != nil {
		var kvWB, raftWB engine_util.WriteBatch
		if _, err := ps.ApplySnapshot(&ready.Snapshot, &kvWB, &raftWB); err != nil {
			log.Errorf("peer storage apply snapshot err:%v", err)
		}
	}

	// TODO(wendongbo): update write batch logic
	// save raft log
	wb := &engine_util.WriteBatch{}
	log.Debugf("peerStorage SaveReadyState Entries(len:%d):%v" , len(ready.Entries), ready.Entries)
	for _, ent := range ready.Entries {
		k := meta.RaftLogKey(ps.region.Id, ent.Index)
		dt, _ := ent.Marshal()
		log.Debugf("write {key:%v, val:%v} into raft engine", k, dt)
		if err := wb.SetMeta(k, &ent); err != nil {
			log.Errorf("write {key:%v, val:%v} into raft engine err:%v", k, dt, err.Error())
		}
	}

	// update raft state
	if l := len(ready.Entries); l > 0 {
		ps.raftState.LastIndex = util.MaxUint64(ps.raftState.LastIndex, ready.Entries[l - 1].Index)
		ps.raftState.LastTerm = ready.Entries[l - 1].Term
	}

	if ready.Term != 0 || ready.Commit != 0 {
		ps.raftState.HardState = &ready.HardState
	}
	// TODO(wendongbo): can be opt if no update
	if err := wb.SetMeta(meta.RaftStateKey(ps.region.Id), ps.raftState); err != nil {
		log.Errorf("%s", err.Error())
	}
	log.Debugf("write raftLocalState:%v", ps.raftState.String())

	wb.MustWriteToDB(ps.Engines.Raft)
	wb.Reset()

	log.Debugf("peerStorage SaveReadyState CommittedEntries(len:%d):%v" , len(ready.CommittedEntries), ready.CommittedEntries)
	for _, ent := range ready.CommittedEntries {
		msg := raft_cmdpb.RaftCmdRequest{}
		if err := msg.Unmarshal(ent.Data); err != nil {
			log.Errorf("peer storage unmarshal Committed Entries err:%v", err.Error())
		}

		// handle admin requests
		if msg.AdminRequest != nil {
			switch msg.AdminRequest.CmdType {
			case raft_cmdpb.AdminCmdType_CompactLog:
				ps.applyState.TruncatedState.Index = msg.AdminRequest.CompactLog.CompactIndex
				ps.applyState.TruncatedState.Term = msg.AdminRequest.CompactLog.CompactTerm
			default:
				log.Warnf("unsupported admin request type:%d %s", msg.AdminRequest.CmdType, raft_cmdpb.AdminCmdType_name[int32(msg.AdminRequest.CmdType)])
			}
		}

		// handle normal requests
		for _, req := range msg.Requests {
			switch req.CmdType {
			case raft_cmdpb.CmdType_Put:
				wb.SetCF(req.Put.Cf, req.Put.Key, req.Put.Value)
			case raft_cmdpb.CmdType_Delete:
				wb.DeleteCF(req.Delete.Cf, req.Delete.Key)
			case raft_cmdpb.CmdType_Snap, raft_cmdpb.CmdType_Get:
				// read operation
			default:
				log.Warnf("SaveReadyState unexpected operation: %d, ent:%v", req.CmdType, req)
			}
		}
	}

	// update ApplyState.AppliedIndex
	if l := len(ready.CommittedEntries); l > 0 {
		ps.applyState.AppliedIndex = util.MaxUint64(ps.applyState.AppliedIndex, ready.CommittedEntries[l-1].Index)
	}
	if err := wb.SetMeta(meta.ApplyStateKey(ps.region.Id), ps.applyState); err != nil {
		log.Errorf("set apply state meta err:%v", err)
	}
	log.Debugf("peer storage applyState:%v", ps.applyState)

	regionState := &rspb.RegionLocalState{
		State: rspb.PeerState_Normal,
		Region: ps.region,
	}
	if err := wb.SetMeta(meta.RegionStateKey(ps.region.Id), regionState); err != nil {
		log.Errorf("set region local state meta err:%v", err)
	}

	log.Debugf("peerStorage SaveReadyState check complete, local state:%v", ps.raftState)
	// TODO(wendongbo):  save RegionLocalState
	wb.MustWriteToDB(ps.Engines.Kv)

	return res, nil
}

func (ps *PeerStorage) ClearData() {
	ps.clearRange(ps.region.GetId(), ps.region.GetStartKey(), ps.region.GetEndKey())
}

func (ps *PeerStorage) clearRange(regionID uint64, start, end []byte) {
	ps.regionSched <- &runner.RegionTaskDestroy{
		RegionId: regionID,
		StartKey: start,
		EndKey:   end,
	}
}
