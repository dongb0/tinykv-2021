package server

import (
	"context"
	"github.com/pingcap-incubator/tinykv/kv/coprocessor"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/storage/raft_storage"
	"github.com/pingcap-incubator/tinykv/kv/transaction/latches"
	"github.com/pingcap-incubator/tinykv/kv/transaction/mvcc"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	coppb "github.com/pingcap-incubator/tinykv/proto/pkg/coprocessor"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/tinykvpb"
	"github.com/pingcap/tidb/kv"
)

var _ tinykvpb.TinyKvServer = new(Server)

// Server is a TinyKV server, it 'faces outwards', sending and receiving messages from clients such as TinySQL.
type Server struct {
	storage storage.Storage

	// (Used in 4A/4B)
	Latches *latches.Latches

	// coprocessor API handler, out of course scope
	copHandler *coprocessor.CopHandler
}

func NewServer(storage storage.Storage) *Server {
	return &Server{
		storage: storage,
		Latches: latches.NewLatches(),
	}
}

// The below functions are Server's gRPC API (implements TinyKvServer).

// Raft commands (tinykv <-> tinykv)
// Only used for RaftStorage, so trivially forward it.
func (server *Server) Raft(stream tinykvpb.TinyKv_RaftServer) error {
	return server.storage.(*raft_storage.RaftStorage).Raft(stream)
}

// Snapshot stream (tinykv <-> tinykv)
// Only used for RaftStorage, so trivially forward it.
func (server *Server) Snapshot(stream tinykvpb.TinyKv_SnapshotServer) error {
	return server.storage.(*raft_storage.RaftStorage).Snapshot(stream)
}

// Transactional API.
func (server *Server) KvGet(_ context.Context, req *kvrpcpb.GetRequest) (*kvrpcpb.GetResponse, error) {
	// Your Code Here (4B).
	// TODO(wdb): handle region err
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		return nil, err
	}
	resp := &kvrpcpb.GetResponse{}
	txn := mvcc.NewMvccTxn(reader, req.Version)
	lock, err := txn.GetLock(req.Key)
	if err != nil {
		return resp, err
	}
	if lock != nil && lock.Ts + lock.Ttl < req.Version { // TODO(wdb): < or <= ??
		resp.Error = &kvrpcpb.KeyError{
			Locked: lock.Info(req.Key),
		}
		return resp, nil
	}

	val, err := txn.GetValue(req.Key)
	if err != nil {
		return nil, err
	}
	resp.Value = val
	if val == nil {
		resp.NotFound = true
	}
	return resp, nil
}

func (server *Server) KvPrewrite(_ context.Context, req *kvrpcpb.PrewriteRequest) (*kvrpcpb.PrewriteResponse, error) {
	// Your Code Here (4B).
	// TODO(wdb): handle region err
	reader, err := server.storage.Reader(req.Context) // TODO(wdb): context?
	if err != nil {
		return nil, err
	}
	resp := &kvrpcpb.PrewriteResponse{}

	txn := mvcc.NewMvccTxn(reader, req.StartVersion)
	for _, m := range req.Mutations {
		lock, err := txn.GetLock(m.Key)
		if err != nil {
			return nil, err
		}
		if lock != nil {
			resp.Errors = append(resp.Errors, &kvrpcpb.KeyError{Locked: lock.Info(m.Key)})
			return resp, nil
		}
		_, ts, _ := txn.MostRecentWrite(m.Key)
		if req.StartVersion < ts {	// write conflict
			resp.Errors = append(resp.Errors, &kvrpcpb.KeyError{Conflict: &kvrpcpb.WriteConflict{
				StartTs: ts,
				ConflictTs: req.StartVersion,
				Key: m.Key,
				Primary: req.PrimaryLock,
			}})
			return resp, nil
		}
	}
	for _, m := range req.Mutations {
		lock := &mvcc.Lock{
			Primary: req.PrimaryLock,
			Ts:      req.StartVersion,
			Ttl:     req.LockTtl,
			Kind: 	 mvcc.WriteKindFromProto(m.Op),
		}
		txn.PutLock(m.Key, lock)
		txn.PutValue(m.Key, m.Value)
	}

	if err := server.storage.Write(req.Context, txn.Writes()); err != nil {
		return nil, err
	}
	return resp, nil
}

func (server *Server) KvCommit(_ context.Context, req *kvrpcpb.CommitRequest) (*kvrpcpb.CommitResponse, error) {
	// Your Code Here (4B).
	resp := &kvrpcpb.CommitResponse{}
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		return nil, err
	}
	txn := mvcc.NewMvccTxn(reader, req.StartVersion)

	for _, k := range req.Keys {
		lock, err := txn.GetLock(k)
		if err != nil {
			return nil, err
		}
		if lock == nil {
			return resp, nil
		}
		if lock.Kind == mvcc.WriteKindRollback { // TODO(wdb):maybe not only rollback?
			resp.Error = &kvrpcpb.KeyError{Retryable: ""}
			return resp, nil
		}
		write := &mvcc.Write{
			StartTS: lock.Ts,
			Kind: lock.Kind,
		}
		txn.PutWrite(k, req.CommitVersion, write)
		txn.DeleteLock(k)
	}
	if err := server.storage.Write(req.Context, txn.Writes()); err != nil {
		return nil, err
	}
	return resp, nil
}

func (server *Server) KvScan(_ context.Context, req *kvrpcpb.ScanRequest) (*kvrpcpb.ScanResponse, error) {
	// Your Code Here (4C).
	resp := &kvrpcpb.ScanResponse{Pairs: make([]*kvrpcpb.KvPair, 0)}
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		return nil, err
	}
	txn := mvcc.NewMvccTxn(reader, req.Version)
	scanner := mvcc.NewScanner(req.StartKey, txn)
	defer scanner.Close()
	for i := uint32(0); i < req.Limit; i++ {
		key, val, _ := scanner.Next()
		if key == nil && val == nil {
			break
		}
		resp.Pairs = append(resp.Pairs, &kvrpcpb.KvPair{Key: key, Value: val})
	}

	return resp, nil
}

func (server *Server) KvCheckTxnStatus(_ context.Context, req *kvrpcpb.CheckTxnStatusRequest) (*kvrpcpb.CheckTxnStatusResponse, error) {
	// Your Code Here (4C).
	resp := &kvrpcpb.CheckTxnStatusResponse{}
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		return nil, err
	}
	txn := mvcc.NewMvccTxn(reader, req.LockTs)
	lock, err := txn.GetLock(req.PrimaryKey)
	if err != nil {
		return nil, err
	}
	if lock == nil { // TODO(wdb): opt
		write, ts, _ := txn.MostRecentWrite(req.PrimaryKey)
		if write == nil {
			resp.Action = kvrpcpb.Action_LockNotExistRollback
			txn.PutWrite(req.PrimaryKey, req.LockTs, &mvcc.Write{StartTS: req.LockTs, Kind: mvcc.WriteKindRollback} )
			if err := server.storage.Write(req.Context, txn.Writes()); err != nil {
				return nil, err
			}
		} else {
			resp.CommitVersion = ts
		}
		return resp, nil
	}

	// lock expire
	if lock.Ts + lock.Ttl < req.CurrentTs {
		resp.Action = kvrpcpb.Action_TTLExpireRollback
		txn.PutWrite(req.PrimaryKey, req.LockTs, &mvcc.Write{StartTS: lock.Ts, Kind: mvcc.WriteKindRollback})
		txn.DeleteValue(req.PrimaryKey)
		txn.DeleteLock(req.PrimaryKey)
	}

	if err := server.storage.Write(req.Context, txn.Writes()); err != nil {
		return nil, err
	}
	return resp, nil
}

func (server *Server) KvBatchRollback(_ context.Context, req *kvrpcpb.BatchRollbackRequest) (*kvrpcpb.BatchRollbackResponse, error) {
	// Your Code Here (4C).
	resp := &kvrpcpb.BatchRollbackResponse{}
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		return nil, err
	}
	txn := mvcc.NewMvccTxn(reader, req.StartVersion)
	for _, k := range req.Keys {
		if write, _, _ := txn.MostRecentWrite(k); write != nil {
			if write.Kind == mvcc.WriteKindRollback {
				return resp, nil
			}
			if write.Kind == mvcc.WriteKindPut {
				resp.Error = &kvrpcpb.KeyError{Abort: ""}
				return resp, nil
			}
		}

		if lock, err := txn.GetLock(k); lock != nil && err == nil && lock.Ts == req.StartVersion {
			txn.DeleteValue(k)
			txn.DeleteLock(k)
		}
		txn.PutWrite(k, req.StartVersion, &mvcc.Write{StartTS: req.StartVersion, Kind: mvcc.WriteKindRollback})
	}
	if err := server.storage.Write(req.Context, txn.Writes()); err != nil {
		return nil, err
	}
	return resp, nil
}

func (server *Server) KvResolveLock(_ context.Context, req *kvrpcpb.ResolveLockRequest) (*kvrpcpb.ResolveLockResponse, error) {
	// Your Code Here (4C).
	resp := &kvrpcpb.ResolveLockResponse{}
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		return nil, err
	}
	txn := mvcc.NewMvccTxn(reader, req.StartVersion)
	lockIter := txn.Reader.IterCF(engine_util.CfLock)
	//lockIter.Seek(mvcc.EncodeKey()) // no need to seek, just scan all?
	for ; lockIter.Valid(); lockIter.Next() {
		item := lockIter.Item()
		lockBytes, _ := item.Value()
		//if err != nil {}
		lock, _ := mvcc.ParseLock(lockBytes)
		if lock.Ts <= txn.StartTS {

			if req.CommitVersion > 0 { // commit
				txn.PutWrite(item.Key(), req.CommitVersion, &mvcc.Write{StartTS: lock.Ts, Kind: mvcc.WriteKindPut})
			} else { // rollback
				txn.PutWrite(item.Key(), lock.Ts, &mvcc.Write{StartTS: lock.Ts, Kind: mvcc.WriteKindRollback})
				txn.DeleteValue(item.Key())
			}
			txn.DeleteLock(item.Key())
		}
	}
	if err := server.storage.Write(req.Context, txn.Writes()); err != nil {
		return nil, err
	}
	return resp, nil
}

// SQL push down commands.
func (server *Server) Coprocessor(_ context.Context, req *coppb.Request) (*coppb.Response, error) {
	resp := new(coppb.Response)
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		if regionErr, ok := err.(*raft_storage.RegionError); ok {
			resp.RegionError = regionErr.RequestErr
			return resp, nil
		}
		return nil, err
	}
	switch req.Tp {
	case kv.ReqTypeDAG:
		return server.copHandler.HandleCopDAGRequest(reader, req), nil
	case kv.ReqTypeAnalyze:
		return server.copHandler.HandleCopAnalyzeRequest(reader, req), nil
	}
	return nil, nil
}
