package server

import (
	"context"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
	"log"
)

// The functions below are Server's Raw API. (implements TinyKvServer).
// Some helper methods can be found in sever.go in the current directory

// RawGet return the corresponding Get response based on RawGetRequest's CF and Key fields
func (server *Server) RawGet(_ context.Context, req *kvrpcpb.RawGetRequest) (*kvrpcpb.RawGetResponse, error) {
	respond := &kvrpcpb.RawGetResponse{}
	reader, err := server.storage.Reader(req.Context)
	defer reader.Close()
	if err != nil {
		respond.Error = err.Error()
		return respond, err
	}
	val, err := reader.GetCF(req.GetCf(), req.GetKey())
	if err != nil {
		log.Println("raw get err:", err.Error())
		return nil, err
	}
	respond.Value = val
	if val == nil {
		respond.NotFound = true
	}
	return respond, nil
}

// RawPut puts the target data into storage and returns the corresponding response
func (server *Server) RawPut(_ context.Context, req *kvrpcpb.RawPutRequest) (*kvrpcpb.RawPutResponse, error) {
	respond := &kvrpcpb.RawPutResponse{}
	data := storage.Modify{Data: storage.Put{
		Cf: req.Cf,
		Key: req.Key,
		Value: req.Value,
	}}
	err := server.storage.Write(nil, []storage.Modify{data})
	if err != nil {
		respond.Error = err.Error()
		return respond, err
	}
	return respond, nil
}

// RawDelete delete the target data from storage and returns the corresponding response
func (server *Server) RawDelete(_ context.Context, req *kvrpcpb.RawDeleteRequest) (*kvrpcpb.RawDeleteResponse, error) {
	respond := &kvrpcpb.RawDeleteResponse{}
	data := storage.Modify{Data: storage.Delete{
		Cf: req.Cf,
		Key: req.Key,
	}}
	err := server.storage.Write(nil, []storage.Modify{data})
	if err != nil {
		respond.Error = err.Error()
		return respond, err
	}
	return respond, nil
}

// RawScan scan the data starting from the start key up to limit. and return the corresponding result
func (server *Server) RawScan(_ context.Context, req *kvrpcpb.RawScanRequest) (*kvrpcpb.RawScanResponse, error) {
	// Hint: Consider using reader.IterCF
	respond := &kvrpcpb.RawScanResponse{}
	reader, err := server.storage.Reader(req.Context)
	defer reader.Close()
	if err != nil {
		respond.Error = err.Error()
		return respond, err
	}
	iter := reader.IterCF(req.GetCf())
	defer iter.Close()
	for iter.Seek(nil); req.Limit != 0 && iter.Valid(); req.Limit-- {
		key := iter.Item().Key()
		val, _ := iter.Item().Value()
		kv := &kvrpcpb.KvPair{Key: key, Value: val}
		//kv.Error = err
		respond.Kvs = append(respond.Kvs, kv)
		iter.Next()
	}

	return respond, nil
}
