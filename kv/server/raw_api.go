package server

import (
	"context"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// The functions below are Server's Raw API. (implements TinyKvServer).
// Some helper methods can be found in sever.go in the current directory
//RawGet return the corresponding Get response based on RawGetRequest's CF and Key fields
func (server *Server) RawGet(_ context.Context, req *kvrpcpb.RawGetRequest) (*kvrpcpb.RawGetResponse, error) {
	// Your Code Here (1).
	reader, err := server.storage.Reader(req.GetContext())
	if err != nil {
		return nil, err
	}
	defer reader.Close()
	val, err := reader.GetCF(req.Cf, req.Key)
	return &kvrpcpb.RawGetResponse{
		Value:    val,
		NotFound: val == nil,
	}, err
}

// RawPut puts the target data into storage and returns the corresponding response
func (server *Server) RawPut(_ context.Context, req *kvrpcpb.RawPutRequest) (*kvrpcpb.RawPutResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using Storage.Modify to store data to be modified
	putOp := storage.Modify{
		Data: storage.Put{
			Key:   req.GetKey(),
			Value: req.GetValue(),
			Cf:    req.GetCf(),
		},
	}
	err := server.storage.Write(req.GetContext(), []storage.Modify{putOp})
	return &kvrpcpb.RawPutResponse{}, err
}

// RawDelete delete the target data from storage and returns the corresponding response
func (server *Server) RawDelete(_ context.Context, req *kvrpcpb.RawDeleteRequest) (*kvrpcpb.RawDeleteResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using Storage.Modify to store data to be deleted
	deleteOp := storage.Modify{
		Data: storage.Delete{
			Key: req.GetKey(),
			Cf:  req.GetCf(),
		},
	}
	err := server.storage.Write(req.GetContext(), []storage.Modify{deleteOp})
	return &kvrpcpb.RawDeleteResponse{}, err
}

// RawScan scan the data starting from the start key up to limit. and return the corresponding result
func (server *Server) RawScan(_ context.Context, req *kvrpcpb.RawScanRequest) (*kvrpcpb.RawScanResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using reader.IterCF
	reader, err := server.storage.Reader(req.GetContext())
	if err != nil {
		return nil, err
	}
	kvs := make([]*kvrpcpb.KvPair, 0, req.GetLimit())
	it := reader.IterCF(req.GetCf())
	it.Seek(req.GetStartKey())
	for it.Valid() {
		kv := it.Item()
		val, err := kv.Value()
		if err != nil {
			return nil, err
		}
		kvs = append(kvs, &kvrpcpb.KvPair{
			Key:   kv.Key(),
			Value: val,
		})
		if uint32(len(kvs)) == req.GetLimit() {
			break
		}
		it.Next()
	}
	it.Close()

	return &kvrpcpb.RawScanResponse{
		Kvs: kvs,
	}, nil
}
