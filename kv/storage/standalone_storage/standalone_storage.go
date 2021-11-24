package standalone_storage

import (
	"github.com/Connor1996/badger"
	"github.com/pingcap-incubator/tinykv/kv/config"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
	log "github.com/sirupsen/logrus"
)

var (
	keySeparator = []byte("_")
)

var (
	_ storage.Storage = (*StandAloneStorage)(nil)
	_ storage.StorageReader = (*standAloneReader)(nil)
)

// StandAloneStorage is an implementation of `Storage` for a single-node TinyKV instance. It does not
// communicate with other nodes and all data is stored locally.
type StandAloneStorage struct {
	db *badger.DB
}

func NewStandAloneStorage(conf *config.Config) *StandAloneStorage {
	// Your Code Here (1).
	opt := badger.DefaultOptions
	opt.Dir = conf.DBPath
	opt.ValueDir = conf.DBPath
	db, err := badger.Open(opt)
	if err != nil {
		log.Println(err)
		return nil
	}
	return &StandAloneStorage{
		db: db,
	}
}

func (s *StandAloneStorage) Start() error {
	// Your Code Here (1).

	return nil
}

func (s *StandAloneStorage) Stop() error {
	return s.db.Close()
}

func (s *StandAloneStorage) Reader(ctx *kvrpcpb.Context) (storage.StorageReader, error) {
	// TODO: should we use context?
	return newStandAloneReader(s.db, s.db.NewTransaction(false)), nil
}

func (s *StandAloneStorage) Write(ctx *kvrpcpb.Context, batch []storage.Modify) error {
	err := s.db.Update(func(txn *badger.Txn) error {
		for _, d := range batch {
			key := []byte(d.Cf())
			key = append(key, keySeparator...)
			key = append(key, d.Key()...)
			//log.Printf("write key:%s, value:%s\n", string(key), string(d.Value()))
			switch d.Data.(type) {
			case storage.Put:
				if err := txn.Set(key, d.Value()); err != nil {
					return err
				}
			case storage.Delete:
				if err := txn.Delete(key); err != nil {
					return err
				}
			}
		}
		log.Printf("stand-alone storage write value succeeded")
		return nil
	})
	return err
}

type standAloneReader struct {
	db *badger.DB
	txn *badger.Txn
}

func newStandAloneReader(db *badger.DB, txn *badger.Txn) standAloneReader {
	return standAloneReader{
		db: db,
		txn: txn,
	}
}

func (s standAloneReader) GetCF(cf string, key []byte) ([]byte, error) {
	var value []byte
	err := s.db.View(func(txn *badger.Txn) error {
		searchKey := []byte(cf)
		searchKey = append(searchKey, keySeparator...)
		searchKey = append(searchKey, key...)
		item, err := txn.Get(searchKey)
		if err != nil {
			if err != badger.ErrKeyNotFound {
				return err
			}
			return nil
		}
		res, err := item.Value()
		if err != nil {
			return err
		}
		value = res
		return nil
	})
	if err != nil {
		return nil, err
	}
	return value, nil
}

func (s standAloneReader) IterCF(cf string) engine_util.DBIterator {
	return engine_util.NewCFIterator(cf, s.txn)
}

func (s standAloneReader) Close() {
	s.txn.Discard()
}

