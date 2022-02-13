package mvcc

import (
	"bytes"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
)

// Scanner is used for reading multiple sequential key/value pairs from the storage layer. It is aware of the implementation
// of the storage layer and returns results suitable for users.
// Invariant: either the scanner is finished and cannot be used, or it is ready to return a value immediately.
type Scanner struct {
	// Your Data Here (4C).
	txn *MvccTxn
	iter engine_util.DBIterator
}

// NewScanner creates a new scanner ready to read from the snapshot in txn.
func NewScanner(startKey []byte, txn *MvccTxn) *Scanner {
	// Your Code Here (4C).
	iter := txn.Reader.IterCF(engine_util.CfWrite)
	iter.Seek(EncodeKey(startKey, txn.StartTS))
	return &Scanner{
		txn: txn,
		iter: iter,
	}
}

func (scan *Scanner) Close() {
	// Your Code Here (4C).
	scan.iter.Close()
}

// Next returns the next key/value pair from the scanner. If the scanner is exhausted, then it will return `nil, nil, nil`.
func (scan *Scanner) Next() ([]byte, []byte, error) {
	// Your Code Here (4C).
	defaultIter := scan.txn.Reader.IterCF(engine_util.CfDefault)
	for scan.iter.Valid() {
		rawKey := scan.iter.Item().Key()
		key := DecodeUserKey(rawKey)
		commitTs := decodeTimestamp(rawKey)
		writeVal, _ := scan.iter.Item().Value()
		write, _ := ParseWrite(writeVal)
		// TODO(wdb): opt logic
		if commitTs > scan.txn.StartTS {
			scan.iter.Next()
			continue
		}

		if write.Kind != WriteKindPut {
			scan.skipKey(key)
			continue
		}

		defaultIter.Seek(EncodeKey(key, commitTs))
		if !defaultIter.Valid() {
			continue
		}

		val, _ := defaultIter.Item().Value()
		scan.skipKey(key)
		//log.Infof("key:%v, val:%v, commitTs:%d", key, val, commitTs)
		return key, val, nil
	}
	return nil, nil, nil
}

func (scan *Scanner) skipKey(key []byte) {
	for scan.iter.Valid() && bytes.Equal(DecodeUserKey(scan.iter.Item().Key()), key) {
		scan.iter.Next()
	}
}
