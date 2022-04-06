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
	startKey []byte
	txn      *MvccTxn
	iterator engine_util.DBIterator
	hasNext  bool
}

// NewScanner creates a new scanner ready to read from the snapshot in txn.
func NewScanner(startKey []byte, txn *MvccTxn) *Scanner {
	// Your Code Here (4C).
	iterator := txn.Reader.IterCF(engine_util.CfWrite)
	return &Scanner{
		startKey: startKey,
		txn:      txn,
		iterator: iterator,
		hasNext:  iterator.Valid(),
	}
}

func (scan *Scanner) Close() {
	// Your Code Here (4C).
	scan.iterator.Close()
}

// Next returns the next key/value pair from the scanner. If the scanner is exhausted, then it will return `nil, nil, nil`.
func (scan *Scanner) Next() ([]byte, []byte, error) {
	// Your Code Here (4C).
	if !scan.hasNext {
		return nil, nil, nil
	}
	startKey := scan.startKey
	scan.iterator.Seek(EncodeKey(startKey, scan.txn.StartTS))
	if !scan.iterator.Valid() {
		scan.hasNext = false
		return nil, nil, nil
	}
	item := scan.iterator.Item()
	gotKey := item.KeyCopy(nil)
	userKey := DecodeUserKey(gotKey)
	if !bytes.Equal(startKey, userKey) {
		scan.startKey = userKey
		return scan.Next()
	}
	for {
		scan.iterator.Next()
		if !scan.iterator.Valid() {
			scan.hasNext = false
			break
		}
		gotKey := scan.iterator.Item().KeyCopy(nil)
		userKey := DecodeUserKey(gotKey)
		if !bytes.Equal(startKey, userKey) {
			scan.startKey = userKey
			break
		}
	}
	writeVal, err := item.ValueCopy(nil)
	if err != nil {
		return startKey, nil, err
	}
	write, err := ParseWrite(writeVal)
	if err != nil {
		return startKey, nil, err
	}
	if write.Kind == WriteKindDelete {
		return startKey, nil, nil
	}
	value, err := scan.txn.Reader.GetCF(engine_util.CfDefault, EncodeKey(startKey, write.StartTS))
	return startKey, value, err
}
