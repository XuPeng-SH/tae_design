package txnbase

import (
	"tae/pkg/iface/txnif"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
)

var NoopStoreFactory = func() txnif.TxnStore { return new(NoopTxnStore) }

type NoopTxnStore struct{}

func (store *NoopTxnStore) BindTxn(txn txnif.AsyncTxn)                              {}
func (store *NoopTxnStore) Close() error                                            { return nil }
func (store *NoopTxnStore) RangeDeleteLocalRows(id uint64, start, end uint32) error { return nil }
func (store *NoopTxnStore) Append(id uint64, data *batch.Batch) error               { return nil }
func (store *NoopTxnStore) UpdateLocalValue(id uint64, row uint32, col uint16, v interface{}) error {
	return nil
}
func (store *NoopTxnStore) AddUpdateNode(id uint64, node txnif.BlockUpdates) error { return nil }
func (store *NoopTxnStore) PrepareRollback() error                                 { return nil }
func (store *NoopTxnStore) PrepareCommit() error                                   { return nil }
func (store *NoopTxnStore) Rollback() error                                        { return nil }
func (store *NoopTxnStore) Commit() error                                          { return nil }

func (store *NoopTxnStore) AddTxnEntry(entry txnif.TxnEntry) {}

// func (store *NoopTxnStore) CreateDBEntry(name string) error                         { return nil }
// func (store *NoopTxnStore) DropDBEntry(name string) error                           { return nil }
// func (store *NoopTxnStore) CreateTableEntry(database string, def interface{}) error { return nil }
// func (store *NoopTxnStore) DropTableEntry(dbName, name string) error                { return nil }
