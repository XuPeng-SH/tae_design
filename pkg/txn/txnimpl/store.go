package txnimpl

import (
	"tae/pkg/iface/txnif"
	"tae/pkg/txn/txnbase"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/metadata/v1"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/mutation/buffer/base"
)

type txnStore struct {
	txnbase.NoopTxnStore
	tables     map[uint64]Table
	driver     txnbase.NodeDriver
	nodesMgr   base.INodeManager
	dbIndex    map[string]uint64
	tableIndex map[string]uint64
	txn        txnif.AsyncTxn
}

var DefaultTxnStoreFactory = func() txnif.TxnStore { return NewStore() }

func NewStore() *txnStore {
	return &txnStore{
		tables: make(map[uint64]Table),
	}
}

func (store *txnStore) Close() error {
	var err error
	for _, table := range store.tables {
		if err = table.Close(); err != nil {
			break
		}
	}
	return err
}

func (store *txnStore) InitTable(id uint64, schema *metadata.Schema) error {
	table := store.tables[id]
	if table != nil {
		return ErrDuplicateNode
	}
	store.tables[id] = NewTable(nil, id, schema, store.driver, store.nodesMgr)
	store.tableIndex[schema.Name] = id
	return nil
}

func (store *txnStore) BindTxn(txn txnif.AsyncTxn) {
	store.txn = txn
}

func (store *txnStore) Append(id uint64, data *batch.Batch) error {
	table := store.tables[id]
	if table.IsDeleted() {
		return txnbase.ErrNotFound
	}
	return table.Append(data)
}

func (store *txnStore) RangeDeleteLocalRows(id uint64, start, end uint32) error {
	table := store.tables[id]
	return table.RangeDeleteLocalRows(start, end)
}

func (store *txnStore) UpdateLocalValue(id uint64, row uint32, col uint16, value interface{}) error {
	table := store.tables[id]
	return table.UpdateLocalValue(row, col, value)
}

func (store *txnStore) AddUpdateNode(id uint64, node txnif.BlockUpdates) error {
	table := store.tables[id]
	return table.AddUpdateNode(node)
}

// func (store *txnStore) PrepareRollback() error { return nil }
// func (store *txnStore) PrepareCommit() error   { return nil }
// func (store *txnStore) Rollback() error        { return nil }
// func (store *txnStore) Commit() error          { return nil }

// func (store *txnStore) FindKeys(db, table uint64, keys [][]byte) []uint32 {
// 	// TODO
// 	return nil
// }

// func (store *txnStore) FindKey(db, table uint64, key []byte) uint32 {
// 	// TODO
// 	return 0
// }

// func (store *txnStore) HasKey(db, table uint64, key []byte) bool {
// 	// TODO
// 	return false
// }
