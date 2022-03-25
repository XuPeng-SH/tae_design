package txn

import (
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/metadata/v1"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/mutation/buffer/base"
)

type Store struct {
	tables     map[uint64]Table
	driver     NodeDriver
	nodesMgr   base.INodeManager
	dbIndex    map[string]uint64
	tableIndex map[string]uint64
}

func NewStore() *Store {
	return &Store{
		tables: make(map[uint64]Table),
	}
}

func (store *Store) Close() error {
	var err error
	for _, table := range store.tables {
		if err = table.Close(); err != nil {
			break
		}
	}
	return err
}

func (store *Store) InitTable(id uint64, schema *metadata.Schema) error {
	table := store.tables[id]
	if table != nil {
		return ErrDuplicateNode
	}
	store.tables[id] = NewTable(nil, id, schema, store.driver, store.nodesMgr)
	store.tableIndex[schema.Name] = id
	return nil
}

func (store *Store) Append(id uint64, data *batch.Batch) error {
	table := store.tables[id]
	if table.IsDeleted() {
		return ErrNotFound
	}
	return table.Append(data)
}

func (store *Store) RangeDeleteLocalRows(id uint64, start, end uint32) error {
	table := store.tables[id]
	return table.RangeDeleteLocalRows(start, end)
}

func (store *Store) UpdateLocalValue(id uint64, row uint32, col uint16, value interface{}) error {
	table := store.tables[id]
	return table.UpdateLocalValue(row, col, value)
}

func (store *Store) AddUpdateNode(id uint64, node *blockUpdates) error {
	table := store.tables[id]
	return table.AddUpdateNode(node)
}

// func (store *Store) FindKeys(db, table uint64, keys [][]byte) []uint32 {
// 	// TODO
// 	return nil
// }

// func (store *Store) FindKey(db, table uint64, key []byte) uint32 {
// 	// TODO
// 	return 0
// }

// func (store *Store) HasKey(db, table uint64, key []byte) bool {
// 	// TODO
// 	return false
// }
