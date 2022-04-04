package txnimpl

import (
	"tae/pkg/catalog"
	"tae/pkg/iface/handle"
	"tae/pkg/iface/txnif"
	"tae/pkg/txn/txnbase"
	"time"

	"github.com/jiangxinmeng1/logstore/pkg/entry"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/mutation/buffer/base"
	"github.com/sirupsen/logrus"
)

type txnStore struct {
	txnbase.NoopTxnStore
	tables      map[uint64]Table
	driver      txnbase.NodeDriver
	nodesMgr    base.INodeManager
	dbIndex     map[string]uint64
	tableIndex  map[string]uint64
	txn         txnif.AsyncTxn
	catalog     *catalog.Catalog
	database    handle.Database
	createEntry txnif.TxnEntry
	dropEntry   txnif.TxnEntry
	cmdMgr      *commandManager
	logs        []entry.Entry
	warChecker  *warChecker
}

var TxnStoreFactory = func(catalog *catalog.Catalog, driver txnbase.NodeDriver) txnbase.TxnStoreFactory {
	return func() txnif.TxnStore {
		return newStore(catalog, driver)
	}
}

func newStore(catalog *catalog.Catalog, driver txnbase.NodeDriver) *txnStore {
	return &txnStore{
		tables:  make(map[uint64]Table),
		catalog: catalog,
		cmdMgr:  newCommandManager(driver),
		driver:  driver,
		logs:    make([]entry.Entry, 0),
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

func (store *txnStore) UseDatabase(name string) (err error) {
	if err = store.checkDatabase(name); err != nil {
		return
	}
	store.database, err = store.txn.GetDatabase(name)
	return err
}

func (store *txnStore) checkDatabase(name string) (err error) {
	if store.database != nil {
		if store.database.GetName() != name {
			return txnbase.ErrTxnDifferentDatabase
		}
	}
	return
}

func (store *txnStore) GetDatabase(name string) (db handle.Database, err error) {
	if err = store.checkDatabase(name); err != nil {
		return
	}
	meta, err := store.catalog.GetDBEntry(name, store.txn)
	if err != nil {
		return
	}
	db = newDatabase(store.txn, meta)
	store.database = db
	return
}

func (store *txnStore) CreateDatabase(name string) (handle.Database, error) {
	if store.database != nil {
		return nil, txnbase.ErrTxnDifferentDatabase
	}
	meta, err := store.catalog.CreateDBEntry(name, store.txn)
	if err != nil {
		return nil, err
	}
	store.createEntry = meta
	store.database = newDatabase(store.txn, meta)
	return store.database, nil
}

func (store *txnStore) DropDatabase(name string) (db handle.Database, err error) {
	if err = store.checkDatabase(name); err != nil {
		return
	}
	meta, err := store.catalog.DropDBEntry(name, store.txn)
	if err != nil {
		return
	}
	store.dropEntry = meta
	store.database = newDatabase(store.txn, meta)
	return store.database, err
}

func (store *txnStore) CreateRelation(def interface{}) (relation handle.Relation, err error) {
	schema := def.(*catalog.Schema)
	db := store.database.GetMeta().(*catalog.DBEntry)
	meta, err := db.CreateTableEntry(schema, store.txn)
	if err != nil {
		return
	}
	table, err := store.getOrSetTable(meta.GetID())
	if err != nil {
		return
	}
	relation = newRelation(store.txn, meta)
	table.SetCreateEntry(meta)
	return
}

func (store *txnStore) DropRelationByName(name string) (relation handle.Relation, err error) {
	db := store.database.GetMeta().(*catalog.DBEntry)
	meta, err := db.DropTableEntry(name, store.txn)
	if err != nil {
		return nil, err
	}
	table, err := store.getOrSetTable(meta.GetID())
	if err != nil {
		return nil, err
	}
	relation = newRelation(store.txn, meta)
	table.SetDropEntry(meta)
	return
}

func (store *txnStore) GetRelationByName(name string) (relation handle.Relation, err error) {
	db := store.database.GetMeta().(*catalog.DBEntry)
	meta, err := db.GetTableEntry(name, store.txn)
	if err != nil {
		return
	}
	relation = newRelation(store.txn, meta)
	return
}

func (store *txnStore) CreateSegment(tid uint64) (seg handle.Segment, err error) {
	var table Table
	if table, err = store.getOrSetTable(tid); err != nil {
		return
	}
	return table.CreateSegment()
}

func (store *txnStore) getOrSetTable(id uint64) (table Table, err error) {
	table = store.tables[id]
	if table == nil {
		var entry *catalog.TableEntry
		if entry, err = store.database.GetMeta().(*catalog.DBEntry).GetTableEntryByID(id); err != nil {
			return
		}
		relation := newRelation(store.txn, entry)
		if store.warChecker == nil {
			store.warChecker = newWarChecker(store.txn, entry.GetDB())
		}
		table = newTxnTable(store.txn, relation, store.driver, store.nodesMgr, store.warChecker)
		store.tables[id] = table
	}
	return
}

func (store *txnStore) CreateBlock(tid, sid uint64) (blk handle.Block, err error) {
	var table Table
	if table, err = store.getOrSetTable(tid); err != nil {
		return
	}
	return table.CreateBlock(sid)
}

func (store *txnStore) ApplyRollback() (err error) {
	entry := store.createEntry
	if entry == nil {
		entry = store.dropEntry
	}
	if entry != nil {
		if err = entry.ApplyRollback(); err != nil {
			return
		}
	}
	for _, table := range store.tables {
		if err = table.ApplyRollback(); err != nil {
			break
		}
	}
	return
}

func (store *txnStore) ApplyCommit() (err error) {
	now := time.Now()
	for _, e := range store.logs {
		e.WaitDone()
		e.Free()
	}
	if store.createEntry != nil {
		if err = store.createEntry.ApplyCommit(); err != nil {
			return
		}
	}
	for _, table := range store.tables {
		if err = table.ApplyCommit(); err != nil {
			break
		}
	}
	logrus.Debugf("Txn-%d ApplyCommit Takes %s", store.txn.GetID(), time.Since(now))
	return
}

func (store *txnStore) PrepareCommit() (err error) {
	now := time.Now()
	if store.warChecker != nil {
		if err = store.warChecker.check(); err != nil {
			return err
		}
	}
	if store.createEntry != nil {
		if err = store.createEntry.PrepareCommit(); err != nil {
			return
		}
	}
	for _, table := range store.tables {
		if err = table.PrepareCommit(); err != nil {
			break
		}
	}
	if store.dropEntry != nil {
		if err = store.dropEntry.PrepareCommit(); err != nil {
			return
		}
	}
	// TODO: prepare commit inserts and updates

	if store.createEntry != nil {
		csn := store.cmdMgr.GetCSN()
		cmd, err := store.createEntry.MakeCommand(uint32(csn))
		if err != nil {
			panic(err)
		}
		store.cmdMgr.AddCmd(cmd)
	} else if store.dropEntry != nil {
		csn := store.cmdMgr.GetCSN()
		cmd, err := store.dropEntry.MakeCommand(uint32(csn))
		if err != nil {
			panic(err)
		}
		store.cmdMgr.AddCmd(cmd)
	}
	for _, table := range store.tables {
		if err = table.CollectCmd(store.cmdMgr); err != nil {
			panic(err)
		}
	}

	logEntry, err := store.cmdMgr.ApplyTxnRecord()
	if err != nil {
		panic(err)
	}
	if logEntry != nil {
		store.logs = append(store.logs, logEntry)
	}
	logrus.Debugf("Txn-%d PrepareCommit Takes %s", store.txn.GetID(), time.Since(now))

	return
}

func (store *txnStore) AddTxnEntry(t txnif.TxnEntryType, entry txnif.TxnEntry) {
	// switch t {
	// case TxnEntryCreateDatabase:
	// 	store.createEntry = entry
	// case TxnEntryCretaeTable:
	// 	create := entry.(*catalog.TableEntry)

	// }
}

func (store *txnStore) PrepareRollback() error {
	var err error
	if store.createEntry != nil {
		if err := store.catalog.RemoveEntry(store.createEntry.(*catalog.DBEntry)); err != nil {
			return err
		}
	} else if store.dropEntry != nil {
		if err := store.createEntry.(*catalog.DBEntry).PrepareRollback(); err != nil {
			return err
		}
	}

	for _, table := range store.tables {
		if err = table.PrepareRollback(); err != nil {
			break
		}
	}
	return err
}

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
