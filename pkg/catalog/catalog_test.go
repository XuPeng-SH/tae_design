package catalog

import (
	"fmt"
	"os"
	"path/filepath"
	"tae/pkg/iface/handle"
	"tae/pkg/iface/txnif"
	"tae/pkg/txn/txnbase"
	"testing"

	"github.com/stretchr/testify/assert"
)

type testTxnStore struct {
	txn txnif.TxnReader
	txnbase.NoopTxnStore
	catalog *Catalog
	entries map[txnif.TxnEntry]bool
}

func (store *testTxnStore) AddTxnEntry(entry txnif.TxnEntry) {
	store.entries[entry] = true
}

func (store *testTxnStore) BindTxn(txn txnif.AsyncTxn) {
	store.txn = txn
}

func (store *testTxnStore) PrepareCommit() error {
	for e, _ := range store.entries {
		if err := e.PrepareCommit(); err != nil {
			return err
		}
	}
	return nil
}

func (store *testTxnStore) Commit() error {
	for e, _ := range store.entries {
		if err := e.Commit(); err != nil {
			return err
		}
	}
	return nil
}

type testDatabaseHandle struct {
	*txnbase.TxnDatabase
	catalog *Catalog
	entry   *DBEntry
}

type testTableHandle struct {
	*txnbase.TxnRelation
	catalog *Catalog
	entry   *TableEntry
}

func newTestDBHandle(catalog *Catalog, txn txnif.AsyncTxn, entry *DBEntry) *testDatabaseHandle {
	return &testDatabaseHandle{
		TxnDatabase: &txnbase.TxnDatabase{
			Txn: txn,
		},
		catalog: catalog,
		entry:   entry,
	}
}

func newTestTableHandle(catalog *Catalog, txn txnif.AsyncTxn, entry *TableEntry) *testTableHandle {
	return &testTableHandle{
		TxnRelation: &txnbase.TxnRelation{
			Txn: txn,
		},
		catalog: catalog,
		entry:   entry,
	}
}

func (h *testDatabaseHandle) CreateRelation(def interface{}) (rel handle.Relation, err error) {
	schema := def.(*Schema)
	tbl, err := h.entry.CreateTableEntry(schema, h.Txn)
	if err != nil {
		return nil, err
	}
	rel = newTestTableHandle(h.catalog, h.Txn, tbl)
	return
}

func (h *testDatabaseHandle) String() string {
	return h.entry.String()
}

type testTxn struct {
	*txnbase.Txn
	catalog *Catalog
}

func (txn *testTxn) CreateDatabase(name string) (handle.Database, error) {
	entry, err := txn.catalog.CreateDBEntry(name, txn)
	if err != nil {
		return nil, err
	}
	txn.Store.AddTxnEntry(entry)
	h := newTestDBHandle(txn.catalog, txn, entry)
	return h, nil
}

func (txn *testTxn) GetDatabase(name string) (handle.Database, error) {
	entry, err := txn.catalog.GetDBEntry(name, txn)
	if err != nil {
		return nil, err
	}
	return newTestDBHandle(txn.catalog, txn, entry), nil
}

func (txn *testTxn) DropDatabase(name string) (handle.Database, error) {
	entry, err := txn.catalog.DropDBEntry(name, txn)
	if err != nil {
		return nil, err
	}
	return newTestDBHandle(txn.catalog, txn, entry), nil
}

func initTestPath(t *testing.T) string {
	dir := filepath.Join("/tmp", t.Name())
	os.RemoveAll(dir)
	return dir
}

func testTxnFactory(catalog *Catalog) txnbase.TxnFactory {
	return func(mgr *txnbase.TxnManager, store txnif.TxnStore, id uint64, ts uint64, info []byte) txnif.AsyncTxn {
		txn := new(testTxn)
		txn.Txn = txnbase.NewTxn(mgr, store, id, ts, info)
		txn.catalog = catalog
		return txn
	}
}

func testStoreFactory(catalog *Catalog) txnbase.TxnStoreFactory {
	return func() txnif.TxnStore {
		store := new(testTxnStore)
		store.catalog = catalog
		store.entries = make(map[txnif.TxnEntry]bool)
		return store
	}

}

func TestCreateDB1(t *testing.T) {
	dir := initTestPath(t)
	catalog := MockCatalog(dir, "mock", nil)
	defer catalog.Close()

	txnMgr := txnbase.NewTxnManager(testStoreFactory(catalog), testTxnFactory(catalog))
	txnMgr.Start()
	defer txnMgr.Stop()

	txn1 := txnMgr.StartTxn(nil)

	name := fmt.Sprintf("%s-%d", t.Name(), 1)
	db1, err := txn1.CreateDatabase(name)
	assert.Nil(t, err)
	t.Log(db1.String())

	assert.Equal(t, 1, len(catalog.entries))
	cnt := 0
	catalog.link.Loop(func(n *DLNode) bool {
		t.Log(n.payload.(*DBEntry).GetID())
		cnt++
		return true
	}, true)
	assert.Equal(t, 1, cnt)

	_, err = txn1.CreateDatabase(name)
	assert.Equal(t, ErrDuplicate, err)

	txn2 := txnMgr.StartTxn(nil)

	_, err = txn2.CreateDatabase(name)
	assert.Equal(t, txnif.TxnWWConflictErr, err)

	_, err = txn1.GetDatabase(name)
	assert.Nil(t, err)

	txn1.Commit()

	err = db1.(*testDatabaseHandle).entry.PrepareCommit()
	assert.Nil(t, err)

	assert.True(t, db1.(*testDatabaseHandle).entry.HasCreated())
	assert.True(t, db1.(*testDatabaseHandle).entry.IsCommitting())

	err = db1.(*testDatabaseHandle).entry.Commit()
	assert.Nil(t, err)
	assert.False(t, db1.(*testDatabaseHandle).entry.IsCommitting())

	_, err = txn2.CreateDatabase(name)
	assert.Equal(t, ErrDuplicate, err)

	_, err = txn2.DropDatabase(name)
	assert.Equal(t, ErrNotFound, err)

	txn3 := txnMgr.StartTxn(nil)
	_, err = txn3.DropDatabase(name)
	assert.Nil(t, err)
	assert.True(t, db1.(*testDatabaseHandle).entry.IsDroppedUncommitted())

	_, err = txn3.CreateDatabase(name)
	assert.Nil(t, err)

	cnt = 0
	catalog.link.Loop(func(n *DLNode) bool {
		// t.Log(n.payload.(*DBEntry).String())
		cnt++
		return true
	}, true)
	assert.Equal(t, 2, cnt)

	txn4 := txnMgr.StartTxn(nil)

	h, err := txn4.GetDatabase(name)
	assert.NotNil(t, h)
	assert.Equal(t, db1.(*testDatabaseHandle).entry, h.(*testDatabaseHandle).entry)
}

//
// TXN1-S     TXN2-S      TXN1-C  TXN3-S TXN4-S  TXN3-C TXN5-S
//  |            |           |      |      |       |      |                                Time
// -+-+---+---+--+--+----+---+--+---+-+----+-+-----+------+-+------------------------------------>
//    |   |   |     |    |      |     |      |              |
//    |   |   |     |    |      |     |      |            [TXN5]: GET TBL [NOTFOUND]
//    |   |   |     |    |      |     |    [TXN4]: GET TBL [OK] | DROP DB1-TB1 [W-W]
//    |   |   |     |    |      |   [TXN3]: GET TBL [OK] | DROP DB1-TB1 [OK] | GET TBL [NOT FOUND]
//    |   |   |     |    |    [TXN2]: DROP DB [NOTFOUND]
//    |   |   |     |  [TXN2]: DROP DB [NOTFOUND]
//    |   |   |   [TXN2]:  GET DB [NOTFOUND] | CREATE DB [W-W]
//    |   | [TXN1]: CREATE DB1-TB1 [DUP]
//    | [TXN1]: CREATE DB1-TB1 [OK] | GET TBL [OK]
//  [TXN1]: CREATE DB1 [OK] | GET DB [OK]
func TestTableEntry1(t *testing.T) {
	dir := initTestPath(t)
	catalog := MockCatalog(dir, "mock", nil)
	defer catalog.Close()

	factory := func() txnif.TxnStore {
		store := new(testTxnStore)
		store.catalog = catalog
		store.entries = make(map[txnif.TxnEntry]bool)
		return store
	}
	txnMgr := txnbase.NewTxnManager(factory, nil)
	txnMgr.Start()
	defer txnMgr.Stop()

	txn1 := txnMgr.StartTxn(nil)
	db1, err := catalog.CreateDBEntry("db1", txn1)
	assert.Nil(t, err)
	txn1.GetStore().AddTxnEntry(db1)
	t.Log(db1.String())

	schema := MockSchema(2)
	schema.Name = "tb1"
	tb1, err := db1.CreateTableEntry(schema, txn1)
	assert.Nil(t, err)
	t.Log(tb1)
	txn1.GetStore().AddTxnEntry(tb1)

	_, err = db1.GetTableEntry(schema.Name, txn1)
	assert.Nil(t, err)

	_, err = db1.CreateTableEntry(schema, txn1)
	assert.Equal(t, ErrDuplicate, err)

	txn2 := txnMgr.StartTxn(nil)
	_, err = catalog.GetDBEntry("db1", txn2)
	assert.Equal(t, err, ErrNotFound)

	_, err = catalog.CreateDBEntry("db1", txn2)
	assert.Equal(t, err, txnif.TxnWWConflictErr)

	_, err = catalog.DropDBEntry("db1", txn2)
	assert.Equal(t, err, ErrNotFound)

	err = txn1.Commit()
	assert.Nil(t, err)

	err = txn1.GetStore().PrepareCommit()
	assert.Nil(t, err)
	err = txn1.GetStore().Commit()
	assert.Nil(t, err)
	// err = db1.PrepareCommit()
	// assert.Nil(t, err)
	// err = tb1.PrepareCommit()
	// assert.Nil(t, err)
	// t.Log(db1.String())
	// t.Log(tb1.String())

	// err = db1.Commit()
	// err = tb1.Commit()

	_, err = catalog.DropDBEntry("db1", txn2)
	assert.Equal(t, err, ErrNotFound)

	txn3 := txnMgr.StartTxn(nil)
	db, err := catalog.GetDBEntry("db1", txn3)
	assert.Nil(t, err)

	_, err = db.DropTableEntry(schema.Name, txn3)
	assert.Nil(t, err)
	t.Log(tb1.String())

	_, err = db.GetTableEntry(schema.Name, txn3)
	assert.Equal(t, ErrNotFound, err)

	txn4 := txnMgr.StartTxn(nil)
	db, err = catalog.GetDBEntry("db1", txn4)
	assert.Nil(t, err)
	_, err = db.GetTableEntry(schema.Name, txn4)
	assert.Nil(t, err)

	_, err = db.DropTableEntry(schema.Name, txn4)
	assert.Equal(t, txnif.TxnWWConflictErr, err)

	err = txn3.Commit()
	assert.Nil(t, err)

	err = tb1.PrepareCommit()
	assert.Nil(t, err)
	t.Log(tb1.String())

	txn5 := txnMgr.StartTxn(nil)
	db, err = catalog.GetDBEntry("db1", txn5)
	assert.Nil(t, err)
	_, err = db.GetTableEntry(schema.Name, txn5)
	assert.Equal(t, ErrNotFound, err)
}
