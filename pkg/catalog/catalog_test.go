package catalog

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"tae/pkg/iface/handle"
	"tae/pkg/iface/txnif"
	"tae/pkg/txn/txnbase"
	"testing"
	"time"

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
		err := e.PrepareCommit()
		if err != nil {
			return err
		}
	}
	return nil
}

func (store *testTxnStore) Commit() error {
	for e, _ := range store.entries {
		err := e.Commit()
		if err != nil {
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
	h.Txn.GetStore().AddTxnEntry(tbl)
	rel = newTestTableHandle(h.catalog, h.Txn, tbl)
	return
}

func (h *testDatabaseHandle) DropRelationByName(name string) (rel handle.Relation, err error) {
	entry, err := h.entry.DropTableEntry(name, h.Txn)
	if err != nil {
		return nil, err
	}
	h.Txn.GetStore().AddTxnEntry(entry)
	rel = newTestTableHandle(h.catalog, h.Txn, entry)
	return
}

func (h *testDatabaseHandle) String() string {
	return h.entry.String()
}

func (h *testDatabaseHandle) GetRelationByName(name string) (rel handle.Relation, err error) {
	entry, err := h.entry.GetTableEntry(name, h.Txn)
	if err != nil {
		return nil, err
	}
	return newTestTableHandle(h.catalog, h.Txn, entry), nil
}

func (h *testTableHandle) String() string {
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
	txn.Store.AddTxnEntry(entry)
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

	txnMgr := txnbase.NewTxnManager(testStoreFactory(catalog), testTxnFactory(catalog))
	txnMgr.Start()
	defer txnMgr.Stop()

	txn1 := txnMgr.StartTxn(nil)
	name := "db1"
	db1, err := txn1.CreateDatabase(name)
	assert.Nil(t, err)
	t.Log(db1.String())

	schema := MockSchema(2)
	schema.Name = "tb1"
	tb1, err := db1.CreateRelation(schema)
	assert.Nil(t, err)
	t.Log(tb1.String())

	_, err = db1.GetRelationByName(schema.Name)
	assert.Nil(t, err)

	_, err = db1.CreateRelation(schema)
	assert.Equal(t, ErrDuplicate, err)

	txn2 := txnMgr.StartTxn(nil)
	_, err = txn2.GetDatabase(schema.Name)
	assert.Equal(t, err, ErrNotFound)

	_, err = txn2.CreateDatabase(name)
	assert.Equal(t, err, txnif.TxnWWConflictErr)

	_, err = txn2.DropDatabase(name)
	assert.Equal(t, err, ErrNotFound)

	err = txn1.Commit()
	assert.Nil(t, err)

	_, err = txn2.DropDatabase(name)
	assert.Equal(t, err, ErrNotFound)

	txn3 := txnMgr.StartTxn(nil)
	db, err := txn3.GetDatabase(name)
	assert.Nil(t, err)

	_, err = db.DropRelationByName(schema.Name)
	assert.Nil(t, err)
	t.Log(tb1.String())

	_, err = db.GetRelationByName(schema.Name)
	assert.Equal(t, ErrNotFound, err)

	txn4 := txnMgr.StartTxn(nil)
	db, err = txn4.GetDatabase(name)
	assert.Nil(t, err)
	_, err = db.GetRelationByName(schema.Name)
	assert.Nil(t, err)

	_, err = db.DropRelationByName(schema.Name)
	assert.Equal(t, txnif.TxnWWConflictErr, err)

	err = txn3.Commit()
	assert.Nil(t, err)

	t.Log(tb1.String())

	txn5 := txnMgr.StartTxn(nil)
	db, err = txn5.GetDatabase(name)
	assert.Nil(t, err)
	_, err = db.GetRelationByName(schema.Name)
	assert.Equal(t, ErrNotFound, err)
}

func TestTableEntry2(t *testing.T) {
	dir := initTestPath(t)
	catalog := MockCatalog(dir, "mock", nil)
	defer catalog.Close()

	txnMgr := txnbase.NewTxnManager(testStoreFactory(catalog), testTxnFactory(catalog))
	txnMgr.Start()
	defer txnMgr.Stop()

	txn1 := txnMgr.StartTxn(nil)
	name := "db1"
	db, err := txn1.CreateDatabase(name)
	assert.Nil(t, err)
	schema := MockSchema(2)
	schema.Name = "tb1"
	_, err = db.CreateRelation(schema)
	assert.Nil(t, err)

	for i := 0; i < 1000; i++ {
		s := MockSchema(1)
		s.Name = fmt.Sprintf("xx%d", i)
		_, err = db.CreateRelation(s)
		assert.Nil(t, err)
	}
	err = txn1.Commit()
	assert.Nil(t, err)

	txn2 := txnMgr.StartTxn(nil)
	db, err = txn2.GetDatabase(name)
	assert.Nil(t, err)
	rel, err := db.DropRelationByName(schema.Name)
	assert.Nil(t, err)
	t.Log(rel.String())
	db, err = txn2.DropDatabase(name)
	assert.Nil(t, err)
	t.Log(db.String())

	var wg sync.WaitGroup
	txns := []txnif.AsyncTxn{txn2}
	for i := 0; i < 10; i++ {
		txn := txnMgr.StartTxn(nil)
		txns = append(txns, txn)
	}
	now := time.Now()
	for _, txn := range txns {
		wg.Add(1)
		go func(ttxn txnif.AsyncTxn) {
			defer wg.Done()
			for i := 0; i < 1000; i++ {
				database, err := ttxn.GetDatabase(name)
				if err != nil {
					// t.Logf("db-ttxn=%d, %s", ttxn.GetID(), err)
				} else {
					// t.Logf("db-ttxn=%d, %v", ttxn.GetID(), err)
					_, err := database.GetRelationByName(schema.Name)
					if err != nil {
						// t.Logf("rel-ttxn=%d, %s", ttxn.GetID(), err)
					}
				}
			}
		}(txn)
	}
	wg.Wait()
	t.Log(time.Since(now))
}

func TestTableEntry3(t *testing.T) {
	dir := initTestPath(t)
	catalog := MockCatalog(dir, "mock", nil)
	defer catalog.Close()

	txnMgr := txnbase.NewTxnManager(testStoreFactory(catalog), testTxnFactory(catalog))
	txnMgr.Start()
	defer txnMgr.Stop()
	name := "db1"
	var wg sync.WaitGroup
	flow := func() {
		defer wg.Done()
		txn := txnMgr.StartTxn(nil)
		_, err := txn.GetDatabase(name)
		if err == ErrNotFound {
			_, err = txn.CreateDatabase(name)
			if err != nil {
				return
			}
		} else {
			_, err = txn.DropDatabase(name)
			if err != nil {
				return
			}
		}
		err = txn.Commit()
		assert.Nil(t, err)
	}

	for i := 0; i < 10000; i++ {
		wg.Add(1)
		go flow()
	}
	wg.Wait()
}
