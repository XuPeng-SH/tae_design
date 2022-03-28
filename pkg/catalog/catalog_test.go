package catalog

import (
	"fmt"
	"os"
	"path/filepath"
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

// func (store *testTxnStore) CreateDBEntry(name string) error {
// 	entry, err := store.catalog.CreateDBEntry(name, store.txn)
// 	if err != nil {
// 		return err
// 	}
// 	store.entries[entry.BaseEntry2] = true
// 	return nil
// }

// func (store *testTxnStore) CreateTableEntry(dbName string, def interface{}) error {
// 	schema := def.(*Schema)
// 	db, err := store.catalog.GetDBEntry(dbName, store.txn)
// 	if err != nil {
// 		return err
// 	}
// 	entry, err := db.CreateTableEntry(schema, store.txn)
// 	if err != nil {
// 		return err
// 	}
// 	store.entries[entry.BaseEntry2] = true
// 	return nil
// }

// func (store *testTxnStore) DropDBEntry(name string) error {
// 	entry, err := store.catalog.DropDBEntry(name, store.txn)
// 	if err != nil {
// 		return err
// 	}

// 	store.entries[entry.BaseEntry2] = true
// 	return nil
// }

// func (store *testTxnStore) DropTableEntry(dbName, name string) error {
// 	db, err := store.catalog.GetDBEntry(dbName, store.txn)
// 	if err != nil {
// 		return err
// 	}
// 	entry, err := db.DropTableEntry(name, store.txn)
// 	if err != nil {
// 		return err
// 	}

// 	store.entries[entry.BaseEntry2] = true
// 	return nil
// }

func initTestPath(t *testing.T) string {
	dir := filepath.Join("/tmp", t.Name())
	os.RemoveAll(dir)
	return dir
}

func TestCreateDB1(t *testing.T) {
	dir := initTestPath(t)
	catalog := MockCatalog(dir, "mock", nil)
	defer catalog.Close()

	factory := func() txnif.TxnStore {
		store := new(testTxnStore)
		store.catalog = catalog
		store.entries = make(map[txnif.TxnEntry]bool)
		return store
	}
	txnMgr := txnbase.NewTxnManager(factory)
	txnMgr.Start()
	defer txnMgr.Stop()

	txn1 := txnMgr.StartTxn(nil)

	name := fmt.Sprintf("%s-%d", t.Name(), 1)
	db1, err := catalog.CreateDBEntry(name, txn1)
	assert.Nil(t, err)

	assert.Equal(t, 1, len(catalog.entries))
	cnt := 0
	catalog.link.Loop(func(n *DLNode) bool {
		t.Log(n.payload.(*DBEntry).GetID())
		cnt++
		return true
	}, true)
	assert.Equal(t, 1, cnt)

	_, err = catalog.CreateDBEntry(name, txn1)
	assert.Equal(t, ErrDuplicate, err)

	txn2 := txnMgr.StartTxn(nil)

	_, err = catalog.CreateDBEntry(name, txn2)
	assert.Equal(t, txnif.TxnWWConflictErr, err)

	_, err = catalog.GetDBEntry(db1.name, txn1)
	assert.Nil(t, err)

	txn1.Commit()

	// err = db1.CommitStart(txn1.GetCommitTS())
	// assert.Nil(t, err)
	err = db1.PrepareCommit()
	assert.Nil(t, err)

	assert.True(t, db1.HasCreated())
	assert.True(t, db1.IsCommitting())

	err = db1.Commit()
	assert.Nil(t, err)
	assert.False(t, db1.IsCommitting())

	_, err = catalog.CreateDBEntry(name, txn2)
	assert.Equal(t, ErrDuplicate, err)

	_, err = catalog.DropDBEntry(name, txn2)
	assert.Equal(t, ErrNotFound, err)

	txn3 := txnMgr.StartTxn(nil)
	_, err = catalog.DropDBEntry(name, txn3)
	assert.Nil(t, err)
	assert.True(t, db1.IsDroppedUncommitted())

	_, err = catalog.CreateDBEntry(name, txn3)
	assert.Nil(t, err)

	// assert.Equal(t, 0, len(catalog.entries))
	cnt = 0
	catalog.link.Loop(func(n *DLNode) bool {
		// t.Log(n.payload.(*DBEntry).String())
		cnt++
		return true
	}, true)
	assert.Equal(t, 2, cnt)

	txn4 := txnMgr.StartTxn(nil)

	e, err := catalog.GetDBEntry(db1.name, txn4)
	assert.NotNil(t, e)
	assert.Equal(t, db1, e)

	// txn3.Commit()
	// e.PrepareCommit()
	// t.Logf("%d:%d", txn4.GetStartTS(), txn4.GetCommitTS())
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
	txnMgr := txnbase.NewTxnManager(factory)
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
