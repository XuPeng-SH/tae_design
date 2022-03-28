package catalog

import (
	"fmt"
	"os"
	"path/filepath"
	"tae/pkg/txn"
	"testing"

	"github.com/stretchr/testify/assert"
)

func initTestPath(t *testing.T) string {
	dir := filepath.Join("/tmp", t.Name())
	os.RemoveAll(dir)
	return dir
}

func TestCreateDB1(t *testing.T) {
	dir := initTestPath(t)
	catalog := MockCatalog(dir, "mock", nil)
	defer catalog.Close()

	txnMgr := txn.NewTxnManager()
	txnMgr.Start()
	defer txnMgr.Stop()

	txn1 := txnMgr.StartTxn(nil)
	db1 := NewDBEntry(catalog, fmt.Sprintf("%s-%d", t.Name(), 1), txn1)

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
	assert.Equal(t, txn.TxnWWConflictErr, err)

	_, err = catalog.GetDBEntry(db1.name, txn1)
	assert.Nil(t, err)

	txn1.Commit()

	// err = db1.CommitStart(txn1.GetCommitTS())
	// assert.Nil(t, err)
	err = db1.PrepareCommitLocked()
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
	// e.PrepareCommitLocked()
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

	txnMgr := txn.NewTxnManager()
	txnMgr.Start()
	defer txnMgr.Stop()

	txn1 := txnMgr.StartTxn(nil)
	db1, err := catalog.CreateDBEntry("db1", txn1)
	assert.Nil(t, err)
	t.Log(db1.String())

	schema := MockSchema(2)
	schema.Name = "tb1"
	tb1, err := db1.CreateTableEntry(schema, txn1)
	assert.Nil(t, err)
	t.Log(tb1)

	_, err = db1.GetTableEntry(schema.Name, txn1)
	assert.Nil(t, err)

	_, err = db1.CreateTableEntry(schema, txn1)
	assert.Equal(t, ErrDuplicate, err)

	txn2 := txnMgr.StartTxn(nil)
	_, err = catalog.GetDBEntry("db1", txn2)
	assert.Equal(t, err, ErrNotFound)

	_, err = catalog.CreateDBEntry("db1", txn2)
	assert.Equal(t, err, txn.TxnWWConflictErr)

	_, err = catalog.DropDBEntry("db1", txn2)
	assert.Equal(t, err, ErrNotFound)

	err = txn1.Commit()
	assert.Nil(t, err)

	err = db1.PrepareCommitLocked()
	assert.Nil(t, err)
	err = tb1.PrepareCommitLocked()
	assert.Nil(t, err)
	t.Log(db1.String())
	t.Log(tb1.String())

	err = db1.Commit()
	err = tb1.Commit()

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
	assert.Equal(t, txn.TxnWWConflictErr, err)

	err = txn3.Commit()
	assert.Nil(t, err)

	err = tb1.PrepareCommitLocked()
	assert.Nil(t, err)
	t.Log(tb1.String())

	txn5 := txnMgr.StartTxn(nil)
	db, err = catalog.GetDBEntry("db1", txn5)
	assert.Nil(t, err)
	_, err = db.GetTableEntry(schema.Name, txn5)
	assert.Equal(t, ErrNotFound, err)
}
