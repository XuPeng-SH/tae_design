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

	err = db1.CommitStart(txn1.GetCommitTS())
	assert.Nil(t, err)

	assert.True(t, db1.HasStarted())

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
		t.Log(n.payload.(*DBEntry).String())
		cnt++
		return true
	}, true)
	assert.Equal(t, 2, cnt)

	txn4 := txnMgr.StartTxn(nil)

	e, err := catalog.GetDBEntry(db1.name, txn4)
	assert.NotNil(t, e)
	assert.Equal(t, db1, e)
	// t.Logf("%d:%d", txn4.GetStartTS(), txn4.GetCommitTS())
	t.Log(e.String())
}

// func TestCreateDB2(t *testing.T) {
// 	dir := initTestPath(t)
// 	catalog := MockCatalog(dir, "mock", nil)
// 	defer catalog.Close()

// 	txnMgr := txn.NewTxnManager()
// 	txnMgr.Start()
// 	defer txnMgr.Stop()
// }
