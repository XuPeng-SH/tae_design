package tables

import (
	"os"
	"path/filepath"
	"tae/pkg/catalog"
	"tae/pkg/dataio"
	"tae/pkg/iface/data"
	"tae/pkg/txn/txnbase"
	"tae/pkg/txn/txnimpl"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/mutation/buffer"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/mutation/buffer/base"
	"github.com/stretchr/testify/assert"
)

func initTestPath(t *testing.T) string {
	dir := filepath.Join("/tmp", t.Name())
	os.RemoveAll(dir)
	return dir
}

func initTestContext(t *testing.T, dir string, bufSize uint64) (*catalog.Catalog, *txnbase.TxnManager, txnbase.NodeDriver, base.INodeManager) {
	c := catalog.MockCatalog(dir, "mock", nil)
	driver := txnbase.NewNodeDriver(dir, "store", nil)
	mgr := txnbase.NewTxnManager(txnimpl.TxnStoreFactory(c, driver), txnimpl.TxnFactory(c))
	mgr.Start()
	bufMgr := buffer.NewNodeManager(bufSize, nil)
	return c, mgr, driver, bufMgr
}

func TestBlock1(t *testing.T) {
	dir := initTestPath(t)
	c, mgr, driver, bufMgr := initTestContext(t, dir, 100000)
	defer driver.Close()
	defer c.Close()
	defer mgr.Stop()
	txn := mgr.StartTxn(nil)
	db, _ := txn.CreateDatabase("db")
	schema := catalog.MockSchema(1)
	schema.BlockMaxRows = 1000
	rel, _ := db.CreateRelation(schema)
	tableMeta := rel.GetMeta().(*catalog.TableEntry)

	factory := dataio.SegmentFileMockFactory
	table := newTable(tableMeta, factory, bufMgr)
	_, _, err := table.GetAppender()
	assert.Equal(t, data.ErrAppendableSegmentNotFound, err)
	seg, _ := rel.CreateSegment()
	blk, _ := seg.CreateBlock()
	id := blk.GetMeta().(*catalog.BlockEntry).AsCommonID()
	appender, err := table.SetAppender(id)
	assert.Nil(t, err)
	assert.NotNil(t, appender)
	t.Log(bufMgr.String())

	toAppend, err := appender.PrepareAppend(schema.BlockMaxRows * 2)
	assert.Equal(t, schema.BlockMaxRows, toAppend)
	assert.Nil(t, err)
	t.Log(toAppend)
	assert.Nil(t, appender.ApplyAppend(nil, 0, toAppend, nil))
	appender.Close()
	t.Log(bufMgr.String())
}
