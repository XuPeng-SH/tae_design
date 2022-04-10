package tables

import (
	"os"
	"path/filepath"
	"tae/pkg/catalog"
	"tae/pkg/common"
	com "tae/pkg/common"
	"tae/pkg/dataio"
	"tae/pkg/iface/data"
	"tae/pkg/tables"
	"tae/pkg/txn/txnbase"
	"tae/pkg/txn/txnimpl"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/mock"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/mutation/buffer"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/mutation/buffer/base"
	"github.com/stretchr/testify/assert"
)

func initTestPath(t *testing.T) string {
	dir := filepath.Join("/tmp", t.Name())
	os.RemoveAll(dir)
	return dir
}

func initTestContext(t *testing.T, dir string, txnBufSize, mutBufSize uint64) (*catalog.Catalog, *txnbase.TxnManager, txnbase.NodeDriver, base.INodeManager, base.INodeManager) {
	c := catalog.MockCatalog(dir, "mock", nil)
	driver := txnbase.NewNodeDriver(dir, "store", nil)
	txnBufMgr := buffer.NewNodeManager(txnBufSize, nil)
	mutBufMgr := buffer.NewNodeManager(mutBufSize, nil)
	factory := tables.NewDataFactory(dataio.SegmentFileMockFactory, mutBufMgr)
	mgr := txnbase.NewTxnManager(txnimpl.TxnStoreFactory(c, driver, txnBufMgr, factory), txnimpl.TxnFactory(c))
	mgr.Start()
	return c, mgr, driver, txnBufMgr, mutBufMgr
}

func TestTables1(t *testing.T) {
	dir := initTestPath(t)
	c, mgr, driver, txnBufMgr, mutBufMgr := initTestContext(t, dir, 100000, 1000000)
	defer driver.Close()
	defer c.Close()
	defer mgr.Stop()
	txn := mgr.StartTxn(nil)
	db, _ := txn.CreateDatabase("db")
	schema := catalog.MockSchema(1)
	schema.BlockMaxRows = 1000
	schema.SegmentMaxBlocks = 2
	rel, _ := db.CreateRelation(schema)
	tableMeta := rel.GetMeta().(*catalog.TableEntry)

	dataFactory := tables.NewDataFactory(dataio.SegmentFileMockFactory, txnBufMgr)
	tableFactory := dataFactory.MakeTableFactory()
	table := tableFactory(tableMeta)
	_, _, err := table.GetAppender()
	assert.Equal(t, data.ErrAppendableSegmentNotFound, err)
	seg, _ := rel.CreateSegment()
	blk, _ := seg.CreateBlock()
	id := blk.GetMeta().(*catalog.BlockEntry).AsCommonID()
	appender, err := table.SetAppender(id)
	assert.Nil(t, err)
	assert.NotNil(t, appender)
	t.Log(txnBufMgr.String())

	blkCnt := 3
	rows := schema.BlockMaxRows * uint32(blkCnt)
	toAppend, err := appender.PrepareAppend(rows)
	assert.Equal(t, schema.BlockMaxRows, toAppend)
	assert.Nil(t, err)
	t.Log(toAppend)
	assert.Nil(t, appender.ApplyAppend(nil, 0, toAppend, nil))
	assert.True(t, table.HasAppendableSegment())

	toAppend, err = appender.PrepareAppend(rows - toAppend)
	assert.Equal(t, uint32(0), toAppend)
	appender.Close()

	_, _, err = table.GetAppender()
	assert.Equal(t, data.ErrAppendableBlockNotFound, err)
	blk, _ = seg.CreateBlock()
	id = blk.GetMeta().(*catalog.BlockEntry).AsCommonID()
	appender, err = table.SetAppender(id)
	assert.Nil(t, err)

	toAppend, err = appender.PrepareAppend(rows - toAppend)
	assert.Equal(t, schema.BlockMaxRows, toAppend)
	assert.Nil(t, appender.ApplyAppend(nil, toAppend, toAppend, nil))
	assert.False(t, table.HasAppendableSegment())

	_, _, err = table.GetAppender()
	assert.Equal(t, data.ErrAppendableSegmentNotFound, err)

	seg, _ = rel.CreateSegment()
	blk, _ = seg.CreateBlock()
	id = blk.GetMeta().(*catalog.BlockEntry).AsCommonID()
	appender, err = table.SetAppender(id)
	assert.Nil(t, err)
	toAppend, err = appender.PrepareAppend(rows - toAppend*2)
	assert.Equal(t, schema.BlockMaxRows, toAppend)
	assert.Nil(t, appender.ApplyAppend(nil, toAppend*2, toAppend, nil))
	assert.True(t, table.HasAppendableSegment())

	t.Log(txnBufMgr.String())
	t.Log(mutBufMgr.String())
	t.Log(c.SimplePPString(com.PPL1))
}

func TestTxn1(t *testing.T) {
	dir := initTestPath(t)
	c, mgr, driver, txnBufMgr, mutBufMgr := initTestContext(t, dir, common.K*80, common.G)
	defer driver.Close()
	defer c.Close()
	defer mgr.Stop()

	schema := catalog.MockSchema(1)
	schema.BlockMaxRows = 10000
	schema.SegmentMaxBlocks = 4
	txn := mgr.StartTxn(nil)
	db, _ := txn.CreateDatabase("db")
	rel, _ := db.CreateRelation(schema)
	bat := mock.MockBatch(schema.Types(), 4000)
	for i := 0; i < 20; i++ {
		err := rel.Append(bat)
		assert.Nil(t, err)
	}

	t.Log(txnBufMgr.String())
	t.Log(mutBufMgr.String())
	now := time.Now()
	err := txn.Commit()
	assert.Nil(t, err)
	t.Logf("Commit takes: %s", time.Since(now))
	t.Log(txnBufMgr.String())
	t.Log(mutBufMgr.String())
}
