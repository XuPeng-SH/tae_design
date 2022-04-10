package tables

import (
	"os"
	"path/filepath"
	"tae/pkg/catalog"
	com "tae/pkg/common"
	"tae/pkg/dataio"
	"tae/pkg/iface/data"
	"tae/pkg/iface/txnif"
	"tae/pkg/txn/txnbase"
	"tae/pkg/txn/txnimpl"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/common"
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
	c := catalog.MockCatalog(dir, "mock", nil, nil)
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
	schema.SegmentMaxBlocks = 2
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

	t.Log(bufMgr.String())
	t.Log(c.SimplePPString(com.PPL1))
}

func TestInsertInfo(t *testing.T) {
	ts := common.NextGlobalSeqNum()
	capacity := uint32(10000)
	info := newInsertInfo(nil, ts, capacity)
	cnt := int(capacity) - 1
	now := time.Now()
	txns := make([]txnif.TxnReader, 0)
	for i := 0; i < cnt; i++ {
		txn := newMockTxn()
		txn.TxnCtx.CommitTS = common.NextGlobalSeqNum()
		txn.TxnCtx.State = txnif.TxnStateCommitted
		info.RecordTxnLocked(uint32(i), txn, nil)
		txns = append(txns, txn)
	}
	t.Logf("Record takes %s", time.Since(now))
	{
		txn := newMockTxn()
		txn.TxnCtx.CommitTS = common.NextGlobalSeqNum()
		txn.TxnCtx.State = txnif.TxnStateCommitted
		info.RecordTxnLocked(uint32(cnt), txn, nil)
		txns = append(txns, txn)
	}
	now = time.Now()

	t.Logf("Record takes %s", time.Since(now))
	// tsCol, _ := info.ts.CopyToVector()
	// t.Log(tsCol.String())
	now = time.Now()
	for _, txn := range txns {
		info.ApplyCommitLocked(txn)
	}

	t.Logf("Commit takes %s", time.Since(now))
	now = time.Now()
	offset := info.GetVisibleOffsetLocked(txns[0].GetStartTS())
	t.Logf("GetVisibleOffset takes %s", time.Since(now))
	assert.Equal(t, -1, offset)
	offset = info.GetVisibleOffsetLocked(txns[len(txns)-1].GetCommitTS())
	assert.Equal(t, int(capacity-1), offset)
}
