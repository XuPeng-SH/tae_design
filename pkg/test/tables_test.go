package tables

import (
	"os"
	"path/filepath"
	"sync"
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
	"github.com/panjf2000/ants/v2"
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
	bat := mock.MockBatch(schema.Types(), uint64(rows))
	_, err = appender.ApplyAppend(bat, 0, toAppend, nil)
	assert.Nil(t, err)
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
	_, err = appender.ApplyAppend(bat, toAppend, toAppend, nil)
	assert.Nil(t, err)
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
	_, err = appender.ApplyAppend(bat, toAppend*2, toAppend, nil)
	assert.Nil(t, err)
	assert.True(t, table.HasAppendableSegment())

	t.Log(txnBufMgr.String())
	t.Log(mutBufMgr.String())
	t.Log(c.SimplePPString(com.PPL1))
}

func TestTxn1(t *testing.T) {
	dir := initTestPath(t)
	c, mgr, driver, txnBufMgr, mutBufMgr := initTestContext(t, dir, common.M*1, common.G)
	defer driver.Close()
	defer c.Close()
	defer mgr.Stop()

	schema := catalog.MockSchema(1)
	schema.BlockMaxRows = 10000
	schema.SegmentMaxBlocks = 4
	batchRows := uint64(schema.BlockMaxRows) * 2 / 5
	batchCnt := 2
	bat := mock.MockBatch(schema.Types(), batchRows)
	{
		txn := mgr.StartTxn(nil)
		db, _ := txn.CreateDatabase("db")
		db.CreateRelation(schema)
		err := txn.Commit()
		assert.Nil(t, err)
	}
	var wg sync.WaitGroup
	now := time.Now()
	doAppend := func() {
		defer wg.Done()
		txn := mgr.StartTxn(nil)
		db, _ := txn.GetDatabase("db")
		rel, err := db.GetRelationByName(schema.Name)
		assert.Nil(t, err)
		for i := 0; i < batchCnt; i++ {
			err := rel.Append(bat)
			assert.Nil(t, err)
		}
		err = txn.Commit()
		assert.Nil(t, err)
	}
	p, _ := ants.NewPool(4)
	loopCnt := 20
	for i := 0; i < loopCnt; i++ {
		wg.Add(1)
		p.Submit(doAppend)
	}

	wg.Wait()

	t.Logf("Append takes: %s", time.Since(now))
	expectBlkCnt := (uint32(batchRows)*uint32(batchCnt)*uint32(loopCnt)-1)/schema.BlockMaxRows + 1
	expectSegCnt := (expectBlkCnt-1)/uint32(schema.SegmentMaxBlocks) + 1
	t.Log(expectBlkCnt)
	t.Log(expectSegCnt)
	t.Log(txnBufMgr.String())
	t.Log(mutBufMgr.String())
	{
		txn := mgr.StartTxn(nil)
		db, _ := txn.GetDatabase("db")
		rel, _ := db.GetRelationByName(schema.Name)
		seg, err := rel.CreateSegment()
		assert.Nil(t, err)
		_, err = seg.CreateBlock()
		assert.Nil(t, err)
	}
	{
		txn := mgr.StartTxn(nil)
		db, _ := txn.GetDatabase("db")
		rel, _ := db.GetRelationByName(schema.Name)
		segIt := rel.MakeSegmentIt()
		segCnt := uint32(0)
		blkCnt := uint32(0)
		for segIt.Valid() {
			segCnt++
			blkIt := segIt.GetSegment().MakeBlockIt()
			for blkIt.Valid() {
				blkCnt++
				blkIt.Next()
			}
			segIt.Next()
		}
		assert.Equal(t, expectSegCnt, segCnt)
		assert.Equal(t, expectBlkCnt, blkCnt)
	}
	t.Log(c.SimplePPString(com.PPL1))
}

func TestTxn2(t *testing.T) {
	dir := initTestPath(t)
	c, mgr, driver, _, _ := initTestContext(t, dir, common.G, common.G)
	defer driver.Close()
	defer c.Close()
	defer mgr.Stop()

	var wg sync.WaitGroup
	run := func() {
		defer wg.Done()
		txn := mgr.StartTxn(nil)
		if _, err := txn.CreateDatabase("db"); err != nil {
			assert.Nil(t, txn.Rollback())
		} else {
			assert.Nil(t, txn.Commit())
		}
		t.Log(txn.String())
	}
	wg.Add(2)
	go run()
	go run()
	wg.Wait()
	t.Log(c.SimplePPString(com.PPL1))
}
