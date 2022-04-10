package txnimpl

import (
	"bytes"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"sync/atomic"
	"tae/pkg/catalog"
	com "tae/pkg/common"
	"tae/pkg/iface/txnif"
	"tae/pkg/txn/txnbase"
	"tae/pkg/updates"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/common"

	"github.com/RoaringBitmap/roaring"
	"github.com/RoaringBitmap/roaring/roaring64"
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	gvec "github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/mock"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/mutation/buffer"
	"github.com/panjf2000/ants/v2"
	"github.com/stretchr/testify/assert"
)

// 1. 30 concurrency
// 2. 10000 node
// 3. 512K buffer
// 4. 1K(30%), 4K(25%), 8K(%20), 16K(%15), 32K(%10)

func init() {
	rand.Seed(time.Now().UnixNano())
}

func getNodes() int {
	v := rand.Intn(100)
	if v < 30 {
		return 1 * 2
	} else if v < 55 {
		return 2 * 2
	} else if v < 75 {
		return 3 * 2
	} else if v < 90 {
		return 4 * 2
	}
	return 5 * 2
}

func initTestPath(t *testing.T) string {
	dir := filepath.Join("/tmp", t.Name())
	os.RemoveAll(dir)
	return dir
}

func makeTable(t *testing.T, dir string, colCnt int, bufSize uint64) *txnTable {
	mgr := buffer.NewNodeManager(bufSize, nil)
	driver := txnbase.NewNodeDriver(dir, "store", nil)
	id := common.NextGlobalSeqNum()
	schema := catalog.MockSchemaAll(colCnt)
	rel := mockTestRelation(id, schema)
	return newTxnTable(nil, rel, driver, mgr, nil, nil)
}

func TestInsertNode(t *testing.T) {
	dir := initTestPath(t)
	tbl := makeTable(t, dir, 1, common.K*6)
	defer tbl.driver.Close()
	bat := mock.MockBatch(tbl.GetSchema().Types(), 1024)
	p, _ := ants.NewPool(5)

	var wg sync.WaitGroup
	var all uint64

	worker := func(id uint64) func() {
		return func() {
			defer wg.Done()
			cnt := getNodes()
			nodes := make([]*insertNode, cnt)
			for i := 0; i < cnt; i++ {
				var cid common.ID
				cid.BlockID = id
				cid.Idx = uint16(i)
				n := NewInsertNode(tbl, tbl.nodesMgr, cid, tbl.driver)
				nodes[i] = n
				h := tbl.nodesMgr.Pin(n)
				var err error
				if err = n.Expand(common.K*1, func() error {
					n.Append(bat, 0)
					return nil
				}); err != nil {
					err = n.Expand(common.K*1, func() error {
						n.Append(bat, 0)
						return nil
					})
				}
				if err != nil {
					assert.NotNil(t, err)
				}
				h.Close()
			}
			for _, n := range nodes {
				// n.ToTransient()
				n.Close()
			}
			atomic.AddUint64(&all, uint64(len(nodes)))
		}
	}
	idAlloc := common.NewIdAlloctor(1)
	for {
		id := idAlloc.Alloc()
		if id > 10 {
			break
		}
		wg.Add(1)
		p.Submit(worker(id))
	}
	wg.Wait()
	t.Log(all)
	t.Log(tbl.nodesMgr.String())
	t.Log(common.GPool.String())
}

func TestTable(t *testing.T) {
	dir := initTestPath(t)
	tbl := makeTable(t, dir, 1, common.K*20)
	defer tbl.driver.Close()

	bat := mock.MockBatch(tbl.GetSchema().Types(), 1024)
	for i := 0; i < 100; i++ {
		err := tbl.Append(bat)
		assert.Nil(t, err)
	}
	t.Log(tbl.nodesMgr.String())
	tbl.RangeDeleteLocalRows(1024+20, 1024+30)
	tbl.RangeDeleteLocalRows(1024*2+38, 1024*2+40)
	t.Log(t, tbl.LocalDeletesToString())
	assert.True(t, tbl.IsLocalDeleted(1024+20))
	assert.True(t, tbl.IsLocalDeleted(1024+30))
	assert.False(t, tbl.IsLocalDeleted(1024+19))
	assert.False(t, tbl.IsLocalDeleted(1024+31))
}

func TestUpdate(t *testing.T) {
	dir := initTestPath(t)
	tbl := makeTable(t, dir, 2, common.K*10)
	defer tbl.driver.Close()
	tbl.GetSchema().PrimaryKey = 1
	bat := mock.MockBatch(tbl.GetSchema().Types(), 1024)

	bats := txnbase.SplitBatch(bat, 2)

	for _, b := range bats {
		err := tbl.BatchDedupLocal(b)
		assert.Nil(t, err)
		err = tbl.Append(b)
		assert.Nil(t, err)
	}

	row := uint32(999)
	assert.False(t, tbl.IsLocalDeleted(row))
	rows := tbl.Rows()
	err := tbl.UpdateLocalValue(row, 0, 999)
	assert.Nil(t, err)
	assert.True(t, tbl.IsLocalDeleted(row))
	assert.Equal(t, rows+1, tbl.Rows())
}

func TestAppend(t *testing.T) {
	dir := initTestPath(t)
	tbl := makeTable(t, dir, 2, common.K*20)
	defer tbl.driver.Close()

	tbl.GetSchema().PrimaryKey = 1

	rows := uint64(txnbase.MaxNodeRows) / 8 * 3
	brows := rows / 3
	bat := mock.MockBatch(tbl.GetSchema().Types(), rows)

	bats := txnbase.SplitBatch(bat, 3)

	err := tbl.BatchDedupLocal(bats[0])
	assert.Nil(t, err)
	err = tbl.Append(bats[0])
	assert.Nil(t, err)
	assert.Equal(t, int(brows), int(tbl.Rows()))
	assert.Equal(t, int(brows), int(tbl.index.Count()))

	err = tbl.BatchDedupLocal(bats[0])
	assert.NotNil(t, err)

	err = tbl.BatchDedupLocal(bats[1])
	assert.Nil(t, err)
	err = tbl.Append(bats[1])
	assert.Nil(t, err)
	assert.Equal(t, 2*int(brows), int(tbl.Rows()))
	assert.Equal(t, 2*int(brows), int(tbl.index.Count()))

	err = tbl.BatchDedupLocal(bats[2])
	assert.Nil(t, err)
	err = tbl.Append(bats[2])
	assert.Nil(t, err)
	assert.Equal(t, 3*int(brows), int(tbl.Rows()))
	assert.Equal(t, 3*int(brows), int(tbl.index.Count()))
}

func TestIndex(t *testing.T) {
	index := NewSimpleTableIndex()
	err := index.Insert(1, 10)
	assert.Nil(t, err)
	err = index.Insert("one", 10)
	assert.Nil(t, err)
	row, err := index.Find("one")
	assert.Nil(t, err)
	assert.Equal(t, 10, int(row))
	err = index.Delete("one")
	assert.Nil(t, err)
	_, err = index.Find("one")
	assert.NotNil(t, err)

	schema := catalog.MockSchemaAll(14)
	bat := mock.MockBatch(schema.Types(), 500)

	idx := NewSimpleTableIndex()
	err = idx.BatchDedup(bat.Vecs[0])
	assert.Nil(t, err)
	err = idx.BatchInsert(bat.Vecs[0], 0, gvec.Length(bat.Vecs[0]), 0, true)
	assert.NotNil(t, err)

	err = idx.BatchDedup(bat.Vecs[1])
	assert.Nil(t, err)
	err = idx.BatchInsert(bat.Vecs[1], 0, gvec.Length(bat.Vecs[1]), 0, true)
	assert.Nil(t, err)

	window := gvec.New(bat.Vecs[1].Typ)
	gvec.Window(bat.Vecs[1], 20, 22, window)
	assert.Equal(t, 2, gvec.Length(window))
	err = idx.BatchDedup(window)
	assert.NotNil(t, err)

	idx = NewSimpleTableIndex()
	err = idx.BatchDedup(bat.Vecs[12])
	assert.Nil(t, err)
	err = idx.BatchInsert(bat.Vecs[12], 0, gvec.Length(bat.Vecs[12]), 0, true)
	assert.Nil(t, err)

	window = gvec.New(bat.Vecs[12].Typ)
	gvec.Window(bat.Vecs[12], 20, 22, window)
	assert.Equal(t, 2, gvec.Length(window))
	err = idx.BatchDedup(window)
	assert.NotNil(t, err)
}

func TestLoad(t *testing.T) {
	dir := initTestPath(t)
	tbl := makeTable(t, dir, 14, common.K*2000)
	defer tbl.driver.Close()
	tbl.GetSchema().PrimaryKey = 13

	bat := mock.MockBatch(tbl.GetSchema().Types(), 60000)
	bats := txnbase.SplitBatch(bat, 5)
	// for _, b := range bats {
	// 	tbl.Append(b)
	// }
	// t.Log(tbl.Rows())
	// t.Log(len(tbl.inodes))

	err := tbl.Append(bats[0])
	assert.Nil(t, err)

	t.Log(tbl.nodesMgr.String())
	v, err := tbl.GetLocalValue(100, 0)
	assert.Nil(t, err)
	t.Log(tbl.nodesMgr.String())
	t.Logf("Row %d, Col %d, Val %v", 100, 0, v)
}

func TestNodeCommand(t *testing.T) {
	dir := initTestPath(t)
	tbl := makeTable(t, dir, 14, common.K*1000)
	defer tbl.driver.Close()
	tbl.GetSchema().PrimaryKey = 13

	bat := mock.MockBatch(tbl.GetSchema().Types(), 15000)
	err := tbl.Append(bat)
	assert.Nil(t, err)

	err = tbl.RangeDeleteLocalRows(100, 200)
	assert.Nil(t, err)

	for i, inode := range tbl.inodes {
		cmd, entry, err := inode.MakeCommand(uint32(i), false)
		assert.Nil(t, err)
		if i == 0 {
			assert.Equal(t, 2, len(cmd.(*AppendCmd).Cmds))
		} else {
			assert.Equal(t, 1, len(cmd.(*AppendCmd).Cmds))
		}
		if entry != nil {
			entry.WaitDone()
			entry.Free()
		}
		t.Log(cmd.String())
	}
}

func TestBuildCommand(t *testing.T) {
	dir := initTestPath(t)
	tbl := makeTable(t, dir, 14, common.K*2000)
	defer tbl.driver.Close()
	tbl.GetSchema().PrimaryKey = 13

	bat := mock.MockBatch(tbl.GetSchema().Types(), 55000)
	err := tbl.Append(bat)
	assert.Nil(t, err)

	err = tbl.RangeDeleteLocalRows(100, 200)
	assert.Nil(t, err)

	t.Log(tbl.nodesMgr.String())
	cmdSeq := uint32(1)
	cmd, entries, err := tbl.buildCommitCmd(&cmdSeq)
	assert.Nil(t, err)
	tbl.Close()
	assert.Equal(t, 0, tbl.nodesMgr.Count())
	t.Log(cmd.String())
	for _, e := range entries {
		e.WaitDone()
		e.Free()
	}
	t.Log(tbl.nodesMgr.String())
}

func TestColumnNode(t *testing.T) {
	ncnt := 1000
	nodes := make([]*updates.ColumnUpdates, ncnt)

	target := common.ID{}
	start := time.Now()
	ecnt := 100
	schema := catalog.MockSchema(2)
	for i, _ := range nodes {
		node := updates.NewColumnUpdates(&target, schema.ColDefs[0], nil)
		nodes[i] = node
		for j := i * ecnt; j < i*ecnt+ecnt; j++ {
			node.Update(uint32(j), int32(j))
		}
	}
	t.Log(time.Since(start))
	start = time.Now()
	node := updates.NewColumnUpdates(&target, schema.ColDefs[0], nil)
	for _, n := range nodes {
		node.MergeLocked(n)
	}
	t.Log(time.Since(start))
	assert.Equal(t, ncnt*ecnt, node.GetUpdateCntLocked())

	var w bytes.Buffer
	err := node.WriteTo(&w)
	assert.Nil(t, err)

	buf := w.Bytes()
	r := bytes.NewBuffer(buf)
	n2 := updates.NewColumnUpdates(nil, nil, nil)
	err = n2.ReadFrom(r)
	assert.Nil(t, err)
	assert.True(t, node.EqualLocked(n2))
}

func TestApplyToColumn1(t *testing.T) {
	target := common.ID{}
	deletes := &roaring.Bitmap{}
	deletes.Add(1)
	schema := catalog.MockSchema(2)
	node := updates.NewColumnUpdates(&target, schema.ColDefs[0], nil)
	node.Update(3, []byte("update"))
	deletes.AddRange(3, 4)

	vec := &gvec.Vector{}
	vec.Typ.Oid = types.T_varchar
	col := &types.Bytes{
		Data:    make([]byte, 0),
		Offsets: make([]uint32, 0),
		Lengths: make([]uint32, 0),
	}
	for i := 0; i < 5; i++ {
		col.Offsets = append(col.Offsets, uint32(len(col.Data)))
		data := "val" + strconv.Itoa(i)
		col.Data = append(col.Data, []byte(data)...)
		col.Lengths = append(col.Lengths, uint32(len(data)))
	}
	vec.Col = col

	vec.Nsp = &nulls.Nulls{}
	vec.Nsp.Np = &roaring64.Bitmap{}
	vec.Nsp.Np.Add(2)
	// vec.Nsp.Np.Add(1)
	// vec.Nsp.Np.Add(3)
	vec.Nsp.Np.Add(4)
	// vec.Nsp.Np.Add(0)

	fmt.Printf("%s\n%v\n->\n", vec.Col, vec.Nsp.Np)
	res := node.ApplyToColumn(vec, deletes)
	fmt.Printf("%s\n%v\n", res.Col, res.Nsp.Np)
}

func TestApplyToColumn2(t *testing.T) {
	target := common.ID{}
	deletes := &roaring.Bitmap{}
	deletes.Add(1)
	schema := catalog.MockSchema(2)
	node := updates.NewColumnUpdates(&target, schema.ColDefs[0], nil)
	node.Update(0, int32(8))
	deletes.AddRange(2, 4)

	vec := &gvec.Vector{}
	vec.Typ.Oid = types.T_int32
	vec.Col = []int32{1, 2, 3, 4}

	vec.Nsp = &nulls.Nulls{}
	vec.Nsp.Np = &roaring64.Bitmap{}
	vec.Nsp.Np.Add(2)
	vec.Nsp.Np.Add(1)
	vec.Nsp.Np.Add(3)
	vec.Nsp.Np.Add(0)

	fmt.Printf("%v\n%v\n->\n", vec.Col, vec.Nsp.Np)
	res := node.ApplyToColumn(vec, deletes)
	fmt.Printf("%v\n%v\n", res.Col, res.Nsp.Np)
}

func TestApplyToColumn3(t *testing.T) {
	target := common.ID{}
	schema := catalog.MockSchema(2)
	node := updates.NewColumnUpdates(&target, schema.ColDefs[0], nil)
	node.Update(3, []byte("update"))

	vec := &gvec.Vector{}
	vec.Typ.Oid = types.T_varchar
	col := &types.Bytes{
		Data:    make([]byte, 0),
		Offsets: make([]uint32, 0),
		Lengths: make([]uint32, 0),
	}
	for i := 0; i < 5; i++ {
		col.Offsets = append(col.Offsets, uint32(len(col.Data)))
		data := "val" + strconv.Itoa(i)
		col.Data = append(col.Data, []byte(data)...)
		col.Lengths = append(col.Lengths, uint32(len(data)))
	}
	vec.Col = col

	deletes := &roaring.Bitmap{}
	deletes.Add(1)
	fmt.Printf("%s\n->\n", vec.Col)
	res := node.ApplyToColumn(vec, deletes)
	fmt.Printf("%s\n", res.Col)
}

func TestApplyToColumn4(t *testing.T) {
	target := common.ID{}
	schema := catalog.MockSchema(2)
	node := updates.NewColumnUpdates(&target, schema.ColDefs[0], nil)
	node.Update(3, int32(8))

	vec := &gvec.Vector{}
	vec.Typ.Oid = types.T_int32
	vec.Col = []int32{1, 2, 3, 4}

	fmt.Printf("%v\n->\n", vec.Col)
	res := node.ApplyToColumn(vec, nil)
	fmt.Printf("%v\n", res.Col)
}

func TestTxnManager1(t *testing.T) {
	mgr := txnbase.NewTxnManager(TxnStoreFactory(nil, nil, nil, nil), TxnFactory(nil))
	mgr.Start()
	txn := mgr.StartTxn(nil)

	lock := sync.Mutex{}
	seqs := make([]int, 0)

	txn.SetPrepareCommitFn(func(i interface{}) error {
		time.Sleep(time.Millisecond * 100)
		lock.Lock()
		seqs = append(seqs, 2)
		lock.Unlock()
		return nil
	})

	var wg sync.WaitGroup
	short := func() {
		defer wg.Done()
		txn2 := mgr.StartTxn(nil)
		txn2.SetPrepareCommitFn(func(i interface{}) error {
			lock.Lock()
			seqs = append(seqs, 4)
			lock.Unlock()
			return nil
		})
		time.Sleep(10 * time.Millisecond)
		lock.Lock()
		seqs = append(seqs, 1)
		lock.Unlock()
		txn.GetTxnState(true)
		lock.Lock()
		seqs = append(seqs, 3)
		lock.Unlock()
		txn2.Commit()
	}

	for i := 0; i < 1; i++ {
		wg.Add(1)
		go short()
	}

	txn.Commit()
	wg.Wait()
	defer mgr.Stop()
	expected := []int{1, 2, 3, 4}
	assert.Equal(t, expected, seqs)
}

func initTestContext(t *testing.T, dir string) (*catalog.Catalog, *txnbase.TxnManager, txnbase.NodeDriver) {
	c := catalog.MockCatalog(dir, "mock", nil)
	driver := txnbase.NewNodeDriver(dir, "store", nil)
	mgr := txnbase.NewTxnManager(TxnStoreFactory(c, driver, nil, nil), TxnFactory(c))
	mgr.Start()
	return c, mgr, driver
}

// 1. Txn1 create database "db" and table "tb1". Commit
// 2. Txn2 drop database
// 3. Txn3 create table "tb2"
// 4. Txn2 commit
// 5. Txn3 commit
func TestTransaction1(t *testing.T) {
	dir := initTestPath(t)
	c, mgr, driver := initTestContext(t, dir)
	defer driver.Close()
	defer c.Close()
	defer mgr.Stop()

	txn1 := mgr.StartTxn(nil)
	name := "db"
	schema := catalog.MockSchema(1)
	db, err := txn1.CreateDatabase(name)
	assert.Nil(t, err)
	_, err = db.CreateRelation(schema)
	assert.Nil(t, err)
	err = txn1.Commit()
	assert.Nil(t, err)

	txn2 := mgr.StartTxn(nil)
	db2, err := txn2.DropDatabase(name)
	assert.Nil(t, err)
	t.Log(db2.String())

	txn3 := mgr.StartTxn(nil)
	db3, err := txn3.GetDatabase(name)
	assert.Nil(t, err)
	t.Log(db3.String())
	schema = catalog.MockSchema(1)
	rel, err := db3.CreateRelation(schema)
	assert.Nil(t, err)
	t.Log(rel.String())

	err = txn2.Commit()
	assert.Nil(t, err)
	err = txn3.Commit()
	assert.Equal(t, txnif.TxnStateRollbacked, txn3.GetTxnState(true))
	t.Log(txn3.String())
	// assert.NotNil(t, err)
	t.Log(db2.String())
	t.Log(rel.String())
	t.Log(c.SimplePPString(com.PPL1))
}

func TestTransaction2(t *testing.T) {
	dir := initTestPath(t)
	c, mgr, driver := initTestContext(t, dir)
	defer driver.Close()
	defer c.Close()
	defer mgr.Stop()

	name := "db"
	txn1 := mgr.StartTxn(nil)
	db, err := txn1.CreateDatabase(name)
	assert.Nil(t, err)
	t.Log(db.String())

	schema := catalog.MockSchema(1)
	rel, err := db.CreateRelation(schema)
	assert.Nil(t, err)
	t.Log(rel.String())

	err = txn1.Commit()
	assert.Nil(t, err)
	t.Log(db.String())
	assert.Equal(t, txn1.GetCommitTS(), db.GetMeta().(*catalog.DBEntry).CreateAt)
	assert.Nil(t, db.GetMeta().(*catalog.DBEntry).Txn)
	assert.Equal(t, txn1.GetCommitTS(), rel.GetMeta().(*catalog.TableEntry).CreateAt)
	assert.Nil(t, rel.GetMeta().(*catalog.TableEntry).Txn)

	txn2 := mgr.StartTxn(nil)
	get, err := txn2.GetDatabase(name)
	assert.Nil(t, err)
	t.Log(get.String())

	dropped, err := txn2.DropDatabase(name)
	assert.Nil(t, err)
	t.Log(dropped.String())

	get, err = txn2.GetDatabase(name)
	assert.Equal(t, catalog.ErrNotFound, err)
	t.Log(err)

	txn3 := mgr.StartTxn(nil)

	err = txn3.UseDatabase(name)
	assert.Nil(t, err)
	err = txn3.UseDatabase("xx")
	assert.NotNil(t, err)

	db3, err := txn3.GetDatabase(name)
	assert.Nil(t, err)

	rel, err = db3.GetRelationByName(schema.Name)
	assert.Nil(t, err)
	t.Log(rel.String())
}

func TestTransaction3(t *testing.T) {
	dir := initTestPath(t)
	c, mgr, driver := initTestContext(t, dir)
	defer driver.Close()
	defer mgr.Stop()
	defer c.Close()

	pool, _ := ants.NewPool(20)

	var wg sync.WaitGroup

	flow := func(i int) func() {
		return func() {
			defer wg.Done()
			txn := mgr.StartTxn(nil)
			name := fmt.Sprintf("db-%d", i)
			db, err := txn.CreateDatabase(name)
			assert.Nil(t, err)
			schema := catalog.MockSchemaAll(13)
			_, err = db.CreateRelation(schema)
			assert.Nil(t, err)
			err = txn.Commit()
			assert.Nil(t, err)
		}
	}

	for i := 0; i < 100; i++ {
		wg.Add(1)
		pool.Submit(flow(i))
	}
	wg.Wait()
}

func TestSegment1(t *testing.T) {
	dir := initTestPath(t)
	c, mgr, driver := initTestContext(t, dir)
	defer driver.Close()
	defer mgr.Stop()
	defer c.Close()

	txn1 := mgr.StartTxn(nil)
	name := "db"
	schema := catalog.MockSchema(1)
	db, err := txn1.CreateDatabase(name)
	assert.Nil(t, err)
	rel, err := db.CreateRelation(schema)
	assert.Nil(t, err)
	_, err = rel.CreateSegment()
	assert.Nil(t, err)
	err = txn1.Commit()
	assert.Nil(t, err)

	txn2 := mgr.StartTxn(nil)
	db, err = txn2.GetDatabase(name)
	assert.Nil(t, err)
	rel, err = db.GetRelationByName(schema.Name)
	assert.Nil(t, err)
	segIt := rel.MakeSegmentIt()
	cnt := 0
	for segIt.Valid() {
		iseg := segIt.GetSegment()
		t.Log(iseg.String())
		cnt++
		segIt.Next()
	}
	assert.Equal(t, 1, cnt)

	_, err = rel.CreateSegment()
	assert.Nil(t, err)

	segIt = rel.MakeSegmentIt()
	cnt = 0
	for segIt.Valid() {
		iseg := segIt.GetSegment()
		t.Log(iseg.String())
		cnt++
		segIt.Next()
	}
	assert.Equal(t, 2, cnt)

	txn3 := mgr.StartTxn(nil)
	db, _ = txn3.GetDatabase(name)
	rel, _ = db.GetRelationByName(schema.Name)
	segIt = rel.MakeSegmentIt()
	cnt = 0
	for segIt.Valid() {
		iseg := segIt.GetSegment()
		t.Log(iseg.String())
		cnt++
		segIt.Next()
	}
	assert.Equal(t, 1, cnt)

	err = txn2.Commit()
	assert.Nil(t, err)

	segIt = rel.MakeSegmentIt()
	cnt = 0
	for segIt.Valid() {
		iseg := segIt.GetSegment()
		t.Log(iseg.String())
		cnt++
		segIt.Next()
	}
	assert.Equal(t, 1, cnt)
}

func TestSegment2(t *testing.T) {
	dir := initTestPath(t)
	c, mgr, driver := initTestContext(t, dir)
	defer driver.Close()
	defer mgr.Stop()
	defer c.Close()

	txn1 := mgr.StartTxn(nil)
	db, _ := txn1.CreateDatabase("db")
	schema := catalog.MockSchema(1)
	rel, _ := db.CreateRelation(schema)
	segCnt := 10
	for i := 0; i < segCnt; i++ {
		_, err := rel.CreateSegment()
		assert.Nil(t, err)
	}

	it := rel.MakeSegmentIt()
	cnt := 0
	for it.Valid() {
		cnt++
		// iseg := it.GetSegment()
		it.Next()
	}
	assert.Equal(t, segCnt, cnt)
	// err := txn1.Commit()
	// assert.Nil(t, err)
	t.Log(c.SimplePPString(com.PPL1))
}

func TestBlock1(t *testing.T) {
	dir := initTestPath(t)
	c, mgr, driver := initTestContext(t, dir)
	defer driver.Close()
	defer mgr.Stop()
	defer c.Close()

	txn1 := mgr.StartTxn(nil)
	db, _ := txn1.CreateDatabase("db")
	schema := catalog.MockSchema(1)
	rel, _ := db.CreateRelation(schema)
	seg, _ := rel.CreateSegment()

	blkCnt := 100
	for i := 0; i < blkCnt; i++ {
		_, err := seg.CreateBlock()
		assert.Nil(t, err)
	}

	it := seg.MakeBlockIt()
	cnt := 0
	for it.Valid() {
		cnt++
		it.Next()
	}
	assert.Equal(t, blkCnt, cnt)

	err := txn1.Commit()
	assert.Nil(t, err)
	txn2 := mgr.StartTxn(nil)
	db, _ = txn2.GetDatabase("db")
	rel, _ = db.GetRelationByName(schema.Name)
	segIt := rel.MakeSegmentIt()
	cnt = 0
	for segIt.Valid() {
		seg = segIt.GetSegment()
		it = seg.MakeBlockIt()
		for it.Valid() {
			cnt++
			it.Next()
		}
		segIt.Next()
	}
	assert.Equal(t, blkCnt, cnt)
}
