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
	"tae/pkg/txn/txnbase"
	"testing"
	"time"

	"github.com/RoaringBitmap/roaring"
	"github.com/RoaringBitmap/roaring/roaring64"
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	gvec "github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/common"
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
	return NewTable(nil, id, schema, driver, mgr)
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
	nodes := make([]*columnUpdates, ncnt)

	target := common.ID{}
	start := time.Now()
	ecnt := 100
	schema := catalog.MockSchema(2)
	for i, _ := range nodes {
		node := NewColumnUpdates(&target, schema.ColDefs[0], nil)
		nodes[i] = node
		for j := i * ecnt; j < i*ecnt+ecnt; j++ {
			node.Update(uint32(j), int32(j))
		}
	}
	t.Log(time.Since(start))
	start = time.Now()
	node := NewColumnUpdates(&target, schema.ColDefs[0], nil)
	for _, n := range nodes {
		node.MergeLocked(n)
	}
	t.Log(time.Since(start))
	assert.Equal(t, ncnt*ecnt, int(node.txnMask.GetCardinality()))
	assert.Equal(t, ncnt*ecnt, len(node.txnVals))

	var w bytes.Buffer
	err := node.WriteTo(&w)
	assert.Nil(t, err)

	buf := w.Bytes()
	r := bytes.NewBuffer(buf)
	n2 := NewColumnUpdates(nil, nil, nil)
	err = n2.ReadFrom(r)
	assert.Nil(t, err)
	assert.Equal(t, node.txnMask.GetCardinality(), n2.txnMask.GetCardinality())
	assert.Equal(t, node.txnVals, n2.txnVals)
}

func TestApplyUpdateNode(t *testing.T) {
	target := common.ID{}
	deletes := &roaring.Bitmap{}
	deletes.Add(1)
	schema := catalog.MockSchema(2)
	node := NewColumnUpdates(&target, schema.ColDefs[0], nil)
	node.Update(0, []byte("update"))
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

func TestApplyUpdateNode2(t *testing.T) {
	target := common.ID{}
	deletes := &roaring.Bitmap{}
	deletes.Add(1)
	schema := catalog.MockSchema(2)
	node := NewColumnUpdates(&target, schema.ColDefs[0], nil)
	node.Update(0, int8(8))
	deletes.AddRange(2, 4)

	vec := &gvec.Vector{}
	vec.Typ.Oid = types.T_int8
	vec.Col = []int8{1, 2, 3, 4}

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

func TestTxnManager1(t *testing.T) {
	mgr := txnbase.NewTxnManager(TxnStoreFactory(nil), TxnFactory(nil))
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

func TestTransaction1(t *testing.T) {
	mgr := txnbase.NewTxnManager(TxnStoreFactory(nil), TxnFactory(nil))
	mgr.Start()
	defer mgr.Stop()

	// txn1 := mgr.StartTxn(nil)
}
