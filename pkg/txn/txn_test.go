package txn

import (
	"math/rand"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	gvec "github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/metadata/v1"
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

func TestInsertNode(t *testing.T) {
	dir := initTestPath(t)
	mgr := buffer.NewNodeManager(common.K*6, nil)
	driver := NewNodeDriver(dir, "store", nil)
	defer driver.Close()

	idAlloc := common.NewIdAlloctor(1)
	schema := metadata.MockSchema(1)

	tbl := NewTable(0, schema, driver, mgr)
	bat := mock.MockBatch(schema.Types(), 1024)
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
				n := NewInsertNode(tbl, mgr, cid, driver)
				nodes[i] = n
				h := mgr.Pin(n)
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
				n.ToTransient()
				n.Close()
			}
			atomic.AddUint64(&all, uint64(len(nodes)))
		}
	}
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
	t.Log(mgr.String())
	t.Log(common.GPool.String())
}

func TestTable(t *testing.T) {
	dir := initTestPath(t)
	mgr := buffer.NewNodeManager(common.K*10, nil)
	driver := NewNodeDriver(dir, "store", nil)
	defer driver.Close()
	schema := metadata.MockSchema(1)
	bat := mock.MockBatch(schema.Types(), 1024)

	id := common.NextGlobalSeqNum()
	tbl := NewTable(id, schema, driver, mgr)
	for i := 0; i < 100; i++ {
		err := tbl.Append(bat)
		assert.Nil(t, err)
	}
	t.Log(mgr.String())
	tbl.RangeDeleteLocalRows(1024+20, 1024+30)
	tbl.RangeDeleteLocalRows(1024*2+38, 1024*2+40)
	t.Log(t, tbl.PrintLocalDeletes())
	assert.True(t, tbl.IsLocalDeleted(1024+20))
	assert.True(t, tbl.IsLocalDeleted(1024+30))
	assert.False(t, tbl.IsLocalDeleted(1024+19))
	assert.False(t, tbl.IsLocalDeleted(1024+31))
}

func TestUpdate(t *testing.T) {
	dir := initTestPath(t)
	mgr := buffer.NewNodeManager(common.K*10, nil)
	driver := NewNodeDriver(dir, "store", nil)
	defer driver.Close()
	schema := metadata.MockSchema(1)
	bat := mock.MockBatch(schema.Types(), 1024)

	id := common.NextGlobalSeqNum()
	tbl := NewTable(id, schema, driver, mgr)
	for i := 0; i < 2; i++ {
		err := tbl.Append(bat)
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

	schema := metadata.MockSchemaAll(14)
	bat := mock.MockBatch(schema.Types(), 500)
	// bat2 := mock.MockBatch(schema.Types(), 100)

	idx := NewSimpleTableIndex()
	err = idx.BatchDedup(bat.Vecs[0])
	assert.Nil(t, err)
	err = idx.BatchInsert(bat.Vecs[0], 0, true)
	assert.NotNil(t, err)

	err = idx.BatchDedup(bat.Vecs[1])
	assert.Nil(t, err)
	err = idx.BatchInsert(bat.Vecs[1], 0, true)
	assert.Nil(t, err)

	window := gvec.New(bat.Vecs[1].Typ)
	gvec.Window(bat.Vecs[1], 20, 22, window)
	assert.Equal(t, 2, gvec.Length(window))
	err = idx.BatchDedup(window)
	assert.NotNil(t, err)

	idx = NewSimpleTableIndex()
	err = idx.BatchDedup(bat.Vecs[12])
	assert.Nil(t, err)
	err = idx.BatchInsert(bat.Vecs[12], 0, true)
	assert.Nil(t, err)

	window = gvec.New(bat.Vecs[12].Typ)
	gvec.Window(bat.Vecs[12], 20, 22, window)
	assert.Equal(t, 2, gvec.Length(window))
	err = idx.BatchDedup(window)
	assert.NotNil(t, err)
}
