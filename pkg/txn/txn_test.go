package txn

import (
	"math/rand"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/metadata/v1"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/mock"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/mutation/buffer"
	"github.com/panjf2000/ants/v2"
)

// const (
// 	ETRedo = entry.ETCustomizeStart + 1
// )

// func SetCommitIdToLogEntry(commitId uint64, entry store.AsyncEntry) {
// 	buf := entry.GetMeta().GetReservedBuf()[store.EntryTypeSize+store.EntrySizeSize : store.EntryTypeSize+store.EntrySizeSize+8]
// 	binary.BigEndian.PutUint64(buf, commitId)
// }

// func GetCommitIdFromLogEntry(entry store.AsyncEntry) uint64 {
// 	buf := entry.GetMeta().GetReservedBuf()[store.EntryTypeSize+store.EntrySizeSize : store.EntryTypeSize+store.EntrySizeSize+8]
// 	return binary.BigEndian.Uint64(buf)
// }

// type redoNode struct {
// 	*buffer.Node
// 	store   store.AwareStore
// 	vec     *vector.Vector
// 	entId   uint64
// 	tempary bool
// }

// func newNode(mgr base.INodeManager, id common.ID, store store.AwareStore) *redoNode {
// 	impl := new(redoNode)
// 	impl.Node = buffer.NewNode(impl, mgr, id, 0)
// 	impl.DestroyFunc = impl.OnDestory
// 	impl.UnloadFunc = impl.OnUnload
// 	impl.store = store
// 	return impl
// }

// func (n *redoNode) OnDestory() {
// 	// id := n.GetID()
// 	// log.Infof("Destroying %s", id.String())
// }

// func (n *redoNode) SetTemp() {
// 	n.tempary = true
// }

// func (n *redoNode) OnUnload() {
// 	if n.tempary {
// 		return
// 	}
// 	id := n.GetID()
// 	log.Infof("Unloading %s", id.String())
// 	if n.vec == nil {
// 		return
// 	}
// 	if n.entId != 0 {
// 		return
// 	}
// 	e := n.MakeEntry()
// 	n.store.AppendEntry(e)
// 	e.WaitDone()
// 	e.Free()
// }

// func (n *redoNode) MakeEntry() store.AsyncEntry {
// 	id := common.NextGlobalSeqNum()
// 	e := store.NewAsyncBaseEntry()
// 	e.Meta.SetType(ETRedo)
// 	SetCommitIdToLogEntry(id, e)
// 	buf, _ := n.vec.Show()
// 	e.Unmarshal(buf)
// 	n.entId = id
// 	return e
// }

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
		return 1 * 5
	} else if v < 55 {
		return 2 * 5
	} else if v < 75 {
		return 3 * 5
	} else if v < 90 {
		return 4 * 50
	}
	return 5 * 5
}

func initTestPath(t *testing.T) string {
	dir := filepath.Join("/tmp", t.Name())
	os.Remove(dir)
	return dir
}

func TestInsertNode(t *testing.T) {
	dir := initTestPath(t)
	mgr := buffer.NewNodeManager(common.M*2, nil)
	driver := NewNodeDriver(dir, "store", nil)
	defer driver.Close()

	idAlloc := common.NewIdAlloctor(1)
	schema := metadata.MockSchema(1)
	bat := mock.MockBatch(schema.Types(), 1024)
	p, _ := ants.NewPool(30)

	var wg sync.WaitGroup
	var all uint64

	worker := func(id uint64) func() {
		return func() {
			defer wg.Done()
			cnt := getNodes()
			cnt = 50
			nodes := make([]Node, cnt)
			for i := 0; i < cnt; i++ {
				var cid common.ID
				cid.BlockID = id
				cid.Idx = uint16(i)
				n := NewInsertNode(mgr, cid, driver)
				nodes[i] = n
				mgr.RegisterNode(n)
				h := mgr.Pin(n)
				n.Expand(common.K*4, func() error {
					n.data = bat.Vecs[0]
					return nil
				})
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
		if id > 100 {
			break
		}
		wg.Add(1)
		p.Submit(worker(id))
	}
	wg.Wait()
	t.Log(all)
	t.Log(mgr.String())
}
