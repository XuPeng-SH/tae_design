package txn

import (
	"sync/atomic"

	"github.com/jiangxinmeng1/logstore/pkg/entry"

	"github.com/RoaringBitmap/roaring/roaring64"
	gbat "github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/vector"

	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/mutation/buffer"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/mutation/buffer/base"
)

type NodeState = int32

const (
	TransientNode NodeState = iota
	PersistNode
)

type NodeType int8

const (
	NTInsert NodeType = iota
	NTUpdate
	NTDelete
	NTCreateTable
	NTDropTable
	NTCreateDB
	NTDropDB
)

const (
	MaxNodeRows uint32 = 10000
)

type Node interface {
	Type() NodeType
	ToTransient()
	Close() error
}

type InsertNode interface {
	Node
	Append(data *gbat.Batch, offset uint32) (appended uint32, err error)
	GetSpace() uint32
}

type insertNode struct {
	*buffer.Node
	driver NodeDriver
	// data     batch.IBatch
	data     *vector.Vector
	sequence uint64
	typ      NodeState
	deletes  *roaring64.Bitmap
}

func NewInsertNode(mgr base.INodeManager, id common.ID, driver NodeDriver) *insertNode {
	impl := new(insertNode)
	impl.Node = buffer.NewNode(impl, mgr, id, 0)
	impl.driver = driver
	impl.typ = PersistNode
	impl.UnloadFunc = impl.OnUnload
	impl.DestroyFunc = impl.OnDestory
	return impl
}

func (n *insertNode) Type() NodeType { return NTInsert }

func (n *insertNode) makeEntry() NodeEntry {
	e := entry.GetBase()
	e.SetType(ETInsertNode)
	buf, _ := n.data.Show()
	e.Unmarshal(buf)
	return e
}

func (n *insertNode) IsTransient() bool {
	return atomic.LoadInt32(&n.typ) == TransientNode
}

func (n *insertNode) ToTransient() {
	atomic.StoreInt32(&n.typ, TransientNode)
}

func (n *insertNode) OnDestory() {
	// if n.data != nil {
	// 	n.data.Close()
	// }
}
func (n *insertNode) OnUnload() {
	if n.IsTransient() {
		return
	}
	if atomic.LoadUint64(&n.sequence) != 0 {
		return
	}
	e := n.makeEntry()
	if seq, err := n.driver.AppendEntry(e); err != nil {
		panic(err)
	} else {
		atomic.StoreUint64(&n.sequence, seq)
	}
	e.WaitDone()
	e.Free()
}

func (n *insertNode) Append(data *gbat.Batch, offset uint32) (uint32, error) {
	// TODO
	return 0, nil
}

func (n *insertNode) GetSpace() uint32 {
	// TODO
	return 0
}

func (n *insertNode) DeleteRows(interval *common.Range) error {
	if n.deletes == nil {
		n.deletes = roaring64.New()
	}
	n.deletes.AddRange(interval.Left, interval.Right)
	return nil
}

// TODO: Engine merge delete info or just provide raw delete info?
