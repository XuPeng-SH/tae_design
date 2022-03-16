package txn

import (
	"sync/atomic"

	"github.com/jiangxinmeng1/logstore/pkg/entry"

	"github.com/RoaringBitmap/roaring/roaring64"
	gbat "github.com/matrixorigin/matrixone/pkg/container/batch"
	gvec "github.com/matrixorigin/matrixone/pkg/container/vector"

	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/container/batch"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/container/vector"
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
	base.INode
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
	driver   NodeDriver
	data2    batch.IBatch
	data     *gvec.Vector
	sequence int64
	typ      NodeState
	deletes  *roaring64.Bitmap
	rows     uint32
}

func NewInsertNode(mgr base.INodeManager, id common.ID, driver NodeDriver) *insertNode {
	impl := new(insertNode)
	impl.Node = buffer.NewNode(impl, mgr, id, 0)
	impl.driver = driver
	impl.typ = PersistNode
	impl.UnloadFunc = impl.OnUnload
	impl.DestroyFunc = impl.OnDestory
	impl.sequence = -1
	mgr.RegisterNode(impl)
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
	if n.data2 != nil {
		n.data2.Close()
	}
}
func (n *insertNode) OnUnload() {
	if n.IsTransient() {
		return
	}
	if atomic.LoadInt64(&n.sequence) != -1 {
		return
	}
	if n.data == nil {
		return
	}
	e := n.makeEntry()
	if seq, err := n.driver.AppendEntry(e); err != nil {
		panic(err)
	} else {
		atomic.StoreInt64(&n.sequence, int64(seq))
		// id := n.GetID()
		// logrus.Infof("Unloading %s", id.String())
	}
	e.WaitDone()
	e.Free()
}

func (n *insertNode) Append(data *gbat.Batch, offset uint32) (uint32, error) {
	if n.data2 == nil {
		var cnt int
		var err error
		vecs := make([]vector.IVector, len(data.Vecs))
		attrs := make([]int, len(data.Vecs))
		for i, vec := range data.Vecs {
			attrs[i] = i
			vecs[i] = vector.NewVector(vec.Typ, uint64(MaxNodeRows))
			cnt, err = vecs[i].AppendVector(vec, int(offset))
			if err != nil {
				return 0, err
			}
		}
		if n.data2, err = batch.NewBatch(attrs, vecs); err != nil {
			return 0, err
		}
		n.rows = uint32(n.data2.Length())
		return uint32(cnt), nil
	}

	var cnt int
	for i, attr := range n.data2.GetAttrs() {
		vec, err := n.data2.GetVectorByAttr(attr)
		if err != nil {
			return 0, err
		}
		cnt, err = vec.AppendVector(data.Vecs[i], int(offset))
		if err != nil {
			return 0, err
		}
		n.rows = uint32(vec.Length())
	}
	return uint32(cnt), nil
}

func (n *insertNode) GetSpace() uint32 {
	return MaxNodeRows - n.rows
}

func (n *insertNode) DeleteRows(interval *common.Range) error {
	if n.deletes == nil {
		n.deletes = roaring64.New()
	}
	n.deletes.AddRange(interval.Left, interval.Right)
	return nil
}

// TODO: Engine merge delete info or just provide raw delete info?
