package txn

import (
	"bytes"
	"fmt"
	"sync/atomic"

	"github.com/jiangxinmeng1/logstore/pkg/entry"
	"github.com/sirupsen/logrus"

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
	PrepareAppend(data *gbat.Batch, offset uint32) (toAppend uint32)
	Append(data *gbat.Batch, offset uint32) (appended uint32, err error)
	RangeDelete(start, end uint32) error
	IsRowDeleted(row uint32) bool
	PrintDeletes() string
	Window(start, end uint32) (*gbat.Batch, error)
	GetSpace() uint32
	Rows() uint32
	GetValue(col int, row uint32) (interface{}, error)
}

type insertNode struct {
	*buffer.Node
	driver  NodeDriver
	data    batch.IBatch
	lsn     uint64
	typ     NodeState
	deletes *roaring64.Bitmap
	rows    uint32
	table   Table
}

func NewInsertNode(tbl Table, mgr base.INodeManager, id common.ID, driver NodeDriver) *insertNode {
	impl := new(insertNode)
	impl.Node = buffer.NewNode(impl, mgr, id, 0)
	impl.driver = driver
	impl.typ = PersistNode
	impl.UnloadFunc = impl.OnUnload
	impl.DestroyFunc = impl.OnDestory
	impl.LoadFunc = impl.OnLoad
	impl.table = tbl
	mgr.RegisterNode(impl)
	return impl
}

func (n *insertNode) Type() NodeType { return NTInsert }

func (n *insertNode) makeLogEntry() NodeEntry {
	cmd := NewBatchCmd(n.data, n.table.GetSchema().Types())
	buf, err := cmd.Marshal()
	e := entry.GetBase()
	e.SetType(ETInsertNode)
	// buf, err := MarshalBatch(n.data)
	if err != nil {
		panic(err)
	}
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
	if n.data != nil {
		n.data.Close()
	}
}

func (n *insertNode) OnLoad() {
	if n.IsTransient() {
		return
	}

	lsn := atomic.LoadUint64(&n.lsn)
	if lsn == 0 {
		return
	}
	e, err := n.driver.LoadEntry(GroupUC, lsn)
	if err != nil {
		panic(err)
	}
	logrus.Infof("GetPayloadSize=%d", e.GetPayloadSize())
	buf := e.GetPayload()
	r := bytes.NewBuffer(buf)
	cmd, err := BuildCommandFrom(r)
	if err != nil {
		panic(err)
	}
	n.data = cmd.(*BatchCmd).Bat
	// v, err := n.GetValue(n.table.GetSchema().PrimaryKey, 10)
}

func (n *insertNode) OnUnload() {
	if n.IsTransient() || n.table.IsCommitted() || n.table.IsRollbacked() {
		return
	}
	if atomic.LoadUint64(&n.lsn) != 0 {
		return
	}
	if n.data == nil {
		return
	}
	e := n.makeLogEntry()
	if seq, err := n.driver.AppendEntry(GroupUC, e); err != nil {
		panic(err)
	} else {
		atomic.StoreUint64(&n.lsn, seq)
		id := n.GetID()
		logrus.Infof("Unloading lsn=%d id=%s", seq, id.SegmentString())
	}
	e.WaitDone()
	e.Free()
}

func (n *insertNode) PrepareAppend(data *gbat.Batch, offset uint32) uint32 {
	length := gvec.Length(data.Vecs[0])
	left := uint32(length) - offset
	nodeLeft := MaxNodeRows - n.rows
	if left <= nodeLeft {
		return left
	}
	return nodeLeft
}

func (n *insertNode) Append(data *gbat.Batch, offset uint32) (uint32, error) {
	if n.data == nil {
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
		if n.data, err = batch.NewBatch(attrs, vecs); err != nil {
			return 0, err
		}
		n.rows = uint32(n.data.Length())
		return uint32(cnt), nil
	}

	var cnt int
	for i, attr := range n.data.GetAttrs() {
		vec, err := n.data.GetVectorByAttr(attr)
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

func (n *insertNode) Rows() uint32 {
	return n.rows
}

func (n *insertNode) GetValue(col int, row uint32) (interface{}, error) {
	vec, err := n.data.GetVectorByAttr(col)
	if err != nil {
		return nil, err
	}
	v, err := vec.GetValue(int(row))
	return v, err
}

func (n *insertNode) RangeDelete(start, end uint32) error {
	if n.deletes == nil {
		n.deletes = roaring64.New()
	}
	n.deletes.AddRange(uint64(start), uint64(end)+1)
	return nil
}

func (n *insertNode) IsRowDeleted(row uint32) bool {
	if n.deletes == nil {
		return false
	}
	return n.deletes.Contains(uint64(row))
}

func (n *insertNode) PrintDeletes() string {
	if n.deletes == nil {
		return fmt.Sprintf("NoDeletes")
	}
	return n.deletes.String()
}

// TODO: Rewrite later
func (n *insertNode) Window(start, end uint32) (*gbat.Batch, error) {
	attrs := make([]string, len(n.table.GetSchema().ColDefs))
	for i, _ := range attrs {
		attrs[i] = n.table.GetSchema().ColDefs[i].Name
	}
	ret := gbat.New(true, attrs)
	for i, attr := range n.data.GetAttrs() {
		src, err := n.data.GetVectorByAttr(attr)
		if err != nil {
			return nil, err
		}
		srcVec, _ := src.GetLatestView().CopyToVector()
		destVec := gvec.New(srcVec.Typ)
		gvec.Window(srcVec, int(start), int(end)+1, destVec)
		ret.Vecs[i] = destVec
	}
	return ret, nil
}

// TODO: Engine merge delete info or just provide raw delete info?
