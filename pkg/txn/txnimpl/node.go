package txnimpl

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"sync/atomic"
	"tae/pkg/iface/txnif"
	"tae/pkg/txn/txnbase"

	"github.com/RoaringBitmap/roaring"
	"github.com/jiangxinmeng1/logstore/pkg/entry"
	"github.com/sirupsen/logrus"

	gbat "github.com/matrixorigin/matrixone/pkg/container/batch"
	gvec "github.com/matrixorigin/matrixone/pkg/container/vector"

	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/container/batch"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/mutation/buffer"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/mutation/buffer/base"
)

const (
	NTInsert txnbase.NodeType = iota
	NTUpdate
	NTDelete
	NTCreateTable
	NTDropTable
	NTCreateDB
	NTDropDB
)

type InsertNode interface {
	base.INode
	PrepareAppend(data *gbat.Batch, offset uint32) (toAppend uint32)
	Append(data *gbat.Batch, offset uint32) (appended uint32, err error)
	RangeDelete(start, end uint32) error
	IsRowDeleted(row uint32) bool
	PrintDeletes() string
	Window(start, end uint32) (*gbat.Batch, error)
	GetSpace() uint32
	Rows() uint32
	GetValue(col int, row uint32) (interface{}, error)
	MakeCommand(uint32, bool) (txnif.TxnCmd, txnbase.NodeEntry, error)
	ToTransient()
	WriteSubCommandInfo(w io.Writer) error
	ReadSubCommandInfo(r io.Reader) error
	SubCommandInfoToString() string
}

type insertNode struct {
	*buffer.Node
	driver           txnbase.NodeDriver
	data             batch.IBatch
	lsn              uint64
	typ              txnbase.NodeState
	deletes          *roaring.Bitmap
	rows             uint32
	table            Table
	subCommandsCount uint32
	infos            [][]byte
}

func NewInsertNode(tbl Table, mgr base.INodeManager, id common.ID, driver txnbase.NodeDriver) *insertNode {
	impl := new(insertNode)
	impl.Node = buffer.NewNode(impl, mgr, id, 0)
	impl.driver = driver
	impl.typ = txnbase.PersistNode
	impl.UnloadFunc = impl.OnUnload
	impl.DestroyFunc = impl.OnDestory
	impl.LoadFunc = impl.OnLoad
	impl.table = tbl
	mgr.RegisterNode(impl)
	return impl
}

func (n *insertNode) MakeCommand(id uint32, forceFlush bool) (cmd txnif.TxnCmd, entry txnbase.NodeEntry, err error) {
	if n.data == nil {
		return
	}
	composedCmd := NewAppendCmd(id, n)
	if n.lsn == 0 && forceFlush {
		entry = n.execUnload()
	}
	if n.lsn == 0 {
		batCmd := txnbase.NewBatchCmd(n.data, n.table.GetSchema().Types())
		composedCmd.AddCmd(batCmd)
	} else {
		ptrCmd := new(txnbase.PointerCmd)
		ptrCmd.Lsn = n.lsn
		ptrCmd.Group = txnbase.GroupUC
		composedCmd.AddCmd(ptrCmd)
	}
	if n.deletes != nil {
		delCmd := txnbase.NewDeleteBitmapCmd(n.deletes)
		composedCmd.AddCmd(delCmd)
	}
	return composedCmd, entry, nil
}

func (n *insertNode) MakeSubCommands(seg *mockBlocks) error {
	blk := seg.GetBlock()
	offset := uint32(0)
	appended, err, info := blk.PrepareAppend(n.data, offset)
	if err != nil {
		return err
	}
	n.appendSubcommandInfo(info)
	offset += appended
	for offset < n.rows {
		seg.CreateBlock()
		blk = seg.GetBlock()
		appended, err, info = blk.PrepareAppend(n.data, offset)
		if err != nil {
			return err
		}
		n.appendSubcommandInfo(info)
		offset += appended
	}
	return nil
}

func (n *insertNode) appendSubcommandInfo(info []byte) {
	n.subCommandsCount++
	if n.infos == nil {
		n.infos = make([][]byte, 0)
	}
	n.infos = append(n.infos, info)
}

func (n *insertNode) marshalSubCommandInfo() []byte {
	buf := make([]byte, 128)
	pos := 0
	length := len(n.infos)
	binary.BigEndian.PutUint32(buf[pos:pos+8], uint32(length))
	pos += 4
	for _, info := range n.infos {
		length := len(info)
		if pos+length+8 < len(buf) {
			buf = append(buf, make([]byte, 128)...)
		}
		binary.BigEndian.PutUint64(buf[pos:pos+8], uint64(length))
		pos += 8
		copy(buf[pos:pos+length], info)
		pos += length
	}
	return buf[:pos]
}

func (n *insertNode) UnmarshalSubCommandInfo(buf []byte) {
	pos := 0
	length := binary.BigEndian.Uint32(buf[pos : pos+4])
	pos += 4
	n.infos = make([][]byte, length)
	for i := range n.infos {
		length := binary.BigEndian.Uint64(buf[pos : pos+8])
		pos += 8
		n.infos[i] = make([]byte, length)
		copy(n.infos[i], buf[pos:pos+int(length)])
		pos += int(length)
	}
}

func (n *insertNode) WriteSubCommandInfo(w io.Writer) (err error) {
	if err = binary.Write(w, binary.BigEndian, n.subCommandsCount); err != nil {
		return
	}
	buf := n.marshalSubCommandInfo()
	length := uint64(len(buf))
	if err = binary.Write(w, binary.BigEndian, length); err != nil {
		return
	}
	_, err = w.Write(buf)
	return err
}

func (n *insertNode) ReadSubCommandInfo(r io.Reader) (err error) {
	if err = binary.Read(r, binary.BigEndian, n.subCommandsCount); err != nil {
		return
	}
	length := uint64(0)
	if err = binary.Read(r, binary.BigEndian, length); err != nil {
		return
	}
	buf := make([]byte, length)
	if _, err = r.Read(buf); err != nil {
		return
	}
	n.UnmarshalSubCommandInfo(buf)
	return
}

func (n *insertNode) SubCommandInfoToString() string{
	s:=fmt.Sprintf("sub command count:%d\n",n.subCommandsCount)
	for _,info:=range n.infos{
		s=fmt.Sprintf("%s %s\n",s,info)
	}
	return s
}

func (n *insertNode) Type() txnbase.NodeType { return NTInsert }

func (n *insertNode) makeLogEntry() txnbase.NodeEntry {
	cmd := txnbase.NewBatchCmd(n.data, n.table.GetSchema().Types())
	buf, err := cmd.Marshal()
	e := entry.GetBase()
	e.SetType(ETInsertNode)
	if err != nil {
		panic(err)
	}
	e.Unmarshal(buf)
	return e
}

func (n *insertNode) IsTransient() bool {
	return atomic.LoadInt32(&n.typ) == txnbase.TransientNode
}

func (n *insertNode) ToTransient() {
	atomic.StoreInt32(&n.typ, txnbase.TransientNode)
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
	e, err := n.driver.LoadEntry(txnbase.GroupUC, lsn)
	if err != nil {
		panic(err)
	}
	logrus.Infof("GetPayloadSize=%d", e.GetPayloadSize())
	buf := e.GetPayload()
	r := bytes.NewBuffer(buf)
	cmd, err := txnbase.BuildCommandFrom(r)
	if err != nil {
		panic(err)
	}
	n.data = cmd.(*txnbase.BatchCmd).Bat
	// v, err := n.GetValue(n.table.GetSchema().PrimaryKey, 10)
}

func (n *insertNode) Close() error {
	n.ToTransient()
	return n.Node.Close()
}

func (n *insertNode) OnUnload() {
	entry := n.execUnload()
	if entry != nil {
		entry.WaitDone()
		entry.Free()
	}
}

func (n *insertNode) execUnload() (entry txnbase.NodeEntry) {
	if n.IsTransient() {
		return
	}
	if atomic.LoadUint64(&n.lsn) != 0 {
		return
	}
	if n.data == nil {
		return
	}
	entry = n.makeLogEntry()
	if seq, err := n.driver.AppendEntry(txnbase.GroupUC, entry); err != nil {
		panic(err)
	} else {
		atomic.StoreUint64(&n.lsn, seq)
		id := n.GetID()
		logrus.Infof("Unloading lsn=%d id=%s", seq, id.SegmentString())
	}
	// e.WaitDone()
	// e.Free()
	return
}

func (n *insertNode) PrepareAppend(data *gbat.Batch, offset uint32) uint32 {
	length := gvec.Length(data.Vecs[0])
	left := uint32(length) - offset
	nodeLeft := txnbase.MaxNodeRows - n.rows
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
			vecs[i] = vector.NewVector(vec.Typ, uint64(txnbase.MaxNodeRows))
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
	return txnbase.MaxNodeRows - n.rows
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
		n.deletes = roaring.New()
	}
	n.deletes.AddRange(uint64(start), uint64(end)+1)
	return nil
}

func (n *insertNode) IsRowDeleted(row uint32) bool {
	if n.deletes == nil {
		return false
	}
	return n.deletes.Contains(row)
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
