package txn

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"

	"github.com/RoaringBitmap/roaring/roaring64"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/container/batch"
)

const (
	CmdPointer int16 = iota
	CmdDeleteBitmap
	CmdBatch
	CmdComposed
)

type TxnCmd interface {
	WriteTo(io.Writer) error
	ReadFrom(io.Reader) error
	Marshal() ([]byte, error)
	Unmarshal([]byte) error
	GetType() int16
}

type BaseCmd struct{}

type PointerCmd struct {
	BaseCmd
	Group uint32
	Lsn   uint64
}

type LocalDeletesCmd struct {
	BaseCmd
	Deletes map[uint32]*roaring64.Bitmap
}

type BatchCmd struct {
	BaseCmd
	Bat   batch.IBatch
	Types []types.Type
}

type ComposedCmd struct {
	BaseCmd
	Cmds []TxnCmd
}

func NewLocalDeletesCmd() *LocalDeletesCmd {
	return &LocalDeletesCmd{
		Deletes: make(map[uint32]*roaring64.Bitmap),
	}
}

func MakeDeletesCmd(deletes map[uint32]*roaring64.Bitmap) *LocalDeletesCmd {
	return &LocalDeletesCmd{
		Deletes: deletes,
	}
}

func NewBatchCmd(bat batch.IBatch, colTypes []types.Type) *BatchCmd {
	return &BatchCmd{
		Bat:   bat,
		Types: colTypes,
	}
}

func NewComposedCmd() *ComposedCmd {
	return &ComposedCmd{
		Cmds: make([]TxnCmd, 0),
	}
}

func (e *PointerCmd) GetType() int16 {
	return CmdPointer
}

func (e *PointerCmd) WriteTo(w io.Writer) (err error) {
	if err = binary.Write(w, binary.BigEndian, e.GetType()); err != nil {
		return
	}
	if err = binary.Write(w, binary.BigEndian, e.Group); err != nil {
		return
	}
	if err = binary.Write(w, binary.BigEndian, e.Lsn); err != nil {
		return
	}
	return
}

func (e *PointerCmd) Marshal() (buf []byte, err error) {
	var bbuf bytes.Buffer
	if err = e.WriteTo(&bbuf); err != nil {
		return
	}
	buf = bbuf.Bytes()
	return
}

func (e *PointerCmd) ReadFrom(r io.Reader) (err error) {
	if err = binary.Read(r, binary.BigEndian, &e.Group); err != nil {
		return
	}
	if err = binary.Read(r, binary.BigEndian, &e.Lsn); err != nil {
		return
	}
	return
}

func (e *PointerCmd) Unmarshal(buf []byte) error {
	bbuf := bytes.NewBuffer(buf)
	err := e.ReadFrom(bbuf)
	return err
}

func (e *LocalDeletesCmd) AddDelete(key uint32, bm *roaring64.Bitmap) {
	e.Deletes[key] = bm
}

func (e *LocalDeletesCmd) GetType() int16 {
	return CmdDeleteBitmap
}

func (e *LocalDeletesCmd) ReadFrom(r io.Reader) (err error) {
	var cnt uint32
	if err = binary.Read(r, binary.BigEndian, &cnt); err != nil {
		return
	}
	if cnt == 0 {
		return
	}
	e.Deletes = make(map[uint32]*roaring64.Bitmap)
	for i := 0; i < int(cnt); i++ {
		var k uint32
		if err = binary.Read(r, binary.BigEndian, &k); err != nil {
			break
		}
		bm := roaring64.NewBitmap()
		if _, err = bm.ReadFrom(r); err != nil {
			break
		}
		e.Deletes[k] = bm
	}
	return
}

func (e *LocalDeletesCmd) WriteTo(w io.Writer) (err error) {
	if e == nil {
		return
	}
	if err = binary.Write(w, binary.BigEndian, e.GetType()); err != nil {
		return
	}
	cnt := uint32(len(e.Deletes))
	if err = binary.Write(w, binary.BigEndian, cnt); err != nil {
		return
	}
	for k, v := range e.Deletes {
		if err = binary.Write(w, binary.BigEndian, k); err != nil {
			break
		}
		if _, err = v.WriteTo(w); err != nil {
			break
		}
	}
	return
}

func (e *LocalDeletesCmd) Marshal() (buf []byte, err error) {
	var bbuf bytes.Buffer
	if err = e.WriteTo(&bbuf); err != nil {
		return
	}
	buf = bbuf.Bytes()
	return
}

func (e *LocalDeletesCmd) Unmarshal(buf []byte) error {
	bbuf := bytes.NewBuffer(buf)
	err := e.ReadFrom(bbuf)
	return err
}

func (e *BatchCmd) GetType() int16 {
	return CmdBatch
}

func (e *BatchCmd) Marshal() (buf []byte, err error) {
	var bbuf bytes.Buffer
	if err = e.WriteTo(&bbuf); err != nil {
		return
	}
	buf = bbuf.Bytes()
	return
}

func (e *BatchCmd) Unmarshal(buf []byte) error {
	bbuf := bytes.NewBuffer(buf)
	err := e.ReadFrom(bbuf)
	return err
}

func (e *BatchCmd) ReadFrom(r io.Reader) (err error) {
	e.Types, e.Bat, err = UnmarshalBatchFrom(r)
	return err
}

func (e *BatchCmd) WriteTo(w io.Writer) (err error) {
	if err = binary.Write(w, binary.BigEndian, e.GetType()); err != nil {
		return
	}
	colsBuf, err := MarshalBatch(e.Types, e.Bat)
	if err != nil {
		return
	}
	_, err = w.Write(colsBuf)
	return
}

func (e *ComposedCmd) GetType() int16 {
	return CmdComposed
}

func (cc *ComposedCmd) Marshal() (buf []byte, err error) {
	var bbuf bytes.Buffer
	if err = cc.WriteTo(&bbuf); err != nil {
		return
	}
	return
}

func (cc *ComposedCmd) Unmarshal(buf []byte) (err error) {
	bbuf := bytes.NewBuffer(buf)
	err = cc.ReadFrom(bbuf)
	return err
}

func (cc *ComposedCmd) WriteTo(w io.Writer) (err error) {
	if err = binary.Write(w, binary.BigEndian, cc.GetType()); err != nil {
		return
	}
	cmds := uint32(len(cc.Cmds))
	if err = binary.Write(w, binary.BigEndian, cmds); err != nil {
		return
	}
	for _, cmd := range cc.Cmds {
		if err = cmd.WriteTo(w); err != nil {
			break
		}
	}
	return
}

func (cc *ComposedCmd) ReadFrom(r io.Reader) (err error) {
	cmds := uint32(0)
	if err = binary.Read(r, binary.BigEndian, &cmds); err != nil {
		return
	}
	cc.Cmds = make([]TxnCmd, cmds)
	for i := 0; i < int(cmds); i++ {
		if cc.Cmds[i], err = BuildCommandFrom(r); err != nil {
			break
		}
	}
	return
}

func (cc *ComposedCmd) AddCmd(cmd TxnCmd) {
	cc.Cmds = append(cc.Cmds, cmd)
}

func BuildCommandFrom(r io.Reader) (cmd TxnCmd, err error) {
	var cmdType int16
	if err = binary.Read(r, binary.BigEndian, &cmdType); err != nil {
		return
	}
	switch cmdType {
	case CmdPointer:
		cmd = new(PointerCmd)
	case CmdDeleteBitmap:
		cmd = new(LocalDeletesCmd)
	case CmdBatch:
		cmd = new(BatchCmd)
	case CmdComposed:
		cmd = new(ComposedCmd)
	default:
		panic(fmt.Sprintf("not support cmd type: %d", cmdType))
	}
	err = cmd.ReadFrom(r)
	return
}

// type WaitablePointerEntry struct {
// 	PointerCmd
// 	NodeEntry
// }

// type TableInsertCommitEntry struct {
// 	pointers []*PointerCmd
// 	pendings []*WaitablePointerEntry
// 	tail     NodeEntry
// }

// func NewTableInsertCommitEntry() *TableInsertCommitEntry {
// 	// TODO
// 	return new(TableInsertCommitEntry)
// }

// func (te *TableInsertCommitEntry) AddPointer(p *PointerCmd) {
// 	// TODO
// }

// func (te *TableInsertCommitEntry) AddPending(group uint32, lsn uint64, e NodeEntry) {
// 	// TODO
// }

// func (te *TableInsertCommitEntry) AddTail(e NodeEntry) {
// 	// TODO
// }

// func (te *TableInsertCommitEntry) Marshal() (buf []byte, err error) {
// 	// TODO
// 	return
// }
