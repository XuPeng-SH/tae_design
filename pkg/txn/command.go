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
	String() string
}

type BaseCmd struct{}

type PointerCmd struct {
	BaseCmd
	Group uint32
	Lsn   uint64
}

type DeleteBitmapCmd struct {
	BaseCmd
	Bitmap *roaring64.Bitmap
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

func NewDeleteBitmapCmd(bitmap *roaring64.Bitmap) *DeleteBitmapCmd {
	return &DeleteBitmapCmd{
		Bitmap: bitmap,
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

func (e *PointerCmd) String() string {
	s := fmt.Sprintf("PointerCmd: Group=%d, Lsn=%d", e.Group, e.Lsn)
	return s
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

func (e *DeleteBitmapCmd) GetType() int16 {
	return CmdDeleteBitmap
}

func (e *DeleteBitmapCmd) ReadFrom(r io.Reader) (err error) {
	e.Bitmap = roaring64.NewBitmap()
	_, err = e.Bitmap.ReadFrom(r)
	return
}

func (e *DeleteBitmapCmd) WriteTo(w io.Writer) (err error) {
	if e == nil {
		return
	}
	if err = binary.Write(w, binary.BigEndian, e.GetType()); err != nil {
		return
	}
	_, err = e.Bitmap.WriteTo(w)
	return
}

func (e *DeleteBitmapCmd) Marshal() (buf []byte, err error) {
	var bbuf bytes.Buffer
	if err = e.WriteTo(&bbuf); err != nil {
		return
	}
	buf = bbuf.Bytes()
	return
}

func (e *DeleteBitmapCmd) Unmarshal(buf []byte) error {
	bbuf := bytes.NewBuffer(buf)
	err := e.ReadFrom(bbuf)
	return err
}

func (e *DeleteBitmapCmd) String() string {
	s := fmt.Sprintf("DeleteBitmapCmd: Cardinality=%d", e.Bitmap.GetCardinality())
	return s
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

func (e *BatchCmd) String() string {
	s := fmt.Sprintf("BatchCmd: Rows=%d", e.Bat.Length())
	return s
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

func (cc *ComposedCmd) String() string {
	s := fmt.Sprintf("ComposedCmd: Cnt=%d", len(cc.Cmds))
	for _, cmd := range cc.Cmds {
		s = fmt.Sprintf("%s\n\t%s", s, cmd.String())
	}
	return s
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
		cmd = new(DeleteBitmapCmd)
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
