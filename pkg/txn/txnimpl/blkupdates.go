package txnimpl

import (
	"encoding/binary"
	"io"
	"sync"
	"tae/pkg/catalog"
	"tae/pkg/iface/txnif"
	"tae/pkg/txn/txnbase"

	"github.com/RoaringBitmap/roaring"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/common"
)

type blockUpdates struct {
	rwlocker     *sync.RWMutex
	schema       *catalog.Schema
	id           *common.ID
	cols         map[uint16]*columnUpdates
	baseDeletes  *roaring.Bitmap
	localDeletes *roaring.Bitmap
}

func NewBlockUpdates(id *common.ID, schema *catalog.Schema, rwlocker *sync.RWMutex, baseDeletes *roaring.Bitmap) *blockUpdates {
	if rwlocker == nil {
		rwlocker = new(sync.RWMutex)
	}
	return &blockUpdates{
		id:          id,
		schema:      schema,
		rwlocker:    rwlocker,
		cols:        make(map[uint16]*columnUpdates),
		baseDeletes: baseDeletes,
	}
}

func (n *blockUpdates) GetID() *common.ID { return n.id }

func (n *blockUpdates) DeleteLocked(start, end uint32) error {
	for i := start; i <= end; i++ {
		if (n.baseDeletes != nil && n.baseDeletes.Contains(i)) || (n.localDeletes != nil && n.localDeletes.Contains(i)) {
			return txnif.TxnWWConflictErr
		}
	}
	if n.localDeletes == nil {
		n.localDeletes = roaring.NewBitmap()
	}
	n.localDeletes.AddRange(uint64(start), uint64(end+1))
	return nil
}

func (n *blockUpdates) UpdateLocked(row uint32, colIdx uint16, v interface{}) error {
	if (n.baseDeletes != nil && n.baseDeletes.Contains(row)) || n.localDeletes.Contains(row) {
		return txnif.TxnWWConflictErr
	}
	col, ok := n.cols[colIdx]
	if !ok {
		col = NewColumnUpdates(n.id, n.schema.ColDefs[colIdx], n.rwlocker)
		n.cols[colIdx] = col
	}
	return col.UpdateLocked(row, v)
}

func (n *blockUpdates) GetColumnUpdatesLocked(colIdx uint16) txnif.ColumnUpdates {
	return n.cols[colIdx]
}

func (n *blockUpdates) MergeColumnLocked(ob txnif.BlockUpdates, colIdx uint16) error {
	o := ob.(*blockUpdates)
	if o.localDeletes != nil {
		if n.localDeletes == nil {
			n.localDeletes = roaring.NewBitmap()
		}
		n.localDeletes.Or(o.localDeletes)
	}
	col := o.cols[colIdx]
	if col == nil {
		return nil
	}
	currCol := n.cols[colIdx]
	if currCol == nil {
		currCol = NewColumnUpdates(n.id, n.schema.ColDefs[colIdx], n.rwlocker)
		n.cols[colIdx] = currCol
	}
	currCol.MergeLocked(col)
	return nil
}

func (n *blockUpdates) MergeLocked(o *blockUpdates) error {
	if o.localDeletes != nil {
		if n.localDeletes == nil {
			n.localDeletes = roaring.NewBitmap()
		}
		n.localDeletes.Or(o.localDeletes)
	}
	for colIdx, col := range o.cols {
		currCol := n.cols[colIdx]
		if currCol == nil {
			currCol = NewColumnUpdates(n.id, n.schema.ColDefs[colIdx], n.rwlocker)
			n.cols[colIdx] = currCol
		}
		currCol.MergeLocked(col)
	}
	return nil
}

func (n *blockUpdates) ReadFrom(r io.Reader) error {
	buf := make([]byte, txnbase.IDSize)
	var err error
	if _, err = r.Read(buf); err != nil {
		return err
	}
	n.id = txnbase.UnmarshalID(buf)
	deleteCnt := uint32(0)
	if err = binary.Read(r, binary.BigEndian, &deleteCnt); err != nil {
		return err
	}
	if deleteCnt != 0 {
		buf = make([]byte, deleteCnt)
		if _, err = r.Read(buf); err != nil {
			return err
		}
	}
	colCnt := uint16(0)
	if err = binary.Read(r, binary.BigEndian, &colCnt); err != nil {
		return err
	}
	for i := uint16(0); i < colCnt; i++ {
		colIdx := uint16(0)
		if err = binary.Read(r, binary.BigEndian, &colIdx); err != nil {
			return err
		}
		col := NewColumnUpdates(nil, nil, n.rwlocker)
		if err = col.ReadFrom(r); err != nil {
			return err
		}
	}
	return err
}

func (n *blockUpdates) WriteTo(w io.Writer) error {
	_, err := w.Write(txnbase.MarshalID(n.id))
	if err != nil {
		return err
	}
	if n.localDeletes == nil {
		if err = binary.Write(w, binary.BigEndian, uint32(0)); err != nil {
			return err
		}
	} else {
		buf, err := n.localDeletes.ToBytes()
		if err != nil {
			return err
		}
		if err = binary.Write(w, binary.BigEndian, uint32(len(buf))); err != nil {
			return err
		}
		if _, err = w.Write(buf); err != nil {
			return err
		}
	}
	if err = binary.Write(w, binary.BigEndian, uint16(len(n.cols))); err != nil {
		return err
	}
	for colIdx, col := range n.cols {
		if err = binary.Write(w, binary.BigEndian, colIdx); err != nil {
			return err
		}
		if err = col.WriteTo(w); err != nil {
			return err
		}
	}
	return err
}

func (n *blockUpdates) MakeCommand(id uint32, forceFlush bool) (cmd txnbase.TxnCmd, entry txnbase.NodeEntry, err error) {
	cmd = NewUpdateCmd(id, n)
	return
}
