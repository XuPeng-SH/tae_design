package updates

import (
	"encoding/binary"
	"io"
	"sync"
	"tae/pkg/catalog"
	com "tae/pkg/common"
	"tae/pkg/iface/txnif"
	"tae/pkg/txn/txnbase"

	"github.com/RoaringBitmap/roaring"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/common"
)

type BlockUpdates struct {
	rwlocker     *sync.RWMutex
	id           *common.ID
	meta         *catalog.BlockEntry
	cols         map[uint16]*ColumnUpdates
	baseDeletes  *roaring.Bitmap
	localDeletes *roaring.Bitmap
	txn          txnif.AsyncTxn
	startTs      uint64
	commitTs     uint64
}

func NewEmptyBlockUpdates() *BlockUpdates {
	return &BlockUpdates{
		cols: make(map[uint16]*ColumnUpdates),
	}
}

func NewBlockUpdates(txn txnif.AsyncTxn, meta *catalog.BlockEntry, rwlocker *sync.RWMutex, baseDeletes *roaring.Bitmap) *BlockUpdates {
	if rwlocker == nil {
		rwlocker = new(sync.RWMutex)
	}
	updates := &BlockUpdates{
		rwlocker:    rwlocker,
		id:          meta.AsCommonID(),
		meta:        meta,
		cols:        make(map[uint16]*ColumnUpdates),
		baseDeletes: baseDeletes,
		txn:         txn,
	}
	if txn != nil {
		updates.startTs = txn.GetStartTS()
		updates.commitTs = txnif.UncommitTS
	}
	return updates
}

func (n *BlockUpdates) GetID() *common.ID { return n.id }

func (n *BlockUpdates) DeleteLocked(start, end uint32) error {
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

func (n *BlockUpdates) UpdateLocked(row uint32, colIdx uint16, v interface{}) error {
	if (n.baseDeletes != nil && n.baseDeletes.Contains(row)) || n.localDeletes.Contains(row) {
		return txnif.TxnWWConflictErr
	}
	col, ok := n.cols[colIdx]
	if !ok {
		col = NewColumnUpdates(n.id, n.meta.GetSegment().GetTable().GetSchema().ColDefs[colIdx], n.rwlocker)
		n.cols[colIdx] = col
	}
	return col.UpdateLocked(row, v)
}

func (n *BlockUpdates) GetColumnUpdatesLocked(colIdx uint16) txnif.ColumnUpdates {
	return n.cols[colIdx]
}

func (n *BlockUpdates) MergeColumnLocked(ob txnif.BlockUpdates, colIdx uint16) error {
	o := ob.(*BlockUpdates)
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
		currCol = NewColumnUpdates(n.id, n.meta.GetSegment().GetTable().GetSchema().ColDefs[colIdx], n.rwlocker)
		n.cols[colIdx] = currCol
	}
	currCol.MergeLocked(col)
	return nil
}

func (n *BlockUpdates) MergeLocked(o *BlockUpdates) error {
	if o.localDeletes != nil {
		if n.localDeletes == nil {
			n.localDeletes = roaring.NewBitmap()
		}
		n.localDeletes.Or(o.localDeletes)
	}
	for colIdx, col := range o.cols {
		currCol := n.cols[colIdx]
		if currCol == nil {
			currCol = NewColumnUpdates(n.id, n.meta.GetSegment().GetTable().GetSchema().ColDefs[colIdx], n.rwlocker)
			n.cols[colIdx] = currCol
		}
		currCol.MergeLocked(col)
	}
	return nil
}

func (n *BlockUpdates) ReadFrom(r io.Reader) error {
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

func (n *BlockUpdates) WriteTo(w io.Writer) error {
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

func (n *BlockUpdates) MakeCommand(id uint32, forceFlush bool) (cmd txnif.TxnCmd, entry txnbase.NodeEntry, err error) {
	cmd = NewUpdateCmd(id, n)
	return
}

func (n *BlockUpdates) Compare(o com.NodePayload) int {
	// op := o.(*BlockUpdates)
	return 0
}

func (n *BlockUpdates) PrepareCommit() error {
	n.rwlocker.Lock()
	defer n.rwlocker.Unlock()
	if n.commitTs != txnif.UncommitTS {
		panic("not expected")
	}
	n.commitTs = n.txn.GetCommitTS()
	return nil
}

func (n *BlockUpdates) ApplyCommit() (err error) {
	n.rwlocker.Lock()
	defer n.rwlocker.Unlock()
	if n.txn == nil {
		panic("not expected")
	}
	n.txn = nil
	return
}

func (n *BlockUpdates) TxnCanRead(txn txnif.AsyncTxn, rwlocker *sync.RWMutex) bool {
	if txn == nil {
		return true
	}
	updateTxn := n.txn
	// The update txn was committed, it is visible to read txn that started after the commitTs
	if updateTxn == nil {
		return n.commitTs < txn.GetStartTS()
	}

	// Read in the same txn
	if updateTxn.GetID() == txn.GetID() {
		return true
	}

	// The update txn is not committed
	if n.commitTs == txnif.UncommitTS {
		return false
	}

	// The update txn is committing and the commitTs is after the read txn startTs
	if n.commitTs > txn.GetStartTS() {
		return false
	}

	// The update txn is committing and the commitTs is before the read txn startTs
	if rwlocker != nil {
		rwlocker.RUnlock()
	}
	state := updateTxn.GetTxnState(true)
	if rwlocker != nil {
		rwlocker.RLock()
	}
	return state != txnif.TxnStateRollbacked
}
