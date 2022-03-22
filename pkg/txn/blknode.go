package txn

import (
	"sync"

	"github.com/RoaringBitmap/roaring"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/metadata/v1"
)

type blockNode struct {
	rwlocker     *sync.RWMutex
	schema       *metadata.Schema
	id           *common.ID
	cols         map[uint16]*columnNode
	baseDeletes  *roaring.Bitmap
	localDeletes *roaring.Bitmap
}

func NewBlockNode(id *common.ID, schema *metadata.Schema, rwlocker *sync.RWMutex, baseDeletes *roaring.Bitmap) *blockNode {
	if rwlocker == nil {
		rwlocker = new(sync.RWMutex)
	}
	return &blockNode{
		id:          id,
		schema:      schema,
		rwlocker:    rwlocker,
		cols:        make(map[uint16]*columnNode),
		baseDeletes: baseDeletes,
	}
}

func (n *blockNode) DeleteLocked(start, end uint32) error {
	for i := start; i <= end; i++ {
		if (n.baseDeletes != nil && n.baseDeletes.Contains(i)) || n.localDeletes.Contains(i) {
			return TxnWWConflictErr
		}
	}
	n.localDeletes.AddRange(uint64(start), uint64(end+1))
	return nil
}

func (n *blockNode) UpdateLocked(row uint32, colIdx uint16, v interface{}) error {
	if (n.baseDeletes != nil && n.baseDeletes.Contains(row)) || n.localDeletes.Contains(row) {
		return TxnWWConflictErr
	}
	col, ok := n.cols[colIdx]
	if !ok {
		col = NewColumnNode(n.id, n.rwlocker)
		n.cols[colIdx] = col
	}
	return col.UpdateLocked(row, v)
}

func (n *blockNode) MergeLocked(o *blockNode) error {
	if o.localDeletes != nil {
		if n.localDeletes == nil {
			n.localDeletes = roaring.NewBitmap()
		}
		n.localDeletes.Or(o.localDeletes)
	}
	for colIdx, col := range o.cols {
		currCol := n.cols[colIdx]
		if currCol == nil {
			currCol = NewColumnNode(n.id, n.rwlocker)
			n.cols[colIdx] = currCol
		}
		currCol.MergeLocked(col)
	}
	return nil
}
