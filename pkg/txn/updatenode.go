package txn

import (
	"errors"
	"sync"

	"github.com/RoaringBitmap/roaring"
	gvec "github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/metadata/v1"
)

var (
	TxnWWConflictErr = errors.New("tae: w-w conflict error")
)

type updateNode struct {
	rwlock       *sync.RWMutex
	schema       *metadata.Schema
	target       *common.ID
	txnMask      *roaring.Bitmap
	txnVals      map[uint32]interface{}
	localDeletes *roaring.Bitmap
	baseDeletes  *roaring.Bitmap
}

func NewUpdateNode(target *common.ID, schema *metadata.Schema, baseDeletes *roaring.Bitmap, deltaDeletes *roaring.Bitmap, rwlock *sync.RWMutex) *updateNode {
	if rwlock == nil {
		rwlock = &sync.RWMutex{}
	}
	if deltaDeletes == nil {
		deltaDeletes = roaring.NewBitmap()
	}
	return &updateNode{
		rwlock:       rwlock,
		schema:       schema,
		target:       target,
		txnMask:      roaring.NewBitmap(),
		txnVals:      make(map[uint32]interface{}),
		localDeletes: deltaDeletes,
		baseDeletes:  baseDeletes,
	}
}

func (n *updateNode) Update(row uint32, v interface{}) error {
	n.rwlock.Lock()
	err := n.UpdateLocked(row, v)
	n.rwlock.Unlock()
	return err
}

func (n *updateNode) UpdateLocked(row uint32, v interface{}) error {
	if (n.baseDeletes != nil && n.baseDeletes.Contains(row)) || n.localDeletes.Contains(row) {
		return TxnWWConflictErr
	}
	if _, ok := n.txnVals[row]; ok {
		return TxnWWConflictErr
	}
	n.txnMask.Add(row)
	n.txnVals[row] = v
	return nil
}

func (n *updateNode) DeleteLocked(start uint32, end uint32) error {
	for i := start; i <= end; i++ {
		if (n.baseDeletes != nil && n.baseDeletes.Contains(i)) || n.localDeletes.Contains(i) {
			return TxnWWConflictErr
		}
	}
	n.localDeletes.AddRange(uint64(start), uint64(end+1))
	return nil
}

func (n *updateNode) MergeLocked(o *updateNode) error {
	n.localDeletes.Or(o.localDeletes)
	for k, v := range o.txnVals {
		n.txnMask.Add(k)
		n.txnVals[k] = v
	}
	return nil
}

// TODO
func (n *updateNode) ApplyUpdates(vec *gvec.Vector) *gvec.Vector {
	return nil
}
