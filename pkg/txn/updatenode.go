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
	rwlock  *sync.RWMutex
	schema  *metadata.Schema
	target  common.ID
	txnMask *roaring.Bitmap
	txnVals map[uint32]interface{}
}

func NewUpdateNode(target common.ID, schema *metadata.Schema, rwlock *sync.RWMutex) *updateNode {
	if rwlock == nil {
		rwlock = &sync.RWMutex{}
	}
	return &updateNode{
		rwlock:  rwlock,
		schema:  schema,
		target:  target,
		txnMask: roaring.NewBitmap(),
		txnVals: make(map[uint32]interface{}),
	}
}

func (n *updateNode) Update(row uint32, v interface{}) error {
	n.rwlock.Lock()
	err := n.UpdateLocked(row, v)
	n.rwlock.Unlock()
	return err
}

func (n *updateNode) UpdateLocked(row uint32, v interface{}) error {
	if _, ok := n.txnVals[row]; ok {
		return TxnWWConflictErr
	}
	n.txnMask.Add(row)
	n.txnVals[row] = v
	return nil
}

func (n *updateNode) MergeLocked(o *updateNode) error {
	for k, v := range o.txnVals {
		n.txnMask.Add(k)
		n.txnVals[k] = v
	}
	return nil
}

// TODO
func (n *updateNode) ApplyUpdates(vec *gvec.Vector, deletes *roaring.Bitmap) *gvec.Vector {
	return nil
}
