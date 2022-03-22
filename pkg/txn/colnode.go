package txn

import (
	"errors"
	"sync"

	"github.com/RoaringBitmap/roaring"
	gvec "github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/common"
)

var (
	TxnWWConflictErr = errors.New("tae: w-w conflict error")
)

type columnNode struct {
	rwlock  *sync.RWMutex
	target  *common.ID
	txnMask *roaring.Bitmap
	txnVals map[uint32]interface{}
}

func NewColumnNode(target *common.ID, rwlock *sync.RWMutex) *columnNode {
	if rwlock == nil {
		rwlock = &sync.RWMutex{}
	}
	return &columnNode{
		rwlock:  rwlock,
		target:  target,
		txnMask: roaring.NewBitmap(),
		txnVals: make(map[uint32]interface{}),
	}
}

func (n *columnNode) Update(row uint32, v interface{}) error {
	n.rwlock.Lock()
	err := n.UpdateLocked(row, v)
	n.rwlock.Unlock()
	return err
}

func (n *columnNode) UpdateLocked(row uint32, v interface{}) error {
	if _, ok := n.txnVals[row]; ok {
		return TxnWWConflictErr
	}
	n.txnMask.Add(row)
	n.txnVals[row] = v
	return nil
}

func (n *columnNode) MergeLocked(o *columnNode) error {
	for k, v := range o.txnVals {
		n.txnMask.Add(k)
		n.txnVals[k] = v
	}
	return nil
}

// TODO
func (n *columnNode) ApplyUpdates(vec *gvec.Vector) *gvec.Vector {
	return nil
}
