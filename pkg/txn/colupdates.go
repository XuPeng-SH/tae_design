package txn

import (
	"errors"
	"sync"

	"github.com/RoaringBitmap/roaring"
	"github.com/RoaringBitmap/roaring/roaring64"
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	gvec "github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/common"
)

var (
	TxnWWConflictErr = errors.New("tae: w-w conflict error")
)

type columnUpdates struct {
	rwlock  *sync.RWMutex
	target  *common.ID
	txnMask *roaring.Bitmap
	txnVals map[uint32]interface{}
}

func NewColumnUpdates(target *common.ID, rwlock *sync.RWMutex) *columnUpdates {
	if rwlock == nil {
		rwlock = &sync.RWMutex{}
	}
	return &columnUpdates{
		rwlock:  rwlock,
		target:  target,
		txnMask: roaring.NewBitmap(),
		txnVals: make(map[uint32]interface{}),
	}
}

func (n *columnUpdates) Update(row uint32, v interface{}) error {
	n.rwlock.Lock()
	err := n.UpdateLocked(row, v)
	n.rwlock.Unlock()
	return err
}

func (n *columnUpdates) UpdateLocked(row uint32, v interface{}) error {
	if _, ok := n.txnVals[row]; ok {
		return TxnWWConflictErr
	}
	n.txnMask.Add(row)
	n.txnVals[row] = v
	return nil
}

func (n *columnUpdates) MergeLocked(o *columnUpdates) error {
	for k, v := range o.txnVals {
		n.txnMask.Add(k)
		n.txnVals[k] = v
	}
	return nil
}

func (n *columnUpdates) ApplyToColumn(vec *gvec.Vector, deletes *roaring.Bitmap) *gvec.Vector {
	txnMaskIterator := n.txnMask.Iterator()
	col := vec.Col
	if txnMaskIterator.HasNext() {
		switch vec.Typ.Oid {
		case types.T_int8, types.T_int16, types.T_int32, types.T_int64, types.T_uint8, types.T_uint16, types.T_uint32, types.T_uint64,
			types.T_decimal, types.T_float32, types.T_float64, types.T_date, types.T_datetime:
			for txnMaskIterator.HasNext() {
				row := txnMaskIterator.Next()
				SetFixSizeTypeValue(vec, row, n.txnVals[row])
				if vec.Nsp.Np.Contains(uint64(row)) {
					vec.Nsp.Np.Flip(uint64(row), uint64(row+1))
				}
			}
		case types.T_char, types.T_varchar, types.T_json:
			data := col.(*types.Bytes)
			pre := -1
			for txnMaskIterator.HasNext() {
				row := txnMaskIterator.Next()
				if pre != -1 {
					UpdateOffsets(data, pre, int(row))
				}
				val := n.txnVals[row].([]byte)
				suffix := data.Data[data.Offsets[row]+data.Lengths[row]:]
				data.Lengths[row] = uint32(len(val))
				val=append(val, suffix...)
				data.Data = append(data.Data[:data.Offsets[row]], val...)
				pre = int(row)
				if vec.Nsp.Np.Contains(uint64(row)) {
					vec.Nsp.Np.Flip(uint64(row), uint64(row+1))
				}
			}
			if pre != -1 {
				UpdateOffsets(data, pre, len(data.Lengths)-1)
			}
		}
	}
	deletesIterator := deletes.Iterator()
	if deletesIterator.HasNext() {
		nsp := &nulls.Nulls{}
		nsp.Np = &roaring64.Bitmap{}
		nspIterator := vec.Nsp.Np.Iterator()
		deleted := 0
		switch vec.Typ.Oid {
		case types.T_int8, types.T_int16, types.T_int32, types.T_int64, types.T_uint8, types.T_uint16, types.T_uint32, types.T_uint64,
			types.T_decimal, types.T_float32, types.T_float64, types.T_date, types.T_datetime:
			for deletesIterator.HasNext() {
				row := deletesIterator.Next()
				DeleteFixSizeTypeValue(vec, row-uint32(deleted))
				var n uint64
				if nspIterator.HasNext() {
					for nspIterator.HasNext() {
						n = nspIterator.PeekNext()
						if uint32(n) < row {
							nspIterator.Next()
						} else {
							if uint32(n) == row {
								nspIterator.Next()
							}
							break
						}
						nsp.Np.Add(n - uint64(deleted))
					}
				}
				deleted++
			}
			for nspIterator.HasNext() {
				n := nspIterator.Next()
				nsp.Np.Add(n - uint64(deleted))
			}
		case types.T_char, types.T_varchar, types.T_json:
			data := col.(*types.Bytes)
			pre := -1
			for deletesIterator.HasNext() {
				row := deletesIterator.Next()
				currRow := row - uint32(deleted)
				if pre != -1 {
					if int(currRow) == len(data.Lengths)-1 {
					UpdateOffsets(data, pre-1, int(currRow))
					}else {
						UpdateOffsets(data, pre-1, int(currRow)+1)
					}
				}
				if int(currRow) == len(data.Lengths)-1 {
					data.Data = data.Data[:data.Offsets[currRow]]
					data.Lengths = data.Lengths[:currRow]
					data.Offsets = data.Offsets[:currRow]
				} else {
					data.Data = append(data.Data[:data.Offsets[currRow]], data.Data[data.Offsets[currRow+1]:]...)
					data.Lengths = append(data.Lengths[:currRow], data.Lengths[currRow+1:]...)
					data.Offsets = append(data.Offsets[:currRow], data.Offsets[currRow+1:]...) 
				}
				var n uint64
				if nspIterator.HasNext() {
					for nspIterator.HasNext() {
						n = nspIterator.PeekNext()
						if uint32(n) < row {
							nspIterator.Next()
						} else {
							if uint32(n) == row {
								nspIterator.Next()
							}
							break
						}
						nsp.Np.Add(n - uint64(deleted))
					}
				}
				deleted++
				pre = int(currRow)
			}
			for nspIterator.HasNext() {
				n := nspIterator.Next()
				nsp.Np.Add(n - uint64(deleted))
			}
			if pre != -1 {
				UpdateOffsets(data, pre-1, len(data.Lengths)-1)
			}
		}
		vec.Nsp = nsp
	}
	return vec
}
