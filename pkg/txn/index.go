package txn

import (
	"errors"
	"sync"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	gvec "github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/container/vector"
)

var (
	ErrNotFound   = errors.New("tae: not found error")
	ErrDuplicated = errors.New("tae: duplicated error")
)

type TableIndex interface {
	BatchDedup(*gvec.Vector) error
	BatchInsert(*gvec.Vector, uint32, bool) error
	Insert(interface{}, uint32) error
	Delete(interface{}) error
	Find(interface{}) (uint32, error)
	Name() string
	Count() int
}

// TODO
type artTableIndex struct{}

type simpleTableIndex struct {
	sync.RWMutex
	tree map[interface{}]uint32
}

func NewSimpleTableIndex() *simpleTableIndex {
	return &simpleTableIndex{
		tree: make(map[interface{}]uint32),
	}
}

func (idx *simpleTableIndex) Name() string { return "SimpleIndex" }
func (idx *simpleTableIndex) Count() int {
	idx.RLock()
	cnt := len(idx.tree)
	idx.RUnlock()
	return cnt
}

func (idx *simpleTableIndex) Insert(v interface{}, row uint32) error {
	idx.Lock()
	defer idx.Unlock()
	_, ok := idx.tree[v]
	if ok {
		return ErrDuplicated
	}
	idx.tree[v] = row
	return nil
}

func (idx *simpleTableIndex) Delete(v interface{}) error {
	idx.Lock()
	defer idx.Unlock()
	_, ok := idx.tree[v]
	if !ok {
		return ErrNotFound
	}
	delete(idx.tree, v)
	return nil
}

func (idx *simpleTableIndex) Find(v interface{}) (uint32, error) {
	idx.RLock()
	defer idx.RUnlock()
	row, ok := idx.tree[v]
	if !ok {
		return 0, ErrNotFound
	}
	return uint32(row), nil
}

func (idx *simpleTableIndex) BatchInsert(col *gvec.Vector, row uint32, dedupCol bool) error {
	idx.Lock()
	defer idx.Unlock()
	vals := col.Col
	switch col.Typ.Oid {
	case types.T_int8:
		data := vals.([]int8)
		if dedupCol {
			set := make(map[int8]bool)
			for _, v := range data {
				if _, ok := set[v]; ok {
					return ErrDuplicated
				}
				set[v] = true
			}
		}
		for _, v := range data {
			idx.tree[v] = row
			row++
		}
	case types.T_int16:
		data := vals.([]int16)
		if dedupCol {
			set := make(map[int16]bool)
			for _, v := range data {
				if _, ok := set[v]; ok {
					return ErrDuplicated
				}
				set[v] = true
			}
		}
		for _, v := range data {
			idx.tree[v] = row
			row++
		}
	case types.T_int32:
		data := vals.([]int32)
		if dedupCol {
			set := make(map[int32]bool)
			for _, v := range data {
				if _, ok := set[v]; ok {
					return ErrDuplicated
				}
				set[v] = true
			}
		}
		for _, v := range data {
			idx.tree[v] = row
			row++
		}
	case types.T_int64:
		data := vals.([]int64)
		if dedupCol {
			set := make(map[int64]bool)
			for _, v := range data {
				if _, ok := set[v]; ok {
					return ErrDuplicated
				}
				set[v] = true
			}
		}
		for _, v := range data {
			idx.tree[v] = row
			row++
		}
	case types.T_uint8:
		data := vals.([]uint8)
		if dedupCol {
			set := make(map[uint8]bool)
			for _, v := range data {
				if _, ok := set[v]; ok {
					return ErrDuplicated
				}
				set[v] = true
			}
		}
		for _, v := range data {
			idx.tree[v] = row
			row++
		}
	case types.T_uint16:
		data := vals.([]uint16)
		if dedupCol {
			set := make(map[uint16]bool)
			for _, v := range data {
				if _, ok := set[v]; ok {
					return ErrDuplicated
				}
				set[v] = true
			}
		}
		for _, v := range data {
			idx.tree[v] = row
			row++
		}
	case types.T_uint32:
		data := vals.([]uint32)
		if dedupCol {
			set := make(map[uint32]bool)
			for _, v := range data {
				if _, ok := set[v]; ok {
					return ErrDuplicated
				}
				set[v] = true
			}
		}
		for _, v := range data {
			idx.tree[v] = row
			row++
		}
	case types.T_uint64:
		data := vals.([]uint64)
		if dedupCol {
			set := make(map[uint64]bool)
			for _, v := range data {
				if _, ok := set[v]; ok {
					return ErrDuplicated
				}
				set[v] = true
			}
		}
		for _, v := range data {
			idx.tree[v] = row
			row++
		}
	case types.T_decimal:
		data := vals.([]types.Decimal)
		if dedupCol {
			set := make(map[types.Decimal]bool)
			for _, v := range data {
				if _, ok := set[v]; ok {
					return ErrDuplicated
				}
				set[v] = true
			}
		}
		for _, v := range data {
			idx.tree[v] = row
			row++
		}
	case types.T_float32:
		data := vals.([]float32)
		if dedupCol {
			set := make(map[float32]bool)
			for _, v := range data {
				if _, ok := set[v]; ok {
					return ErrDuplicated
				}
				set[v] = true
			}
		}
		for _, v := range data {
			idx.tree[v] = row
			row++
		}
	case types.T_float64:
		data := vals.([]float64)
		if dedupCol {
			set := make(map[float64]bool)
			for _, v := range data {
				if _, ok := set[v]; ok {
					return ErrDuplicated
				}
				set[v] = true
			}
		}
		for _, v := range data {
			idx.tree[v] = row
			row++
		}
	case types.T_date:
		data := vals.([]types.Date)
		if dedupCol {
			set := make(map[types.Date]bool)
			for _, v := range data {
				if _, ok := set[v]; ok {
					return ErrDuplicated
				}
				set[v] = true
			}
		}
		for _, v := range data {
			idx.tree[v] = row
			row++
		}
	case types.T_datetime:
		data := vals.([]types.Datetime)
		if dedupCol {
			set := make(map[types.Datetime]bool)
			for _, v := range data {
				if _, ok := set[v]; ok {
					return ErrDuplicated
				}
				set[v] = true
			}
		}
		for _, v := range data {
			idx.tree[v] = row
			row++
		}
	case types.T_char, types.T_varchar, types.T_json:
		data := vals.(*types.Bytes)
		if dedupCol {
			set := make(map[string]bool)
			for i, s := range data.Offsets {
				e := s + data.Lengths[i]
				v := string(data.Data[s:e])
				if _, ok := set[v]; ok {
					return ErrDuplicated
				}
				set[v] = true
			}
		}
		for i, s := range data.Offsets {
			e := s + data.Lengths[i]
			v := string(data.Data[s:e])
			idx.tree[v] = row
			row++
		}
	default:
		return vector.VecTypeNotSupportErr
	}
	return nil
}

// TODO: rewrite
func (idx *simpleTableIndex) BatchDedup(col *gvec.Vector) error {
	idx.RLock()
	defer idx.RUnlock()
	vals := col.Col
	switch col.Typ.Oid {
	case types.T_int8:
		data := vals.([]int8)
		for _, v := range data {
			if _, ok := idx.tree[v]; ok {
				return ErrDuplicated
			}
		}
	case types.T_int16:
		data := vals.([]int16)
		for _, v := range data {
			if _, ok := idx.tree[v]; ok {
				return ErrDuplicated
			}
		}
	case types.T_int32:
		data := vals.([]int32)
		for _, v := range data {
			if _, ok := idx.tree[v]; ok {
				return ErrDuplicated
			}
		}
	case types.T_int64:
		data := vals.([]int64)
		for _, v := range data {
			if _, ok := idx.tree[v]; ok {
				return ErrDuplicated
			}
		}
	case types.T_uint8:
		data := vals.([]uint8)
		for _, v := range data {
			if _, ok := idx.tree[v]; ok {
				return ErrDuplicated
			}
		}
	case types.T_uint16:
		data := vals.([]uint16)
		for _, v := range data {
			if _, ok := idx.tree[v]; ok {
				return ErrDuplicated
			}
		}
	case types.T_uint32:
		data := vals.([]uint32)
		for _, v := range data {
			if _, ok := idx.tree[v]; ok {
				return ErrDuplicated
			}
		}
	case types.T_uint64:
		data := vals.([]uint64)
		for _, v := range data {
			if _, ok := idx.tree[v]; ok {
				return ErrDuplicated
			}
		}
	case types.T_decimal:
		data := vals.([]types.Decimal)
		for _, v := range data {
			if _, ok := idx.tree[v]; ok {
				return ErrDuplicated
			}
		}
	case types.T_float32:
		data := vals.([]float32)
		for _, v := range data {
			if _, ok := idx.tree[v]; ok {
				return ErrDuplicated
			}
		}
	case types.T_float64:
		data := vals.([]float64)
		for _, v := range data {
			if _, ok := idx.tree[v]; ok {
				return ErrDuplicated
			}
		}
	case types.T_date:
		data := vals.([]types.Date)
		for _, v := range data {
			if _, ok := idx.tree[v]; ok {
				return ErrDuplicated
			}
		}
	case types.T_datetime:
		data := vals.([]types.Datetime)
		for _, v := range data {
			if _, ok := idx.tree[v]; ok {
				return ErrDuplicated
			}
		}
	case types.T_char, types.T_varchar, types.T_json:
		data := vals.(*types.Bytes)
		// bytes := make([]string, 0, len(data.Lengths))
		for i, s := range data.Offsets {
			e := s + data.Lengths[i]
			v := string(data.Data[s:e])
			// bytes = append(bytes, v)
			if _, ok := idx.tree[v]; ok {
				return ErrDuplicated
			}
		}
	default:
		return vector.VecTypeNotSupportErr
	}
	return nil
}
