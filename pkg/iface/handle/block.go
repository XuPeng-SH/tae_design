package handle

import (
	"io"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/common"
)

type BlockIt interface {
	Iterator
	GetBlock() Block
}

type FilterOp int16

const (
	FilterEq FilterOp = iota
	FilterBtw
)

type Filter struct {
	Op  FilterOp
	Col *vector.Vector
}

type BlockReader interface {
	io.Closer
	ID() uint64
	String() string
	GetByFilter(filter Filter, offsetOnly bool) (*batch.Batch, error)
	GetBatch(ctx interface{}) (*batch.Batch, error)
	GetMeta() interface{}
	Fingerprint() *common.ID
}

type BlockWriter interface {
	io.Closer
	String() string
	Append(data *batch.Batch, offset uint32) (uint32, error)
	Update(row uint32, col uint16, v interface{}) error
	RangeDelete(start, end uint32) error

	PushDeleteOp(filter Filter) error
	PushUpdateOp(filter Filter, attr string, val interface{}) error
}

type Block interface {
	BlockReader
	BlockWriter
}
