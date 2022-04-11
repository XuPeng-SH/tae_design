package data

import (
	"io"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/common"
)

type BlockAppender interface {
	io.Closer
	GetID() *common.ID
	PrepareAppend(rows uint32) (n uint32, err error)
	ApplyAppend(bat *batch.Batch, offset, length uint32, ctx interface{}) (uint32, error)
}

type Block interface {
	MakeAppender() (BlockAppender, error)
	IsAppendable() bool
	// CopyBatch(cs []uint64, attrs []string, compressed []*bytes.Buffer, deCompressed []*bytes.Buffer) (*batch.Batch, error)
}
