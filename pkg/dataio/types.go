package dataio

import (
	"io"

	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/common"

	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/container/batch"
)

type SegmentFileFactory = func(dir string, id uint64) SegmentFile

type SegmentFile interface {
	io.Closer
	IsAppendable() bool
	IsSorted() bool
	Destory() error
	GetBlockFile(uint64) BlockFile
}

type BlockFile interface {
	io.Closer
	Destory() error
	GetColumnInfo(attr string) common.FileInfo
	IsSorted() bool
	IsAppendable() bool
	// FileType() common.FileType
	Rows() uint32
	GetSegmentFile() SegmentFile
	WriteData(batch.IBatch) error
	LoadData() (batch.IBatch, error)
	Sync() error
	MaxTS() uint64
}
