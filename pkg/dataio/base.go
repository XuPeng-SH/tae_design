package dataio

import (
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/container/batch"
)

type NoopSegmentFile struct{}

func (sf *NoopSegmentFile) Close() (err error)   { return }
func (sf *NoopSegmentFile) IsAppendable() bool   { return true }
func (sf *NoopSegmentFile) IsSorted() bool       { return false }
func (sf *NoopSegmentFile) Destory() (err error) { return }

func (sf *NoopSegmentFile) GetBlockFile(uint64) (bf BlockFile) { return }

type NoopBlockFile struct{}

func (bf *NoopBlockFile) Close() (err error)   { return }
func (bf *NoopBlockFile) Destory() (err error) { return }

func (bf *NoopBlockFile) IsSorted() bool     { return false }
func (bf *NoopBlockFile) IsAppendable() bool { return true }
func (bf *NoopBlockFile) Rows() uint32       { return 0 }

func (bf *NoopBlockFile) GetSegmentFile() (sf SegmentFile)        { return }
func (bf *NoopBlockFile) WriteData(batch.IBatch) (err error)      { return }
func (bf *NoopBlockFile) LoadData() (bat batch.IBatch, err error) { return }
func (bf *NoopBlockFile) Sync() (err error)                       { return }
func (bf *NoopBlockFile) MaxTS() uint64                           { return 0 }

func (bf *NoopBlockFile) GetColumnInfo(attr string) (info common.FileInfo) { return }
