package tables

import (
	"tae/pkg/catalog"
	"tae/pkg/dataio"
	"tae/pkg/iface/data"

	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/mutation/buffer/base"
)

func OpenSegmenFile(dir string, id common.ID) (file dataio.SegmentFile, err error) {
	return
}

type dataSegment struct {
	meta   *catalog.SegmentEntry
	file   dataio.SegmentFile
	aBlk   data.Block
	bufMgr base.INodeManager
}

func newSegment(meta *catalog.SegmentEntry, factory dataio.SegmentFileFactory, bufMgr base.INodeManager) *dataSegment {
	segFile := factory("xxx", meta.GetID())
	// TODO:
	// 1. Open segment file
	// 2. Read segment file meta and reset segment and block meta
	seg := &dataSegment{
		meta:   meta,
		file:   segFile,
		bufMgr: bufMgr,
	}
	return seg
}

func (segment *dataSegment) GetID() uint64 { return segment.meta.GetID() }

func (segment *dataSegment) GetAppender() (id *common.ID, appender data.BlockAppender, err error) {
	id = segment.meta.AsCommonID()
	if segment.aBlk == nil {
		if !segment.file.IsAppendable() {
			err = data.ErrNotAppendable
			return
		}
		return
	}
	appender, err = segment.aBlk.MakeAppender()
	if err != nil && segment.file.IsAppendable() {
		err = nil
	}
	return
}

func (segment *dataSegment) SetAppender(blkId uint64) (appender data.BlockAppender, err error) {
	blk, err := segment.meta.GetBlockEntryByID(blkId)
	if err != nil {
		panic(err)
	}
	// TODO: Push to flush queue
	if segment.aBlk != nil {
	}
	segment.aBlk = newBlock(blk, segment.file, segment.bufMgr)
	appender, err = segment.aBlk.MakeAppender()
	return
}
