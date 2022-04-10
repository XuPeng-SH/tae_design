package tables

import (
	"tae/pkg/catalog"
	"tae/pkg/dataio"
	"tae/pkg/iface/data"

	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/mutation/buffer/base"
)

type dataTable struct {
	meta        *catalog.TableEntry
	aSeg        data.Segment
	fileFactory dataio.SegmentFileFactory
	bufMgr      base.INodeManager
}

func newTable(meta *catalog.TableEntry, fileFactory dataio.SegmentFileFactory, bufMgr base.INodeManager) *dataTable {
	return &dataTable{
		meta:        meta,
		fileFactory: fileFactory,
		bufMgr:      bufMgr,
	}
}

func (table *dataTable) HasAppendableSegment() bool {
	if table.aSeg == nil {
		return false
	}
	return table.aSeg.IsAppendable()
}

func (table *dataTable) GetAppender() (id *common.ID, appender data.BlockAppender, err error) {
	if table.aSeg == nil {
		err = data.ErrAppendableSegmentNotFound
		return
	}
	return table.aSeg.GetAppender()
}

func (table *dataTable) setAppendableSegment(id uint64) {
	if seg, err := table.meta.GetSegmentByID(id); err != nil {
		panic(err)
	} else {
		table.aSeg = seg.GetSegmentData()
	}
}

func (table *dataTable) SetAppender(id *common.ID) (appender data.BlockAppender, err error) {
	if table.aSeg == nil || table.aSeg.GetID() != id.SegmentID {
		table.setAppendableSegment(id.SegmentID)
		_, appender, err = table.aSeg.GetAppender()
		if err == nil {
			return
		}
	}
	return table.aSeg.SetAppender(id.BlockID)
}
