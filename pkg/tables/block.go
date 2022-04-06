package tables

import (
	"tae/pkg/catalog"
	"tae/pkg/dataio"
	"tae/pkg/iface/data"
	"tae/pkg/iface/txnif"

	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/mutation/buffer/base"
)

type dataBlock struct {
	meta   *catalog.BlockEntry
	node   *appendableNode
	file   dataio.BlockFile
	bufMgr base.INodeManager
}

func newBlock(meta *catalog.BlockEntry, segFile dataio.SegmentFile, bufMgr base.INodeManager) *dataBlock {
	file := segFile.GetBlockFile(meta.GetID())
	var node *appendableNode
	if meta.IsAppendable() {
		node = newNode(bufMgr, meta, file)
	}
	return &dataBlock{
		meta: meta,
		file: file,
		node: node,
	}
}

func (blk *dataBlock) IsAppendable() bool {
	if !blk.meta.IsAppendable() {
		return false
	}
	if blk.node.Rows(nil, true) == blk.meta.GetSegment().GetTable().GetSchema().BlockMaxRows {
		return false
	}
	return true
}

func (blk *dataBlock) Rows(txn txnif.AsyncTxn, coarse bool) uint32 {
	if blk.IsAppendable() {
		return blk.node.Rows(txn, coarse)
	}
	return blk.file.Rows()
}

func (blk *dataBlock) MakeAppender() (appender data.BlockAppender, err error) {
	if !blk.IsAppendable() {
		err = data.ErrNotAppendable
		return
	}
	appender = newAppender(blk.node)
	return
}
