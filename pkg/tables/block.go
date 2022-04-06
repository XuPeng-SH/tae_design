package tables

import (
	"tae/pkg/catalog"
	"tae/pkg/dataio"
	"tae/pkg/iface/data"
	"tae/pkg/iface/txnif"

	gbat "github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/container/batch"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/mutation/buffer"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/mutation/buffer/base"
)

type appendableNode struct {
	*buffer.Node
	file dataio.BlockFile
	meta *catalog.BlockEntry
	data batch.IBatch
	rows uint32
	mgr  base.INodeManager
}

func newNode(mgr base.INodeManager, meta *catalog.BlockEntry, file dataio.BlockFile) *appendableNode {
	impl := new(appendableNode)
	id := meta.AsCommonID()
	impl.Node = buffer.NewNode(impl, mgr, *id, 0)
	impl.UnloadFunc = impl.OnUnload
	impl.LoadFunc = impl.OnLoad
	impl.DestroyFunc = impl.OnDestory
	impl.file = file
	impl.mgr = mgr
	impl.meta = meta
	mgr.RegisterNode(impl)
	return impl
}

func (node *appendableNode) Rows(txn txnif.AsyncTxn, coarse bool) uint32 {
	if coarse {
		return node.rows
	}
	// TODO: fine row count
	// 1. Load txn ts zonemap
	// 2. Calculate fine row count
	return 0
}

func (node *appendableNode) OnDestory() {
	if err := node.file.Destory(); err != nil {
		panic(err)
	}
}

func (node *appendableNode) OnLoad() {
	var err error
	if node.data, err = node.file.LoadData(); err != nil {
		panic(err)
	}
}

func (node *appendableNode) OnUnload() {
	if err := node.file.WriteData(node.data, nil, nil); err != nil {
		panic(err)
	}
	if err := node.file.Sync(); err != nil {
		panic(err)
	}
}

func (node *appendableNode) PrepareAppend(rows uint32) (n uint32, err error) {
	left := node.meta.GetSegment().GetTable().GetSchema().BlockMaxRows - node.rows
	if left == 0 {
		return
	}
	if rows > left {
		n = left
	} else {
		n = rows
	}
	return
	// key = txnbase.KeyEncoder.EncodeBlock(
	// 	node.meta.GetSegment().GetTable().GetDB().GetID(),
	// 	node.meta.GetSegment().GetTable().GetID(),
	// 	node.meta.GetSegment().GetID(),
	// 	node.meta.GetID(),
	// )
}

type blockAppender struct {
	node   *appendableNode
	handle base.INodeHandle
}

func newAppender(node *appendableNode) *blockAppender {
	appender := new(blockAppender)
	appender.node = node
	appender.handle = node.mgr.Pin(node)
	return appender
}

func (appender *blockAppender) Close() error {
	if appender.handle != nil {
		appender.handle.Close()
		appender.handle = nil
	}
	return nil
}

func (appender *blockAppender) GetID() *common.ID {
	return appender.node.meta.AsCommonID()
}

func (appender *blockAppender) PrepareAppend(rows uint32) (n uint32, err error) {
	return appender.node.PrepareAppend(rows)
}

func (appender *blockAppender) ApplyAppend(bat *gbat.Batch, offset, length uint32, ctx interface{}) (err error) {
	return appender.node.Expand(uint64(length*20), func() error {
		return nil
	})
}

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
