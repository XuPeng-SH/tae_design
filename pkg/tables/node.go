package tables

import (
	"tae/pkg/catalog"
	"tae/pkg/dataio"
	"tae/pkg/iface/txnif"

	gbat "github.com/matrixorigin/matrixone/pkg/container/batch"
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

func (node *appendableNode) ApplyAppend(bat *gbat.Batch, offset, length uint32, ctx interface{}) (err error) {
	node.rows += length
	return
}
