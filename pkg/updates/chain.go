package updates

import (
	"sync"
	"tae/pkg/catalog"
	com "tae/pkg/common"
	"tae/pkg/iface/txnif"
)

type BlockUpdateChain struct {
	*com.Link
	*sync.RWMutex
	rwlocker *sync.RWMutex
	meta     *catalog.BlockEntry
}

func NewUpdateChain(rwlocker *sync.RWMutex, meta *catalog.BlockEntry) *BlockUpdateChain {
	if rwlocker == nil {
		rwlocker = new(sync.RWMutex)
	}
	return &BlockUpdateChain{
		Link:    new(com.Link),
		RWMutex: rwlocker,
		meta:    meta,
	}
}

func (chain *BlockUpdateChain) GetMeta() *catalog.BlockEntry { return chain.meta }

func (chain *BlockUpdateChain) AddNode(txn txnif.AsyncTxn) *BlockUpdateNode {
	// TODO: scan chain and fill base deletes and updates
	updates := NewBlockUpdates(txn, chain.meta, nil, nil)
	node := NewBlockUpdateNode(chain, updates)
	return node
}

func (chain *BlockUpdateChain) AddMergeNode() *BlockUpdateNode {
	chain.RLock()
	defer chain.RUnlock()
	var merge *BlockUpdates
	chain.LoopChainLocked(func(updates *BlockUpdates) bool {
		updates.RLock()
		if updates.GetCommitTSLocked() == txnif.UncommitTS {
			updates.RUnlock()
			return true
		}
		if merge == nil {
			merge = NewMergeBlockUpdates(updates.GetCommitTSLocked(), chain.meta, nil, nil)
		}
		merge.MergeLocked(updates)
		ret := true
		if updates.IsMerge() {
			ret = false
		}
		updates.RUnlock()
		return ret
	}, false)
	if merge == nil {
		return nil
	}
	node := NewBlockUpdateNode(chain, merge)
	return node
}

func (chain *BlockUpdateChain) LoopChainLocked(fn func(updates *BlockUpdates) bool, reverse bool) {
	wrapped := func(node *com.DLNode) bool {
		updates := node.GetPayload().(*BlockUpdates)
		return fn(updates)
	}
	chain.Loop(wrapped, reverse)
}
