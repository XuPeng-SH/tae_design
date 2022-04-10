package updates

import (
	"tae/pkg/catalog"
	com "tae/pkg/common"
)

type BlockUpdateNode struct {
	*com.DLNode
	blkupdates *BlockUpdates
	chain      *BlockUpdateChain
}

func NewBlockUpdateNode(chain *BlockUpdateChain, blkupdates *BlockUpdates) *BlockUpdateNode {
	dlNode := chain.Insert(blkupdates)
	return &BlockUpdateNode{
		DLNode:     dlNode,
		chain:      chain,
		blkupdates: blkupdates,
	}
}

func (node *BlockUpdateNode) GetMeta() *catalog.BlockEntry { return node.chain.GetMeta() }
func (node *BlockUpdateNode) GetChain() *BlockUpdateChain  { return node.chain }
func (node *BlockUpdateNode) GetUpdates() *BlockUpdates    { return node.blkupdates }
