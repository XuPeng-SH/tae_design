package updates

import (
	"tae/pkg/catalog"
	com "tae/pkg/common"
)

type BlockUpdateNode struct {
	*com.DLNode
	*BlockUpdates
	chain *BlockUpdateChain
}

func NewBlockUpdateNode(chain *BlockUpdateChain, blkupdates *BlockUpdates) *BlockUpdateNode {
	// dlNode := chain.Insert(blkupdates)
	node := &BlockUpdateNode{
		chain:        chain,
		BlockUpdates: blkupdates,
	}
	node.DLNode = chain.Insert(node)
	return node
}

func (node *BlockUpdateNode) GetMeta() *catalog.BlockEntry { return node.chain.GetMeta() }
func (node *BlockUpdateNode) GetChain() *BlockUpdateChain  { return node.chain }

func (n *BlockUpdateNode) Compare(o com.NodePayload) int {
	return n.BlockUpdates.Compare(o.(*BlockUpdateNode).BlockUpdates)
}

func (n *BlockUpdateNode) PrepareCommit() (err error) {
	n.chain.Lock()
	defer n.chain.Unlock()
	if err = n.BlockUpdates.PrepareCommit(); err != nil {
		return err
	}
	n.chain.Update(n.DLNode)
	return
}
