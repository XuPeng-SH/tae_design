package catalog

import (
	"fmt"
	"sync"
	"tae/pkg/common"
	"tae/pkg/iface"

	"github.com/google/btree"
)

type nodeList struct {
	common.SSLLNode
	host     interface{}
	rwlocker *sync.RWMutex
	name     string
}

func newNodeList(host interface{}, rwlocker *sync.RWMutex, name string) *nodeList {
	return &nodeList{
		SSLLNode: *common.NewSSLLNode(),
		host:     host,
		rwlocker: rwlocker,
		name:     name,
	}
}

func (n *nodeList) Less(item btree.Item) bool {
	return n.name < item.(*nodeList).name
}

func (n *nodeList) CreateNode(id uint64) *nameNode {
	nn := newNameNode(n.host, id)
	n.rwlocker.Lock()
	defer n.rwlocker.Unlock()
	n.Insert(nn)
	return nn
}

func (n *nodeList) DeleteNode(id uint64) (deleted *nameNode, empty bool) {
	n.rwlocker.Lock()
	defer n.rwlocker.Unlock()
	var prev common.ISSLLNode
	prev = n
	curr := n.GetNext()
	depth := 0
	for curr != nil {
		nid := curr.(*nameNode).Id
		if id == nid {
			prev.ReleaseNextNode()
			deleted = curr.(*nameNode)
			next := curr.GetNext()
			if next == nil && depth == 0 {
				empty = true
			}
			break
		}
		prev = curr
		curr = curr.GetNext()
		depth++
	}
	return
}

func (n *nodeList) ForEachNodes(fn func(*nameNode) bool) {
	n.rwlocker.RLock()
	defer n.rwlocker.RUnlock()
	n.ForEachNodesLocked(fn)
}

func (n *nodeList) ForEachNodesLocked(fn func(*nameNode) bool) {
	curr := n.GetNext()
	for curr != nil {
		nn := curr.(*nameNode)
		if ok := fn(nn); !ok {
			break
		}
		curr = curr.GetNext()
	}
}

func (n *nodeList) LengthLocked() int {
	length := 0
	fn := func(*nameNode) bool {
		length++
		return true
	}
	n.ForEachNodesLocked(fn)
	return length
}

func (n *nodeList) Length() int {
	n.rwlocker.RLock()
	defer n.rwlocker.RUnlock()
	return n.LengthLocked()
}

// func (n *nodeList) GetTable() *Table {
// 	n.rwlocker.RLock()
// 	defer n.rwlocker.RUnlock()
// 	return n.GetNext().(*nameNode).GetTable()
// }

func (n *nodeList) GetDBNode() *DLNode {
	n.rwlocker.RLock()
	defer n.rwlocker.RUnlock()
	return n.GetNext().(*nameNode).GetDBNode()
}

func (n *nodeList) TxnGetDBNodeLocked(txnCtx iface.TxnReader) *DLNode {
	var dn *DLNode
	fn := func(nn *nameNode) bool {
		dlNode := nn.GetDBNode()
		entry := dlNode.payload.(*DBEntry)
		if entry.IsSameTxn(txnCtx.GetStartTS()) {
			dn = dlNode
			return false
		} else {
			if entry.CreateAt != 0 && entry.CreateAt <= txnCtx.GetStartTS() {
				dn = dlNode
				return false
			}
		}
		return true
	}
	n.ForEachNodes(fn)
	return dn
}

func (n *nodeList) PString(level PPLevel) string {
	curr := n.GetNext()
	if curr == nil {
		return fmt.Sprintf("TableNode[\"%s\"](Len=0)", n.name)
	}
	node := curr.(*nameNode)
	s := fmt.Sprintf("TableNode[\"%s\"](Len=%d)->[%d", n.name, n.Length(), node.Id)
	if level == PPL0 {
		s = fmt.Sprintf("%s]", s)
		return s
	}

	curr = curr.GetNext()
	for curr != nil {
		node := curr.(*nameNode)
		s = fmt.Sprintf("%s->%d", s, node.Id)
		curr = curr.GetNext()
	}
	s = fmt.Sprintf("%s]", s)
	return s
}

type nameNode struct {
	common.SSLLNode
	Id   uint64
	host interface{}
}

func newNameNode(host interface{}, id uint64) *nameNode {
	return &nameNode{
		Id:       id,
		SSLLNode: *common.NewSSLLNode(),
		host:     host,
	}
}

func (n *nameNode) GetDBNode() *DLNode {
	if n == nil {
		return nil
	}
	return n.host.(*Catalog).entries[n.Id]
}

// func (n *nameNode) GetTable() *Table {
// 	if n == nil {
// 		return nil
// 	}
// 	return n.host.(*DBEntry).TableSet[n.Id]
// }
