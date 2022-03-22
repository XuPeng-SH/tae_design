package txn

import (
	"fmt"
	"testing"
	"time"

	"github.com/RoaringBitmap/roaring"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/common"
	"github.com/stretchr/testify/assert"
)

type testUpdateNode struct {
	updates  *blockUpdates
	startTs  uint64
	commitTs uint64
	next     *testUpdateNode
	prev     *testUpdateNode
	ntype    int8
	id       *common.ID
}

func newTestUpdateNode(ntype int8, id *common.ID, start uint64, deletes *roaring.Bitmap) *testUpdateNode {
	return &testUpdateNode{
		id:       id,
		ntype:    ntype,
		startTs:  start,
		commitTs: ^uint64(0),
		updates:  NewBlockUpdates(id, nil, nil, deletes),
	}
}

func (update *testUpdateNode) repr() string {
	commitState := "C"
	if !update.hasCommitted() {
		commitState = "UC"
	}
	ntype := "Txn"
	if update.ntype == 1 {
		ntype = "Merge"
	}
	nextStr := "nil"
	if update.next != nil {
		nextStr = fmt.Sprintf("%s", update.next.id.BlockString())
	}
	s := fmt.Sprintf("[%s:%s:%s](%d-%d)->%s", ntype, commitState, update.id.BlockString(), update.startTs, update.commitTs, nextStr)
	return s
}

func (update *testUpdateNode) hasCommitted() bool {
	return update.commitTs != ^uint64(0)
}

func (update *testUpdateNode) isMergedNode() bool {
	return update.ntype == 1
}

func (update *testUpdateNode) less(o *testUpdateNode) bool {
	if update.hasCommitted() && !o.hasCommitted() {
		return true
	}
	if !update.hasCommitted() && o.hasCommitted() {
		return false
	}
	if update.hasCommitted() && o.hasCommitted() {
		if update.commitTs < o.commitTs {
			return true
		} else if update.commitTs > o.commitTs {
			return false
		}
		if o.isMergedNode() {
			return true
		}
		return false
	}
	return update.startTs < o.startTs
}

func (update *testUpdateNode) commit(ts uint64) {
	if update.hasCommitted() {
		panic("not expected")
	}
	if ts <= update.startTs || ts == ^uint64(0) {
		panic("not expected")
	}
	update.commitTs = ts
}

func sortNodes(node *testUpdateNode) *testUpdateNode {
	curr := node
	head := curr
	prev := node.prev
	next := node.next
	for (curr != nil && next != nil) && curr.less(next) {
		if head == curr {
			head = next
		}
		if prev != nil {
			prev.next = next
		}
		next.prev = prev

		prev = next
		next = next.next

		prev.next = curr
		curr.prev = prev

		curr.next = next
		if next != nil {
			next.prev = curr
		}
	}
	return head
}

func insertLink(node, head *testUpdateNode) *testUpdateNode {
	if head == nil {
		head = node
		return head
	}
	node.next = head
	head.prev = node
	head = sortNodes(node)
	return head
}

func loopLink(t *testing.T, head *testUpdateNode, fn func(node *testUpdateNode)) {
	curr := head
	for curr != nil {
		fn(curr)
		curr = curr.next
	}
}

func TestUpdates(t *testing.T) {
	id := common.ID{}
	committed := 10
	nodes := make([]*testUpdateNode, 0)
	var head *testUpdateNode
	for i := 0; i < committed; i++ {
		nid := id.Next()
		node := newTestUpdateNode(0, nid, uint64(committed-i)*10, nil)
		head = insertLink(node, head)
		nodes = append(nodes, node)
	}

	loopLink(t, head, func(node *testUpdateNode) {
		t.Log(node.repr())
	})
	now := time.Now()
	commitTs := (committed + 1) * 10
	mergeIdx := len(nodes) / 2
	for i := len(nodes) - 1; i >= 0; i-- {
		nodes[i].commit(uint64(commitTs + committed - i))
		// nodes[i].commit(uint64(commitTs + i))
		sortNodes(nodes[i])
	}

	mergeNode := newTestUpdateNode(1, nodes[mergeIdx].id, nodes[mergeIdx].startTs, nil)
	mergeNode.commit(nodes[mergeIdx].commitTs)

	head = nodes[0]
	for head.prev != nil {
		head = head.prev
	}

	insertLink(mergeNode, head)

	loopLink(t, head, func(node *testUpdateNode) {
		t.Log(node.repr())
	})
	t.Log(time.Since(now))
	assert.Equal(t, mergeNode.next, nodes[mergeIdx])
}
