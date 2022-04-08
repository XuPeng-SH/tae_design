package updates

import (
	"bytes"
	"fmt"
	"tae/pkg/catalog"
	"tae/pkg/txn/txnbase"
	"testing"
	"time"

	"github.com/RoaringBitmap/roaring"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/common"
	"github.com/stretchr/testify/assert"
)

type testUpdateNode struct {
	updates  *BlockUpdates
	startTs  uint64
	commitTs uint64
	next     *testUpdateNode
	prev     *testUpdateNode
	ntype    int8
	id       *common.ID
}

func newTestUpdateNode(ntype int8, schema *catalog.Schema, id *common.ID, start uint64, deletes *roaring.Bitmap) *testUpdateNode {
	return &testUpdateNode{
		id:       id,
		ntype:    ntype,
		startTs:  start,
		commitTs: ^uint64(0),
		updates:  NewBlockUpdates(id, schema, nil, deletes),
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

func loopLink(t *testing.T, head *testUpdateNode, fn func(node *testUpdateNode) bool) {
	curr := head
	for curr != nil {
		goNext := fn(curr)
		if !goNext {
			break
		}
		curr = curr.next
	}
}

func findHead(n *testUpdateNode) *testUpdateNode {
	head := n
	for head.prev != nil {
		head = head.prev
	}
	return head
}

func TestUpdatesMerge(t *testing.T) {
	id := common.ID{}
	var head *testUpdateNode
	cnt1 := 11
	cnt2 := 10
	nodes := make([]*testUpdateNode, 0)
	schema := catalog.MockSchema(1)
	for i := 0; i < cnt1+cnt2; i++ {
		nid := id.Next()
		start := uint64(i) * 2
		node := newTestUpdateNode(0, schema, nid, uint64(i)*2, nil)
		node.updates.DeleteLocked(uint32(i)*10, uint32(i+1)*10-1)
		err := node.updates.UpdateLocked(uint32(i+1)*10000, 0, (i+1)*10000)
		if i < cnt1 {
			node.commit(start + 1)
		} else {
			nodes = append(nodes, node)
		}
		assert.Nil(t, err)
		head = insertLink(node, head)
	}

	makeMerge := func() *testUpdateNode {
		var merge *testUpdateNode
		loopLink(t, head, func(n *testUpdateNode) bool {
			if n.isMergedNode() {
				return false
			}
			if !n.hasCommitted() {
				return true
			}
			if merge == nil {
				merge = newTestUpdateNode(1, schema, n.id, n.startTs, nil)
				merge.commit(n.commitTs)
			}
			merge.updates.MergeLocked(n.updates)
			return true
		})
		return merge
	}

	merge := makeMerge()
	insertLink(merge, head)
	t.Log(merge.updates.localDeletes.String())
	assert.Equal(t, cnt1*10, int(merge.updates.localDeletes.GetCardinality()))
	assert.Equal(t, cnt1, int(merge.updates.cols[0].txnMask.GetCardinality()))

	commitTs := head.startTs + uint64(100)
	for i := len(nodes) - 1; i >= 0; i-- {
		n := nodes[i]
		if n.hasCommitted() {
			continue
		}
		n.commit(commitTs)
		commitTs++
		sortNodes(n)
	}

	head = findHead(head)

	cnt := 0
	loopLink(t, head, func(n *testUpdateNode) bool {
		if n.hasCommitted() && !n.isMergedNode() {
			cnt++
		}
		return true
	})
	assert.Equal(t, cnt1+cnt2, cnt)

	merge = makeMerge()
	head = findHead(head)
	insertLink(merge, head)
	head = findHead(head)
	loopLink(t, head, func(n *testUpdateNode) bool {
		t.Log(n.repr())
		return true
	})
	assert.Equal(t, cnt2*10, int(merge.updates.localDeletes.GetCardinality()))
	assert.Equal(t, cnt2, int(merge.updates.cols[0].txnMask.GetCardinality()))
}

func TestUpdates(t *testing.T) {
	id := common.ID{}
	committed := 10
	nodes := make([]*testUpdateNode, 0)
	schema := catalog.MockSchema(1)
	var head *testUpdateNode
	for i := 0; i < committed; i++ {
		nid := id.Next()
		node := newTestUpdateNode(0, schema, nid, uint64(committed-i)*10, nil)
		head = insertLink(node, head)
		nodes = append(nodes, node)
	}

	head = findHead(head)
	loopLink(t, head, func(node *testUpdateNode) bool {
		t.Log(node.repr())
		return true
	})
	now := time.Now()
	commitTs := (committed + 1) * 10
	mergeIdx := len(nodes) / 2
	for i := len(nodes) - 1; i >= 0; i-- {
		nodes[i].commit(uint64(commitTs + committed - i))
		// nodes[i].commit(uint64(commitTs + i))
		sortNodes(nodes[i])
	}

	mergeNode := newTestUpdateNode(1, schema, nodes[mergeIdx].id, nodes[mergeIdx].startTs, nil)
	mergeNode.commit(nodes[mergeIdx].commitTs)

	head = findHead(nodes[0])

	insertLink(mergeNode, head)

	loopLink(t, head, func(node *testUpdateNode) bool {
		t.Log(node.repr())
		return true
	})
	t.Log(time.Since(now))
	assert.Equal(t, mergeNode.next, nodes[mergeIdx])

	var w bytes.Buffer
	err := mergeNode.updates.WriteTo(&w)
	assert.Nil(t, err)

	m1 := mergeNode.updates
	buf := w.Bytes()
	r := bytes.NewBuffer(buf)
	m2 := NewBlockUpdates(nil, nil, nil, nil)
	err = m2.ReadFrom(r)
	assert.Equal(t, *m1.id, *m2.id)
	// assert.True(t, m1.localDeletes.Equals(m2.localDeletes))
	assert.Equal(t, len(m1.cols), len(m2.cols))

	cmd, _, _ := mergeNode.updates.MakeCommand(1, false)
	w.Reset()
	err = cmd.WriteTo(&w)
	assert.Nil(t, err)

	buf = w.Bytes()
	r = bytes.NewBuffer(buf)

	cmd2, err := txnbase.BuildCommandFrom(r)
	assert.Nil(t, err)
	assert.NotNil(t, cmd2)
}
