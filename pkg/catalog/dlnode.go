package catalog

type Link struct {
	head *DLNode
	tail *DLNode
}

func (l *Link) GetHead() *DLNode {
	return l.head
}

func (l *Link) GetTail() *DLNode {
	return l.tail
}

func (l *Link) Insert(payload NodePayload) *DLNode {
	var (
		n    *DLNode
		tail *DLNode
	)
	n, l.head, tail = InsertDLNode(payload, l.head)
	if tail != nil {
		l.tail = tail
	}
	return n
}

func (l *Link) Delete(n *DLNode) {
	prev := n.prev
	next := n.next
	if prev != nil && next != nil {
		prev.next = next
		next.prev = prev
	} else if prev == nil && next != nil {
		l.head = next
		next.prev = nil
	} else if prev != nil && next == nil {
		l.tail = prev
		prev.next = nil
	} else {
		l.head = nil
		l.tail = nil
	}
}

func (l *Link) Loop(fn func(n *DLNode) bool, reverse bool) {
	if reverse {
		LoopDLink(l.tail, fn, reverse)
	} else {
		LoopDLink(l.head, fn, reverse)
	}
}

type NodePayload interface {
	Compare(NodePayload) int
}

type DLNode struct {
	prev, next *DLNode
	payload    NodePayload
}

func (l *DLNode) Compare(o *DLNode) int {
	return l.payload.Compare(o.payload)
}

func (l *DLNode) Sort() (*DLNode, *DLNode) {
	curr := l
	head := curr
	prev := l.prev
	next := l.next
	var tail *DLNode
	for (curr != nil && next != nil) && (curr.Compare(next) < 0) {
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
	if next == nil {
		tail = curr
	}
	return head, tail
}

func InsertDLNode(payload NodePayload, head *DLNode) (node, nhead *DLNode, ntail *DLNode) {
	node = &DLNode{
		payload: payload,
	}
	if head == nil {
		nhead = node
		ntail = node
		return
	}

	node.next = head
	head.prev = node
	nhead, ntail = node.Sort()
	return
}

func FindHead(n *DLNode) *DLNode {
	head := n
	for head.prev != nil {
		head = head.prev
	}
	return head
}

func LoopDLink(head *DLNode, fn func(node *DLNode) bool, reverse bool) {
	curr := head
	for curr != nil {
		goNext := fn(curr)
		if !goNext {
			break
		}
		if reverse {
			curr = curr.prev
		} else {
			curr = curr.next
		}
	}
}
