package txnbase

import (
	"tae/pkg/buffer/base"

	"github.com/jiangxinmeng1/logstore/pkg/entry"
)

const (
	MaxNodeRows uint32 = 10000
)

type NodeEntry entry.Entry

type NodeState = int32

const (
	TransientNode NodeState = iota
	PersistNode
)

type NodeType int8
type Node interface {
	base.INode
	Type() NodeType
	ToTransient()
	Close() error
}
