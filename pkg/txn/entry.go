package txn

import (
	"github.com/jiangxinmeng1/logstore/pkg/entry"
)

type NodeEntry entry.Entry

const (
	ETInsertNode = entry.ETCustomizedStart + 1
)
