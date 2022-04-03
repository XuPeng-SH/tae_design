package txnimpl

import (
	"github.com/jiangxinmeng1/logstore/pkg/entry"
)

const (
	ETInsertNode = entry.ETCustomizedStart + 1 + iota
	ETTxnRecord
)
