package iface

import (
	"tae/pkg/iface/txnif"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
)

type StateMachine interface {
	Append(id uint64, data *batch.Batch, txn txnif.AsyncTxn) error
	BatchDedup(id uint64, col *vector.Vector, txn txnif.AsyncTxn) error
}
