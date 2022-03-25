package iface

import (
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
)

type StateMachine interface {
	Append(id uint64, data *batch.Batch, txn AsyncTxn) error
	BatchDedup(id uint64, col *vector.Vector, txn AsyncTxn) error
}
