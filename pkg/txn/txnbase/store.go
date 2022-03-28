package txnbase

import (
	"tae/pkg/iface/txnif"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
)

var NoopStoreFactory = func() txnif.TxnStore { return new(noopTxnStore) }

type noopTxnStore struct{}

func (store *noopTxnStore) Close() error                                            { return nil }
func (store *noopTxnStore) RangeDeleteLocalRows(id uint64, start, end uint32) error { return nil }
func (store *noopTxnStore) Append(id uint64, data *batch.Batch) error               { return nil }
func (store *noopTxnStore) UpdateLocalValue(id uint64, row uint32, col uint16, v interface{}) error {
	return nil
}
func (store *noopTxnStore) AddUpdateNode(id uint64, node txnif.BlockUpdates) error { return nil }
