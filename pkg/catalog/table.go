package catalog

import (
	"fmt"
	"sync"
	"tae/pkg/common"
	"tae/pkg/iface/txnif"
)

type TableEntry struct {
	*BaseEntry2
	db      *DBEntry
	schema  *Schema
	entries map[uint64]*DLNode
	link    *Link
}

func NewTableEntry(db *DBEntry, schema *Schema, txnCtx txnif.AsyncTxn) *TableEntry {
	id := db.catalog.NextTable()
	e := &TableEntry{
		BaseEntry2: &BaseEntry2{
			CommitInfo2: CommitInfo2{
				Txn:    txnCtx,
				CurrOp: OpCreate,
			},
			RWMutex: new(sync.RWMutex),
			ID:      id,
		},
		db:     db,
		schema: schema,
	}
	return e
}

func MockStaloneTableEntry(id uint64, schema *Schema) *TableEntry {
	return &TableEntry{
		BaseEntry2: &BaseEntry2{
			RWMutex: new(sync.RWMutex),
			ID:      id,
		},
		schema: schema,
	}
}

// func (entry *TableEntry) MarshalTxnRecord() (buf []byte, err error) {
// 	entry.RLock()
// 	if entry.CreateAndDropInSameTxn() {
// 		entry.RUnlock()
// 		return
// 	}
// 	var w bytes.Buffer
// 	switch entry.CurrOp {
// 	case OpCreate:
// 		if err = binary.Write(&w, binary.BigEndian, TxnETCreateTable); err != nil {
// 			return
// 		}
// 		if err = binary.Write(&w, binary.BigEndian, entry.GetID()); err != nil {
// 			return
// 		}
// 		if err = binary.Write(&w, binary.BigEndian, entry.CreateAt); err != nil {
// 			return
// 		}
// 		var schemaBuf []byte
// 		if schemaBuf, err = entry.schema.Marshal(); err != nil {
// 			return
// 		}
// 		if _, err = w.Write(schemaBuf); err != nil {
// 			return
// 		}
// 	case OpSoftDelete:
// 		if err = binary.Write(&w, binary.BigEndian, TxnETDropTable); err != nil {
// 			return
// 		}
// 		if err = binary.Write(&w, binary.BigEndian, entry.GetID()); err != nil {
// 			return
// 		}
// 		if err = binary.Write(&w, binary.BigEndian, entry.DeleteAt); err != nil {
// 			return
// 		}
// 	}
// 	buf = w.Bytes()
// 	return
// }

func (entry *TableEntry) addEntryLocked(segment *SegmentEntry) {
	n := entry.link.Insert(segment)
	entry.entries[segment.GetID()] = n
}

func (entry *TableEntry) deleteEntryLocked(segment *SegmentEntry) error {
	if n, ok := entry.entries[segment.GetID()]; !ok {
		return ErrNotFound
	} else {
		entry.link.Delete(n)
	}
	return nil
}

func (entry *TableEntry) GetSchema() *Schema {
	return entry.schema
}

func (entry *TableEntry) Compare(o NodePayload) int {
	oe := o.(*TableEntry).BaseEntry2
	return entry.DoCompre(oe)
}

func (entry *TableEntry) GetDB() *DBEntry {
	return entry.db
}

func (entry *TableEntry) PPString(level common.PPLevel, depth int, prefix string) string {
	return fmt.Sprintf("%s%s%s", common.RepeatStr("\t", depth), prefix, entry.String())
}

func (entry *TableEntry) String() string {
	entry.RLock()
	defer entry.RUnlock()
	return fmt.Sprintf("TABLE%s[name=%s]", entry.BaseEntry2.String(), entry.schema.Name)
}
