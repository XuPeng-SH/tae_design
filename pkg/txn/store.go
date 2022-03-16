package txn

type Store struct {
	dbs map[uint64]*DB
}

type DB struct {
	create, delete Node
	tables         map[uint64]*Table
}

func (store *Store) FindKeys(db, table uint64, keys [][]byte) []uint32 {
	// TODO
	return nil
}

func (store *Store) FindKey(db, table uint64, key []byte) uint32 {
	// TODO
	return 0
}

func (store *Store) HasKey(db, table uint64, key []byte) bool {
	// TODO
	return false
}
