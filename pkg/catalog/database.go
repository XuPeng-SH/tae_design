package catalog

type DBEntry struct {
	*BaseEntry
	host    *Catalog
	name    string
	entries map[uint64]*TableEntry

	nameNodes map[string]*nodeList
}
