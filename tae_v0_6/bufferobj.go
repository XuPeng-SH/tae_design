package demo

type IBufferObject interface {
	GetCatalog() ICatalog
	GetTable(id TableID) (ITable, error)
	CoarseSize() int
}
