package iface

type DatabaseReader interface {
	Relations() []string
	Relation(name string) *Relation
}

type DatabaseWriter interface {
	Create(name string, def ResourceDef) (Relation, error)
	Drop(name string) (Relation, error)
}

type Database interface {
	DatabaseReader
	DatabaseWriter
}
