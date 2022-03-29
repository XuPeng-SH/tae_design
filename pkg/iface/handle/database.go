package handle

import "io"

type Database interface {
	io.Closer

	CreateRelation(def interface{}) (Relation, error)
	DropRelationByName(name string) (Relation, error)

	GetRelationBy(name string) (Relation, error)
	RelationCnt() int64
	Relations() []Relation

	MakeRelationIt() RelationIt
}
