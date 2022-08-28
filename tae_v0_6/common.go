package demo

import (
	"io"
	"sync"
)

type Location struct{}
type Vector struct{}      // container.Vector
type DBEntry struct{}     // catalog.DBEntry
type TableEntry struct{}  // catalog.TableEntry
type Expr struct{}        // plan.Expr
type Transaction struct{} // txnbase.TxnCtx
type Timestamp struct{}   // actually timestamp.Timestamp

type Iterator interface {
	sync.Locker
	RLock()
	RUnlock()
	io.Closer
	Valid() bool
	Next()
	GetError() error
}

type CmdType int16

type ICommand interface {
	WriteTo(io.Writer) error
	ReadFrom(io.Reader) error
	Marshal() ([]byte, error)
	Unmarshal([]byte) error
	GetType() CmdType
	String() string
}
