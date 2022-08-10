package demo

import (
	"io"
	"sync"
)

type Iterator interface {
	sync.Locker
	RLock()
	RUnlock()
	io.Closer
	Valid() bool
	Next()
	GetError() error
}
