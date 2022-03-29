package handle

import "io"

type Iterator interface {
	io.Closer
	Valid() bool
	Next()
}
