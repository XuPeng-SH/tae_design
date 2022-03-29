package handle

type Iterator interface {
	Valid() bool
	Next()
}
