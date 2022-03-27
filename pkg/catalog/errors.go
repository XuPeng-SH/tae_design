package catalog

import "errors"

var (
	ErrNotFound  = errors.New("tae catalog: not found")
	ErrDuplicate = errors.New("tae catalog: duplicate")

	ErrValidation = errors.New("tae catalog: validataion")
)
