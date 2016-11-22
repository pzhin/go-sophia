package sophia

import (
	"unsafe"
)

type Document struct {
	*store
}

func NewDocument(ptr unsafe.Pointer) *Document {
	return &Document{
		store: newStore(ptr),
	}
}

func (d *Document) Destroy() {
	sp_close(d.ptr)
}
