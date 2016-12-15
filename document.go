package sophia

import (
	"unsafe"
)

type Document struct {
	*store
}

func newDocument(ptr unsafe.Pointer) *Document {
	return &Document{
		store: newStore(ptr),
	}
}

func (d *Document) Destroy() {
	sp_close(d.ptr)
}
