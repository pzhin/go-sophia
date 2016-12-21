package sophia

import (
	"unsafe"
)

type Document struct {
	*varStore
}

func newDocument(ptr unsafe.Pointer, size int) *Document {
	return &Document{
		varStore: newVarStore(ptr, size),
	}
}

func (d *Document) Destroy() {
	spDestroy(d.ptr)
}
