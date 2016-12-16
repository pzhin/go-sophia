package sophia

import (
	"unsafe"
)

type Document struct {
	*varStore
}

func newDocument(ptr unsafe.Pointer) *Document {
	return &Document{
		varStore: newVarStore(ptr),
	}
}

func (d *Document) Destroy() {
	spDestroy(d.ptr)
}
