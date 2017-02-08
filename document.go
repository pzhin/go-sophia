package sophia

import (
	"errors"
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

func (d *Document) Destroy() error {
	if !spDestroy(d.ptr) {
		return errors.New("document: failed to destroy")
	}
	return nil
}
