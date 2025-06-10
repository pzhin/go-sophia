package sophia

import (
	"errors"
	"unsafe"
)

// Document is a representation of a row in a database.
// Destroy should be called after Document usage.
type Document struct {
	varStore
}

func newDocument(ptr unsafe.Pointer, size int, cache CStringCache) Document {
	return Document{
		varStore: newVarStore(ptr, size, cache),
	}
}

// Destroy call C function that releases all resources associated with the Document
func (d *Document) Destroy() error {
	if !spDestroy(d.ptr) {
		return errors.New("document: failed to destroy")
	}
	return nil
}
