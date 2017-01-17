package sophia

import (
	"fmt"
	"unsafe"
)

// DataStore provides access to data
type DataStore interface {
	// Get retrieves the row for the set of keys.
	Get(doc *Document) (*Document, error)
	// Set sets the row of the set of keys.
	Set(doc *Document) error
	// Upsert sets the row of the set of keys.
	Upsert(doc *Document) error
	// Delete deletes row with specified set of keys.
	Delete(doc *Document) error
}

type dataStore struct {
	ptr unsafe.Pointer
	env *Environment
}

func newDataStore(ptr unsafe.Pointer, env *Environment) *dataStore {
	return &dataStore{
		ptr: ptr,
		env: env,
	}
}

func (d *dataStore) Get(doc *Document) (*Document, error) {
	ptr := spGet(d.ptr, doc.ptr)
	if ptr == nil {
		return nil, fmt.Errorf("failed Get document: err=%v", d.env.Error())
	}
	return newDocument(ptr, 0), nil
}

func (d *dataStore) Set(doc *Document) error {
	if !spSet(d.ptr, doc.ptr) {
		return fmt.Errorf("failed Set document: err=%v", d.env.Error())
	}
	return nil
}

func (d *dataStore) Upsert(doc *Document) error {
	panic("not supported yet")
	if !spUpsert(d.ptr, doc.ptr) {
		return fmt.Errorf("failed Upsert document: err=%v", d.env.Error())
	}
	return nil
}

func (d *dataStore) Delete(doc *Document) error {
	if !spDelete(d.ptr, doc.ptr) {
		return fmt.Errorf("failed Delete document: err=%v", d.env.Error())
	}
	return nil
}
