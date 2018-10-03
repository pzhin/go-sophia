package sophia

import (
	"errors"
	"fmt"
	"unsafe"
)

// ErrNotFound error constant for 'NotFount' cases
var ErrNotFound = errors.New("document not found")

// DataStore provides access to data
type dataStore struct {
	ptr unsafe.Pointer
	env *Environment
}

// Get retrieves the row for the set of keys.
func (d *dataStore) Get(doc Document) (Document, error) {
	ptr := spGet(d.ptr, doc.ptr)
	if ptr == nil {
		err := d.env.Error()
		if err == nil {
			return Document{}, ErrNotFound
		}
		return Document{}, fmt.Errorf("failed Get document: err=%v", err)
	}
	return newDocument(ptr, 0), nil
}

// Set sets the row of the set of keys.
func (d *dataStore) Set(doc Document) error {
	if !spSet(d.ptr, doc.ptr) {
		return fmt.Errorf("failed Set document: err=%v", d.env.Error())
	}
	return nil
}

// Upsert sets the row of the set of keys.
func (d *dataStore) Upsert(doc Document) error {
	if !spUpsert(d.ptr, doc.ptr) {
		return fmt.Errorf("failed Upsert document: err=%v", d.env.Error())
	}
	return nil
}

// Delete deletes row with specified set of keys.
func (d *dataStore) Delete(doc Document) error {
	if !spDelete(d.ptr, doc.ptr) {
		return fmt.Errorf("failed Delete document: err=%v", d.env.Error())
	}
	return nil
}

func newDataStore(ptr unsafe.Pointer, env *Environment) *dataStore {
	return &dataStore{
		ptr: ptr,
		env: env,
	}
}
