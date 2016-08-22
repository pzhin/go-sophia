package sophia

import (
	"errors"
	"fmt"
	"unsafe"
)

const (
	GreaterThan      = ">"
	GT               = GreaterThan
	GreaterThanEqual = ">="
	GTE              = GreaterThanEqual
	LessThan         = "<"
	LT               = LessThan
	LessThanEqual    = "<="
	LTE              = LessThanEqual
)

// Database is used for accessing a database.
type Database struct {
	ptr  *unsafe.Pointer
	name string
	env  *Environment
}

// Close closes the database and frees its associated memory. You must
// call Close on any database opened with Open()
func (db *Database) Close() error {
	err := sp_close(db.ptr)
	if nil != err {
		return err
	}
	if nil != db.env {
		return db.env.Close()
	}
	return nil
}

func (db *Database) Document() (doc *Document) {
	ptr := sp_document(db.ptr)
	if ptr == nil {
		return
	}
	doc = &Document{
		ptr: ptr,
	}
	return
}

// Get retrieves the value for the key.
func (db *Database) Get(key []byte) ([]byte, error) {
	var size int64
	doc := db.Document()
	if nil == doc {
		return nil, errors.New("failed get document")
	}
	defer sp_close(doc.ptr)
	doc.SetString("key", string(key), len(key))
	vptr := sp_get(db.ptr, doc.ptr)
	if vptr == nil {
		return nil, fmt.Errorf("failed Get: key=%q ", key)
	}
	value := goBytes(vptr, size)
	return value, nil
}

// Set sets the value of the key.
func (db *Database) Set(key, value []byte) error {
	doc := db.Document()
	if nil == doc {
		return errors.New("failed get document")
	}
	defer sp_close(doc.ptr)
	doc.SetString("key", string(key), len(key))
	doc.SetString("value", string(value), len(value))
	if !sp_set(db.ptr, doc.ptr) {
		return fmt.Errorf("failed Set: key=%q value=%q", key, value)
	}
	return nil
}

// Set sets the value of the key.
func (db *Database) Upsert(key, value []byte) error {
	doc := db.Document()
	if nil == doc {
		return errors.New("failed get document")
	}
	defer sp_close(doc.ptr)
	doc.SetString("key", string(key), len(key))
	doc.SetString("value", string(value), len(value))
	if !sp_upsert(db.ptr, doc.ptr) {
		return fmt.Errorf("failed Upsert: key=%q value=%q", key, value)
	}
	return nil
}

// Delete deletes the key from the database.
func (db *Database) Delete(key []byte) error {
	doc := db.Document()
	if nil == doc {
		return errors.New("failed get document")
	}
	defer sp_close(doc.ptr)
	doc.SetString("key", string(key), len(key))
	if !sp_delete(db.ptr, doc.ptr) {
		return fmt.Errorf("failed Delete: key=%q", key)
	}
	return nil
}

// Cursor returns a Cursor for iterating over rows in the database.
//
// If no key is provided, the Cursor will iterate over all rows.
//
// The order flag decides the direction of the iteration, and whether
// the key is included or excluded.
//
// Iterate over values with Fetch or Next methods.
func (db *Database) Cursor(criteria CursorCriteria) (*Cursor, error) {
	cPtr := sp_cursor(db.env.ptr)
	if nil == cPtr {
		return nil, errors.New("failed create cursor")
	}
	doc := db.Document()
	if nil == doc {
		return nil, errors.New("failed get document")
	}
	err := criteria.apply(doc)
	if err != nil {
		return nil, err
	}
	cur := &Cursor{
		ptr: cPtr,
		doc: doc,
	}
	return cur, nil
}
