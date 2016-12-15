package sophia

import (
	"fmt"
	"unsafe"
)

// Database is used for accessing a database.
type Database struct {
	ptr    unsafe.Pointer
	env    *Environment
	name   string
	schema *Schema
}

func (db *Database) Document() (doc *Document) {
	ptr := sp_document(db.ptr)
	if ptr == nil {
		return
	}
	doc = newDocument(ptr)
	return
}

// Get retrieves the value for the key.
func (db *Database) Get(doc *Document) (*Document, error) {
	ptr := sp_get(db.ptr, doc.ptr)
	if ptr == nil {
		return nil, fmt.Errorf("failed Get document: err=%v", db.env.Error())
	}
	return newDocument(ptr), nil
}

// Set sets the value of the key.
func (db *Database) Set(doc *Document) error {
	if !sp_set(db.ptr, doc.ptr) {
		return fmt.Errorf("failed Set document: err=%v", db.env.Error())
	}
	return nil
}

// Set sets the value of the key.
func (db *Database) Upsert(doc *Document) error {
	panic("not supported yet")
	if !sp_upsert(db.ptr, doc.ptr) {
		return fmt.Errorf("failed Upsert document: err=%v", db.env.Error())
	}
	return nil
}

// Delete deletes the key from the database.
func (db *Database) Delete(doc *Document) error {
	if !sp_delete(db.ptr, doc.ptr) {
		return fmt.Errorf("failed Delete document: err=%v", db.env.Error())
	}
	return nil
}

// Cursor returns a Cursor for iterating over rows in the database
func (db *Database) Cursor(criteria CursorCriteria) (*cursor, error) {
	cPtr := sp_cursor(db.env.ptr)
	if nil == cPtr {
		return nil, fmt.Errorf("failed create cursor: err=%v", db.env.Error())
	}
	doc := db.Document()
	if nil == doc {
		return nil, fmt.Errorf("failed get document: err=%v", db.env.Error())
	}
	cur := &cursor{
		ptr: cPtr,
		doc: doc,
	}
	err := criteria.(*cursorCriteria).apply(cur, db.schema)
	if err != nil {
		return nil, err
	}
	return cur, nil
}
