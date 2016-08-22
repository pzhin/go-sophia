package sophia

import (
	"errors"
	"fmt"
	"unsafe"
)

type Order string

const (
	GreaterThan      Order = ">"
	GT               Order = GreaterThan
	GreaterThanEqual Order = ">="
	GTE              Order = GreaterThanEqual
	LessThan         Order = "<"
	LT               Order = LessThan
	LessThanEqual    Order = "<="
	LTE              Order = LessThanEqual
)

// Database is used for accessing a database.
type Database struct {
	ptr    unsafe.Pointer
	env    *Environment
	name   string
	schema *Schema
}

// Close closes the database and frees its associated memory. You must
// call Close on any database opened with Open()
func (db *Database) Close() error {
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
// TODO :: add destroy func to Document, it must be destroyed after usage
func (db *Database) Get(doc *Document) (*Document, error) {
	vptr := sp_get(db.ptr, doc.ptr)
	if vptr == nil {
		return nil, errors.New("failed Get document")
	}
	return NewDocument(vptr), nil
}

// Set sets the value of the key.
func (db *Database) Set(doc *Document) error {
	if !sp_set(db.ptr, doc.ptr) {
		return errors.New("failed Set document")
	}
	return nil
}

// Set sets the value of the key.
func (db *Database) Upsert(doc *Document) error {
	if !sp_upsert(db.ptr, doc.ptr) {
		return errors.New("failed Upsert document")
	}
	return nil
}

// Delete deletes the key from the database.
func (db *Database) Delete(doc *Document) error {
	if !sp_delete(db.ptr, doc.ptr) {
		return errors.New("failed Delete document")
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
	cur := &Cursor{
		ptr: cPtr,
		doc: doc,
	}
	err := criteria.(*cursorCriteria).apply(cur)
	if err != nil {
		return nil, err
	}
	return cur, nil
}

type Schema struct {
	// name -> type
	keys      map[string]string
	keysNames []string
	// name -> type
	values      map[string]string
	valuesNames []string
}

func (s *Schema) AddKey(name, typ string) error {
	if s.keys == nil {
		s.keys = make(map[string]string)
	}
	if _, ok := s.keys[name]; ok {
		return fmt.Errorf("dublicate key, '%v' has been already defined", name)
	}
	s.keysNames = append(s.keysNames, name)
	s.keys[name] = typ
	return nil
}

func (s *Schema) AddValue(name, typ string) error {
	if s.values == nil {
		s.values = make(map[string]string)
	}
	if _, ok := s.values[name]; ok {
		return fmt.Errorf("dublicate value, '%v' is already defined", name)
	}
	s.valuesNames = append(s.valuesNames, name)
	s.values[name] = typ
	return nil
}
