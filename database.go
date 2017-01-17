package sophia

import (
	"fmt"
)

// Database extends DataStore interface
// Take it's name from sophia
// Usually object with same features are called 'table'
type Database interface {
	DataStore
	Document() (doc *Document)
	Cursor(doc *Document) (Cursor, error)
}

// Database is used for accessing a database.
type database struct {
	*dataStore
	name        string
	schema      *Schema
	fieldsCount int
}

// Document creates a Document for a single or multi-statement transactions
func (db *database) Document() *Document {
	ptr := spDocument(db.ptr)
	if ptr == nil {
		return nil
	}
	return newDocument(ptr, db.fieldsCount)
}

// Cursor returns a Cursor for iterating over rows in the database
func (db *database) Cursor(doc *Document) (Cursor, error) {
	cPtr := spCursor(db.env.ptr)
	if nil == cPtr {
		return nil, fmt.Errorf("failed create cursor: err=%v", db.env.Error())
	}
	if nil == doc {
		return nil, fmt.Errorf("failed get document: err=%v", db.env.Error())
	}
	cur := &cursor{
		ptr: cPtr,
		doc: doc,
	}
	return cur, nil
}
