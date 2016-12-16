package sophia

import (
	"fmt"
)

type Database interface {
	DataStore
	Document() (doc *Document)
	Cursor(criteria CursorCriteria) (Cursor, error)
}

// Database is used for accessing a database.
type database struct {
	*dataStore
	name   string
	schema *Schema
}

// Document creates a Document for a single or multi-statement transactions
func (db *database) Document() (doc *Document) {
	ptr := spDocument(db.ptr)
	if ptr == nil {
		return
	}
	doc = newDocument(ptr)
	return
}

// Cursor returns a Cursor for iterating over rows in the database
func (db *database) Cursor(criteria CursorCriteria) (Cursor, error) {
	cPtr := spCursor(db.env.ptr)
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
