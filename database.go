package sophia

import (
	"errors"
	"fmt"
)


// DatabaseConfig a structure for the description of the database to be created.
type DatabaseConfig struct {
	// Name of database.
	// It will be used to set and get values specific to this base.
	Name string
	// Schema of database.
	// It is used to describe the keys and values that will be stored in the database.
	Schema *Schema
}

// Database is used for accessing a database.
// Take it's name from sophia.
// Usually object with same features is called 'table'.
type Database struct {
	*dataStore
	name        string
	schema      *Schema
	fieldsCount int
}

// Document creates a Document for a single or multi-statement transactions
func (db *Database) Document() *Document {
	ptr := spDocument(db.ptr)
	if ptr == nil {
		return nil
	}
	return newDocument(ptr, db.fieldsCount)
}

// Cursor returns a Cursor for iterating over rows in the database
func (db *Database) Cursor(doc *Document) (Cursor, error) {
	if nil == doc {
		return nil, errors.New("failed to create cursor: nil Document")
	}
	cPtr := spCursor(db.env.ptr)
	if nil == cPtr {
		return nil, fmt.Errorf("failed to create cursor: err=%v", db.env.Error())
	}
	cur := &cursor{
		ptr: cPtr,
		doc: doc,
	}
	return cur, nil
}
