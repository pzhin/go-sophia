package sophia

import (
	"errors"
	"unsafe"
)

type Order string

// Cursor iterates over key-values in a database.
type Cursor interface {
	// Next fetches the next row for the cursor
	// Returns next row if it exists else it will return nil
	Next() *Document
	// Close closes cursor
	// Cursor won't be accessible after this
	Close() error
}

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

const (
	cursorPrefix = "prefix"
	cursorOrder  = "order"
)

var ErrClosedCursorUsage = errors.New("cursor is already closed")

// Cursor iterates over key-values in a database.
type cursor struct {
	ptr    unsafe.Pointer
	doc    *Document
	schema *Schema
	check  func(d *Document) (match, stop bool)
	closed bool
}

// Close closes the cursor. If a cursor is not closed, future operations
// on the database can hang indefinitely.
func (cur *cursor) Close() error {
	if cur.closed {
		return ErrClosedCursorUsage
	}
	cur.doc.Free()
	cur.closed = true
	return spDestroy(cur.ptr)
}

// Next fetches the next row for the cursor
// Returns next row if it exists else it will return nil
func (cur *cursor) Next() *Document {
	if cur.closed {
		return nil
	}
	ptr := spGet(cur.ptr, cur.doc.ptr)
	if ptr == nil {
		return nil
	}
	d := newDocument(ptr, 0)
	cur.doc = d
	match, stop := cur.check(d)
	if stop {
		return nil
	}
	if !match {
		return cur.Next()
	}
	return d
}
