package sophia

import (
	"errors"
	"unsafe"
)

// Order string type of sophia cursor order
type Order string

// Constants for sophia cursor order
// They are used while creating cursor to select it's direction
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
	// CursorPrefix uses for setting cursor prefix
	CursorPrefix = "prefix"
	// CursorOrder uses for setting cursor order
	CursorOrder = "order"
)

// Cursor iterates over key-values in a database.
type Cursor interface {
	// Next fetches the next row for the cursor
	// Returns next row if it exists else it will return nil
	Next() *Document
	// Close closes cursor
	// Cursor won't be accessible after this
	Close() error
}

// ErrCursorClosed will be returned in case of closed cursor usage
var ErrCursorClosed = errors.New("usage of closed cursor")

// Cursor iterates over key-values in a database.
type cursor struct {
	ptr    unsafe.Pointer
	doc    *Document
	closed bool
}

// Close closes the cursor. If a cursor is not closed, future operations
// on the database can hang indefinitely.
func (cur *cursor) Close() error {
	if cur.closed {
		return ErrCursorClosed
	}
	cur.doc.Free()
	cur.closed = true
	if !spDestroy(cur.ptr) {
		return errors.New("cursor: failed to close")
	}
	return nil
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
	cur.doc.ptr = ptr
	return cur.doc
}
