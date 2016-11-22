package sophia

import (
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

const (
	cursorPrefix = "prefix"
	cursorOrder  = "order"
)

// Cursor iterates over key-values in a database.
type cursor struct {
	ptr    unsafe.Pointer
	doc    *Document
	schema *Schema
	check  checkFunc
}

// Close closes the cursor. If a cursor is not closed, future operations
// on the database can hang indefinitely.
func (cur *cursor) Close() error {
	return sp_close(cur.ptr)
}

// Fetch fetches the next row for the cursor, and returns
// true if there is a next row, false if the cursor has reached the
// end of the rows.
func (cur *cursor) Next() *Document {
	ptr := sp_get(cur.ptr, cur.doc.ptr)
	if ptr == nil {
		return nil
	}
	cur.doc.Free()
	d := NewDocument(ptr)
	cur.doc = d
	if !cur.check(d) {
		return cur.Next()
	}
	return d
}
