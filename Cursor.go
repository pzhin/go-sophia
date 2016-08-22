package sophia

import (
	"unsafe"
	"errors"
)

type CursorCriteria struct {
	crs map[string]interface{}
}

func (cc *CursorCriteria) Add(key string, order interface{}) error {
	if _, ok := cc.crs[key]; ok {
		return errors.New("dublicate key field")
	}
	cc.crs[key] = order
	return nil
}

func (cc *CursorCriteria) apply(doc *Document) error {
	for key, order := range cc.crs {
		switch order.(type) {
		case int64:
			doc.SetInt(key, order.(int64))
		case string:
			doc.SetString(key, order.(string), 0)
		default:
			return errors.New("unsupported criteria type")
		}
	}
	return nil
}

// Cursor iterates over key-values in a database.
type Cursor struct {
	ptr *unsafe.Pointer
	doc *Document
}

// Close closes the cursor. If a cursor is not closed, future operations
// on the database can hang indefinitely.
func (cur *Cursor) Close() error {
	return sp_close(cur.ptr)
}

// Fetch fetches the next row for the cursor, and returns
// true if there is a next row, false if the cursor has reached the
// end of the rows.
func (cur *Cursor) Next() *Document {
	ptr := sp_get(cur.ptr, cur.doc.ptr)
	return &Document{ptr: ptr}
}
