package sophia

import (
	"errors"
	"fmt"
	"reflect"
	"unsafe"
)

const (
	cursorPrefix = "prefix"
	cursorOrder  = "order"
)

type CriteriaType int

const (
	CriteriaMatch CriteriaType = iota
	CriteriaRange
)

type criteria struct {
	t     CriteriaType
	field string
	value interface{}
}

type CursorCriteria interface {
	Order(Order)
	Prefix(string)
	Add(CriteriaType, string, interface{})
}

type cursorCriteria struct {
	crs    map[string]*criteria
	checks map[string]func(*Document) bool
}

func NewCursorCriteria() CursorCriteria {
	return &cursorCriteria{
		crs:    make(map[string]*criteria),
		checks: make(map[string]func(*Document) bool),
	}
}

func (cc *cursorCriteria) Order(order Order) {
	cc.Add(CriteriaMatch, cursorOrder, order)
}

func (cc *cursorCriteria) Prefix(prefix string) {
	cc.Add(CriteriaMatch, cursorPrefix, prefix)
}

func (cc *cursorCriteria) Add(typ CriteriaType, key string, value interface{}) {
	cc.crs[key] = &criteria{
		t:     typ,
		field: key,
		value: value,
	}
}

func (cc *cursorCriteria) apply(cur *Cursor) error {
	for key, cr := range cc.crs {
		val := reflect.ValueOf(cr.value)
		switch val.Kind() {
		case reflect.Int64, reflect.Int, reflect.Int32, reflect.Int16, reflect.Int8:
			cur.doc.SetInt(key, val.Int())
			cc.checks[cr.field] = generateCheckMatchInt(cr)
		case reflect.String:
			cur.doc.SetString(key, val.String())
			if key != cursorOrder && key != cursorPrefix {
				cc.checks[cr.field] = generateCheckMatchString(cr)
			}
		case reflect.Slice:
			if val.Len() != 2 {
				return fmt.Errorf("wrong number of range query params: expected=%v actual=%v", 2, val.Len())
			}
			cur.doc.Set(key, val.Index(0).Interface())
			cc.checks[cr.field] = generateCheck(cr)
		default:
			return errors.New("unsupported criteria type")
		}
	}
	cur.check = cc.check
	return nil
}

func (cc *cursorCriteria) check(doc *Document) bool {
	for _, check := range cc.checks {
		if !check(doc) {
			return false
		}
	}
	return true
}

func generateCheckMatchInt(cr *criteria) func(d *Document) bool {
	v := cr.value
	i := reflect.ValueOf(v).Int()
	return func(d *Document) bool {
		return i == d.GetInt(cr.field)
	}
}

func generateCheckMatchString(cr *criteria) func(d *Document) bool {
	v := cr.value
	s := reflect.ValueOf(v).String()
	return func(d *Document) bool {
		var size int
		return s == d.GetString(cr.field, &size)
	}
}

func generateCheck(cr *criteria) func(d *Document) bool {
	v := cr.value
	val0 := reflect.ValueOf(v).Index(0)
	val1 := reflect.ValueOf(v).Index(1)
	switch val0.Kind() {
	case reflect.Int64, reflect.Int, reflect.Int32, reflect.Int16, reflect.Int8:
		i0, i1 := val0.Int(), val1.Int()
		return func(d *Document) bool {
			sv := d.GetInt(cr.field)
			return sv >= i0 && sv <= i1
		}
	case reflect.String:
		s0, s1 := val0.String(), val1.String()
		return func(d *Document) bool {
			var size int
			sv := d.GetString(cr.field, &size)
			return sv >= s0 && sv <= s1
		}
	default:
		return func(d *Document) bool { return false }
	}
}

// Cursor iterates over key-values in a database.
type Cursor struct {
	ptr    unsafe.Pointer
	doc    *Document
	schema *Schema
	check  func(doc *Document) bool
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
