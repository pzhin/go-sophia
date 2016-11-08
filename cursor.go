package sophia

import (
	"errors"
	"fmt"
	"reflect"
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

type criteriaType int

const (
	// Exact match
	criteriaMatch criteriaType = iota
	// Inclusive range
	criteriaRange
)

type criteria struct {
	t     criteriaType
	field string
	value interface{}
}

type CursorCriteria interface {
	Order(Order)
	Prefix(string)
	Match(key string, value interface{})
	Range(key string, from, to interface{})
}

type cursorCriteria struct {
	crs    map[string]*criteria
	checks map[string]checkFunc
}

type checkFunc func(d *Document) bool

var noopCheck checkFunc = func(d *Document) bool { return false }

func NewCursorCriteria() CursorCriteria {
	return &cursorCriteria{
		crs:    make(map[string]*criteria),
		checks: make(map[string]checkFunc),
	}
}

func (cc *cursorCriteria) Order(order Order) {
	cc.set(criteriaMatch, cursorOrder, order)
}

func (cc *cursorCriteria) Prefix(prefix string) {
	cc.set(criteriaMatch, cursorPrefix, prefix)
}

// Match adds condition of exact match
func (cc *cursorCriteria) Match(key string, value interface{}) {
	cc.set(criteriaMatch, key, value)
}

// Range - inclusive range [from;to]
// In case of nil value 'from' takes minimum value and 'to' takes maximum value
// 'from' and 'to' must be same kind and 'from' must be less than 'to'
func (cc *cursorCriteria) Range(key string, from, to interface{}) {
	val0 := reflect.ValueOf(from)
	val1 := reflect.ValueOf(to)
	if val0.Kind() != val1.Kind() {
		panic(fmt.Sprintf("kinds of range criteria bounds must be same, got '%v' and '%v'",
			val0.Kind(), val1.Kind()))
	}
	cc.set(criteriaRange, key, []interface{}{from, to})
}

func (cc *cursorCriteria) set(typ criteriaType, key string, value interface{}) {
	cc.crs[key] = &criteria{
		t:     typ,
		field: key,
		value: value,
	}
}

func (cc *cursorCriteria) apply(cur *Cursor) error {
	order := cc.crs[cursorOrder]
	custom := true
	if order != nil && (order.value == LT || order.value == LTE) {
		custom = false
	}
	for key, cr := range cc.crs {
		switch cr.t {
		case criteriaMatch:
			cur.doc.Set(key, cr.value)
			cc.checks[cr.field] = generateCheckMatch(cr)
		case criteriaRange:
			val := reflect.ValueOf(cr.value).Index(0)
			if !custom {
				val = reflect.ValueOf(cr.value).Index(1)
			}
			if !isNil(val) {
				cur.doc.Set(key, val.Elem().Interface())
			}
			cc.checks[cr.field] = generateCheckRange(cr)
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

// TODO :: implement custom types
func generateCheckMatch(cr *criteria) checkFunc {
	val := reflect.ValueOf(cr.value)
	switch val.Kind() {
	case reflect.Int64, reflect.Int, reflect.Int32, reflect.Int16, reflect.Int8:
		i := val.Int()
		return func(d *Document) bool {
			return i == d.GetInt(cr.field)
		}
	case reflect.Uint64, reflect.Uint, reflect.Uint32, reflect.Uint16, reflect.Uint8:
		i := val.Uint()
		return func(d *Document) bool {
			return int64(i) == d.GetInt(cr.field)
		}
	case reflect.String:
		str := val.String()
		var size int
		return func(d *Document) bool {
			return str == d.GetString(cr.field, &size)
		}
	}
	return noopCheck
}

// TODO :: implement custom types
// TODO :: optimize, we don't need to check value if it is nil
func generateCheckRange(cr *criteria) checkFunc {
	v := cr.value
	field := cr.field
	val0 := reflect.ValueOf(v).Index(0).Elem()
	val1 := reflect.ValueOf(v).Index(1).Elem()
	switch val0.Kind() {
	case reflect.Int64, reflect.Int, reflect.Int32, reflect.Int16, reflect.Int8:
		return generateCompareInt(val0, val1, field)
	case reflect.Uint64, reflect.Uint, reflect.Uint32, reflect.Uint16, reflect.Uint8:
		return generateCompareUint(val0, val1, field)
	case reflect.String:
		return generateCompareString(val0, val1, field)
	}
	return noopCheck
}

func generateCompareInt(val0, val1 reflect.Value, field string) checkFunc {
	switch {
	case isNil(val0) && isNil(val1):
		return noopCheck
	case isNil(val0):
		i := val1.Int()
		return func(d *Document) bool {
			return d.GetInt(field) <= i
		}
	case isNil(val1):
		i := val0.Int()
		return func(d *Document) bool {
			return d.GetInt(field) >= i
		}
	}
	i0 := val0.Int()
	i1 := val1.Int()
	return func(d *Document) bool {
		sv := d.GetInt(field)
		return sv >= i0 && sv <= i1
	}
}

func generateCompareUint(val0, val1 reflect.Value, field string) checkFunc {
	switch {
	case isNil(val0) && isNil(val1):
		return noopCheck
	case isNil(val0):
		i := val1.Uint()
		return func(d *Document) bool {
			return uint64(d.GetInt(field)) <= i
		}
	case isNil(val1):
		i := val0.Uint()
		return func(d *Document) bool {
			return uint64(d.GetInt(field)) >= i
		}
	}
	i0 := val0.Uint()
	i1 := val1.Uint()
	return func(d *Document) bool {
		sv := uint64(d.GetInt(field))
		return sv >= i0 && sv <= i1
	}
}

func generateCompareString(val0, val1 reflect.Value, field string) checkFunc {
	fmt.Printf("generateCompareString\n")
	var size int
	switch {
	case isNil(val0) && isNil(val1):
		return noopCheck
	case isNil(val0):
		i := val1.String()
		return func(d *Document) bool {
			return d.GetString(field, &size) <= i
		}
	case isNil(val1):
		i := val0.String()
		return func(d *Document) bool {
			return d.GetString(field, &size) >= i
		}
	}
	i0 := val0.String()
	i1 := val1.String()
	return func(d *Document) bool {
		sv := d.GetString(field, &size)
		return sv >= i0 && sv <= i1
	}
}

// Cursor iterates over key-values in a database.
type Cursor struct {
	ptr    unsafe.Pointer
	doc    *Document
	schema *Schema
	check  checkFunc
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
