package sophia

import (
	"fmt"
	"reflect"
)

type criteriaType int

const (
	// Exact match
	criteriaMatch criteriaType = iota
	// Inclusive range
	criteriaRange
)

type CursorCriteria interface {
	Order(Order)
	Prefix(string)
	Match(key string, value interface{})
	Range(key string, from, to interface{})
}

type checkFunc func(d *Document) bool

var noopCheck checkFunc = func(d *Document) bool { return true }

type criteria struct {
	t     criteriaType
	field string
	value interface{}
}

type cursorCriteria struct {
	crs    map[string]*criteria
	checks []checkFunc
}

func NewCursorCriteria() CursorCriteria {
	return &cursorCriteria{
		crs:    make(map[string]*criteria),
		checks: make([]checkFunc, 0),
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

// Range - range request with inclusive 'from' and exclusive 'to' [from;to)
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

func (cc *cursorCriteria) apply(cur *cursor, schema *Schema) error {
	order := cc.crs[cursorOrder]
	custom := true
	if order != nil && (order.value == LT || order.value == LTE) {
		custom = false
	}
	fields := append(schema.keysNames, schema.valuesNames...)
	for _, field := range fields {
		cr, ok := cc.crs[field]
		if !ok {
			continue
		}
		var check checkFunc
		switch cr.t {
		case criteriaMatch:
			cur.doc.Set(field, cr.value)
			check = generateCheckMatch(cr)
		case criteriaRange:
			val0 := reflect.ValueOf(cr.value).Index(0).Elem()
			val1 := reflect.ValueOf(cr.value).Index(1).Elem()
			if custom && !isNil(val0) {
				cur.doc.Set(field, val0.Interface())
			}
			if !custom && !isNil(val1) {
				cur.doc.Set(field, val1.Interface())
			}
			check = generateCheckRange(cr.field, val0, val1)
		}
		if check != nil {
			cc.checks = append(cc.checks, check)
		}
	}
	if len(cc.checks) > 0 {
		cur.check = cc.check
	} else {
		cur.check = func(doc *Document) (bool, bool) { return true, false }
	}
	return nil
}

func (cc *cursorCriteria) check(doc *Document) (bool, bool) {
	match := cc.checks[0](doc)
	if !match {
		return false, true
	}
	for i := 1; i < len(cc.checks); i++ {
		if !cc.checks[i](doc) {
			return false, false
		}
	}
	return true, false
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
	return nil
}

// TODO :: implement custom types
func generateCheckRange(field string, val0, val1 reflect.Value) checkFunc {
	if isNil(val0) && isNil(val1) {
		return nil
	}

	switch val0.Kind() {
	case reflect.Int64, reflect.Int, reflect.Int32, reflect.Int16, reflect.Int8:
		return generateCompareInt(val0, val1, field)
	case reflect.Uint64, reflect.Uint, reflect.Uint32, reflect.Uint16, reflect.Uint8:
		return generateCompareUint(val0, val1, field)
	case reflect.String:
		return generateCompareString(val0, val1, field)
	}

	return nil
}

func generateCompareInt(val0, val1 reflect.Value, field string) checkFunc {
	if isNil(val0) {
		i := val1.Int()
		return func(d *Document) bool {
			return d.GetInt(field) < i
		}
	}

	if isNil(val1) {
		i := val0.Int()
		return func(d *Document) bool {
			return d.GetInt(field) >= i
		}
	}

	i0 := val0.Int()
	i1 := val1.Int()
	return func(d *Document) bool {
		sv := d.GetInt(field)
		return sv >= i0 && sv < i1
	}
}

func generateCompareUint(val0, val1 reflect.Value, field string) checkFunc {
	if isNil(val0) {
		i := val1.Uint()
		return func(d *Document) bool {
			return uint64(d.GetInt(field)) < i
		}
	}

	if isNil(val1) {
		i := val0.Uint()
		return func(d *Document) bool {
			return uint64(d.GetInt(field)) >= i
		}
	}

	i0 := val0.Uint()
	i1 := val1.Uint()
	return func(d *Document) bool {
		sv := uint64(d.GetInt(field))
		return sv >= i0 && sv < i1
	}
}

func generateCompareString(val0, val1 reflect.Value, field string) checkFunc {
	var size int
	if isNil(val0) {
		i := val1.String()
		return func(d *Document) bool {
			return d.GetString(field, &size) < i
		}
	}

	if isNil(val1) {
		i := val0.String()
		return func(d *Document) bool {
			return d.GetString(field, &size) >= i
		}
	}

	i0 := val0.String()
	i1 := val1.String()
	return func(d *Document) bool {
		sv := d.GetString(field, &size)
		return sv >= i0 && sv < i1
	}
}
