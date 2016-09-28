package sophia

import (
	"C"
	"reflect"
	"unsafe"
)

type Document struct {
	ptr    unsafe.Pointer
	fields []unsafe.Pointer
}

func NewDocument(ptr unsafe.Pointer) *Document {
	return &Document{
		ptr: ptr,
	}
}

func (d *Document) Set(path string, val interface{}) bool {
	v := reflect.ValueOf(val)
	switch v.Kind() {
	case reflect.String:
		return d.SetString(path, v.String())
	case reflect.Int, reflect.Int64, reflect.Int8, reflect.Int16, reflect.Int32:
		return d.SetInt(path, v.Int())
	case reflect.Uint, reflect.Uint64, reflect.Uint8, reflect.Uint16, reflect.Uint32:
		return d.SetInt(path, int64(v.Uint()))
	default:
		cPath := C.CString(path)
		d.fields = append(d.fields, unsafe.Pointer(cPath))
		size := int(reflect.TypeOf(val).Size())
		return sp_setstring(d.ptr, cPath, (unsafe.Pointer)(reflect.ValueOf(val).Pointer()), size)
	}
}

func (d *Document) SetString(path string, value string) bool {
	sh := (*reflect.StringHeader)(unsafe.Pointer(&value))
	cPath := C.CString(path)
	d.fields = append(d.fields, unsafe.Pointer(cPath))
	return sp_setstring(d.ptr, cPath, unsafe.Pointer(sh.Data), len(value))
}

func (d *Document) SetInt(path string, value int64) bool {
	return sp_setint(d.ptr, path, value)
}

func (d *Document) GetString(path string, size *int) string {
	ptr := sp_getstring(d.ptr, path, size)
	sh := &reflect.StringHeader{
		Len:  *size,
		Data: uintptr(ptr),
	}
	return *(*string)(unsafe.Pointer(sh))
}

func (d *Document) GetInt(key string) int64 {
	return sp_getint(d.ptr, key)
}

func (d *Document) Destroy() {
	sp_close(d.ptr)
}

func (d *Document) Free() {
	for _, f := range d.fields {
		free(f)
	}
	d.fields = d.fields[:0]
}
