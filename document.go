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

//func (d *Document) Set(key string, val interface{}) bool {
//	size := reflect.TypeOf(val).Size()
//	v := reflect.ValueOf(val)
//	switch v.Kind() {
//	case reflect.String:
//		return C.sp_setstring(d.ptr, key, val, C.int(size)) == 0
//	case reflect.Int, reflect.Int64, reflect.Int8, reflect.Int16, reflect.Int32:
//		return sp_setint(obj, path, v.Int())
//	case reflect.Uint, reflect.Uint64, reflect.Uint8, reflect.Uint16, reflect.Uint32:
//		return sp_setint(obj, path, int64(v.Uint()))
//	default:
//		cPath := C.CString(path)
//		//defer free(unsafe.Pointer(cPath))
//		return C.sp_setstring(obj, cPath, (unsafe.Pointer)(reflect.ValueOf(val).Pointer()), C.int(size)) == 0
//	}
//	return sp_setstring(d.ptr, key, value, int(size))
//}

func (d *Document) Set(key string, value interface{}) bool {
	val := reflect.ValueOf(value)
	switch val.Kind() {
	case reflect.String:
		return d.SetString(key, val.String())
	case reflect.Int64, reflect.Int, reflect.Int32, reflect.Int16, reflect.Int8:
		return d.SetInt(key, val.Int())
	default:
		return false
	}
}

func (d *Document) SetString(key string, value string) bool {
	sh := (*reflect.StringHeader)(unsafe.Pointer(&value))
	cPath := C.CString(key)
	ok := sp_setstring(d.ptr, cPath, unsafe.Pointer(sh.Data), len(value))
	d.fields = append(d.fields, unsafe.Pointer(cPath))
	return ok
}

func (d *Document) SetInt(key string, value int64) bool {
	return sp_setint(d.ptr, key, value)
}

func (d *Document) GetString(key string, size *int) string {
	ptr := sp_getstring(d.ptr, key, size)
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
