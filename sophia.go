package sophia

import (
	"reflect"
	"unsafe"
)

type store struct {
	ptr unsafe.Pointer

	pointers []unsafe.Pointer
}

func newStore(ptr unsafe.Pointer) *store {
	return &store{
		ptr:      ptr,
		pointers: make([]unsafe.Pointer, 0),
	}
}

// TODO :: implement another types
func (s *store) Set(path string, val interface{}) bool {
	v := reflect.ValueOf(val)
	switch v.Kind() {
	case reflect.String:
		return s.SetString(path, v.String())
	case reflect.Int, reflect.Int64, reflect.Int8, reflect.Int16, reflect.Int32:
		return s.SetInt(path, v.Int())
	case reflect.Uint, reflect.Uint64, reflect.Uint8, reflect.Uint16, reflect.Uint32:
		return s.SetInt(path, int64(v.Uint()))
	default:
		cPath := cString(path)
		s.pointers = append(s.pointers, unsafe.Pointer(cPath))
		size := int(reflect.TypeOf(val).Size())
		return sp_setstring(s.ptr, cPath, (unsafe.Pointer)(reflect.ValueOf(val).Pointer()), size)
	}
}

func (s *store) SetString(path, val string) bool {
	cPath := cString(path)
	cVal := cString(val)
	s.pointers = append(s.pointers, unsafe.Pointer(cPath), unsafe.Pointer(cVal))
	return sp_setstring_s(s.ptr, cPath, cVal, len(val))
}

func (s *store) SetInt(path string, val int64) bool {
	cPath := cString(path)
	s.pointers = append(s.pointers, unsafe.Pointer(cPath))
	return sp_setint(s.ptr, cPath, val)
}

func (s *store) Get(path string, size *int) unsafe.Pointer {
	return sp_getstring(s.ptr, path, size)
}

func (s *store) GetString(path string, size *int) string {
	ptr := sp_getstring(s.ptr, path, size)
	sh := reflect.StringHeader{Data: uintptr(ptr), Len: *size}
	return *(*string)(unsafe.Pointer(&sh))
}

func (s *store) GetObject(path string) unsafe.Pointer {
	return sp_getobject(s.ptr, path)
}

func (s *store) GetInt(key string) int64 {
	return sp_getint(s.ptr, key)
}

func (s *store) Free() {
	for _, f := range s.pointers {
		free(f)
	}
	s.pointers = s.pointers[:0]
}
