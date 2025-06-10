package sophia

import (
	"reflect"
	"unsafe"
)

// varStore manages memory allocation and free for C variables
// Interface for C sophia object
// Only for internal usage
type varStore struct {
	// ptr Pointer to C sophia object
	ptr unsafe.Pointer

	// pointers slice of pointers to allocated C variables,
	// that must be freed after store usage
	pointers []unsafe.Pointer

	// cache used to store path C strings
	cache CStringCache
}

func newVarStore(ptr unsafe.Pointer, size int, cache CStringCache) varStore {
	ret := varStore{ptr: ptr, cache: cache}
	if size > 0 {
		ret.pointers = make([]unsafe.Pointer, 0, size)
	}
	return ret
}

func (s *varStore) IsEmpty() bool {
	return s.ptr == nil
}

// TODO :: implement custom types
func (s *varStore) Set(path string, val interface{}) bool {
	v := reflect.ValueOf(val)

	switch v.Kind() {
	case reflect.String:
		return s.SetString(path, v.String())
	case reflect.Int, reflect.Int64, reflect.Int8, reflect.Int16, reflect.Int32:
		return s.SetInt(path, v.Int())
	case reflect.Uint, reflect.Uint64, reflect.Uint8, reflect.Uint16, reflect.Uint32:
		return s.SetInt(path, int64(v.Uint()))
	}

	cPath := s.cache.Acquire(path)
	defer s.cache.Release(path)

	size := int(reflect.TypeOf(val).Size())
	return spSetString(s.ptr, cPath, (unsafe.Pointer)(reflect.ValueOf(val).Pointer()), size)
}

func (s *varStore) SetString(path, val string) bool {
	cPath := s.cache.Acquire(path)
	defer s.cache.Release(path)
	cVal := cString(val)
	s.pointers = append(s.pointers, unsafe.Pointer(cVal))
	return spSetString(s.ptr, cPath, unsafe.Pointer(cVal), len(val))
}

func (s *varStore) SetInt(path string, val int64) bool {
	cPath := s.cache.Acquire(path)
	defer s.cache.Release(path)
	return spSetInt(s.ptr, cPath, val)
}

func (s *varStore) Get(path string, size *int) unsafe.Pointer {
	cPath := s.cache.Acquire(path)
	defer s.cache.Release(path)
	return spGetString(s.ptr, cPath, size)
}

// GetString returns string without extra allocations.
// We can use C pointer to string to make Go string without allocation.
// C memory will be freed on Document Destroy() call.
// So for long-term usage you should to make copy of string to avoid data corruption.
func (s *varStore) GetString(path string, size *int) string {
	cPath := s.cache.Acquire(path)
	defer s.cache.Release(path)
	ptr := spGetString(s.ptr, cPath, size)
	sh := reflect.StringHeader{Data: uintptr(ptr), Len: *size}
	return *(*string)(unsafe.Pointer(&sh))
}

func (s *varStore) GetObject(path string) unsafe.Pointer {
	cPath := s.cache.Acquire(path)
	defer s.cache.Release(path)
	return spGetObject(s.ptr, cPath)
}

func (s *varStore) GetInt(path string) int64 {
	cPath := s.cache.Acquire(path)
	defer s.cache.Release(path)
	return spGetInt(s.ptr, cPath)
}

// Free frees allocated memory for all C variables, that were in this store
// This always should be called to prevent memory leaks
func (s *varStore) Free() {
	for _, f := range s.pointers {
		free(f)
	}
	s.pointers = s.pointers[:0]
}
