package sophia

import (
	"errors"
	"unsafe"
)

/*
#cgo LDFLAGS: -lsophia
#include <sophia.h>
#include <stdio.h>
#include <stdlib.h>
extern void    *sp_env(void);
extern void    *sp_document(void*);
extern int      sp_setstring(void*, const char*, const void*, int);
extern int      sp_setint(void*, const char*, int64_t);
extern void    *sp_getobject(void*, const char*);
extern void    *sp_getstring(void*, const char*, int*);
extern int64_t  sp_getint(void*, const char*);
extern int      sp_open(void*);
extern int      sp_destroy(void*);
extern int      sp_set(void*, void*);
extern int      sp_upsert(void*, void*);
extern int      sp_delete(void*, void*);
extern void    *sp_get(void*, void*);
extern void    *sp_cursor(void*);
extern void    *sp_begin(void*);
extern int      sp_prepare(void*);
extern int      sp_commit(void*);
char* pointer_to_string(void* ptr)
{
	return (char*)ptr;
}
int sp_setstringS(void* obj, char* path, char* val, int size)
{
	return sp_setstring(obj, path, (void*)val, size);
}
*/
import "C"

type size_t C.size_t

// sp_close closes the pointer and sets it to nil
// to ensure it cannot be closed twice.
func sp_close(p unsafe.Pointer) error {
	if nil == p {
		return nil
	}
	if 0 != C.sp_destroy(p) {
		return errors.New("failed close resource")
	}
	p = nil
	return nil
}

// TODO :: check that all memmory will be freed
// wrapper for sp_setstring
func sp_setstring(obj unsafe.Pointer, path *C.char, val unsafe.Pointer, size int) bool {
	return C.sp_setstring(obj, path, val, C.int(size)) == 0
}

func sp_setstring_s(obj unsafe.Pointer, path *C.char, val *C.char, size int) bool {
	return C.sp_setstringS(obj, path, val, C.int(size)) == 0
}

// TODO :: memory leak
func sp_getstring(obj unsafe.Pointer, path string, size *int) unsafe.Pointer {
	cPath := C.CString(path)
	cSize := C.int(*size)
	ptr := unsafe.Pointer(C.sp_getstring(obj, cPath, &cSize))
	//defer free(unsafe.Pointer(cPath))
	*size = int(cSize)
	return ptr
}

func GoString(ptr unsafe.Pointer) string {
	cStr := C.pointer_to_string(ptr)
	return C.GoString(cStr)
}

func sp_setint(obj unsafe.Pointer, path string, val int64) bool {
	cPath := C.CString(path)
	cVal := C.int64_t(val)
	e := C.sp_setint(obj, cPath, cVal)
	return e == 0
}

func sp_getint(obj unsafe.Pointer, path string) int64 {
	cPath := C.CString(path)
	ptr := C.sp_getint(obj, cPath)
	return *(*int64)(unsafe.Pointer(&ptr))
}

func sp_getobject(obj unsafe.Pointer, path string) unsafe.Pointer {
	cPath := C.CString(path)
	return unsafe.Pointer(C.sp_getobject(obj, cPath))
}

func sp_open(ptr unsafe.Pointer) bool {
	return C.sp_open(ptr) == 0
}

func sp_env() unsafe.Pointer {
	return unsafe.Pointer(C.sp_env())
}

func sp_cursor(ptr unsafe.Pointer) unsafe.Pointer {
	return unsafe.Pointer(C.sp_cursor(ptr))
}

func sp_document(ptr unsafe.Pointer) unsafe.Pointer {
	return unsafe.Pointer(C.sp_document(ptr))
}

func sp_get(ptr1, ptr2 unsafe.Pointer) unsafe.Pointer {
	return unsafe.Pointer(C.sp_get(ptr1, ptr2))
}

func sp_set(ptr1, ptr2 unsafe.Pointer) bool {
	return C.sp_set(ptr1, ptr2) == 0
}

func sp_upsert(ptr1, ptr2 unsafe.Pointer) bool {
	return C.sp_upsert(ptr1, ptr2) == 0
}

func sp_delete(ptr1, ptr2 unsafe.Pointer) bool {
	return C.sp_delete(ptr1, ptr2) == 0
}

func free(ptr unsafe.Pointer) {
	C.free(ptr)
}

func goBytes(ptr unsafe.Pointer, size int64) []byte {
	return C.GoBytes(ptr, C.int(size))
}
