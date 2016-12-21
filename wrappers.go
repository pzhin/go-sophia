package sophia

import (
	"errors"
	"unsafe"
)

/*
#cgo LDFLAGS: -lrt
#include <sophia.h>
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
*/
import "C"

// spDestroy wrapper for sp_destroy
// destroys C sophia object
func spDestroy(p unsafe.Pointer) error {
	if nil == p {
		return nil
	}
	if 0 != C.sp_destroy(p) {
		return errors.New("failed close resource")
	}
	return nil
}

// spSetString wrapper for sp_setstring for common cases
func spSetString(obj unsafe.Pointer, path *C.char, val unsafe.Pointer, size int) bool {
	return C.sp_setstring(obj, path, val, C.int(size)) == 0
}

func spGetString(obj unsafe.Pointer, path *C.char, size *int) unsafe.Pointer {
	cSize := C.int(*size)
	ptr := unsafe.Pointer(C.sp_getstring(obj, path, &cSize))
	*size = int(cSize)
	return ptr
}

// spSetInt wrapper for sp_setint
func spSetInt(obj unsafe.Pointer, path *C.char, val int64) bool {
	return C.sp_setint(obj, path, C.int64_t(val)) == 0
}

// spGetInt wrapper for sp_getint
func spGetInt(obj unsafe.Pointer, path *C.char) int64 {
	ptr := C.sp_getint(obj, path)
	return *(*int64)(unsafe.Pointer(&ptr))
}

// spGetObject wrapper for sp_getobject
func spGetObject(obj unsafe.Pointer, path *C.char) unsafe.Pointer {
	return unsafe.Pointer(C.sp_getobject(obj, path))
}

// spOpen wrapper for sp_open
func spOpen(ptr unsafe.Pointer) bool {
	return C.sp_open(ptr) == 0
}

// spEnv wrapper for sp_env
func spEnv() unsafe.Pointer {
	return unsafe.Pointer(C.sp_env())
}

// spCursor wrapper for sp_cursor
func spCursor(ptr unsafe.Pointer) unsafe.Pointer {
	return unsafe.Pointer(C.sp_cursor(ptr))
}

// spDocument wrapper for sp_document
func spDocument(ptr unsafe.Pointer) unsafe.Pointer {
	return unsafe.Pointer(C.sp_document(ptr))
}

// spGet wrapper for sp_get
func spGet(obj, doc unsafe.Pointer) unsafe.Pointer {
	return unsafe.Pointer(C.sp_get(obj, doc))
}

// spSet wrapper for sp_set
func spSet(obj, doc unsafe.Pointer) bool {
	return C.sp_set(obj, doc) == 0
}

// spUpsert wrapper for sp_upsert
func spUpsert(obj, doc unsafe.Pointer) bool {
	return C.sp_upsert(obj, doc) == 0
}

// spDelete wrapper for sp_delete
func spDelete(obj, doc unsafe.Pointer) bool {
	return C.sp_delete(obj, doc) == 0
}

// spCommit wrapper for sp_commit
func spCommit(tx unsafe.Pointer) int {
	return int(C.sp_commit(tx))
}

// spBegin wrapper for sp_begin
func spBegin(env unsafe.Pointer) unsafe.Pointer {
	return unsafe.Pointer(C.sp_begin(env))
}

func free(ptr unsafe.Pointer) {
	C.free(ptr)
}

func goString(ptr unsafe.Pointer) string {
	return C.GoString((*C.char)(ptr))
}

func cString(str string) *C.char {
	return C.CString(str)
}
