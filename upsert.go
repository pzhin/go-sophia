package sophia

import (
	"reflect"
	"sync"
	"unsafe"
)

/*
#include <inttypes.h>
#include <stdio.h>
extern void goUpsertCall(int count,
		char **src,    uint32_t *src_size,
		char **upsert, uint32_t *upsert_size,
		char **result, uint32_t *result_size,
		void *arg);
*/
import "C"

// keyUpsertTemplate template for upsert settings key
const (
	keyUpsertTemplate    = "db.%v.upsert"
	keyUpsertArgTemplate = "db.%v.upsert_arg"
)

// upsertFunc golang binding to upsert_callback.
// It is a wrapper for UpsertFunc, that converts C types to golang ones
type upsertFunc func(count C.int,
	src **C.char, srcSize *C.uint32_t,
	upsert **C.char, upsertSize *C.uint32_t,
	result **C.char, resultSize *C.uint32_t,
	arg unsafe.Pointer) C.int

// UpsertFunc golang equivalent of upsert_callback.
// src, upsert, result
// Should return 0 in case of success, otherwise -1
type UpsertFunc func(count int,
	src []unsafe.Pointer, srcSize uint32,
	upsert []unsafe.Pointer, upsertSize uint32,
	result []unsafe.Pointer, resultSize uint32,
	arg unsafe.Pointer) int

//export goUpsertCall
func goUpsertCall(count C.int,
	src **C.char, src_size *C.uint32_t,
	upsert **C.char, upsert_size *C.uint32_t,
	result **C.char, result_size *C.uint32_t,
	arg unsafe.Pointer) {
	index := *(*int)(arg)
	fn := getUpsert(index)
	upsertArg := getUpsertArg(index)
	fn(count, src, src_size, upsert, upsert_size, result, result_size, upsertArg)
}

var upsertMap = make(map[int]upsertFunc)
var upsertMu sync.RWMutex
var upsertIndex int

var upsertArgMap = make(map[int]unsafe.Pointer)
var upsertArgMu sync.RWMutex

func getUpsertArg(index int) unsafe.Pointer {
	upsertArgMu.RLock()
	defer upsertArgMu.RUnlock()
	return upsertArgMap[index]
}

func registerUpsertArg(index int, arg interface{}) {
	upsertArgMu.Lock()
	defer upsertArgMu.Unlock()
	val := reflect.ValueOf(arg)
	if arg == nil || val.IsNil() {
		return
	}
	upsertArgMap[index] = unsafe.Pointer(val.Pointer())
}

func registerUpsert(upsertFunc UpsertFunc) (unsafe.Pointer, int) {
	upsertMu.Lock()
	defer upsertMu.Unlock()
	index := upsertIndex
	upsertIndex++
	upsertMap[index] = func(count C.int,
		src **C.char, srcSize *C.uint32_t,
		upsert **C.char, upsertSize *C.uint32_t,
		result **C.char, resultSize *C.uint32_t,
		arg unsafe.Pointer) C.int {
		if src == nil {
			return C.int(0)
		}
		var sSize uint32
		if srcSize != nil {
			sSize = uint32(*srcSize)
		}
		uSize := uint32(*upsertSize)
		rSize := uint32(*resultSize)
		countN := int(count)

		slice1 := (*[1 << 4]unsafe.Pointer)(unsafe.Pointer(src))[:countN:countN]
		slice2 := (*[1 << 4]unsafe.Pointer)(unsafe.Pointer(upsert))[:countN:countN]
		slice3 := (*[1 << 4]unsafe.Pointer)(unsafe.Pointer(result))[:countN:countN]

		res := upsertFunc(countN,
			slice1, sSize,
			slice2, uSize,
			slice3, rSize,
			arg)

		return C.int(res)
	}
	ptr := C.goUpsertCall
	return ptr, index
}

func getUpsert(index int) upsertFunc {
	upsertMu.RLock()
	defer upsertMu.RUnlock()
	return upsertMap[index]
}

func unregisterUpsert(index int) {
	upsertMu.Lock()
	defer upsertMu.Unlock()
	delete(upsertMap, index)
}
