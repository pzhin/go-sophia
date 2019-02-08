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
// Should return 0 in case of success, otherwise -1.
type UpsertFunc func(count int,
	src []unsafe.Pointer, srcSize []uint32,
	upsert []unsafe.Pointer, upsertSize []uint32,
	result []unsafe.Pointer, resultSize []uint32,
	arg unsafe.Pointer) int

//export goUpsertCall
func goUpsertCall(count C.int,
	src **C.char, src_size *C.uint32_t,
	upsert **C.char, upsert_size *C.uint32_t,
	result **C.char, result_size *C.uint32_t,
	arg unsafe.Pointer) {

	index := (*int)(arg)
	fn := getUpsert(index)
	upsertArg := getUpsertArg(index)
	fn(count, src, src_size, upsert, upsert_size, result, result_size, upsertArg)
}

var upsertMap = make(map[*int]upsertFunc)
var upsertMu sync.RWMutex
var upsertIndex int

var upsertArgMap = make(map[*int]unsafe.Pointer)
var upsertArgMu sync.RWMutex

func getUpsertArg(index *int) unsafe.Pointer {
	upsertArgMu.RLock()
	defer upsertArgMu.RUnlock()
	return upsertArgMap[index]
}

func registerUpsertArg(index *int, arg interface{}) {
	upsertArgMu.Lock()
	defer upsertArgMu.Unlock()
	if arg == nil {
		return
	}
	val := reflect.ValueOf(arg)
	if val.CanAddr() {
		upsertArgMap[index] = unsafe.Pointer(val.Pointer())
		return
	}

	switch val.Kind() {
	case reflect.String:
		str := val.String()
		upsertArgMap[index] = unsafe.Pointer(&str)
	case reflect.Int, reflect.Int64, reflect.Int8, reflect.Int16, reflect.Int32:
		i := val.Int()
		upsertArgMap[index] = unsafe.Pointer(&i)
	case reflect.Uint, reflect.Uint64, reflect.Uint8, reflect.Uint16, reflect.Uint32:
		i := val.Uint()
		upsertArgMap[index] = unsafe.Pointer(&i)
	}
}

func registerUpsert(upsertFunc UpsertFunc) (unsafe.Pointer, *int) {
	upsertMu.Lock()
	defer upsertMu.Unlock()

	index := upsertIndex
	upsertIndex++
	indexPtr := &index

	upsertMap[indexPtr] = func(count C.int,
		srcC **C.char, srcSizesC *C.uint32_t,
		upsertC **C.char, upsertSizesC *C.uint32_t,
		resultC **C.char, resultSizesC *C.uint32_t,
		arg unsafe.Pointer) C.int {

		countN := int(count)

		// We receive C pointer to pointer which can be interpreted as an array of pointers.
		// Here we cast C pointer to pointer to Go slice of pointers.
		upsertSizes := (*[16]uint32)(unsafe.Pointer(upsertSizesC))[:countN]
		resultSizes := (*[16]uint32)(unsafe.Pointer(resultSizesC))[:countN]

		upsert := (*[16]unsafe.Pointer)(unsafe.Pointer(upsertC))[:countN]
		result := (*[16]unsafe.Pointer)(unsafe.Pointer(resultC))[:countN]

		var src []unsafe.Pointer
		var srcSizes []uint32
		if srcC != nil {
			srcSizes = (*[16]uint32)(unsafe.Pointer(srcSizesC))[:countN]
			src = (*[16]unsafe.Pointer)(unsafe.Pointer(srcC))[:countN]
		}

		res := upsertFunc(countN,
			src, srcSizes,
			upsert, upsertSizes,
			result, resultSizes,
			arg)

		return C.int(res)
	}
	ptr := C.goUpsertCall
	return ptr, indexPtr
}

func getUpsert(index *int) upsertFunc {
	upsertMu.RLock()
	defer upsertMu.RUnlock()
	return upsertMap[index]
}

func unregisterUpsert(index *int) {
	upsertMu.Lock()
	defer upsertMu.Unlock()
	delete(upsertMap, index)
}
