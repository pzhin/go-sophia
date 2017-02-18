package sophia

import (
	"sync"

	"C"
)

var cache = map[string]*C.char{}
var mLock = sync.Mutex{}

func getCStringFromCache(str string) *C.char {
	cStr, ok := cache[str]
	if ok {
		return cStr
	}
	mLock.Lock()
	cStr, ok = cache[str]
	if ok {
		return cStr
	}
	cStr = cString(str)
	cache[str] = cStr
	mLock.Unlock()
	return cStr
}
