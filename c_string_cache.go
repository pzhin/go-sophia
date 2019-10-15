package sophia

import (
	"sync"

	"C"
)

var cache = map[string]*C.char{}
var mLock sync.RWMutex

func getCStringFromCache(str string) *C.char {
	mLock.RLock()
	cStr, ok := cache[str]
	mLock.RUnlock()
	if ok {
		return cStr
	}

	mLock.Lock()
	cStr, ok = cache[str]
	if !ok {
		cStr = cString(str)
		cache[str] = cStr
	}
	mLock.Unlock()
	return cStr
}
