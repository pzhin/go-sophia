package sophia

/*
#include <stdlib.h>
*/
import "C"

import (
	"sync"
	"unsafe"
)

// StaticCache caches strings indefinitely without reference counting or cleanup.
type StaticCache struct {
	mu    sync.RWMutex
	cache map[string]*C.char
}

var _ CStringCache = (*StaticCache)(nil)

func NewStaticCache() *StaticCache {
	return &StaticCache{cache: make(map[string]*C.char)}
}

func (c *StaticCache) Acquire(s string) *C.char {
	c.mu.RLock()
	ptr, ok := c.cache[s]
	c.mu.RUnlock()
	if ok {
		return ptr
	}
	ptr = cString(s)
	c.mu.Lock()
	if existing, ok := c.cache[s]; ok {
		c.mu.Unlock()
		free(unsafe.Pointer(ptr))
		return existing
	}
	c.cache[s] = ptr
	c.mu.Unlock()
	return ptr
}

func (c *StaticCache) Release(string) {}

func (c *StaticCache) Clear() {
	c.mu.Lock()
	defer c.mu.Unlock()
	for k, v := range c.cache {
		free(unsafe.Pointer(v))
		delete(c.cache, k)
	}
}
