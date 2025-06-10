package sophia

/*
#include <stdlib.h>
*/
import "C"

import (
	"sync"
	"unsafe"
)

// sizeEntry stores pointer and reference count for SizedCache.
type sizeEntry struct {
	ptr  *C.char
	refs int
}

// SizedCache caches C strings with a maximum number of entries.
type SizedCache struct {
	mu      sync.RWMutex
	entries map[string]*sizeEntry
	maxSize int
}

var _ CStringCache = (*SizedCache)(nil)

// NewSizedCache creates a cache limited by entry count.
func NewSizedCache(limit int) *SizedCache {
	return &SizedCache{entries: make(map[string]*sizeEntry), maxSize: limit}
}

func (c *SizedCache) Acquire(s string) *C.char {
	c.mu.RLock()
	if e, ok := c.entries[s]; ok {
		e.refs++
		c.mu.RUnlock()
		return e.ptr
	}
	c.mu.RUnlock()

	ptr := cString(s)
	c.mu.Lock()
	if e, ok := c.entries[s]; ok {
		e.refs++
		c.mu.Unlock()
		free(unsafe.Pointer(ptr))
		return e.ptr
	}
	c.entries[s] = &sizeEntry{ptr: ptr, refs: 1}
	c.mu.Unlock()
	return ptr
}

func (c *SizedCache) Release(s string) {
	c.mu.Lock()
	e, ok := c.entries[s]
	if !ok || e.refs == 0 {
		c.mu.Unlock()
		return
	}
	e.refs--
	if e.refs == 0 && c.maxSize > 0 && len(c.entries) >= c.maxSize {
		free(unsafe.Pointer(e.ptr))
		delete(c.entries, s)
	}
	c.mu.Unlock()
}

func (c *SizedCache) Clear() {
	c.mu.Lock()
	for k, e := range c.entries {
		free(unsafe.Pointer(e.ptr))
		delete(c.entries, k)
	}
	c.mu.Unlock()
}
