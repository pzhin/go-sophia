package sophia

/*
#include <stdlib.h>
*/
import "C"

import (
	"sync"
	"time"
	"unsafe"
)

// ttlEntry holds a cached C string with reference count and last usage timestamp.
type ttlEntry struct {
	ptr  *C.char
	refs int
	last int64 // unix nano of last zero ref
}

// TTLCache caches C strings and evicts zero-ref entries after ttl.
type TTLCache struct {
	mu        sync.Mutex
	entries   map[string]*ttlEntry
	zeroQueue []string
	ttl       time.Duration
}

var _ CStringCache = (*TTLCache)(nil)

// NewTTLCache creates an unlimited reference-counted cache without TTL.
func NewTTLCache() *TTLCache { return NewTTLCacheWithTTL(0) }

// NewTTLCacheWithTTL creates a reference-counted cache with TTL for unused entries.
func NewTTLCacheWithTTL(ttl time.Duration) *TTLCache {
	return &TTLCache{entries: make(map[string]*ttlEntry), ttl: ttl}
}

func (c *TTLCache) Acquire(s string) *C.char {
	now := time.Now().UnixNano()
	c.mu.Lock()
	if e, ok := c.entries[s]; ok {
		e.refs++
		e.last = 0
		c.removeExpired(now)
		c.mu.Unlock()
		return e.ptr
	}
	ptr := cString(s)
	c.entries[s] = &ttlEntry{ptr: ptr, refs: 1}
	c.removeExpired(now)
	c.mu.Unlock()
	return ptr
}

func (c *TTLCache) Release(s string) {
	now := time.Now().UnixNano()
	c.mu.Lock()
	e, ok := c.entries[s]
	if !ok || e.refs == 0 {
		c.mu.Unlock()
		return
	}
	e.refs--
	if e.refs == 0 {
		e.last = now
		c.zeroQueue = append(c.zeroQueue, s)
	}
	c.removeExpired(now)
	c.mu.Unlock()
}

func (c *TTLCache) Clear() {
	c.mu.Lock()
	for k, e := range c.entries {
		free(unsafe.Pointer(e.ptr))
		delete(c.entries, k)
	}
	c.zeroQueue = c.zeroQueue[:0]
	c.mu.Unlock()
}

func (c *TTLCache) removeExpired(now int64) {
	if c.ttl <= 0 {
		return
	}
	for len(c.zeroQueue) > 0 {
		k := c.zeroQueue[0]
		e, ok := c.entries[k]
		if !ok || e.refs != 0 {
			c.zeroQueue = c.zeroQueue[1:]
			continue
		}
		if time.Duration(now-e.last) > c.ttl {
			free(unsafe.Pointer(e.ptr))
			delete(c.entries, k)
			c.zeroQueue = c.zeroQueue[1:]
		} else {
			break
		}
	}
}
