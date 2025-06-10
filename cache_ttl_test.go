package sophia

import (
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestTTLCacheBasic(t *testing.T) {
	c := NewTTLCache()
	p1 := c.Acquire("a")
	p2 := c.Acquire("a")
	require.Equal(t, p1, p2)
	c.Release("a")
	c.Release("a")
	p3 := c.Acquire("a")
	p4 := c.Acquire("a")
	require.Equal(t, p3, p4)
	c.Release("a")
	c.Release("a")
}

func TestTTLCacheTTL(t *testing.T) {
	c := NewTTLCacheWithTTL(10 * time.Millisecond)
	p1 := c.Acquire("a")
	c.Release("a")
	time.Sleep(5 * time.Millisecond)
	p2 := c.Acquire("a")
	require.Equal(t, p1, p2)
	c.Release("a")
	time.Sleep(15 * time.Millisecond)
	p3 := c.Acquire("a")
	require.Equal(t, p2, p3)
	c.Release("a")
}

func TestTTLCacheRace(t *testing.T) {
	cache := NewTTLCache()
	var wg sync.WaitGroup
	workers := runtime.GOMAXPROCS(-1) * 8
	const iterations = 100
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < iterations; j++ {
				cache.Acquire("x")
				cache.Release("x")
			}
		}()
	}
	wg.Wait()
}
