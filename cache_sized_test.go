package sophia

import (
	"testing"
	"unsafe"

	"github.com/stretchr/testify/require"
)

func TestSizedCacheBasic(t *testing.T) {
	c := NewSizedCache(1)
	p1 := c.Acquire("a")
	c.Release("a")
	p2 := c.Acquire("a")
	require.Equal(t, uintptr(unsafe.Pointer(p1)), uintptr(unsafe.Pointer(p2)))
	c.Release("a")
}

func TestSizedCacheEviction(t *testing.T) {
	c := NewSizedCache(1)
	c.Acquire("a")
	c.Acquire("b")
	c.Release("a")
	require.Len(t, c.entries, 1)
	require.Contains(t, c.entries, "b")
	c.Release("b")
	require.Len(t, c.entries, 0)
}
