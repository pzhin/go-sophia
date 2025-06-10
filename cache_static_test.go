package sophia

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestStaticCache(t *testing.T) {
	c := NewStaticCache()
	p1 := c.Acquire("a")
	p2 := c.Acquire("a")
	require.Equal(t, p1, p2)
	c.Release("a")
	c.Clear()
	p3 := c.Acquire("a")
	p4 := c.Acquire("a")
	require.Equal(t, p3, p4)
	c.Release("a")
	c.Release("a")
	c.Clear()
}
