package sophia

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestUnknownFieldType(t *testing.T) {
	typ := FieldType(len(fieldTypeNames))
	require.Panics(t, func() {
		_ = typ.String()
	})
}

func TestUnknownCompressionType(t *testing.T) {
	typ := CompressionType(len(compressionTypeNames))
	require.Panics(t, func() {
		_ = typ.String()
	})
}
