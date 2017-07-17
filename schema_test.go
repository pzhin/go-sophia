package sophia

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSchemaDupKey(t *testing.T) {
	schema := Schema{}
	keyName := "key"
	require.Nil(t, schema.AddKey(keyName, FieldTypeString))

	require.Len(t, schema.keys, 1)
	require.Len(t, schema.keysNames, 1)

	require.Len(t, schema.values, 0)
	require.Len(t, schema.valuesNames, 0)

	require.Equal(t, FieldTypeString, schema.keys[keyName])
	require.Equal(t, keyName, schema.keysNames[0])

	require.NotNil(t, schema.AddKey(keyName, FieldTypeString))

	require.Len(t, schema.keys, 1)
	require.Len(t, schema.keysNames, 1)

	require.Len(t, schema.values, 0)
	require.Len(t, schema.valuesNames, 0)

	require.Equal(t, FieldTypeString, schema.keys[keyName])
	require.Equal(t, keyName, schema.keysNames[0])
}

func TestSchemaDupValue(t *testing.T) {
	schema := Schema{}
	valueName := "key"
	require.Nil(t, schema.AddValue(valueName, FieldTypeString))

	require.Len(t, schema.keys, 0)
	require.Len(t, schema.keysNames, 0)

	require.Len(t, schema.values, 1)
	require.Len(t, schema.valuesNames, 1)

	require.Equal(t, FieldTypeString, schema.values[valueName])
	require.Equal(t, valueName, schema.valuesNames[0])

	require.NotNil(t, schema.AddValue(valueName, FieldTypeString))

	require.Len(t, schema.keys, 0)
	require.Len(t, schema.keysNames, 0)

	require.Len(t, schema.values, 1)
	require.Len(t, schema.valuesNames, 1)

	require.Equal(t, FieldTypeString, schema.values[valueName])
	require.Equal(t, valueName, schema.valuesNames[0])
}
