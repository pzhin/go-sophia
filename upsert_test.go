package sophia

import (
	"os"
	"testing"
	"unsafe"

	"github.com/stretchr/testify/require"
)

func TestDatabaseUpsert(t *testing.T) {
	defer func() {
		require.Nil(t, os.RemoveAll(DBPath))
	}()
	env, err := NewEnvironment()
	require.Nil(t, err)
	require.NotNil(t, env)

	require.True(t, env.Set("sophia.path", DBPath))

	schema := &Schema{}
	require.Nil(t, schema.AddKey("key", FieldTypeUInt32))
	require.Nil(t, schema.AddValue("id", FieldTypeUInt32))

	db, err := env.NewDatabase(&DatabaseConfig{
		Name:   DBName,
		Schema: schema,
		Upsert: upsertCallback,
	})
	require.Nil(t, err)
	require.NotNil(t, db)

	require.Nil(t, env.Open())

	/* increment key 10 times */
	const key uint32 = 1234
	const iterations = 10
	var increment int64 = 1
	for i := 0; i < iterations; i++ {
		doc := db.Document()
		doc.Set("key", key)
		doc.Set("id", increment)
		require.Nil(t, db.Upsert(doc))
	}

	/* get */
	doc := db.Document()
	doc.Set("key", key)

	result, err := db.Get(doc)
	require.Nil(t, err)
	require.NotNil(t, result)
	defer result.Destroy()

	require.Equal(t, iterations*increment, result.GetInt("id"))
}

func TestDatabaseUpsertWithArg(t *testing.T) {
	defer func() {
		require.Nil(t, os.RemoveAll(DBPath))
	}()
	env, err := NewEnvironment()
	require.Nil(t, err)
	require.NotNil(t, env)

	require.True(t, env.Set("sophia.path", DBPath))

	schema := &Schema{}
	require.Nil(t, schema.AddKey("key", FieldTypeUInt32))
	require.Nil(t, schema.AddValue("id", FieldTypeUInt32))

	const upsertArg = 5

	db, err := env.NewDatabase(&DatabaseConfig{
		Name:      DBName,
		Schema:    schema,
		Upsert:    upsertCallbackWithArg,
		UpsertArg: upsertArg,
	})
	require.Nil(t, err)
	require.NotNil(t, db)

	require.Nil(t, env.Open())

	/* increment key 10 times */
	const key uint32 = 1234
	const iterations = 10
	var increment int64 = 1
	for i := 0; i < iterations; i++ {
		doc := db.Document()
		doc.Set("key", key)
		doc.Set("id", increment)
		require.Nil(t, db.Upsert(doc))
	}

	/* get */
	doc := db.Document()
	doc.Set("key", key)

	result, err := db.Get(doc)
	require.Nil(t, err)
	require.NotNil(t, result)
	defer result.Destroy()

	expected := iterations*increment+upsertArg*(iterations-1)
	require.Equal(t, expected, result.GetInt("id"))
}

func upsertCallback(count int,
	src []unsafe.Pointer, srcSize uint32,
	upsert []unsafe.Pointer, upsertSize uint32,
	result []unsafe.Pointer, resultSize uint32,
	arg unsafe.Pointer) int {
	var a uint32 = *(*uint32)(src[1])
	var b uint32 = *(*uint32)(upsert[1])
	ret := a + b
	resPtr := (*uint32)(result[1])
	*resPtr = ret
	return 0
}

func upsertCallbackWithArg(count int,
	src []unsafe.Pointer, srcSize uint32,
	upsert []unsafe.Pointer, upsertSize uint32,
	result []unsafe.Pointer, resultSize uint32,
	arg unsafe.Pointer) int {
	var a uint32 = *(*uint32)(src[1])
	var b uint32 = *(*uint32)(upsert[1])
	var c uint32 = *(*uint32)(arg)
	ret := a + b + c
	resPtr := (*uint32)(result[1])
	*resPtr = ret
	return 0
}

func TestDatabaseUpsertError(t *testing.T) {
	defer func() {
		require.Nil(t, os.RemoveAll(DBPath))
	}()
	env, err := NewEnvironment()
	require.Nil(t, err)
	require.NotNil(t, env)

	require.True(t, env.Set("sophia.path", DBPath))

	schema := &Schema{}
	require.Nil(t, schema.AddKey("key", FieldTypeUInt32))
	require.Nil(t, schema.AddValue("id", FieldTypeUInt32))

	db, err := env.NewDatabase(&DatabaseConfig{
		Name:   DBName,
		Schema: schema,
	})
	require.Nil(t, err)
	require.NotNil(t, db)

	require.Nil(t, env.Open())
	doc := db.Document()
	require.NotNil(t, doc)
	require.True(t, doc.Set("key", 1))
	require.True(t, doc.Set("id", 1))
	require.NotNil(t, db.Upsert(doc))
}
