package sophia

import (
	"fmt"
	"io/ioutil"
	"os"
	"testing"
	"unsafe"

	"github.com/stretchr/testify/require"
)

func TestDatabaseUpsert(t *testing.T) {
	const keyPath = "key"
	const valuePath = "id"
	tmpDir, err := ioutil.TempDir("", "sophia_test")
	require.Nil(t, err)
	defer os.RemoveAll(tmpDir)

	env, err := NewEnvironment()
	require.Nil(t, err)
	require.NotNil(t, env)

	require.True(t, env.SetString(EnvironmentPath, tmpDir))

	schema := &Schema{}
	require.Nil(t, schema.AddKey(keyPath, FieldTypeUInt32))
	require.Nil(t, schema.AddValue(valuePath, FieldTypeUInt32))

	db, err := env.NewDatabase(DatabaseConfig{
		Name:   "test_database",
		Schema: schema,
		Upsert: upsertCallback,
	})
	require.Nil(t, err)
	require.NotNil(t, db)

	require.Nil(t, env.Open())
	defer env.Close()

	/* increment key 10 times */
	const key uint32 = 1234
	const iterations = 10
	var increment int64 = 1
	for i := 0; i < iterations; i++ {
		doc := db.Document()
		doc.Set(keyPath, key)
		doc.Set(valuePath, increment)
		require.Nil(t, db.Upsert(doc))
	}

	/* get */
	doc := db.Document()
	doc.Set(keyPath, key)

	result, err := db.Get(doc)
	require.Nil(t, err)
	require.NotNil(t, result)
	defer result.Destroy()

	require.Equal(t, iterations*increment, result.GetInt(valuePath))
}

func TestDatabaseUpsertWithArg(t *testing.T) {
	const (
		keyPath   = "key"
		valuePath = "id"
		upsertArg = 5
	)
	tmpDir, err := ioutil.TempDir("", "sophia_test")
	require.Nil(t, err)
	defer os.RemoveAll(tmpDir)

	env, err := NewEnvironment()
	require.Nil(t, err)
	require.NotNil(t, env)

	require.True(t, env.SetString(EnvironmentPath, tmpDir))

	schema := &Schema{}
	require.Nil(t, schema.AddKey(keyPath, FieldTypeUInt32))
	require.Nil(t, schema.AddValue(valuePath, FieldTypeUInt32))

	db, err := env.NewDatabase(DatabaseConfig{
		Name:      "test_database",
		Schema:    schema,
		Upsert:    upsertCallbackWithArg,
		UpsertArg: upsertArg,
	})
	require.Nil(t, err)
	require.NotNil(t, db)

	require.Nil(t, env.Open())
	defer env.Close()

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

	expected := iterations*increment + upsertArg*(iterations-1)
	require.Equal(t, expected, result.GetInt("id"))
}

func upsertCallback(count int,
	src []unsafe.Pointer, srcSize []uint32,
	upsert []unsafe.Pointer, upsertSize []uint32,
	result []unsafe.Pointer, resultSize []uint32,
	arg unsafe.Pointer) int {

	if src == nil {
		return 0
	}
	var a uint32 = *(*uint32)(src[1])
	var b uint32 = *(*uint32)(upsert[1])
	ret := a + b
	resPtr := (*uint32)(result[1])
	*resPtr = ret
	return 0
}

func upsertCallbackWithArg(count int,
	src []unsafe.Pointer, srcSize []uint32,
	upsert []unsafe.Pointer, upsertSize []uint32,
	result []unsafe.Pointer, resultSize []uint32,
	arg unsafe.Pointer) int {

	if src == nil {
		return 0
	}
	var a uint32 = *(*uint32)(src[1])
	var b uint32 = *(*uint32)(upsert[1])
	var c uint32 = *(*uint32)(arg)
	ret := a + b + c
	resPtr := (*uint32)(result[1])
	*resPtr = ret
	return 0
}

func TestDatabaseUpsertError(t *testing.T) {
	const keyPath = "key"
	const valuePath = "id"
	tmpDir, err := ioutil.TempDir("", "sophia_test")
	require.Nil(t, err)
	defer os.RemoveAll(tmpDir)

	env, err := NewEnvironment()
	require.Nil(t, err)
	require.NotNil(t, env)

	require.True(t, env.SetString(EnvironmentPath, tmpDir))

	schema := &Schema{}
	require.Nil(t, schema.AddKey(keyPath, FieldTypeUInt32))
	require.Nil(t, schema.AddValue(valuePath, FieldTypeUInt32))

	db, err := env.NewDatabase(DatabaseConfig{
		Name:   "test_database",
		Schema: schema,
	})
	require.Nil(t, err)
	require.NotNil(t, db)

	require.Nil(t, env.Open())
	defer env.Close()
	doc := db.Document()
	require.False(t, doc.IsEmpty())
	require.True(t, doc.Set("key", 1))
	require.True(t, doc.Set("id", 1))
	require.NotNil(t, db.Upsert(doc))
}

func TestDatabaseUpsertArguments(t *testing.T) {
	const keyPath = "key"
	const valuePath = "id"
	tmpDir, err := ioutil.TempDir("", "sophia_test")
	require.Nil(t, err)
	defer os.RemoveAll(tmpDir)

	env, err := NewEnvironment()
	require.Nil(t, err)
	require.NotNil(t, env)

	require.True(t, env.SetString(EnvironmentPath, tmpDir))

	schema := &Schema{}
	require.Nil(t, schema.AddKey(keyPath, FieldTypeUInt32))
	require.Nil(t, schema.AddValue(valuePath, FieldTypeUInt32))

	db, err := env.NewDatabase(DatabaseConfig{
		Name:   "test_database",
		Schema: schema,
		Upsert: func(count int,
			src []unsafe.Pointer, srcSize []uint32,
			upsert []unsafe.Pointer, upsertSize []uint32,
			result []unsafe.Pointer, resultSize []uint32,
			arg unsafe.Pointer) int {

			if count != 4 {
				panic(fmt.Sprintf("count should be equals 4, got: %v", count))
			}

			if src != nil {
				if len(src) != count {
					panic(fmt.Sprintf("length of src should be equals count, got: %v", len(src)))
				}
				if len(srcSize) != count {
					panic(fmt.Sprintf("length of srcSize should be equals count, got: %v", len(src)))
				}
			}

			if len(upsert) != count {
				panic(fmt.Sprintf("length of upsert should be equals count, got: %v", len(upsert)))
			}
			if len(upsertSize) != count {
				panic(fmt.Sprintf("length of upsertSize should be equals count, got: %v", len(upsertSize)))
			}

			if len(result) != count {
				panic(fmt.Sprintf("length of result should be equals count, got: %v", len(result)))
			}
			if len(resultSize) != count {
				panic(fmt.Sprintf("length of resultSize should be equals count, got: %v", len(resultSize)))
			}

			if arg != nil {
				panic(fmt.Sprintf("arg should be nil, got: %#v", arg))
			}

			return upsertCallback(count, src, srcSize, upsert, upsertSize, result, resultSize, arg)
		},
	})
	require.Nil(t, err)
	require.NotNil(t, db)

	require.Nil(t, env.Open())
	defer env.Close()

	const key uint32 = 1234
	const value uint32 = 1
	expectedUpsertArgs := []struct {
		key, value uint32
	}{
		{key: key, value: value},
		{key: key, value: value * 2},
		{key: key, value: value * 3},
		{key: key, value: value * 4},
		{key: key, value: value * 5},
		{key: key, value: value * 6},
	}

	for _, expected := range expectedUpsertArgs {
		doc := db.Document()
		doc.Set(keyPath, key)
		doc.Set(valuePath, value)
		require.Nil(t, db.Upsert(doc))
		doc.Free()

		doc = db.Document()
		doc.Set(keyPath, key)

		result, err := db.Get(doc)
		require.Nil(t, err)
		require.NotNil(t, result)

		require.EqualValues(t, expected.key, result.GetInt(keyPath))
		require.EqualValues(t, expected.key, result.GetInt(keyPath))
	}
}
