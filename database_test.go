package sophia

import (
	"fmt"
	"math/rand"
	"os"
	"testing"

	"io/ioutil"

	"github.com/stretchr/testify/require"
)

// TODO write tests:
//   - test more settings for environment
//   - test more settings for database

func TestDatabaseDocument(t *testing.T) {
	tmpDir, err := ioutil.TempDir("", "sophia_test")
	require.Nil(t, err)
	defer os.RemoveAll(tmpDir)

	env, err := NewEnvironment()
	require.Nil(t, err)
	require.NotNil(t, env)

	require.True(t, env.SetString(EnvironmentPath, tmpDir))

	db, err := env.NewDatabase(DatabaseConfig{
		Name: "test_database",
	})
	require.Nil(t, err)
	require.NotNil(t, db)

	require.Nil(t, env.Open())
	defer env.Close()

	doc := db.Document()
	require.NotNil(t, doc)
	require.Nil(t, env.Error())
}

func TestDatabaseSetInClosedEnvironment(t *testing.T) {
	const (
		keyPath       = "key"
		valuePath     = "value"
		expectedKey   = "key1"
		expectedValue = "value1"
	)
	tmpDir, err := ioutil.TempDir("", "sophia_test")
	require.Nil(t, err)
	defer os.RemoveAll(tmpDir)

	env, err := NewEnvironment()
	require.Nil(t, err)
	require.NotNil(t, env)

	require.True(t, env.SetString(EnvironmentPath, tmpDir))

	db, err := env.NewDatabase(DatabaseConfig{
		Name: "test_database",
	})
	require.Nil(t, err)
	require.NotNil(t, db)

	doc := db.Document()
	require.NotNil(t, doc)
	require.Nil(t, env.Error())
	defer doc.Free()

	require.True(t, doc.SetString(keyPath, expectedKey))
	require.True(t, doc.SetString(valuePath, expectedValue))

	require.NotNil(t, db.Set(doc))
}

func TestDatabaseSet(t *testing.T) {
	const (
		keyPath       = "key"
		valuePath     = "value"
		expectedKey   = "key1"
		expectedValue = "value1"
	)
	tmpDir, err := ioutil.TempDir("", "sophia_test")
	require.Nil(t, err)
	defer os.RemoveAll(tmpDir)

	env, err := NewEnvironment()
	require.Nil(t, err)
	require.NotNil(t, env)

	require.True(t, env.SetString(EnvironmentPath, tmpDir))

	db, err := env.NewDatabase(DatabaseConfig{
		Name: "test_database",
	})
	require.Nil(t, err)
	require.NotNil(t, db)

	require.Nil(t, env.Open())
	defer env.Close()

	doc := db.Document()
	require.NotNil(t, doc)
	require.Nil(t, env.Error())
	defer doc.Free()

	require.True(t, doc.SetString(keyPath, expectedKey))
	require.True(t, doc.SetString(valuePath, expectedValue))

	require.Nil(t, db.Set(doc))
}

func TestDatabaseGetFromClosedEnvironment(t *testing.T) {
	const keyPath = "key"
	const expectedKey = "key1"
	tmpDir, err := ioutil.TempDir("", "sophia_test")
	require.Nil(t, err)
	defer os.RemoveAll(tmpDir)

	env, err := NewEnvironment()
	require.Nil(t, err)
	require.NotNil(t, env)

	require.True(t, env.SetString(EnvironmentPath, tmpDir))

	db, err := env.NewDatabase(DatabaseConfig{
		Name: "test_database",
	})
	require.Nil(t, err)
	require.NotNil(t, db)

	doc := db.Document()
	require.NotNil(t, doc)
	require.Nil(t, env.Error())
	defer doc.Free()

	require.True(t, doc.SetString(keyPath, expectedKey))

	d, err := db.Get(doc)
	require.NotNil(t, err)
	require.Nil(t, d)
}

func TestDatabaseGet(t *testing.T) {
	const (
		keyPath       = "key"
		valuePath     = "value"
		expectedKey   = "key1"
		expectedValue = "value1"
	)
	tmpDir, err := ioutil.TempDir("", "sophia_test")
	require.Nil(t, err)
	defer os.RemoveAll(tmpDir)

	env, err := NewEnvironment()
	require.Nil(t, err)
	require.NotNil(t, env)

	require.True(t, env.SetString(EnvironmentPath, tmpDir))

	db, err := env.NewDatabase(DatabaseConfig{
		Name: "test_database",
	})
	require.Nil(t, err)
	require.NotNil(t, db)

	require.Nil(t, env.Open())
	defer env.Close()

	doc := db.Document()
	require.NotNil(t, doc)
	require.Nil(t, env.Error())

	require.True(t, doc.SetString(keyPath, expectedKey))
	require.True(t, doc.SetString(valuePath, expectedValue))

	require.Nil(t, db.Set(doc))
	doc.Free()

	doc = db.Document()
	require.NotNil(t, doc)
	require.Nil(t, env.Error())
	defer doc.Free()

	require.True(t, doc.SetString(keyPath, expectedKey))

	d, err := db.Get(doc)
	require.Nil(t, err)
	require.NotNil(t, d)
	d.Destroy()
}

func TestDatabaseDeleteFromClosedEnvironment(t *testing.T) {
	const keyPath = "key"
	const expectedKey = "key1"
	tmpDir, err := ioutil.TempDir("", "sophia_test")
	require.Nil(t, err)
	defer os.RemoveAll(tmpDir)

	env, err := NewEnvironment()
	require.Nil(t, err)
	require.NotNil(t, env)

	require.True(t, env.SetString(EnvironmentPath, tmpDir))

	db, err := env.NewDatabase(DatabaseConfig{
		Name: "test_database",
	})
	require.Nil(t, err)
	require.NotNil(t, db)

	doc := db.Document()
	require.NotNil(t, doc)
	require.Nil(t, env.Error())
	defer doc.Free()

	require.True(t, doc.SetString(keyPath, expectedKey))

	require.NotNil(t, db.Delete(doc))
}

func TestDatabaseDelete(t *testing.T) {
	const (
		keyPath       = "key"
		valuePath     = "value"
		expectedKey   = "key1"
		expectedValue = "value1"
	)
	tmpDir, err := ioutil.TempDir("", "sophia_test")
	require.Nil(t, err)
	defer os.RemoveAll(tmpDir)

	env, err := NewEnvironment()
	require.Nil(t, err)
	require.NotNil(t, env)

	require.True(t, env.SetString(EnvironmentPath, tmpDir))

	db, err := env.NewDatabase(DatabaseConfig{
		Name: "test_database",
	})
	require.Nil(t, err)
	require.NotNil(t, db)

	require.Nil(t, env.Open())
	defer env.Close()

	doc := db.Document()
	require.NotNil(t, doc)
	require.Nil(t, env.Error())

	require.True(t, doc.SetString(keyPath, expectedKey))
	require.True(t, doc.SetString(valuePath, expectedValue))

	require.Nil(t, db.Set(doc))
	doc.Free()

	doc = db.Document()
	require.NotNil(t, doc)
	require.Nil(t, env.Error())

	require.True(t, doc.SetString(keyPath, expectedKey))

	require.Nil(t, db.Delete(doc))
	doc.Free()

	doc = db.Document()
	require.NotNil(t, doc)
	require.Nil(t, env.Error())
	defer doc.Free()

	require.True(t, doc.SetString(keyPath, expectedKey))

	d, err := db.Get(doc)
	require.NotNil(t, err)
	require.Equal(t, ErrNotFound, err)
	require.Nil(t, d)
}

func TestDatabaseWithCustomSchema(t *testing.T) {
	const keyPath = "custom_key"
	const valuePath = "custom_value"
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

	const expectedKey int64 = 42
	const expectedValue int64 = 73

	doc := db.Document()
	require.NotNil(t, doc)
	require.True(t, doc.Set(keyPath, expectedKey))
	require.True(t, doc.Set(valuePath, expectedValue))

	err = db.Set(doc)
	doc.Free()
	require.Nil(t, err)

	doc = db.Document()
	require.NotNil(t, doc)
	require.True(t, doc.Set(keyPath, expectedKey))

	d, err := db.Get(doc)
	doc.Free()
	require.Nil(t, err)
	require.NotNil(t, d)

	defer d.Destroy()

	require.Equal(t, expectedKey, d.GetInt(keyPath))
	require.Equal(t, expectedValue, d.GetInt(valuePath))

	doc = db.Document()
	require.NotNil(t, doc)
	require.Nil(t, env.Error())

	require.True(t, doc.Set(keyPath, expectedKey))

	require.Nil(t, db.Delete(doc))
	doc.Free()

	doc = db.Document()
	require.NotNil(t, doc)
	require.Nil(t, env.Error())
	defer doc.Free()

	require.True(t, doc.Set(keyPath, expectedKey))

	d, err = db.Get(doc)
	require.NotNil(t, err)
	require.Equal(t, ErrNotFound, err)
	require.Nil(t, d)
}

func TestDatabaseWithMultipleKeys(t *testing.T) {
	const (
		key1Path  = "key1"
		key2Path  = "key2"
		key3Path  = "key3"
		valuePath = "value"
	)
	tmpDir, err := ioutil.TempDir("", "sophia_test")
	require.Nil(t, err)
	defer os.RemoveAll(tmpDir)

	env, err := NewEnvironment()
	require.Nil(t, err)
	require.NotNil(t, env)

	require.True(t, env.SetString(EnvironmentPath, tmpDir))

	schema := &Schema{}
	require.Nil(t, schema.AddKey(key1Path, FieldTypeUInt32))
	require.Nil(t, schema.AddKey(key2Path, FieldTypeUInt32))
	require.Nil(t, schema.AddKey(key3Path, FieldTypeUInt32))
	require.Nil(t, schema.AddValue(valuePath, FieldTypeUInt64))

	db, err := env.NewDatabase(DatabaseConfig{
		Name:   "test_database",
		Schema: schema,
	})
	require.Nil(t, err)
	require.NotNil(t, db)

	require.Nil(t, env.Open())
	defer env.Close()

	const (
		expectedKey1  int64 = 4
		expectedKey2  int64 = 8
		expectedKey3  int64 = 15
		expectedValue int64 = 16
	)

	doc := db.Document()
	require.NotNil(t, doc)
	require.True(t, doc.Set(key1Path, expectedKey1))
	require.True(t, doc.Set(key2Path, expectedKey2))
	require.True(t, doc.Set(key3Path, expectedKey3))
	require.True(t, doc.Set(valuePath, expectedValue))

	err = db.Set(doc)
	doc.Free()
	require.Nil(t, err)

	doc = db.Document()
	require.NotNil(t, doc)
	require.True(t, doc.Set(key1Path, expectedKey1))
	require.True(t, doc.Set(key2Path, expectedKey2))
	require.True(t, doc.Set(key3Path, expectedKey3))

	d, err := db.Get(doc)
	doc.Free()
	require.Nil(t, err)
	require.NotNil(t, d)

	defer d.Destroy()

	require.Equal(t, expectedKey1, d.GetInt(key1Path))
	require.Equal(t, expectedKey2, d.GetInt(key2Path))
	require.Equal(t, expectedKey3, d.GetInt(key3Path))
	require.Equal(t, expectedValue, d.GetInt(valuePath))

	doc = db.Document()
	require.NotNil(t, doc)
	require.Nil(t, env.Error())

	require.True(t, doc.Set(key1Path, expectedKey1))
	require.True(t, doc.Set(key2Path, expectedKey2))
	require.True(t, doc.Set(key3Path, expectedKey3))

	require.Nil(t, db.Delete(doc))
	doc.Free()

	doc = db.Document()
	require.NotNil(t, doc)
	require.Nil(t, env.Error())
	defer doc.Free()

	require.True(t, doc.Set(key1Path, expectedKey1))
	require.True(t, doc.Set(key2Path, expectedKey2))
	require.True(t, doc.Set(key3Path, expectedKey3))

	d, err = db.Get(doc)
	require.NotNil(t, err)
	require.Equal(t, ErrNotFound, err)
	require.Nil(t, d)
}

func TestDatabaseUseSomeDocumentsAtTheSameTime(t *testing.T) {
	const (
		keyPath        = "key"
		valuePath      = "value"
		expectedKey1   = "key1"
		expectedValue1 = "value1"
		expectedKey2   = "key2"
		expectedValue2 = "value2"
	)
	tmpDir, err := ioutil.TempDir("", "sophia_test")
	require.Nil(t, err)
	defer os.RemoveAll(tmpDir)

	env, err := NewEnvironment()
	require.Nil(t, err)
	require.NotNil(t, env)

	require.True(t, env.SetString(EnvironmentPath, tmpDir))

	db, err := env.NewDatabase(DatabaseConfig{
		Name: "test_database",
	})
	require.Nil(t, err)
	require.NotNil(t, db)

	require.Nil(t, env.Open())
	defer env.Close()

	doc1 := db.Document()
	doc2 := db.Document()

	require.NotNil(t, doc1)
	require.NotNil(t, doc2)

	require.True(t, doc1.Set(keyPath, expectedKey1))
	require.True(t, doc1.Set(valuePath, expectedValue1))

	require.True(t, doc2.Set(keyPath, expectedKey2))
	require.True(t, doc2.Set(valuePath, expectedValue2))

	require.Nil(t, db.Set(doc1))
	doc1.Free()

	require.Nil(t, db.Set(doc2))
	doc2.Free()

	doc := db.Document()
	require.NotNil(t, doc)

	doc.Set(keyPath, expectedKey1)
	d, err := db.Get(doc)
	doc.Free()
	require.NotNil(t, d)
	require.Nil(t, err)
	size := 0
	require.Equal(t, expectedValue1, d.GetString(valuePath, &size))
	require.Equal(t, len(expectedValue1), size)
	d.Destroy()

	doc = db.Document()
	require.NotNil(t, doc)

	doc.Set(keyPath, expectedKey2)
	d, err = db.Get(doc)
	doc.Free()
	require.NotNil(t, d)
	require.Nil(t, err)
	size = 0
	require.Equal(t, expectedValue2, d.GetString(valuePath, &size))
	require.Equal(t, len(expectedValue1), size)
	d.Destroy()
}

func TestDatabaseDeleteNotExistingKey(t *testing.T) {
	const keyPath = "key"
	const expectedKey = "key1"
	tmpDir, err := ioutil.TempDir("", "sophia_test")
	require.Nil(t, err)
	defer os.RemoveAll(tmpDir)

	env, err := NewEnvironment()
	require.Nil(t, err)
	require.NotNil(t, env)

	require.True(t, env.SetString(EnvironmentPath, tmpDir))

	db, err := env.NewDatabase(DatabaseConfig{
		Name: "test_database",
	})
	require.Nil(t, err)
	require.NotNil(t, db)

	require.Nil(t, env.Open())
	defer env.Close()

	doc := db.Document()
	defer doc.Free()
	doc.Set(keyPath, expectedKey)
	require.Nil(t, db.Delete(doc))
}

func BenchmarkDatabaseSet(b *testing.B) {
	const (
		keyPath           = "key"
		valuePath         = "value"
		KeyTemplate       = "key%013v"
		ValueTemplate     = "value%011v"
		RecordsCountBench = 500000
	)
	tmpDir, err := ioutil.TempDir("", "sophia_test")
	require.Nil(b, err)
	defer os.RemoveAll(tmpDir)

	env, err := NewEnvironment()
	require.Nil(b, err)
	require.NotNil(b, env)

	require.True(b, env.SetString(EnvironmentPath, tmpDir))

	db, err := env.NewDatabase(DatabaseConfig{
		Name: "test_database",
	})
	require.Nil(b, err)
	require.NotNil(b, db)

	require.Nil(b, env.Open())
	defer env.Close()

	indices := rand.Perm(RecordsCountBench)

	type pair struct {
		key   string
		value string
	}

	pairs := make([]pair, 0, RecordsCountBench)
	for _, i := range indices {
		pairs = append(pairs, pair{
			key:   fmt.Sprintf(KeyTemplate, i),
			value: fmt.Sprintf(ValueTemplate, i),
		})
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		index := i % RecordsCountBench
		doc := db.Document()
		doc.Set(keyPath, pairs[index].key)
		doc.Set(valuePath, pairs[index].value)
		db.Set(doc)
		doc.Free()
	}
}

func BenchmarkDatabaseGet(b *testing.B) {
	const (
		keyPath           = "key"
		valuePath         = "value"
		KeyTemplate       = "key%013v"
		ValueTemplate     = "value%011v"
		RecordsCountBench = 500000
	)
	tmpDir, err := ioutil.TempDir("", "sophia_test")
	require.Nil(b, err)
	defer os.RemoveAll(tmpDir)

	env, err := NewEnvironment()
	require.Nil(b, err)
	require.NotNil(b, env)

	require.True(b, env.SetString(EnvironmentPath, tmpDir))

	db, err := env.NewDatabase(DatabaseConfig{
		Name: "test_database",
	})
	require.Nil(b, err)
	require.NotNil(b, db)

	require.Nil(b, env.Open())
	defer env.Close()

	indices := rand.Perm(RecordsCountBench)

	type pair struct {
		key   string
		value string
	}

	pairs := make([]pair, 0, RecordsCountBench)
	for _, i := range indices {
		pairs = append(pairs, pair{
			key:   fmt.Sprintf(KeyTemplate, i),
			value: fmt.Sprintf(ValueTemplate, i),
		})
	}

	for _, pair := range pairs {
		doc := db.Document()
		require.True(b, doc.Set(keyPath, pair.key))
		require.True(b, doc.Set(valuePath, pair.value))
		require.Nil(b, db.Set(doc))
		doc.Free()
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		index := i % RecordsCountBench
		doc := db.Document()
		require.True(b, doc.Set(keyPath, pairs[index].key))
		d, _ := db.Get(doc)
		doc.Free()
		d.Destroy()
	}
}
