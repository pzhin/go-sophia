package sophia

import (
	"fmt"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"math"
)

// TODO write tests:
//   - using upsert
//     - success
//     - error
//   - using delete
//     - error
//   - creating schema
//     - duplicate key
//     - duplicate value
//   - test more settings for environment
//   - test more settings for database

const (
	KeyTemplate   = "key%v"
	ValueTemplate = "value%v"

	DBPath       = "sophia"
	DBName       = "test"
	RecordsCount = 500000
)

func TestSophiaDatabaseCRUD(t *testing.T) {
	defer os.RemoveAll(DBPath)
	var (
		env *Environment
		db  Database
	)

	if !t.Run("New Environment", func(t *testing.T) { env = testNewEnvironment(t) }) {
		t.Fatal("Failed to create environment object")
	}
	defer func() { require.Nil(t, env.Close()) }()

	if !t.Run("New Database", func(t *testing.T) { db = testNewDatabase(t, env) }) {
		t.Fatalf("Failed to create database object: %v", env.Error())
	}

	if !t.Run("Set", func(t *testing.T) { testSet(t, db) }) {
		t.Fatalf("Set operations are failed: %v", env.Error())
	}
	if !t.Run("Get", func(t *testing.T) { testGet(t, db) }) {
		t.Fatalf("Get operations are failed: %v", env.Error())
	}
	if !t.Run("Detele", func(t *testing.T) { testDelete(t, db) }) {
		t.Fatalf("FDelete operations are failed: %v", env.Error())
	}
}

func testNewEnvironment(t *testing.T) *Environment {
	env, err := NewEnvironment()
	require.Nil(t, err)
	require.NotNil(t, env)
	return env
}

func testNewDatabase(t *testing.T, env *Environment) Database {
	require.True(t, env.Set("sophia.path", DBPath))

	schema := &Schema{}
	require.Nil(t, schema.AddKey("key", FieldTypeString))
	require.Nil(t, schema.AddValue("value", FieldTypeString))

	db, err := env.NewDatabase(DBName, schema)
	require.Nil(t, err)
	require.NotNil(t, db)
	require.True(t, env.Open())
	return db
}

func testSet(t *testing.T, db Database) {
	for i := 0; i < RecordsCount; i++ {
		doc := db.Document()
		require.True(t, doc.Set("key", fmt.Sprintf(KeyTemplate, i)))
		require.True(t, doc.Set("value", fmt.Sprintf(ValueTemplate, i)))

		require.Nil(t, db.Set(doc))
		doc.Free()
	}
}

func testGet(t *testing.T, db Database) {
	for i := 0; i < RecordsCount; i++ {
		doc := db.Document()
		require.NotNil(t, doc)
		require.True(t, doc.Set("key", fmt.Sprintf(KeyTemplate, i)))
		d, err := db.Get(doc)
		doc.Free()
		require.NotNil(t, d)
		require.Nil(t, err)
		var size int
		require.Equal(t, fmt.Sprintf(KeyTemplate, i), d.GetString("key", &size))
		require.Equal(t, fmt.Sprintf(ValueTemplate, i), d.GetString("value", &size))
		d.Destroy()
		d.Free()
	}
}

func testDelete(t *testing.T, db Database) {
	for i := 0; i < RecordsCount; i++ {
		doc := db.Document()
		require.NotNil(t, doc)
		require.True(t, doc.Set("key", fmt.Sprintf(KeyTemplate, i)))
		require.Nil(t, db.Delete(doc))
		doc.Free()
	}

	for i := 0; i < RecordsCount; i++ {
		doc := db.Document()
		require.NotNil(t, doc)
		require.True(t, doc.Set("key", fmt.Sprintf(KeyTemplate, i)))
		d, err := db.Get(doc)
		doc.Free()
		require.Nil(t, d)
		require.NotNil(t, err)
	}
}

func TestSetIntKV(t *testing.T) {
	defer os.RemoveAll(DBPath)
	env, err := NewEnvironment()
	require.Nil(t, err)
	require.NotNil(t, env)
	defer func() { require.Nil(t, env.Close()) }()

	require.True(t, env.Set("sophia.path", DBPath))

	schema := &Schema{}
	require.Nil(t, schema.AddKey("key", FieldTypeUInt32))
	require.Nil(t, schema.AddValue("value", FieldTypeUInt32))

	db, err := env.NewDatabase(DBName, schema)
	require.Nil(t, err)
	require.NotNil(t, db)
	require.True(t, env.Open())

	for i := 0; i < RecordsCount; i++ {
		doc := db.Document()
		require.NotNil(t, doc)
		require.True(t, doc.Set("key", int64(i)))
		require.True(t, doc.Set("value", int64(i)))

		require.Nil(t, db.Set(doc))
		doc.Free()
	}
	for i := 0; i < RecordsCount; i++ {
		doc := db.Document()
		require.NotNil(t, doc)
		require.True(t, doc.Set("key", int64(i)))
		d, err := db.Get(doc)
		doc.Free()
		require.Nil(t, err)
		require.NotNil(t, d)
		require.Equal(t, int64(i), d.GetInt("key"))
		require.Equal(t, int64(i), d.GetInt("value"))
		d.Destroy()
		d.Free()
	}
}

func TestSetMultiKey(t *testing.T) {
	defer os.RemoveAll(DBPath)
	env, err := NewEnvironment()
	require.Nil(t, err)
	require.NotNil(t, env)
	defer func() { require.Nil(t, env.Close()) }()

	require.True(t, env.Set("sophia.path", DBPath))

	schema := &Schema{}
	require.Nil(t, schema.AddKey("key", FieldTypeUInt32))
	require.Nil(t, schema.AddKey("key_j", FieldTypeUInt32))
	require.Nil(t, schema.AddKey("key_k", FieldTypeUInt32))
	require.Nil(t, schema.AddValue("value", FieldTypeUInt64))

	db, err := env.NewDatabase(DBName, schema)
	require.Nil(t, err)
	require.NotNil(t, db)
	require.True(t, env.Open())

	count := int(math.Pow(RecordsCount, 1/3))

	for i := 0; i < count; i++ {
		for j := 0; j < count; j++ {
			for k := 0; k < count; k++ {
				doc := db.Document()
				require.True(t, doc.Set("key", i))
				require.True(t, doc.Set("key_j", uint64(j)))
				require.True(t, doc.Set("key_k", uint32(k)))
				require.True(t, doc.Set("value", i))

				require.Nil(t, db.Set(doc))
				doc.Free()
			}
		}
	}
	for i := 0; i < count; i++ {
		for j := 0; j < count; j++ {
			for k := 0; k < count; k++ {
				doc := db.Document()
				require.NotNil(t, doc)
				require.True(t, doc.Set("key", int64(i)))
				require.True(t, doc.Set("key_j", int64(j)))
				require.True(t, doc.Set("key_k", int64(k)))
				d, err := db.Get(doc)
				doc.Free()
				require.Nil(t, err)
				require.NotNil(t, d)
				require.Equal(t, int64(i), d.GetInt("key"))
				require.Equal(t, int64(j), d.GetInt("key_j"))
				require.Equal(t, int64(k), d.GetInt("key_k"))
				require.Equal(t, int64(i), d.GetInt("value"))
				d.Destroy()
				d.Free()
			}
		}
	}
}

func BenchmarkDatabase_Set(b *testing.B) {
	defer os.RemoveAll(DBPath)
	env, err := NewEnvironment()
	if !assert.Nil(b, err) {
		b.Fatalf("failed create environment: err=%v", err)
	}
	if !assert.NotNil(b, env) {
		b.Fatal("failed create environment")
	}

	env.Set("sophia.path", DBPath)

	schema := &Schema{}
	schema.AddKey("key", FieldTypeString)
	schema.AddValue("value", FieldTypeString)

	db, err := env.NewDatabase(DBName, schema)
	if !assert.Nil(b, err) {
		b.Fatalf("failed create Database: err=%v", err)
	}
	if !assert.NotNil(b, db) {
		b.Fatal("failed create Database")
	}

	if !env.Open() {
		b.Fatal("failed open environment")
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		doc := db.Document()
		doc.Set("key", fmt.Sprintf(KeyTemplate, i))
		doc.Set("value", fmt.Sprintf(ValueTemplate, i))
		err = db.Set(doc)
		if !assert.Nil(b, err) {
			b.Fatalf("failed set: err=%v", err)
		}
		doc.Free()
	}
}
