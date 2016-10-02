package sophia

import (
	"fmt"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	KeyTemplate   = "key%v"
	ValueTemplate = "value%v"

	DBPath       = "sophia"
	DBName       = "test"
	RecordsCount = 10
)

// TODO :: close db
func TestSophiaDatabaseCRUD(t *testing.T) {
	defer os.RemoveAll(DBPath)
	if !t.Run("Set", testSet) {
		t.Fatal("Set operations are failed")
	}
	if !t.Run("Set int key value", testSetIntKV) {
		t.Fatal("Set int key/values operations are failed")
	}
	if !t.Run("Get", testGet) {
		t.Fatal("Get operations are failed")
	}
	if !t.Run("Get", testDelete) {
		t.Fatal("Delete operations are failed")
	}
	if !t.Run("Set", testSetMultiKey) {
		t.Fatal("Set muoperations are failed")
	}
}

func testSet(t *testing.T) {
	env, err := NewEnvironment()
	require.Nil(t, err)
	require.NotNil(t, env)
	defer env.Close()

	env.SetString("sophia.path", DBPath)

	schema := &Schema{}
	schema.AddKey("key", FieldType_String)
	schema.AddValue("value", FieldType_String)

	db, err := env.NewDatabase(DBName, schema)
	require.Nil(t, err)
	require.NotNil(t, db)
	require.True(t, env.Open())

	for i := 0; i < RecordsCount; i++ {
		doc := db.Document()
		doc.SetString("key", fmt.Sprintf(KeyTemplate, i))
		doc.SetString("value", fmt.Sprintf(ValueTemplate, i))

		err = db.Set(doc)
		doc.Free()
		require.Nil(t, err)
	}
}

func testSetIntKV(t *testing.T) {
	env, err := NewEnvironment()
	require.Nil(t, err)
	require.NotNil(t, env)
	defer env.Close()

	env.SetString("sophia.path", DBPath)

	schema := &Schema{}
	schema.AddKey("key", FieldType_UInt32)
	schema.AddValue("value", FieldType_UInt32)

	db, err := env.NewDatabase(DBName, schema)
	require.Nil(t, err)
	require.NotNil(t, db)
	require.True(t, env.Open())

	for i := 0; i < RecordsCount; i++ {
		doc := db.Document()
		doc.SetInt("key", int64(i))
		doc.SetInt("value", int64(i))

		err = db.Set(doc)
		doc.Free()
		require.Nil(t, err)
	}
}

func testGet(t *testing.T) {
	env, err := NewEnvironment()
	require.Nil(t, err)
	require.NotNil(t, env)
	defer env.Close()

	env.SetString("sophia.path", DBPath)

	schema := &Schema{}
	schema.AddKey("key", FieldType_String)
	schema.AddValue("value", FieldType_String)

	db, err := env.NewDatabase(DBName, schema)
	require.Nil(t, err)
	require.NotNil(t, db)
	require.True(t, env.Open())

	for i := 0; i < RecordsCount; i++ {
		doc := db.Document()
		doc.SetString("key", fmt.Sprintf(KeyTemplate, i))
		d, err := db.Get(doc)
		doc.Free()
		require.Nil(t, err)
		var size int
		require.Equal(t, fmt.Sprintf(KeyTemplate, i), d.GetString("key", &size))
		require.Equal(t, fmt.Sprintf(ValueTemplate, i), d.GetString("value", &size))
		d.Destroy()
		d.Free()
	}
}

func testDelete(t *testing.T) {
	env, err := NewEnvironment()
	require.Nil(t, err)
	require.NotNil(t, env)
	defer env.Close()

	env.SetString("sophia.path", DBPath)

	schema := &Schema{}
	schema.AddKey("key", FieldType_String)
	schema.AddValue("value", FieldType_String)

	db, err := env.NewDatabase(DBName, schema)
	require.Nil(t, err)
	require.NotNil(t, db)
	require.True(t, env.Open())

	for i := 0; i < RecordsCount; i++ {
		doc := db.Document()
		doc.SetString("key", fmt.Sprintf(KeyTemplate, i))
		err := db.Delete(doc)
		doc.Free()
		require.Nil(t, err)
	}

	for i := 0; i < RecordsCount; i++ {
		doc := db.Document()
		doc.SetString("key", fmt.Sprintf(KeyTemplate, i))
		d, err := db.Get(doc)
		require.Nil(t, d)
		require.NotNil(t, err)
		doc.Free()
	}
}

func testSetMultiKey(t *testing.T) {
	env, err := NewEnvironment()
	require.Nil(t, err)
	require.NotNil(t, env)
	defer env.Close()

	env.SetString("sophia.path", DBPath)

	schema := &Schema{}
	schema.AddKey("key", FieldType_UInt32)
	schema.AddKey("key_j", FieldType_UInt32)
	schema.AddKey("key_k", FieldType_UInt32)

	db, err := env.NewDatabase(DBName+"_multi_key", schema)
	require.Nil(t, err)
	require.NotNil(t, db)
	require.True(t, env.Open())

	for i := 0; i < RecordsCount; i++ {
		for j := 0; j < RecordsCount; j++ {
			for k := 0; k < RecordsCount; k++ {
				doc := db.Document()
				doc.SetInt("key", int64(i))
				doc.SetInt("value", int64(i))
				doc.SetInt("key_j", int64(j))
				doc.SetInt("key_k", int64(k))

				err = db.Set(doc)
				doc.Free()
				require.Nil(t, err)
			}
		}
	}
}

func BenchmarkDatabase_Set(b *testing.B) {
	env, err := NewEnvironment()
	if !assert.Nil(b, err) {
		b.Fatalf("failed create environment: err=%v", err)
	}
	if !assert.NotNil(b, env) {
		b.Fatal("failed create environment")
	}

	env.SetString("sophia.path", DBPath)

	schema := &Schema{}
	schema.AddKey("key", FieldType_String)
	schema.AddValue("value", FieldType_String)

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
		doc.SetString("key", fmt.Sprintf(KeyTemplate, i))
		doc.SetString("value", fmt.Sprintf(ValueTemplate, i))
		err = db.Set(doc)
		if !assert.Nil(b, err) {
			b.Fatalf("failed set: err=%v", err)
		}
		doc.Free()
	}
}
