package sophia

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"os"
	"testing"
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
	if !t.Run("Get", testGet) {
		t.Fatal("Get operations are failed")
	}
	if !t.Run("Get", testDelete) {
		t.Fatal("Delete operations are failed")
	}
}

func TestCursor(t *testing.T) {
	if !t.Run("Cursor", testCursorMatch) {
		t.Fatal("Cursor operations are failed")
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
		if !assert.Nil(t, err) {
			t.Fatalf("failed set: err=%v", err)
		}
		doc.Free()
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
		require.Nil(t, err)
		var size int
		require.Equal(t, fmt.Sprintf(KeyTemplate, i), d.GetString("key", &size))
		require.Equal(t, fmt.Sprintf(ValueTemplate, i), d.GetString("value", &size))
		doc.Free()
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
		require.Nil(t, err)
		doc.Free()
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

func testCursorMatch(t *testing.T) {
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

	cr := NewCursorCriteria()
	count := RecordsCount / 2
	cr.Add(CriteriaMatch, "key", count)
	cursor, err := db.Cursor(cr)
	require.Nil(t, err)
	require.NotNil(t, env)
	defer cursor.Close()
	var size int
	for d := cursor.Next(); d != nil; d = cursor.Next() {
		require.Equal(t, fmt.Sprintf(KeyTemplate, count), d.GetString("key", &size))
		require.Equal(t, fmt.Sprintf(ValueTemplate, count), d.GetString("value", &size))
		count++
	}
	require.Equal(t, RecordsCount, count)
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
