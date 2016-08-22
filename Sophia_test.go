package sophia

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"testing"
)

const (
	KeyTemplate   = "ключ%v"
	ValueTemplate = "значение%v"

	RecordsCount = 10
)

// TODO :: close db
func TestSophia(t *testing.T) {
	if !t.Run("Set", testDatabase_Set) {
		t.Fatal("Set operations are failed")
	}
	if !t.Run("Get", testDatabase_Get) {
		t.Fatal("Set operations are failed")
	}
	if !t.Run("Cursor", testCursor_Next) {
		t.Fatal("Cursor operations are failed")
	}
}

func testDatabase_Set(t *testing.T) {
	dbPath := "test"
	dbName := "sophia"
	env, err := NewEnvironment()
	if !assert.Nil(t, err) {
		t.Fatalf("failed create environment: err=%v", err)
	}
	if !assert.NotNil(t, env) {
		t.Fatal("failed create environment")
	}

	env.SetString("sophia.path", dbPath)

	schema := &Schema{}
	schema.AddKey("key", Key_String)
	schema.AddValue("value", Key_String)

	db, err := env.NewDatabase(dbName, schema)
	if !assert.Nil(t, err) {
		t.Fatalf("failed create Database: err=%v", err)
	}
	if !assert.NotNil(t, db) {
		t.Fatal("failed create Database")
	}

	if !env.Open() {
		t.Fatal("failed open environment")
	}

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

func testDatabase_Get(t *testing.T) {
	dbPath := "test"
	dbName := "sophia"
	env, err := NewEnvironment()
	if !assert.Nil(t, err) {
		t.Fatalf("failed create environment: err=%v", err)
	}
	if !assert.NotNil(t, env) {
		t.Fatal("failed create environment")
	}

	env.SetString("sophia.path", dbPath)

	schema := &Schema{}
	schema.AddKey("key", Key_String)
	schema.AddValue("value", Key_String)

	db, err := env.NewDatabase(dbName, schema)
	if !assert.Nil(t, err) {
		t.Fatalf("failed create Database: err=%v", err)
	}
	if !assert.NotNil(t, db) {
		t.Fatal("failed create Database")
	}

	if !env.Open() {
		t.Fatal("failed open environment")
	}

	for i := 0; i < RecordsCount; i++ {
		doc := db.Document()
		doc.SetString("key", fmt.Sprintf(KeyTemplate, i))
		d, err := db.Get(doc)
		if !assert.Nil(t, err) {
			t.Fatalf("failed get: err=%v", err)
		}
		var size int
		if !assert.Equal(t, fmt.Sprintf(KeyTemplate, i), d.GetString("key", &size)) {
			t.Fatalf("incorrect key: size=%v", size)
		}
		if !assert.Equal(t, fmt.Sprintf(ValueTemplate, i), d.GetString("value", &size)) {
			t.Fatalf("incorrect value: size=%v", size)
		}
		doc.Free()
		d.Destroy()
		d.Free()
	}
}

func testCursor_Next(t *testing.T) {
	dbPath := "test"
	dbName := "sophia"
	env, err := NewEnvironment()
	if !assert.Nil(t, err) {
		t.Fatalf("failed create environment: err=%v", err)
	}
	if !assert.NotNil(t, env) {
		t.Fatal("failed create environment")
	}

	env.SetString("sophia.path", dbPath)

	schema := &Schema{}
	schema.AddKey("key", Key_String)
	schema.AddValue("value", Key_String)

	db, err := env.NewDatabase(dbName, schema)
	if !assert.Nil(t, err) {
		t.Fatalf("failed create Database: err=%v", err)
	}
	if !assert.NotNil(t, db) {
		t.Fatal("failed create Database")
	}

	if !env.Open() {
		t.Fatal("failed open environment")
	}

	cr := NewCursorCriteria()
	count := 4
	cr.Order(GTE)
	cr.Add(CriteriaRange, "key", []string{fmt.Sprintf(KeyTemplate, 3), fmt.Sprintf(KeyTemplate, 8)})
	cursor, err := db.Cursor(cr)
	if !assert.Nil(t, err) {
		t.Fatalf("failed create cursor: err=%v", err)
	}
	if !assert.NotNil(t, env) {
		t.Fatal("failed create cursor")
	}
	defer cursor.Close()
	var size int
	for d := cursor.Next(); d != nil; d = cursor.Next() {
		if !assert.Equal(t, fmt.Sprintf(KeyTemplate, count), d.GetString("key", &size)) {
			t.Fatal("incorrect key")
		}
		if !assert.Equal(t, fmt.Sprintf(ValueTemplate, count), d.GetString("value", &size)) {
			t.Fatal("incorrect value")
		}
		count++
	}
	if !assert.Equal(t, 8, count) {
		t.Fatal("incorect records count")
	}
}

func BenchmarkDatabase_Set(b *testing.B) {
	dbPath := "test"
	dbName := "sophia"
	env, err := NewEnvironment()
	if !assert.Nil(b, err) {
		b.Fatalf("failed create environment: err=%v", err)
	}
	if !assert.NotNil(b, env) {
		b.Fatal("failed create environment")
	}

	env.SetString("sophia.path", dbPath)

	schema := &Schema{}
	schema.AddKey("key", Key_String)
	schema.AddValue("value", Key_String)

	db, err := env.NewDatabase(dbName, schema)
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
