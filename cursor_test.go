package sophia

import (
	"fmt"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	"math"
)

// TODO write tests:
//  - using int key/values
//    - match criteria
//    - range criteria
//  - using uint key/values
//    - match criteria
//    - range criteria
//  - using order
//    - using ASC order
//    - using DESC order
//  - using prefix
//  - using different types for range criteria (catch panic)
//  - using one bound for range criteria
//    - upper bound
//    - lower bound
func initDB() {
	env, _ := NewEnvironment()
	defer env.Close()

	env.SetString("sophia.path", DBPath)

	schema := &Schema{}
	schema.AddKey("key", FieldType_String)
	schema.AddValue("value", FieldType_String)

	db, _ := env.NewDatabase(DBName, schema)
	env.Open()

	for i := 0; i < RecordsCount; i++ {
		doc := db.Document()
		doc.SetString("key", fmt.Sprintf(KeyTemplate, i))
		doc.SetString("value", fmt.Sprintf(ValueTemplate, i))

		db.Set(doc)
		doc.Free()
	}
}
func TestCursor(t *testing.T) {
	initDB()
	defer os.RemoveAll(DBPath)
	t.Run("Cursor match", testCursorMatch)
	t.Run("Cursor range", testCursorRange)
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

	id := RecordsCount / 2

	cr := NewCursorCriteria()
	cr.Match("key", fmt.Sprintf(KeyTemplate, id))
	cursor, err := db.Cursor(cr)
	require.Nil(t, err)
	require.NotNil(t, cursor)
	defer cursor.Close()

	var size, counter int
	for d := cursor.Next(); d != nil; d = cursor.Next() {
		require.Equal(t, fmt.Sprintf(KeyTemplate, id), d.GetString("key", &size))
		require.Equal(t, fmt.Sprintf(ValueTemplate, id), d.GetString("value", &size))
		counter++
	}
	require.Equal(t, 1, counter)
}

func testCursorRange(t *testing.T) {
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

	startId := RecordsCount / 4
	expectedCount := int(math.Floor(RecordsCount / 1.8))

	cr := NewCursorCriteria()
	cr.Range("key", fmt.Sprintf(KeyTemplate, startId),
		fmt.Sprintf(KeyTemplate, startId*3))
	cursor, err := db.Cursor(cr)
	require.Nil(t, err)
	require.NotNil(t, cursor)
	defer cursor.Close()

	var counter int
	for d := cursor.Next(); d != nil; d = cursor.Next() {
		counter++
	}
	require.Equal(t, expectedCount, counter)
}
