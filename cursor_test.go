package sophia

import (
	"fmt"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

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
	if !t.Run("Cursor", testCursorMatch) {
		t.Fatal("Cursor operations are failed")
	}
	if !t.Run("Cursor", testCursorRange) {
		t.Fatal("Cursor operations are failed")
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
	id := RecordsCount / 2
	cr.Add(CriteriaMatch, "key", fmt.Sprintf(KeyTemplate, id))
	cursor, err := db.Cursor(cr)
	require.Nil(t, err)
	require.NotNil(t, env)
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

	cr := NewCursorCriteria()
	id := RecordsCount / 4
	cr.Add(CriteriaRange, "key", []string{
		fmt.Sprintf(KeyTemplate, id),
		fmt.Sprintf(KeyTemplate, id*3),
	})
	cursor, err := db.Cursor(cr)
	require.Nil(t, err)
	require.NotNil(t, env)
	defer cursor.Close()
	var size, counter int
	for d := cursor.Next(); d != nil; d = cursor.Next() {
		require.Equal(t, fmt.Sprintf(KeyTemplate, id), d.GetString("key", &size))
		require.Equal(t, fmt.Sprintf(ValueTemplate, id), d.GetString("value", &size))
		id++
		counter++
	}
	require.Equal(t, RecordsCount/2, counter)
}
