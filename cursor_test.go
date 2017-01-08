package sophia

import (
	"fmt"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	"io/ioutil"
)

func TestCursor(t *testing.T) {
	dbDir, err := ioutil.TempDir("", "sophia")
	require.Nil(t, err, "failed to create temp dir for database")
	defer os.RemoveAll(dbDir)
	env, err := NewEnvironment()
	require.Nil(t, err, "failed to create new environment")
	defer env.Close()

	require.True(t, env.SetString("sophia.path", dbDir))

	schema := &Schema{}
	schema.AddKey("key", FieldTypeUInt64)
	schema.AddValue("value", FieldTypeString)

	db, err := env.NewDatabase(DBName, schema)
	require.Nil(t, err, "failed to create database")
	require.Nil(t, env.Open(), "failed to open environment")

	for i := 0; i < RecordsCount; i++ {
		doc := db.Document()
		doc.SetInt("key", int64(i))
		doc.SetString("value", fmt.Sprintf(ValueTemplate, i))

		db.Set(doc)
		doc.Free()
	}
	t.Run("All records", func(t *testing.T) { testCursor(t, db, 0) })
	t.Run("Half records", func(t *testing.T) { testCursor(t, db, RecordsCount/2) })
	t.Run("Quater records", func(t *testing.T) { testCursor(t, db, RecordsCount/4) })
	t.Run("Use closed cursor error", func(t *testing.T){ testCursorError(t, db)})
}

func testCursorError(t *testing.T, db Database) {
	doc := db.Document()
	require.NotNil(t, doc)

	cursor, err := db.Cursor(doc)
	require.Nil(t, err)
	require.NotNil(t, cursor)

	err = cursor.Close()
	require.Nil(t, err)

	require.Error(t, cursor.Close())
	require.Nil(t, cursor.Next())
}

func testCursor(t *testing.T, db Database, start int64) {
	id := start
	doc := db.Document()
	require.NotNil(t, doc)
	if start != 0 {
		doc.SetInt("key", start)
	}

	cursor, err := db.Cursor(doc)
	require.Nil(t, err)
	require.NotNil(t, cursor)
	defer cursor.Close()

	var (
		size    int
		counter int64
	)
	for d := cursor.Next(); d != nil; d = cursor.Next() {
		require.Equal(t, id, d.GetInt("key"))
		require.Equal(t, fmt.Sprintf(ValueTemplate, id), d.GetString("value", &size))
		counter++
		id++
	}
	require.Equal(t, RecordsCount-start, counter)
}
