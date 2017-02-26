package sophia

import (
	"fmt"
	"io/ioutil"
	"os"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCursor(t *testing.T) {
	dbDir, err := ioutil.TempDir("", "sophia")
	require.Nil(t, err, "failed to create temp dir for database")
	defer func() {
		require.Nil(t, os.RemoveAll(dbDir))
	}()
	env, err := NewEnvironment()
	require.Nil(t, err, "failed to create new environment")
	defer func() {
		require.Nil(t, env.Close())
	}()

	require.True(t, env.SetString("sophia.path", dbDir))

	schema := &Schema{}
	require.Nil(t, schema.AddKey("key", FieldTypeUInt64))
	require.Nil(t, schema.AddValue("value", FieldTypeString))

	db, err := env.NewDatabase(&DatabaseConfig{
		Name:   DBName,
		Schema: schema,
	})
	require.Nil(t, err, "failed to create database")
	require.Nil(t, env.Open(), "failed to open environment")

	for i := 0; i < RecordsCount; i++ {
		doc := db.Document()
		require.True(t, doc.SetInt("key", int64(i)))
		require.True(t, doc.SetString("value", fmt.Sprintf(ValueTemplate, i)))

		require.Nil(t, db.Set(doc))
		doc.Free()
	}
	t.Run("All records", func(t *testing.T) { testCursor(t, db, 0) })
	t.Run("Half records", func(t *testing.T) { testCursor(t, db, RecordsCount/2) })
	t.Run("Quater records", func(t *testing.T) { testCursor(t, db, RecordsCount/4) })
	t.Run("Use closed cursor error", func(t *testing.T) { testCursorError(t, db) })
	t.Run("Reverse iterator", func(t *testing.T) { testReverseCursor(t, db) })
}

func testCursorError(t *testing.T, db *Database) {
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

func testCursor(t *testing.T, db *Database, start int64) {
	id := start
	doc := db.Document()
	require.NotNil(t, doc)
	if start != 0 {
		doc.SetInt("key", start)
	}

	cursor, err := db.Cursor(doc)
	require.Nil(t, err)
	require.NotNil(t, cursor)
	defer func() {
		require.Nil(t, cursor.Close())
	}()

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

func testReverseCursor(t *testing.T, db *Database) {
	doc := db.Document()
	require.NotNil(t, doc)

	doc.Set(CursorOrder, LTE)

	cursor, err := db.Cursor(doc)
	require.Nil(t, err)
	require.NotNil(t, cursor)
	defer func() {
		require.Nil(t, cursor.Close())
	}()

	var (
		size int
		id   int64 = RecordsCount - 1
	)
	for d := cursor.Next(); d != nil; d = cursor.Next() {
		require.Equal(t, id, d.GetInt("key"))
		require.Equal(t, fmt.Sprintf(ValueTemplate, id), d.GetString("value", &size))
		id--
	}
}

func TestCursorPrefix(t *testing.T) {
	dbDir, err := ioutil.TempDir("", "sophia")
	require.Nil(t, err, "failed to create temp dir for database")
	defer func() {
		require.Nil(t, os.RemoveAll(dbDir))
	}()
	env, err := NewEnvironment()
	require.Nil(t, err, "failed to create new environment")
	defer func() {
		require.Nil(t, env.Close())
	}()

	require.True(t, env.SetString("sophia.path", dbDir))

	schema := &Schema{}
	require.Nil(t, schema.AddKey("key", FieldTypeString))
	require.Nil(t, schema.AddValue("value", FieldTypeString))

	db, err := env.NewDatabase(&DatabaseConfig{
		Name:   DBName,
		Schema: schema,
	})
	require.Nil(t, err, "failed to create database")
	require.Nil(t, env.Open(), "failed to open environment")

	const base = 36
	for i := int64(0); i < RecordsCount; i++ {
		doc := db.Document()
		require.True(t, doc.SetString("key", strconv.FormatInt(i, base)))
		require.True(t, doc.SetString("value", fmt.Sprintf(ValueTemplate, i)))

		require.Nil(t, db.Set(doc))
		doc.Free()
	}

	// Calculate prefix for inserted rows
	c := RecordsCount
	maxDigit := 1
	for c > base {
		c /= base
		maxDigit *= base
	}
	expectedCount := RecordsCount
	for maxDigit != 1 {
		c := expectedCount / maxDigit
		expectedCount -= c * maxDigit
		maxDigit /= base
	}

	prefix := RecordsCount - expectedCount

	prefixStr := strconv.FormatInt(int64(prefix), base)
	prefixStr = prefixStr[:len(prefixStr)-1]

	doc := db.Document()
	require.NotNil(t, doc)

	doc.Set(CursorPrefix, prefixStr)

	cursor, err := db.Cursor(doc)
	require.Nil(t, err)
	require.NotNil(t, cursor)
	defer func() {
		require.Nil(t, cursor.Close())
	}()

	var (
		size  int
		count int
	)

	// get row that match prefix
	d := cursor.Next()
	require.Equal(t, prefixStr, d.GetString("key", &size))
	require.Equal(t, fmt.Sprintf(ValueTemplate, prefix/base), d.GetString("value", &size))

	// get rows that have additional symbol at the end
	for d := cursor.Next(); d != nil; d = cursor.Next() {
		id := prefix + count
		require.Equal(t, strconv.FormatInt(int64(id), base), d.GetString("key", &size))
		require.Equal(t, fmt.Sprintf(ValueTemplate, id), d.GetString("value", &size))
		count++
	}

	require.Equal(t, expectedCount, count)
}
