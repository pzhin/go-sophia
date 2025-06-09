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
	const (
		keyPath       = "key"
		valuePath     = "value"
		recordsCount  = 100
		valueTemplate = "value%011v"
	)

	tmpDir, err := ioutil.TempDir("", "sophia_test")
	require.Nil(t, err)
	defer os.RemoveAll(tmpDir)

	env, err := NewEnvironment()
	require.Nil(t, err)
	require.NotNil(t, env)

	require.True(t, env.SetString(EnvironmentPath, tmpDir))

	schema := &Schema{}
	require.Nil(t, schema.AddKey("key", FieldTypeUInt64))
	require.Nil(t, schema.AddValue("value", FieldTypeString))

	db, err := env.NewDatabase(DatabaseConfig{
		Name:   "test_database",
		Schema: schema,
	})
	require.Nil(t, err)
	require.NotNil(t, db)

	require.Nil(t, env.Open())
	defer env.Close()

	for i := 0; i < recordsCount; i++ {
		doc := db.Document()
		require.True(t, doc.SetInt(keyPath, int64(i)))
		require.True(t, doc.SetString(valuePath, fmt.Sprintf(valueTemplate, i)))

		require.Nil(t, db.Set(doc))
		doc.Free()
	}
	t.Run("All records", func(t *testing.T) { testCursor(t, db, 0, recordsCount, valueTemplate) })
	t.Run("Half records", func(t *testing.T) { testCursor(t, db, recordsCount/2, recordsCount, valueTemplate) })
	t.Run("Quarter records", func(t *testing.T) { testCursor(t, db, recordsCount/4, recordsCount, valueTemplate) })
	t.Run("Use closed cursor error", func(t *testing.T) { testCursorError(t, db) })
	t.Run("Reverse iterator", func(t *testing.T) { testReverseCursor(t, db, recordsCount, valueTemplate) })
}

func testCursorError(t *testing.T, db *Database) {
	doc := db.Document()
	require.False(t, doc.IsEmpty())

	cursor, err := db.Cursor(doc)
	require.Nil(t, err)
	require.NotNil(t, cursor)

	err = cursor.Close()
	require.Nil(t, err)

	require.Error(t, cursor.Close())
	d := cursor.Next()
	require.True(t, d.IsEmpty())
}

func testCursor(t *testing.T, db *Database, start, count int64, valueTemplate string) {
	id := start
	doc := db.Document()
	require.False(t, doc.IsEmpty())
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
	for d := cursor.Next(); !d.IsEmpty(); d = cursor.Next() {
		require.Equal(t, id, d.GetInt("key"))
		require.Equal(t, fmt.Sprintf(valueTemplate, id), d.GetString("value", &size))
		counter++
		id++
	}
	require.Equal(t, count-start, counter)
}

func testReverseCursor(t *testing.T, db *Database, count int64, valueTemplate string) {
	doc := db.Document()
	require.False(t, doc.IsEmpty())

	doc.Set(CursorOrder, LTE)

	cursor, err := db.Cursor(doc)
	require.Nil(t, err)
	require.NotNil(t, cursor)
	defer func() {
		require.Nil(t, cursor.Close())
	}()

	var (
		size int
		id   int64 = count - 1
	)
	for d := cursor.Next(); !d.IsEmpty(); d = cursor.Next() {
		require.Equal(t, id, d.GetInt("key"))
		require.Equal(t, fmt.Sprintf(valueTemplate, id), d.GetString("value", &size))
		id--
	}
}

func TestCursorPrefix(t *testing.T) {
	const (
		keyPath       = "key"
		valuePath     = "value"
		recordsCount  = 100
		valueTemplate = "value%011v"
	)
	tmpDir, err := ioutil.TempDir("", "sophia_test")
	require.Nil(t, err)
	defer os.RemoveAll(tmpDir)

	env, err := NewEnvironment()
	require.Nil(t, err)
	require.NotNil(t, env)

	require.True(t, env.SetString(EnvironmentPath, tmpDir))

	schema := &Schema{}
	require.Nil(t, schema.AddKey(keyPath, FieldTypeString))
	require.Nil(t, schema.AddValue(valuePath, FieldTypeString))

	db, err := env.NewDatabase(DatabaseConfig{
		Name:   "test_database",
		Schema: schema,
	})
	require.Nil(t, err)
	require.NotNil(t, db)

	require.Nil(t, env.Open())
	defer env.Close()

	const base = 36
	for i := int64(0); i < recordsCount; i++ {
		doc := db.Document()
		require.True(t, doc.SetString(keyPath, strconv.FormatInt(i, base)))
		require.True(t, doc.SetString(valuePath, fmt.Sprintf(valueTemplate, i)))

		require.Nil(t, db.Set(doc))
		doc.Free()
	}

	// Calculate prefix for inserted rows
	c := recordsCount
	maxDigit := 1
	for c > base {
		c /= base
		maxDigit *= base
	}
	expectedCount := recordsCount
	for maxDigit != 1 {
		c := expectedCount / maxDigit
		expectedCount -= c * maxDigit
		maxDigit /= base
	}

	prefix := recordsCount - expectedCount

	prefixStr := strconv.FormatInt(int64(prefix), base)
	prefixStr = prefixStr[:len(prefixStr)-1]

	doc := db.Document()
	require.False(t, doc.IsEmpty())

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
	require.False(t, d.IsEmpty())
	require.Equal(t, prefixStr, d.GetString(keyPath, &size))
	require.Equal(t, fmt.Sprintf(valueTemplate, prefix/base), d.GetString(valuePath, &size))

	// get rows that have additional symbol at the end
	for d := cursor.Next(); !d.IsEmpty(); d = cursor.Next() {
		id := prefix + count
		require.Equal(t, strconv.FormatInt(int64(id), base), d.GetString(keyPath, &size))
		require.Equal(t, fmt.Sprintf(valueTemplate, id), d.GetString(valuePath, &size))
		count++
	}

	require.Equal(t, expectedCount, count)
}
