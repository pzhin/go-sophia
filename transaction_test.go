package sophia

import (
	"fmt"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSophiaDatabaseTx(t *testing.T) {
	defer func() { require.Nil(t, os.RemoveAll(DBPath)) }()
	var (
		env *Environment
		db  *Database
	)

	if !t.Run("New Environment", func(t *testing.T) { env = testNewEnvironment(t) }) {
		t.Fatal("Failed to create environment object")
	}
	defer func() { require.Nil(t, env.Close()) }()

	if !t.Run("New Database", func(t *testing.T) { db = testNewDatabase(t, env) }) {
		t.Fatal("Failed to create database object")
	}

	if !t.Run("Set", func(t *testing.T) { testSetTx(t, env.BeginTx(), db) }) {
		t.Fatal("Set operations are failed")
	}
	t.Run("Get", func(t *testing.T) { testGetTx(t, env.BeginTx(), db) })
	t.Run("Detele", func(t *testing.T) { testDeleteTx(t, env.BeginTx(), db) })
	t.Run("Rollback", func(t *testing.T) { testTxRollback(t, env, db) })
	t.Run("Concurrent", func(t *testing.T) { testConcurrentTx(t, env, db) })
}

func testSetTx(t *testing.T, tx *Transaction, db *Database) {
	for i := 0; i < RecordsCount; i++ {
		doc := db.Document()
		require.True(t, doc.Set("key", fmt.Sprintf(KeyTemplate, i)))
		require.True(t, doc.Set("value", fmt.Sprintf(ValueTemplate, i)))

		require.Nil(t, tx.Set(doc))
		doc.Free()
	}
	require.Equal(t, TxOk, tx.Commit())
}

func testGetTx(t *testing.T, tx *Transaction, db *Database) {
	for i := 0; i < RecordsCount; i++ {
		doc := db.Document()
		require.NotNil(t, doc)
		require.True(t, doc.Set("key", fmt.Sprintf(KeyTemplate, i)))
		d, err := tx.Get(doc)
		doc.Free()
		require.NotNil(t, d)
		require.Nil(t, err)
		var size int
		require.Equal(t, fmt.Sprintf(KeyTemplate, i), d.GetString("key", &size))
		require.Equal(t, fmt.Sprintf(ValueTemplate, i), d.GetString("value", &size))
		d.Destroy()
		d.Free()
	}
	require.Equal(t, TxOk, tx.Commit())
}

func testDeleteTx(t *testing.T, tx *Transaction, db *Database) {
	for i := 0; i < RecordsCount; i++ {
		doc := db.Document()
		require.NotNil(t, doc)
		require.True(t, doc.Set("key", fmt.Sprintf(KeyTemplate, i)))
		require.Nil(t, tx.Delete(doc))
		doc.Free()
	}

	for i := 0; i < RecordsCount; i++ {
		doc := db.Document()
		require.NotNil(t, doc)
		require.True(t, doc.Set("key", fmt.Sprintf(KeyTemplate, i)))
		d, err := tx.Get(doc)
		doc.Free()
		require.Nil(t, d)
		require.NotNil(t, err)
	}
	require.Equal(t, TxOk, tx.Commit())
}

func testTxRollback(t *testing.T, env *Environment, db *Database) {
	tx := env.BeginTx()

	for i := 0; i < RecordsCount; i++ {
		doc := db.Document()
		require.True(t, doc.Set("key", fmt.Sprintf(KeyTemplate, i)))
		require.True(t, doc.Set("value", fmt.Sprintf(ValueTemplate, i)))

		require.Nil(t, tx.Set(doc))
		doc.Free()
	}
	require.Nil(t, tx.Rollback())

	for i := 0; i < RecordsCount; i++ {
		doc := db.Document()
		require.True(t, doc.Set("key", fmt.Sprintf(KeyTemplate, i)))

		d, err := db.Get(doc)
		require.Nil(t, d)
		require.Equal(t, ErrNotFound, err)
		doc.Free()
	}
}

func testConcurrentTx(t *testing.T, env *Environment, db *Database) {
	for i := 0; i < RecordsCount; i++ {
		doc := db.Document()
		require.True(t, doc.Set("key", fmt.Sprintf(KeyTemplate, i)))
		require.True(t, doc.Set("value", fmt.Sprintf(ValueTemplate, i)))

		require.Nil(t, db.Set(doc))
		doc.Free()
	}

	tx1 := env.BeginTx()
	tx2 := env.BeginTx()

	for i := 0; i < RecordsCount; i++ {
		doc := db.Document()
		require.True(t, doc.Set("key", fmt.Sprintf(KeyTemplate, i)))
		require.True(t, doc.Set("value", fmt.Sprintf(ValueTemplate, i+1)))

		require.Nil(t, tx1.Set(doc))
		doc.Free()
	}

	for i := 0; i < RecordsCount; i++ {
		doc := db.Document()
		require.True(t, doc.Set("key", fmt.Sprintf(KeyTemplate, i)))
		require.True(t, doc.Set("value", fmt.Sprintf(ValueTemplate, i+2)))

		require.Nil(t, tx2.Set(doc))
		doc.Free()
	}

	require.Equal(t, TxOk, tx1.Commit())
	require.Equal(t, TxRollback, tx2.Commit())

	var size int
	for i := 0; i < RecordsCount; i++ {
		doc := db.Document()
		require.True(t, doc.Set("key", fmt.Sprintf(KeyTemplate, i)))

		d, err := db.Get(doc)
		require.Nil(t, err)
		require.NotNil(t, d)
		value := d.GetString("value", &size)
		require.Equal(t, fmt.Sprintf(ValueTemplate, i+1), value)
		doc.Free()
		d.Free()
		d.Destroy()
	}
}
