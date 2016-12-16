package sophia

import (
	"fmt"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSophiaDatabaseTxCRUD(t *testing.T) {
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
		t.Fatal("Failed to create database object")
	}

	if !t.Run("Set", func(t *testing.T) { testSetTx(t, env.BeginTx(), db) }) {
		t.Fatal("Set operations are failed")
	}
	if !t.Run("Get", func(t *testing.T) { testGetTx(t, env.BeginTx(), db) }) {
		t.Fatal("Get operations are failed")
	}
	if !t.Run("Detele", func(t *testing.T) { testDeleteTx(t, env.BeginTx(), db) }) {
		t.Fatal("Delete operations are failed")
	}
}

func testSetTx(t *testing.T, tx Transaction, db Database) {
	for i := 0; i < RecordsCount; i++ {
		doc := db.Document()
		require.True(t, doc.Set("key", fmt.Sprintf(KeyTemplate, i)))
		require.True(t, doc.Set("value", fmt.Sprintf(ValueTemplate, i)))

		require.Nil(t, tx.Set(doc))
		doc.Free()
	}
	require.Equal(t, TxOk, tx.Commit())
}

func testGetTx(t *testing.T, tx Transaction, db Database) {
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

func testDeleteTx(t *testing.T, tx Transaction, db Database) {
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
