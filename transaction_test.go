package sophia

import (
	"io/ioutil"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestTxSet(t *testing.T) {
	const (
		keyPath       = "key"
		valuePath     = "value"
		expectedKey   = "key1"
		expectedValue = "value1"
	)
	tmpDir, err := ioutil.TempDir("", "sophia_test")
	require.Nil(t, err)
	defer os.RemoveAll(tmpDir)

	env, err := NewEnvironment()
	require.Nil(t, err)
	require.NotNil(t, env)

	require.True(t, env.SetString(EnvironmentPath, tmpDir))

	db, err := env.NewDatabase(DatabaseConfig{
		Name: "test_database",
	})
	require.Nil(t, err)
	require.NotNil(t, db)

	require.Nil(t, env.Open())
	defer env.Close()

	tx, err := env.BeginTx()
	require.Nil(t, err)

	doc := db.Document()
	require.True(t, doc.Set(keyPath, expectedKey))
	require.True(t, doc.Set(valuePath, expectedValue))

	require.Nil(t, tx.Set(doc))
	doc.Free()

	require.Equal(t, TxOk, tx.Commit())
}

func TestTxGet(t *testing.T) {
	const (
		keyPath       = "key"
		valuePath     = "value"
		expectedKey   = "key1"
		expectedValue = "value1"
	)
	tmpDir, err := ioutil.TempDir("", "sophia_test")
	require.Nil(t, err)
	defer os.RemoveAll(tmpDir)

	env, err := NewEnvironment()
	require.Nil(t, err)
	require.NotNil(t, env)

	require.True(t, env.SetString(EnvironmentPath, tmpDir))

	db, err := env.NewDatabase(DatabaseConfig{
		Name: "test_database",
	})
	require.Nil(t, err)
	require.NotNil(t, db)

	require.Nil(t, env.Open())
	defer env.Close()

	doc := db.Document()
	require.True(t, doc.Set(keyPath, expectedKey))
	require.True(t, doc.Set(valuePath, expectedValue))

	require.Nil(t, db.Set(doc))
	doc.Free()

	tx, err := env.BeginTx()
	require.Nil(t, err)

	doc = db.Document()
	require.NotNil(t, doc)
	require.True(t, doc.Set(keyPath, expectedKey))

	d, err := tx.Get(doc)
	doc.Free()

	require.NotNil(t, d)
	require.Nil(t, err)

	var size int
	require.Equal(t, expectedKey, d.GetString(keyPath, &size))
	require.Equal(t, expectedValue, d.GetString(valuePath, &size))
	d.Destroy()

	require.Equal(t, TxOk, tx.Commit())
}

func TestTxDelete(t *testing.T) {
	const (
		keyPath       = "key"
		valuePath     = "value"
		expectedKey   = "key1"
		expectedValue = "value1"
	)
	tmpDir, err := ioutil.TempDir("", "sophia_test")
	require.Nil(t, err)
	defer os.RemoveAll(tmpDir)

	env, err := NewEnvironment()
	require.Nil(t, err)
	require.NotNil(t, env)

	require.True(t, env.SetString(EnvironmentPath, tmpDir))

	db, err := env.NewDatabase(DatabaseConfig{
		Name: "test_database",
	})
	require.Nil(t, err)
	require.NotNil(t, db)

	require.Nil(t, env.Open())
	defer env.Close()

	doc := db.Document()
	require.True(t, doc.Set(keyPath, expectedKey))
	require.True(t, doc.Set(valuePath, expectedValue))

	require.Nil(t, db.Set(doc))
	doc.Free()

	doc = db.Document()
	require.NotNil(t, doc)
	require.True(t, doc.Set(keyPath, expectedKey))

	d, err := db.Get(doc)
	doc.Free()
	require.NotNil(t, d)
	require.Nil(t, err)

	var size int
	require.Equal(t, expectedKey, d.GetString(keyPath, &size))
	require.Equal(t, expectedValue, d.GetString(valuePath, &size))
	d.Destroy()

	tx, err := env.BeginTx()
	require.Nil(t, err)

	doc = db.Document()
	require.NotNil(t, doc)
	require.True(t, doc.Set(keyPath, expectedKey))
	require.Nil(t, tx.Delete(doc))
	doc.Free()

	doc = db.Document()
	require.NotNil(t, doc)
	require.True(t, doc.Set(keyPath, expectedKey))
	d, err = tx.Get(doc)
	doc.Free()
	require.Nil(t, d)
	require.NotNil(t, err)

	require.Equal(t, TxOk, tx.Commit())
}

func TestTxRollback(t *testing.T) {
	const (
		keyPath       = "key"
		valuePath     = "value"
		expectedKey   = "key1"
		expectedValue = "value1"
	)
	tmpDir, err := ioutil.TempDir("", "sophia_test")
	require.Nil(t, err)
	defer os.RemoveAll(tmpDir)

	env, err := NewEnvironment()
	require.Nil(t, err)
	require.NotNil(t, env)

	require.True(t, env.SetString(EnvironmentPath, tmpDir))

	db, err := env.NewDatabase(DatabaseConfig{
		Name: "test_database",
	})
	require.Nil(t, err)
	require.NotNil(t, db)

	require.Nil(t, env.Open())
	defer env.Close()

	tx, err := env.BeginTx()
	require.Nil(t, err)

	doc := db.Document()
	require.True(t, doc.Set(keyPath, expectedKey))
	require.True(t, doc.Set(valuePath, expectedValue))

	require.Nil(t, tx.Set(doc))
	doc.Free()
	require.Nil(t, tx.Rollback())

	doc = db.Document()
	require.True(t, doc.Set(keyPath, expectedKey))

	d, err := db.Get(doc)
	require.Nil(t, d)
	require.Equal(t, ErrNotFound, err)
	doc.Free()
}

func TestConcurrentTx(t *testing.T) {
	const (
		keyPath        = "key"
		valuePath      = "value"
		expectedKey    = "key1"
		initialValue   = "value1"
		expectedValue1 = "value2"
		expectedValue2 = "value3"
	)
	tmpDir, err := ioutil.TempDir("", "sophia_test")
	require.Nil(t, err)
	defer os.RemoveAll(tmpDir)

	env, err := NewEnvironment()
	require.Nil(t, err)
	require.NotNil(t, env)

	require.True(t, env.SetString(EnvironmentPath, tmpDir))

	db, err := env.NewDatabase(DatabaseConfig{
		Name: "test_database",
	})
	require.Nil(t, err)
	require.NotNil(t, db)

	require.Nil(t, env.Open())
	defer env.Close()

	doc := db.Document()
	require.True(t, doc.Set(keyPath, expectedKey))
	require.True(t, doc.Set(valuePath, initialValue))

	require.Nil(t, db.Set(doc))
	doc.Free()

	tx1, err := env.BeginTx()
	require.Nil(t, err)
	tx2, err := env.BeginTx()
	require.Nil(t, err)

	doc = db.Document()
	require.True(t, doc.Set(keyPath, expectedKey))
	require.True(t, doc.Set(valuePath, expectedValue1))

	require.Nil(t, tx1.Set(doc))
	doc.Free()

	doc = db.Document()
	require.True(t, doc.Set(keyPath, expectedKey))
	require.True(t, doc.Set(valuePath, expectedValue2))

	require.Nil(t, tx2.Set(doc))
	doc.Free()

	require.Equal(t, TxOk, tx1.Commit())
	require.Equal(t, TxRollback, tx2.Commit())

	var size int
	doc = db.Document()
	require.True(t, doc.Set(keyPath, expectedKey))

	d, err := db.Get(doc)
	doc.Free()
	require.Nil(t, err)
	require.NotNil(t, d)
	value := d.GetString(valuePath, &size)
	require.Equal(t, expectedValue1, value)
	d.Destroy()
}
