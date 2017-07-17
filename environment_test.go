package sophia

import (
	"testing"

	"io/ioutil"
	"os"

	"github.com/stretchr/testify/require"
)

func TestNewEnvironment(t *testing.T) {
	env, err := NewEnvironment()
	require.Nil(t, err)
	require.NotNil(t, env)

	require.Nil(t, env.Error())
}

func TestEnvironmentNewDatabaseEmptyConfig(t *testing.T) {
	env, err := NewEnvironment()
	require.Nil(t, err)
	require.NotNil(t, env)

	db, err := env.NewDatabase(DatabaseConfig{})
	require.NotNil(t, err)
	require.Nil(t, db)
}

func TestEnvironmentNewDatabaseIllegalConfig(t *testing.T) {
	env, err := NewEnvironment()
	require.Nil(t, err)
	require.NotNil(t, env)

	db, err := env.NewDatabase(DatabaseConfig{
		Name:     "test",
		DirectIO: true,
	})
	require.NotNil(t, err)
	require.Nil(t, db)
}

func TestEnvironmentNewDatabaseIllegalName(t *testing.T) {
	env, err := NewEnvironment()
	require.Nil(t, err)
	require.NotNil(t, env)

	db, err := env.NewDatabase(DatabaseConfig{
		Name: "test.test",
	})

	require.NotNil(t, err)
	require.Nil(t, db)
}

func TestEnvironmentNewDatabaseDefaultSchema(t *testing.T) {
	env, err := NewEnvironment()
	require.Nil(t, err)
	require.NotNil(t, env)

	db, err := env.NewDatabase(DatabaseConfig{
		Name: "test",
	})
	require.Nil(t, err)
	require.NotNil(t, db)

	require.Equal(t, defaultSchema(), db.schema)
}

func TestEnvironmentEmptyPath(t *testing.T) {
	env, err := NewEnvironment()
	require.Nil(t, err)
	require.NotNil(t, env)

	require.NotNil(t, env.Open())
}

func TestEnvironmentOpenWithoutDatabase(t *testing.T) {
	env, err := NewEnvironment()
	require.Nil(t, err)
	require.NotNil(t, env)

	require.True(t, env.Set(EnvironmentPath, "test"))

	require.NotNil(t, env.Open())
}

func TestEnvironmentCloseTwice(t *testing.T) {
	dbPath, err := ioutil.TempDir("", "sophia")
	require.Nil(t, err)

	defer func() {
		require.Nil(t, os.RemoveAll(dbPath))
	}()

	env, err := NewEnvironment()
	require.Nil(t, err)
	require.NotNil(t, env)

	require.True(t, env.Set(EnvironmentPath, dbPath))

	db, err := env.NewDatabase(DatabaseConfig{
		Name: "test",
	})
	require.Nil(t, err)
	require.NotNil(t, db)

	require.Nil(t, env.Open())
	require.Nil(t, env.Close())
	require.NotNil(t, env.Close())
}
