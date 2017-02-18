package sophia

import (
	"errors"
	"fmt"
)

const errorPath = "sophia.error"

// Environment is used to configure the database before opening.
// Take it's name from sophia
// Usually object with same features are called 'database'
type Environment struct {
	*varStore
}

// NewEnvironment creates a new environment for opening a database.
// Receivers must call Close() on the returned Environment.
func NewEnvironment() (*Environment, error) {
	ptr := spEnv()
	if ptr == nil {
		return nil, errors.New("sp_env failed")
	}
	return &Environment{varStore: newVarStore(ptr, 4)}, nil
}

func (env *Environment) NewDatabase(config *DatabaseConfig) (*Database, error) {
	if !env.SetString("db", config.Name) {
		return nil, fmt.Errorf("failed create database: %v", env.Error())
	}
	i := 0
	for n, typ := range config.Schema.keys {
		env.SetString(fmt.Sprintf("db.%s.scheme", config.Name), n)
		env.SetString(fmt.Sprintf("db.%s.scheme.%s", config.Name, n), fmt.Sprintf("%s,key(%d)", typ.String(), i))
		i++
	}
	for n, typ := range config.Schema.values {
		env.SetString(fmt.Sprintf("db.%s.scheme", config.Name), n)
		env.SetString(fmt.Sprintf("db.%s.scheme.%s", config.Name, n), typ.String())
		i++
	}
	db := env.GetObject(fmt.Sprintf("db.%s", config.Name))
	if db == nil {
		return nil, fmt.Errorf("failed get database: %v", env.Error())
	}
	return &Database{
		dataStore:   newDataStore(db, env),
		name:        config.Name,
		schema:      config.Schema,
		fieldsCount: i,
	}, nil
}

// Close closes the environment and frees its associated memory. You must call
// Close on any Environment created with NewEnvironment.
func (env *Environment) Close() error {
	env.Free()
	if !spDestroy(env.ptr) {
		return errors.New("env: failed to close")
	}
	return nil
}

// Open opens environment
// At a minimum path must be specified and one db declared
func (env *Environment) Open() error {
	if !spOpen(env.ptr) {
		return env.Error()
	}
	return nil
}

func (env *Environment) Error() error {
	var size int
	err := spGetString(env.ptr, getCStringFromCache(errorPath), &size)
	if err != nil {
		str := goString(err)
		free(err)
		return errors.New(str)
	}
	return nil
}

func (env *Environment) BeginTx() *Transaction {
	return &Transaction{
		dataStore: newDataStore(spBegin(env.ptr), env),
	}
}
