package sophia

import (
	"errors"
	"fmt"
	"unsafe"
)

// Environment is used to configure the database before opening.
type Environment struct {
	ptr *unsafe.Pointer
}

// NewEnvironment creates a new environment for opening a database.
// Receivers must call Close() on the returned Environment.
func NewEnvironment() (*Environment, error) {
	env := &Environment{}
	env.ptr = sp_env()
	if nil == env {
		return nil, errors.New("sp_env failed")
	}
	return env, nil
}

func (env *Environment) GetObject(path string) *unsafe.Pointer {
	return sp_getobject(env.ptr, path)
}

func (env *Environment) SetString(path, val string, size int) bool {
	return sp_setstring(env.ptr, path, val, size)
}

func (env *Environment) GetString(path string, size int) string {
	return sp_getstring(env.ptr, path, size)
}

func (env *Environment) NewDatabase(name string) (*Database, error) {
	if !env.SetString("db", name, 0) {
		return nil, errors.New("failed create database")
	}
	db := env.GetObject(fmt.Sprintf("db.%v", name))
	if db == nil {
		return nil, errors.New("failed create database")
	}
	return &Database{
		env:  env,
		ptr:  db,
		name: name,
	}, nil
}

// Close closes the environment and frees its associated memory. You must call
// Close on any Environment created with NewEnvironment.
func (env *Environment) Close() error {
	return sp_close(env.ptr)
}

// Opens environment
// At a minimum path must be specified and one db declared
func (env *Environment) Open() bool {
	return sp_open(env.ptr)
}
