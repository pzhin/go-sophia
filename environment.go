package sophia

import (
	"C"
	"errors"
	"fmt"
	"unsafe"
	"reflect"
)

// Environment is used to configure the database before opening.
type Environment struct {
	ptr unsafe.Pointer
}

// NewEnvironment creates a new environment for opening a database.
// Receivers must call Close() on the returned Environment.
func NewEnvironment() (*Environment, error) {
	ptr := sp_env()
	if ptr == nil {
		return nil, errors.New("sp_env failed")
	}
	return &Environment{ptr: ptr}, nil
}

func (env *Environment) GetObject(path string) unsafe.Pointer {
	return sp_getobject(env.ptr, path)
}

func (env *Environment) SetString(path, val string) bool {
	cPath := C.CString(path)
	cVal := C.CString(val)
	return sp_setstring_s(env.ptr, cPath, cVal, len(val))
}

func (env *Environment) SetInt(path string, val int64) bool {
	return sp_setint(env.ptr, path, val)
}

func (env *Environment) Get(path string, size *int) interface{} {
	return sp_getstring(env.ptr, path, size)
}

func (env *Environment) GetString(path string, size *int) string {
	ptr := sp_getstring(env.ptr, path, size)
	sh := &reflect.StringHeader{
		Len:  *size,
		Data: uintptr(ptr),
	}
	return *(*string)(unsafe.Pointer(sh))
}

func (env *Environment) NewDatabase(name string, schema *Schema) (*Database, error) {
	if !env.SetString("db", name) {
		return nil, errors.New("failed create database")
	}
	i := 0
	for n, typ := range schema.keys {
		env.SetString(fmt.Sprintf("db.%s.scheme", name), n)
		env.SetString(fmt.Sprintf("db.%s.scheme.%s", name, n), fmt.Sprintf("%s,key(%d)", typ.String(), i))
		i++
	}
	for n, typ := range schema.values {
		env.SetString(fmt.Sprintf("db.%s.scheme", name), n)
		env.SetString(fmt.Sprintf("db.%s.scheme.%s", name, n), typ.String())
	}
	db := env.GetObject(fmt.Sprintf("db.%s", name))
	if db == nil {
		return nil, errors.New("failed get database")
	}
	return &Database{
		env:    env,
		ptr:    db,
		name:   name,
		schema: schema,
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

func (env *Environment) Error() error {
	var error_size int
	err := sp_getstring(env.ptr, "sophia.error", &error_size)
	if err != nil {
		str := GoString(err)
		free(err)
		return errors.New(str)
	}
	return nil
}
