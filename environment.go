package sophia

import (
	"errors"
	"fmt"
)

const errorPath = "sophia.error"

var ErrEnvironmentClosed = errors.New("usage of closed environment")

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
	if env.ptr == nil {
		return nil, ErrEnvironmentClosed
	}
	if config == nil {
		return nil, errors.New("illegal configuration: nil configuration")
	}

	if config.DirectIO && !config.DisableMmapMode {
		return nil, errors.New("illegal configuration: both direct_io and mmap is enabled")
	}

	if !env.SetString("db", config.Name) {
		return nil, fmt.Errorf("failed to create database: %v", env.Error())
	}

	if config.Schema == nil {
		config.Schema = defaultSchema()
	}
	fieldsCount := env.initializeSchema(config.Name, config.Schema)

	if config.Upsert != nil {
		ptr, index := registerUpsert(config.Upsert)
		ok := env.Set(fmt.Sprintf(keyUpsertTemplate, config.Name), ptr)
		if !ok {
			unregisterUpsert(index)
			return nil, env.Error()
		}
		registerUpsertArg(index, config.UpsertArg)
		ok = env.Set(fmt.Sprintf(keyUpsertArgTemplate, config.Name), &index)
		if !ok {
			unregisterUpsert(index)
			return nil, env.Error()
		}
	}

	env.configureCompaction(config)

	env.SetInt(fmt.Sprintf(keyMmap, config.Name), boolToInt(!config.DisableMmapMode))
	env.SetInt(fmt.Sprintf(keyDirectIO, config.Name), boolToInt(config.DirectIO))
	env.SetInt(fmt.Sprintf(keySync, config.Name), boolToInt(!config.DisableSync))

	env.SetString(fmt.Sprintf(keyCompression, config.Name), config.Compression.String())

	db := env.GetObject(fmt.Sprintf("db.%s", config.Name))
	if db == nil {
		return nil, fmt.Errorf("failed to get database object: %v", env.Error())
	}
	return &Database{
		dataStore:   newDataStore(db, env),
		name:        config.Name,
		schema:      config.Schema,
		fieldsCount: fieldsCount,
	}, nil
}

func (env *Environment) initializeSchema(name string, schema *Schema) int {
	i := 0
	var schemaPath = fmt.Sprintf("db.%s.scheme", name)
	for n, typ := range schema.keys {
		env.SetString(schemaPath, n)
		keyPath := fmt.Sprintf("db.%s.scheme.%s", name, n)
		key := fmt.Sprintf("%s,key(%d)", typ.String(), i)
		env.SetString(keyPath, key)
		i++
	}
	for n, typ := range schema.values {
		env.SetString(schemaPath, n)
		value := fmt.Sprintf("db.%s.scheme.%s", name, n)
		env.SetString(value, typ.String())
		i++
	}
	return i
}

func (env *Environment) configureCompaction(config *DatabaseConfig) {
	if config.CompactionCacheSize != 0 {
		env.SetInt(fmt.Sprintf(keyCompactionCache, config.Name), config.CompactionCacheSize)
	}
	if config.CompactionExpirePeriod != 0 {
		env.SetInt(fmt.Sprintf(keyCompactionExpirePeriod, config.Name), config.CompactionExpirePeriod)
	}
	if config.CompactionGCPeriod != 0 {
		env.SetInt(fmt.Sprintf(keyCompactionGCPeriod, config.Name), config.CompactionGCPeriod)
	}
	if config.CompactionGCWatermark != 0 {
		env.SetInt(fmt.Sprintf(keyCompactionGCWatermark, config.Name), config.CompactionGCWatermark)
	}
	if config.CompactionNodeSize != 0 {
		env.SetInt(fmt.Sprintf(keyCompactionNodeSize, config.Name), config.CompactionNodeSize)
	}
	if config.CompactionPageSize != 0 {
		env.SetInt(fmt.Sprintf(keyCompactionPageSize, config.Name), config.CompactionPageSize)
	}
	env.SetInt(fmt.Sprintf(keyCompactionPageChecksum, config.Name), boolToInt(!config.DisableCompactionPageChecksum))
}

// Close closes the environment and frees its associated memory.
// You must call Close on any Environment created with NewEnvironment.
func (env *Environment) Close() error {
	if env.ptr == nil {
		return ErrEnvironmentClosed
	}
	env.Free()
	if !spDestroy(env.ptr) {
		return fmt.Errorf("failed to close: %v", env.Error())
	}
	env.ptr = nil
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
	if env.ptr == nil {
		return ErrEnvironmentClosed
	}
	var size int
	err := spGetString(env.ptr, getCStringFromCache(errorPath), &size)
	if err != nil {
		str := goString(err)
		free(err)
		return errors.New(str)
	}
	return nil
}

func (env *Environment) BeginTx() (*Transaction, error) {
	if env.ptr == nil {
		return nil, ErrEnvironmentClosed
	}
	ptr := spBegin(env.ptr)
	if ptr == nil {
		return nil, fmt.Errorf("failed to begin transaction: %v", env.Error())
	}
	return &Transaction{
		dataStore: newDataStore(ptr, env),
	}, nil
}

func boolToInt(val bool) int64 {
	if val {
		return 1
	}
	return 0
}
