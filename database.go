package sophia

import (
	"errors"
	"fmt"
)

const (
	keyCompactionCache = "db.%v.compaction.cache"
	keyCompactionNodeSize = "db.%v.compaction.node_size"
	keyCompactionPageSize = "db.%v.compaction.page_size"
	keyCompactionPageChecksum = "db.%v.compaction.page_checksum"
	keyCompactionExpirePeriod = "db.%v.compaction.expire_period"
	keyCompactionGCWatermark = "db.%v.compaction.gc_wm"
	keyCompactionGCPeriod = "db.%v.compaction.gc_period"
	keyMmap = "db.%v.mmap"
	keyCompression = "db.%v.compression"
	keyDirectIO = "db.%v.direct_io"
	keySync = "db.%v.sync"
)

// DatabaseConfig a structure for the description of the database to be created.
type DatabaseConfig struct {
	// Name of database.
	// It will be used to set and get values specific to this base.
	Name string
	// Schema of database.
	// It is used to describe the keys and values that will be stored in the database.
	Schema *Schema
	// CacheSize precalculated memory usage (cache size) for expected storage capacity and write rates.
	// See more http://sophia.systems/v2.2/admin/memory_requirements.html
	CompactionCacheSize int64
	// CompactionNodeSize set a node file size in bytes.
	// Node file can grow up to two times the size before the old node file is being split.
	CompactionNodeSize int64
	// CompactionNodeSize set size of a page to use.
	CompactionPageSize int64
	// CompactionPageChecksum check checksum during compaction.
	DisableCompactionPageChecksum bool
	// CompactionExpirePeriod set expire check process period in seconds.
	CompactionExpirePeriod int64
	// CompactionGcWm when this value reaches a compaction, operation is scheduled.
	// Garbage collection starts when watermark value reaches a certain percent of duplicates.
	CompactionGCWatermark int64
	// CompactionGCPeriod run check for a gc every CompactionGCPeriod seconds.
	CompactionGCPeriod int64
	// DisableMmapMode can be set to disable mmap mode.
	// By default Sophia uses pread(2) to read data from disk.
	// Using mmap mode, Sophia handles all requests by directly accessing memory-mapped node files memory.
	//
	// It is a good idea to try this mode, even if your dataset is rather small
	// or you need to handle a large ratio of read request with a predictable pattern.
	//
	// Disadvantage of mmap mode, in comparison to RAM Storage,
	// is a possible unpredictable latency behaviour and a OS cache warmup period after recovery.
	DisableMmapMode bool
	// DirectIO can be set to enable O_DIRECT to see what actual read
	// performance might be, if we avoid using file system cache.
	//
	// When a database size is lower then RAM, it probably sits in file system cache and all operations do very little actual IO.
	// In some sense, when a database grows in size load scenario might change from from CPU bound to IO bound.
	// It is possible to set DirectIO=true and MmapMode=false to get it.
	// DirectIO=true and MmapMode=true will cause panic.
	DirectIO bool
	// DisableSync can be set to disable sync node file on compaction completion.
	DisableSync bool
	// Expire can be set to enable or disable key expire.
	Expire bool
	// Compression specify compression driver. Supported: lz4, zstd, none (default).
	Compression CompressionType
	// Upsert is a function that will be called on every upsert operation.
	// If it was not set during the configuration database, upsert operation will not be available
	Upsert UpsertFunc
	// UpsertArg an argument which is additionally passed every call
	UpsertArg interface{}
}

// Database is used for accessing a database.
// Take it's name from sophia.
// Usually object with same features is called 'table'.
type Database struct {
	*dataStore
	name        string
	schema      *Schema
	fieldsCount int
}

// Document creates a Document for a single or multi-statement transactions
func (db *Database) Document() *Document {
	ptr := spDocument(db.ptr)
	if ptr == nil {
		return nil
	}
	return newDocument(ptr, db.fieldsCount)
}

// Cursor returns a Cursor for iterating over rows in the database
func (db *Database) Cursor(doc *Document) (Cursor, error) {
	if nil == doc {
		return nil, errors.New("failed to create cursor: nil Document")
	}
	cPtr := spCursor(db.env.ptr)
	if nil == cPtr {
		return nil, fmt.Errorf("failed to create cursor: err=%v", db.env.Error())
	}
	cur := &cursor{
		ptr: cPtr,
		doc: doc,
	}
	return cur, nil
}
