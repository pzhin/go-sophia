package main

import (
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"unsafe"

	"github.com/pzhin/go-sophia"
)

func upsertCallback(count int, src []unsafe.Pointer, srcSize uint32,
	upsert []unsafe.Pointer, upsertSize uint32, result []unsafe.Pointer,
	resultSize uint32, arg unsafe.Pointer) int {
	ca := *(*uint32)(src[1])
	cb := *(*uint32)(upsert[1])
	cret := ca + cb
	cresPtr := (*uint32)(result[1])
	*cresPtr = cret
	tb := *(*uint32)(upsert[2])
	tresPtr := (*uint32)(result[2])
	*tresPtr = tb
	return 0
}

func main() {
	env, err := sophia.NewEnvironment()
	if err != nil {
		log.Fatal(err)
	}

	tmpDir, err := ioutil.TempDir("", "sophia_test")
	if err != nil {
		log.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	env.Set(sophia.EnvironmentPath, tmpDir)

	schema := &sophia.Schema{}
	schema.AddKey("key", sophia.FieldTypeString)
	schema.AddValue("value", sophia.FieldTypeUInt32)
	schema.AddValue("value2", sophia.FieldTypeUInt32)

	odb, err := env.NewDatabase(sophia.DatabaseConfig{
		Name:                "test",
		Compression:         sophia.CompressionTypeLZ4,
		Schema:              schema,
		Upsert:              upsertCallback,
		CompactionCacheSize: 1 * 1024 * 1024 * 1024,
	})
	if err != nil {
		log.Fatal(err)
	}

	err = env.Open()
	if err != nil {
		log.Fatal(err)
	}

	var value int64
	for i := 0; i < 1e6; i++ {
		doc := odb.Document()
		doc.SetString("key", fmt.Sprintf("val%d", value))
		doc.SetInt("value", value)
		doc.SetInt("value2", value)
		err := odb.Upsert(doc)
		if err != nil {
			log.Fatal(err)
		}
		doc.Free()
		value++
	}

	dc := odb.Document()
	cursor, err := odb.Cursor(dc)
	if err != nil {
		log.Fatal(err)
	}

	for d := cursor.Next(); !d.IsEmpty(); d = cursor.Next() {
		var size int
		fmt.Println(d.GetString("key", &size), ":", d.GetInt("value"), ":", d.GetInt("value2"), ":", size)
	}
}
