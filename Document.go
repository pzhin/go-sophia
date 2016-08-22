package sophia

import "unsafe"

type Document struct {
	ptr *unsafe.Pointer
}

func (d *Document) SetString(key, value string, size int) {
	sp_setstring(d.ptr, key, value, size)
}

func (d *Document) GetString(key string, size int) string {
	return sp_getstring(d.ptr, key, size)
}

func (d *Document) SetInt(key string, value int64) {
	sp_setint(d.ptr, key, value)
}

func (d *Document) GetInt(key string) int64 {
	return sp_getint(d.ptr, key)
}
