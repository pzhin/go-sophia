package sophia

/*
#include <stdlib.h>
*/
import "C"

// CStringCache controls caching of C strings used in Sophia bindings.
type CStringCache interface {
	Acquire(string) *C.char
	Release(string)
	Clear()
}
