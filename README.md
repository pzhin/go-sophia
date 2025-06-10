[![Build Status](https://travis-ci.org/pzhin/go-sophia.svg?branch=master)](https://travis-ci.org/pzhin/go-sophia) [![Coverage Status](https://coveralls.io/repos/github/pzhin/go-sophia/badge.svg?branch=master)](https://coveralls.io/github/pzhin/go-sophia?branch=master) [![Go Report Card](https://goreportcard.com/badge/github.com/pzhin/go-sophia)](https://goreportcard.com/report/github.com/pzhin/go-sophia) [![codebeat badge](https://codebeat.co/badges/dd136517-c8e3-4ab2-8ab4-ae34645cc826)](https://codebeat.co/projects/github-com-pzhin-go-sophia) [![GoDoc](https://godoc.org/github.com/pzhin/go-sophia?status.svg)](https://godoc.org/github.com/pzhin/go-sophia)

<a href="http://sphia.org"><img src="http://media.charlesleifer.com/blog/photos/sophia-logo.png" width="215px" height="95px" /></a>

# go-sophia 
go-sophia is a Go (golang) binding to the Sophia key-value database (http://sophia.systems/)

#Installation
The [sophia](http://sophia.systems/) sources are bundled with the go-sophia, so you should make only `go get github.com/pzhin/go-sophia` to install it.

#Library information
Used Sophia v2.2 (commit 1419633)

#Memory management
go-sophia caches C strings for internal configuration paths. Each Environment,
Database and Transaction holds a `CStringCache` instance used to manage these
strings. A reference-counted implementation is used by default, but users may
provide their own by implementing:

```
type CStringCache interface {
    Acquire(string) *C.char
    Release(string)
    Clear()
}
```

Custom caches can be passed with `NewEnvironmentWithCache` and via
`DatabaseConfig.Cache`. Transactions use the cache of the `Environment` they
were created from. All cached strings should be cleared by calling `Clear()`
when the owning object is closed.
If no cache is provided, an unlimited `SizedCache` is used. This cache keeps all
paths in memory until `Clear` is called, so applications should clear it when
the environment or database is closed.
