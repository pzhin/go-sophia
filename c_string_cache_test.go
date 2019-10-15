package sophia

import (
	"math/rand"
	"runtime"
	"sync"
	"testing"
)

const letters = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"

func TestGetCStringFromCacheRace(t *testing.T) {
	var wg sync.WaitGroup
	workers := runtime.GOMAXPROCS(-1) * 8
	const iterations = 100
	for i := 0; i < workers; i ++ {
		wg.Add(1)
		go func(){
			defer wg.Done()
			for i := 0; i < iterations; i++ {
				getCStringFromCache(generateString(32))
			}
		}()
	}
	wg.Wait()
}

func generateString(n int) string {
	b := make([]byte, n)
	for i := range b {
		b[i] = letters[rand.Intn(len(letters))]
	}
	return string(b)
}
