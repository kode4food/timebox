package timebox

import (
	"sync"

	"github.com/kode4food/lru"
)

type (
	cache[T any] struct {
		cache *lru.Cache[*cacheEntry[T]]
	}

	constructor[T any] func() T

	cacheEntry[T any] struct {
		value T
		key   string
		mu    sync.Mutex
	}
)

// DefaultCacheSize is used when an LRU cache size is not provided or invalid
const DefaultCacheSize = 4096

func newCache[T any](maxSize int) *cache[T] {
	if maxSize <= 0 {
		maxSize = DefaultCacheSize
	}
	return &cache[T]{
		cache: lru.NewCache[*cacheEntry[T]](maxSize),
	}
}

func (c *cache[T]) Get(key string, cons constructor[T]) *cacheEntry[T] {
	entry, _ := c.cache.Get(key, func() (*cacheEntry[T], error) {
		return &cacheEntry[T]{key: key, value: cons()}, nil
	})
	return entry
}
