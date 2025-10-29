package timebox

import (
	"container/list"
	"sync"
)

type (
	lruCache[T any] struct {
		cache   map[string]*list.Element
		lru     *list.List
		maxSize int
		mu      sync.RWMutex
	}

	constructor[T any] func() T

	cacheEntry[T any] struct {
		value T
		key   string
		mu    sync.Mutex
	}
)

const DefaultCacheSize = 4096

func newLRUCache[T any](maxSize int) *lruCache[T] {
	if maxSize <= 0 {
		maxSize = DefaultCacheSize
	}
	return &lruCache[T]{
		cache:   map[string]*list.Element{},
		lru:     list.New(),
		maxSize: maxSize,
	}
}

func (c *lruCache[T]) Get(key string, cons constructor[T]) *cacheEntry[T] {
	c.mu.RLock()
	elem, ok := c.cache[key]
	c.mu.RUnlock()

	if !ok {
		elem = c.createAndCacheEntry(key, cons)
	}

	c.mu.Lock()
	c.lru.MoveToFront(elem)
	c.mu.Unlock()

	return elem.Value.(*cacheEntry[T])
}

func (c *lruCache[T]) createAndCacheEntry(
	key string, cons constructor[T],
) *list.Element {
	c.mu.Lock()
	defer c.mu.Unlock()

	if elem, ok := c.cache[key]; ok {
		return elem
	}

	entry := &cacheEntry[T]{key: key, value: cons()}
	elem := c.lru.PushFront(entry)
	c.cache[key] = elem

	if c.lru.Len() > c.maxSize {
		c.evictLast()
	}

	return elem
}

func (c *lruCache[T]) evictLast() {
	back := c.lru.Back()
	if back != nil {
		c.lru.Remove(back)
		backEntry := back.Value.(*cacheEntry[T])
		delete(c.cache, backEntry.key)
	}
}
