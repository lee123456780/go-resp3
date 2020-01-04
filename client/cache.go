/*
Copyright 2019 Stefan Miller

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package client

import (
	"sync"
)

type cache map[uint32]map[string]RedisValue

// Cache is a key cache supporting Redis client side caching.
type Cache struct {
	mu    sync.RWMutex
	cache cache
}

// NewCache creates a new ClientCache instance.
func NewCache() *Cache {
	return &Cache{cache: make(cache, 0)}
}

// NewCacheSize creates a new ClientCache instance with size slot entries.
func NewCacheSize(size int) *Cache {
	return &Cache{cache: make(cache, size)}
}

// Put inserts a key into the cache.
func (c *Cache) Put(key string, value RedisValue) {
	c.mu.Lock()
	slot := Key(key).Slot()
	if keyMap, ok := c.cache[slot]; ok {
		keyMap[key] = value
	} else {
		c.cache[slot] = map[string]RedisValue{key: value}
	}
	c.mu.Unlock()
}

// Get reads the value of a key from the cache.
func (c *Cache) Get(key string) (RedisValue, bool) {
	c.mu.RLock()
	slot := Key(key).Slot()
	value, ok := c.cache[slot][key]
	c.mu.RUnlock()
	return value, ok
}

func (c *Cache) invalidate(slot uint32) {
	c.mu.Lock()
	delete(c.cache, slot)
	c.mu.Unlock()
}
