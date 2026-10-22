package storage

import (
	"sync"
	"sync/atomic"
)

// cacheShards splits the page cache so concurrent readers rarely share a lock.
const cacheShards = 16

// pageCache holds verified committed pages with per-shard CLOCK eviction.
// Committed pages are immutable, so entries are never invalidated, only
// replaced when a freed page ID is reused by a later commit. A hit takes only
// its shard's read lock and sets the slot's reference bit atomically.
type pageCache struct {
	limit  int
	shards [cacheShards]cacheShard
}

type cacheShard struct {
	mu      sync.RWMutex
	limit   int
	index   map[uint64]*cacheSlot
	slots   []*cacheSlot
	hand    int
	hits    atomic.Uint64
	misses  atomic.Uint64
	evicted atomic.Uint64
}

type cacheSlot struct {
	id   uint64
	page []byte
	ref  atomic.Bool
}

// CacheStats reports page cache activity.
type CacheStats struct {
	Pages, Limit          int
	Hits, Misses, Evicted uint64
}

func newPageCache(limit int) *pageCache {
	if limit < 16 {
		limit = 16
	}
	c := &pageCache{limit: limit}
	per := limit / cacheShards
	if per < 1 {
		per = 1
	}
	for i := range c.shards {
		c.shards[i].limit = per
		c.shards[i].index = make(map[uint64]*cacheSlot, per)
	}
	return c
}

func (c *pageCache) shard(id uint64) *cacheShard { return &c.shards[id&(cacheShards-1)] }

func (c *pageCache) get(id uint64) ([]byte, bool) {
	sh := c.shard(id)
	sh.mu.RLock()
	s, ok := sh.index[id]
	var page []byte
	if ok {
		page = s.page
		s.ref.Store(true)
	}
	sh.mu.RUnlock()
	if !ok {
		sh.misses.Add(1)
		return nil, false
	}
	sh.hits.Add(1)
	return page, true
}

func (c *pageCache) put(id uint64, page []byte) {
	sh := c.shard(id)
	sh.mu.Lock()
	defer sh.mu.Unlock()
	if s, ok := sh.index[id]; ok {
		s.page = page
		s.ref.Store(true)
		return
	}
	if len(sh.slots) < sh.limit {
		s := &cacheSlot{id: id, page: page}
		sh.index[id] = s
		sh.slots = append(sh.slots, s)
		return
	}
	for {
		s := sh.slots[sh.hand]
		sh.hand = (sh.hand + 1) % len(sh.slots)
		if s.ref.Swap(false) {
			continue
		}
		delete(sh.index, s.id)
		sh.evicted.Add(1)
		s.id, s.page = id, page
		sh.index[id] = s
		return
	}
}

func (c *pageCache) stats() CacheStats {
	st := CacheStats{Limit: c.limit}
	for i := range c.shards {
		sh := &c.shards[i]
		sh.mu.RLock()
		st.Pages += len(sh.slots)
		sh.mu.RUnlock()
		st.Hits += sh.hits.Load()
		st.Misses += sh.misses.Load()
		st.Evicted += sh.evicted.Load()
	}
	return st
}
