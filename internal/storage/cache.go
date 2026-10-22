package storage

import "sync"

// pageCache holds verified committed pages with CLOCK eviction. Committed
// pages are immutable, so entries are never invalidated, only replaced when
// a freed page ID is reused by a later commit.
type pageCache struct {
	mu      sync.Mutex
	limit   int
	index   map[uint64]int
	slots   []cacheSlot
	hand    int
	hits    uint64
	misses  uint64
	evicted uint64
}

type cacheSlot struct {
	id   uint64
	page []byte
	ref  bool
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
	return &pageCache{limit: limit, index: make(map[uint64]int, limit)}
}

func (c *pageCache) get(id uint64) ([]byte, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	i, ok := c.index[id]
	if !ok {
		c.misses++
		return nil, false
	}
	c.hits++
	c.slots[i].ref = true
	return c.slots[i].page, true
}

func (c *pageCache) put(id uint64, page []byte) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if i, ok := c.index[id]; ok {
		c.slots[i] = cacheSlot{id: id, page: page, ref: true}
		return
	}
	if len(c.slots) < c.limit {
		c.index[id] = len(c.slots)
		c.slots = append(c.slots, cacheSlot{id: id, page: page})
		return
	}
	for {
		s := &c.slots[c.hand]
		if s.ref {
			s.ref = false
			c.hand = (c.hand + 1) % len(c.slots)
			continue
		}
		delete(c.index, s.id)
		c.evicted++
		*s = cacheSlot{id: id, page: page}
		c.index[id] = c.hand
		c.hand = (c.hand + 1) % len(c.slots)
		return
	}
}

func (c *pageCache) stats() CacheStats {
	c.mu.Lock()
	defer c.mu.Unlock()
	return CacheStats{Pages: len(c.slots), Limit: c.limit, Hits: c.hits, Misses: c.misses, Evicted: c.evicted}
}
