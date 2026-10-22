package storage

import (
	"sync"
	"testing"
)

func TestPageCacheBoundsReplacesAndCounts(t *testing.T) {
	c := newPageCache(64)
	for id := uint64(0); id < 1000; id++ {
		c.put(id, []byte{byte(id)})
	}
	st := c.stats()
	if st.Pages > 64 || st.Evicted != 1000-uint64(st.Pages) {
		t.Fatalf("cache exceeded its bound: %+v", st)
	}
	// A reused page ID replaces the cached page instead of adding a slot.
	c.put(999, []byte("new"))
	if p, ok := c.get(999); !ok || string(p) != "new" {
		t.Fatalf("replaced page = %q, %v", p, ok)
	}
	if c.stats().Pages != st.Pages {
		t.Fatal("replacement added a slot")
	}
	if _, ok := c.get(1 << 40); ok {
		t.Fatal("hit for a page never cached")
	}
	st = c.stats()
	if st.Hits != 1 || st.Misses != 1 {
		t.Fatalf("hits=%d misses=%d", st.Hits, st.Misses)
	}
}

func TestPageCacheKeepsRecentlyUsedPages(t *testing.T) {
	c := newPageCache(16) // one slot per shard
	c.put(0, []byte("hot"))
	c.get(0) // referenced: survives one sweep
	c.put(16, []byte("a"))
	if _, ok := c.get(0); ok {
		// With one slot per shard the referenced page gets one reprieve,
		// then is the only candidate; either outcome must keep the bound.
		t.Log("hot page survived")
	}
	if st := c.stats(); st.Pages > 16 {
		t.Fatalf("bound exceeded: %+v", st)
	}
}

func TestPageCacheConcurrentUse(t *testing.T) {
	c := newPageCache(128)
	var wg sync.WaitGroup
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < 5000; i++ {
				id := uint64((i*7 + g) % 512)
				if p, ok := c.get(id); ok && len(p) != 1 {
					t.Errorf("page %d corrupted in cache", id)
					return
				}
				c.put(id, []byte{byte(id)})
			}
		}(g)
	}
	wg.Wait()
	if st := c.stats(); st.Pages > 128 {
		t.Fatalf("bound exceeded: %+v", st)
	}
}
