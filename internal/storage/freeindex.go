package storage

import "sort"

// freeRec is one free tree record held in memory: pages freed by txid,
// split into chunks numbered by seq.
type freeRec struct {
	txid  uint64
	seq   uint32
	pages []uint64 // ascending
}

// freeIndex is the committed set of free pages: the free tree's records, and
// for each free page the transaction that freed it. It changes only when a
// commit is installed, under the env mutex, while the writer slot is held.
type freeIndex struct {
	recs  map[string]*freeRec // key: string(freeKey(txid, seq))
	pages map[uint64]uint64
}

func newFreeIndex() *freeIndex {
	return &freeIndex{recs: map[string]*freeRec{}, pages: map[uint64]uint64{}}
}

func (x *freeIndex) add(txid uint64, seq uint32, pages []uint64) {
	x.recs[string(freeKey(txid, seq))] = &freeRec{txid: txid, seq: seq, pages: pages}
	for _, id := range pages {
		x.pages[id] = txid
	}
}

// loadFreeIndex reads a free tree, checking that every page is inside the
// file and listed once.
func loadFreeIndex(src pageSource, l layout, root, pageCount uint64) (*freeIndex, error) {
	x := newFreeIndex()
	c := newCursor(src, l, root)
	for err := c.SeekFirst(); c.Valid(); err = c.Next() {
		if err != nil {
			return nil, err
		}
		k, err := c.Key()
		if err != nil {
			return nil, err
		}
		v, err := c.Value()
		if err != nil {
			return nil, err
		}
		txid, seq, err := parseFreeKey(k)
		if err != nil {
			return nil, err
		}
		pages, err := decodeFreePages(v)
		if err != nil {
			return nil, err
		}
		for _, id := range pages {
			if id < 2 || id >= pageCount {
				return nil, corrupt("free tree lists out-of-range page %d", id)
			}
			if _, dup := x.pages[id]; dup {
				return nil, corrupt("free tree lists page %d twice", id)
			}
		}
		x.add(txid, seq, pages)
	}
	if err := c.Err(); err != nil {
		return nil, err
	}
	return x, nil
}

// legacyFreeIndex groups a format v2 freelist into records. They exist only
// in memory until the converting commit writes them.
func legacyFreeIndex(free map[uint64]uint64, l layout) *freeIndex {
	byTx := map[uint64][]uint64{}
	for id, txid := range free {
		byTx[txid] = append(byTx[txid], id)
	}
	x := newFreeIndex()
	for txid, ids := range byTx {
		for seq, chunk := range chunkFreePages(ids, freeChunkCap(l)) {
			x.add(txid, uint32(seq), chunk)
		}
	}
	return x
}

// eligible lists the keys of records whose pages may be reused, oldest first.
func (x *freeIndex) eligible(limit uint64) []string {
	var keys []string
	for k, r := range x.recs {
		if r.txid <= limit {
			keys = append(keys, k)
		}
	}
	sort.Strings(keys) // keys sort by txid, then seq
	return keys
}

// reusable counts pages in records at or before limit.
func (x *freeIndex) reusable(limit uint64) int {
	n := 0
	for _, r := range x.recs {
		if r.txid <= limit {
			n += len(r.pages)
		}
	}
	return n
}

func (x *freeIndex) nextSeq(txid uint64) uint32 {
	var next uint32
	for _, r := range x.recs {
		if r.txid == txid && r.seq >= next {
			next = r.seq + 1
		}
	}
	return next
}

// equal reports whether two indexes hold the same records.
func (x *freeIndex) equal(y *freeIndex) bool {
	if len(x.recs) != len(y.recs) || len(x.pages) != len(y.pages) {
		return false
	}
	for k, r := range x.recs {
		o, ok := y.recs[k]
		if !ok || len(o.pages) != len(r.pages) {
			return false
		}
		for i := range r.pages {
			if r.pages[i] != o.pages[i] {
				return false
			}
		}
	}
	return true
}

// install applies a published commit's free tree edits to the index.
// It removes every edited record first and adds the new ones after, so a page
// that moved between records (reused, then recycled) ends up listed once.
func (x *freeIndex) install(edits map[string][]uint64, consumed []uint64) {
	for _, id := range consumed {
		delete(x.pages, id)
	}
	for k := range edits {
		if old, ok := x.recs[k]; ok {
			for _, id := range old.pages {
				delete(x.pages, id)
			}
			delete(x.recs, k)
		}
	}
	for k, pages := range edits {
		if pages != nil {
			txid, seq, _ := parseFreeKey([]byte(k))
			x.add(txid, seq, pages)
		}
	}
}
