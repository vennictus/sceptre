package storage

import (
	"encoding/binary"
	"sort"
)

// Freelist page layout after the header:
//
//	24 uint64  next freelist page, or 0
//	32 uint32  entry count
//	36 entries: page ID uint64, freeing transaction ID uint64
//
// An entry records a page that was part of the tree before the transaction
// that freed it. Tag 0 marks a page no committed state ever used.
const (
	freeNextOff    = pageHeaderSize
	freeCountOff   = pageHeaderSize + 8
	freeEntriesOff = pageHeaderSize + 12
	freeEntrySize  = 16
)

func freelistCapacity(pageSize int) int { return (pageSize - freeEntriesOff) / freeEntrySize }

func freelistPagesFor(pageSize, entries int) int {
	c := freelistCapacity(pageSize)
	return (entries + c - 1) / c
}

// loadFreelist reads the freelist chain of a committed state.
func loadFreelist(src pageSource, pageCount, head uint64) (map[uint64]uint64, []uint64, error) {
	free := map[uint64]uint64{}
	var pages []uint64
	seen := map[uint64]bool{}
	for id := head; id != 0; {
		if seen[id] {
			return nil, nil, corrupt("freelist chain loops at page %d", id)
		}
		seen[id] = true
		page, err := src.page(id)
		if err != nil {
			return nil, nil, err
		}
		if typeOf(page) != pageFreelist {
			return nil, nil, corrupt("freelist chain reaches %s page %d", typeOf(page), id)
		}
		count := int(binary.LittleEndian.Uint32(page[freeCountOff:]))
		if count > freelistCapacity(len(page)) {
			return nil, nil, corrupt("freelist page %d claims %d entries", id, count)
		}
		for i := 0; i < count; i++ {
			at := freeEntriesOff + i*freeEntrySize
			fid := binary.LittleEndian.Uint64(page[at:])
			if fid < 2 || fid >= pageCount {
				return nil, nil, corrupt("freelist page %d lists out-of-range page %d", id, fid)
			}
			if _, dup := free[fid]; dup {
				return nil, nil, corrupt("freelist lists page %d twice", fid)
			}
			free[fid] = binary.LittleEndian.Uint64(page[at+8:])
		}
		pages = append(pages, id)
		id = binary.LittleEndian.Uint64(page[freeNextOff:])
	}
	for _, id := range pages {
		if _, ok := free[id]; ok {
			return nil, nil, corrupt("freelist page %d lists itself as free", id)
		}
	}
	return free, pages, nil
}

// encodeFreelist spreads entries across the given pages in page-ID order.
func encodeFreelist(pageSize int, free map[uint64]uint64, pages []uint64) map[uint64][]byte {
	ids := make([]uint64, 0, len(free))
	for id := range free {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	capacity := freelistCapacity(pageSize)
	out := make(map[uint64][]byte, len(pages))
	for i, pid := range pages {
		page := newPage(pageSize, pageFreelist)
		if i+1 < len(pages) {
			binary.LittleEndian.PutUint64(page[freeNextOff:], pages[i+1])
		}
		chunk := ids
		if len(chunk) > capacity {
			chunk = chunk[:capacity]
		}
		ids = ids[len(chunk):]
		binary.LittleEndian.PutUint32(page[freeCountOff:], uint32(len(chunk)))
		for j, fid := range chunk {
			at := freeEntriesOff + j*freeEntrySize
			binary.LittleEndian.PutUint64(page[at:], fid)
			binary.LittleEndian.PutUint64(page[at+8:], free[fid])
		}
		out[pid] = page
	}
	return out
}
