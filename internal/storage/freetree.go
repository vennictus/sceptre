package storage

import (
	"encoding/binary"
	"sort"
)

// freeTree is the directory name of the free tree, the B+tree that records
// free pages in format v3 in place of a page chain.
var freeTree = []byte("free")

// freeKeySize is txid (8 bytes) followed by seq (4 bytes).
const freeKeySize = 12

// freeKey builds a free tree key. Both fields are big-endian so that keys
// sort by txid first and then by seq.
func freeKey(txid uint64, seq uint32) []byte {
	k := make([]byte, freeKeySize)
	binary.BigEndian.PutUint64(k[:8], txid)
	binary.BigEndian.PutUint32(k[8:], seq)
	return k
}

// parseFreeKey splits a free tree key into its txid and seq.
func parseFreeKey(k []byte) (txid uint64, seq uint32, err error) {
	if len(k) != freeKeySize {
		return 0, 0, corrupt("free key has %d bytes, want %d", len(k), freeKeySize)
	}
	return binary.BigEndian.Uint64(k[:8]), binary.BigEndian.Uint32(k[8:]), nil
}

// encodeFreePages stores page IDs as little-endian uint64 values in the
// given order.
func encodeFreePages(ids []uint64) []byte {
	v := make([]byte, 8*len(ids))
	for i, id := range ids {
		binary.LittleEndian.PutUint64(v[8*i:], id)
	}
	return v
}

// decodeFreePages reverses encodeFreePages. A free record is never empty and
// its IDs must be strictly ascending.
func decodeFreePages(v []byte) ([]uint64, error) {
	if len(v) == 0 || len(v)%8 != 0 {
		return nil, corrupt("free record has %d bytes, want a non-empty multiple of 8", len(v))
	}
	ids := make([]uint64, len(v)/8)
	for i := range ids {
		ids[i] = binary.LittleEndian.Uint64(v[8*i:])
		if i > 0 && ids[i] <= ids[i-1] {
			return nil, corrupt("free record IDs not ascending at index %d", i)
		}
	}
	return ids, nil
}

// freeChunkCap is the most page IDs whose value still fits inline in a leaf
// when the key is a free key.
func freeChunkCap(l layout) int {
	return (l.maxEntry - slotSize - leafCellHeader - freeKeySize) / 8
}

// chunkFreePages sorts a copy of ids ascending and splits it into consecutive
// chunks of at most cap IDs. It returns nil for empty input.
func chunkFreePages(ids []uint64, cap int) [][]uint64 {
	if len(ids) == 0 {
		return nil
	}
	if cap < 1 {
		cap = 1 // a chunk must hold at least one ID to make progress
	}
	sorted := append([]uint64(nil), ids...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })
	var chunks [][]uint64
	for len(sorted) > cap {
		chunks = append(chunks, sorted[:cap:cap])
		sorted = sorted[cap:]
	}
	return append(chunks, sorted)
}
