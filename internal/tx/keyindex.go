package tx

import (
	"slices"
	"sort"
)

// keyBlock is the target block size of a keyIndex. A block splits in two
// when it reaches twice this size, so an insert moves at most 2*keyBlock
// keys and a seek is two binary searches.
const keyBlock = 128

// keyIndex keeps a transaction's pending keys sorted as they are written,
// so a scan can seek into them without copying or sorting the whole set.
// Keys are split into sorted blocks; every key in block b sorts before every
// key in block b+1.
type keyIndex struct {
	blocks [][]string
}

// find returns the position of the first key >= key. A position past the
// last key is (len(blocks), 0).
func (x *keyIndex) find(key string) (int, int) {
	b := sort.Search(len(x.blocks), func(j int) bool {
		blk := x.blocks[j]
		return blk[len(blk)-1] >= key
	})
	if b == len(x.blocks) {
		return b, 0
	}
	return b, sort.SearchStrings(x.blocks[b], key)
}

// insert adds key, which must not already be present.
func (x *keyIndex) insert(key string) {
	if len(x.blocks) == 0 {
		x.blocks = [][]string{{key}}
		return
	}
	b, i := x.find(key)
	if b == len(x.blocks) {
		b = len(x.blocks) - 1
		i = len(x.blocks[b])
	}
	blk := slices.Insert(x.blocks[b], i, key)
	if len(blk) < 2*keyBlock {
		x.blocks[b] = blk
		return
	}
	x.blocks[b] = slices.Clip(blk[:keyBlock])
	x.blocks = slices.Insert(x.blocks, b+1, slices.Clone(blk[keyBlock:]))
}

// remove deletes key if present.
func (x *keyIndex) remove(key string) {
	b, i := x.find(key)
	if b == len(x.blocks) || x.blocks[b][i] != key {
		return
	}
	x.blocks[b] = slices.Delete(x.blocks[b], i, i+1)
	if len(x.blocks[b]) == 0 {
		x.blocks = slices.Delete(x.blocks, b, b+1)
	}
}

// valid reports whether (b, i) is a key position.
func (x *keyIndex) valid(b, i int) bool { return b < len(x.blocks) && i < len(x.blocks[b]) }

// next returns the position after (b, i).
func (x *keyIndex) next(b, i int) (int, int) {
	if i+1 < len(x.blocks[b]) {
		return b, i + 1
	}
	return b + 1, 0
}
