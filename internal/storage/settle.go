package storage

import (
	"encoding/binary"
	"errors"
	"sort"
)

// settleRounds bounds how often a commit re-applies its free tree edits.
const settleRounds = 32

var errUnsettled = errors.New("storage: free tree did not settle")

type settleState struct {
	freed, consumed, spare int
	next                   uint64
	dirRoot, freeRoot      uint64
}

func (t *WriteTxn) settleState(dirRoot, freeRoot uint64) settleState {
	return settleState{len(t.w.freed), len(t.consumed), len(t.spare), t.next, dirRoot, freeRoot}
}

// settle writes the free tree and the directory for this commit.
//
// Editing the free tree is itself copy-on-write: it frees pages and allocates
// them, which changes what the free tree must say. So the edits are applied
// again until a round neither allocates, frees, nor recycles a page. Every
// page a round touches is private to the transaction afterwards, so later
// rounds rewrite those pages in place and this settles in a few rounds.
//
// It returns the final edits (nil value: record deleted) for installing the
// in-memory index after publication.
func (t *WriteTxn) settle(txid uint64) (dirRoot, freeRoot uint64, edits map[string][]uint64, err error) {
	x := t.e.free
	capacity := freeChunkCap(t.e.l)
	spareSeq := x.nextSeq(0)
	dirRoot, freeRoot = t.base.dirRoot, t.base.freeRoot
	added := map[string]bool{} // keys this commit created in earlier rounds
	for round := 0; round < settleRounds; round++ {
		before := t.settleState(dirRoot, freeRoot)
		edits = map[string][]uint64{}
		if t.convert {
			// Converting a v2 file: every record goes into the new tree.
			for k, r := range x.recs {
				edits[k] = r.pages
			}
		}
		for k, n := range t.taken {
			if rest := x.recs[k].pages[n:]; len(rest) > 0 {
				edits[k] = rest
			} else {
				edits[k] = nil
			}
		}
		for seq, chunk := range chunkFreePages(t.w.freed, capacity) {
			edits[string(freeKey(txid, uint32(seq)))] = chunk
		}
		for i, chunk := range chunkFreePages(t.spare, capacity) {
			edits[string(freeKey(0, spareSeq+uint32(i)))] = chunk
		}
		for k := range added {
			if _, ok := edits[k]; !ok {
				edits[k] = nil // a chunk from an earlier round is no longer needed
			}
		}

		keys := make([]string, 0, len(edits))
		for k := range edits {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for _, k := range keys {
			if v := edits[k]; v == nil {
				freeRoot, _, err = t.w.del(freeRoot, []byte(k))
			} else {
				freeRoot, err = t.w.put(freeRoot, []byte(k), encodeFreePages(v))
				if _, existed := x.recs[k]; !existed {
					added[k] = true
				}
			}
			if err != nil {
				return 0, 0, nil, err
			}
		}
		if dirRoot, err = t.putRoot(dirRoot, dataTree, t.root); err != nil {
			return 0, 0, nil, err
		}
		if dirRoot, err = t.putRoot(dirRoot, freeTree, freeRoot); err != nil {
			return 0, 0, nil, err
		}
		if t.settleState(dirRoot, freeRoot) == before {
			return dirRoot, freeRoot, edits, nil
		}
	}
	return 0, 0, nil, errUnsettled
}

// putRoot records a tree's root in the directory, or removes an empty tree.
func (t *WriteTxn) putRoot(dirRoot uint64, name []byte, root uint64) (uint64, error) {
	if root == 0 {
		dirRoot, _, err := t.w.del(dirRoot, name)
		return dirRoot, err
	}
	var v [8]byte
	binary.LittleEndian.PutUint64(v[:], root)
	return t.w.put(dirRoot, name, v[:])
}
