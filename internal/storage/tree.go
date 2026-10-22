package storage

import (
	"bytes"
	"errors"
)

// maxDepth bounds every descent so corrupt branch pointers cannot loop.
const maxDepth = 64

var errSplit = errors.New("storage: node cannot be split")

// pageSource reads the pages of one snapshot or write transaction.
type pageSource interface {
	page(id uint64) ([]byte, error)
}

// writer applies copy-on-write changes for one write transaction. Committed
// pages are never modified: a changed page is copied to a new ID, and the old
// ID is recorded in freed. Pages this transaction created are private to it
// and are simply replaced.
type writer struct {
	src     pageSource // committed pages of the base snapshot
	l       layout
	dirty   map[uint64][]byte
	freed   []uint64
	alloc   func() (uint64, error)
	recycle func(id uint64) // a page created by this transaction is unused again
}

func (w *writer) page(id uint64) ([]byte, error) {
	if page, ok := w.dirty[id]; ok {
		return page, nil
	}
	return w.src.page(id)
}

// rewrite returns the ID to write a new version of page id to.
func (w *writer) rewrite(id uint64) (uint64, error) {
	if _, ok := w.dirty[id]; ok {
		return id, nil
	}
	w.freed = append(w.freed, id)
	return w.alloc()
}

// drop releases a page that no longer belongs to the tree.
func (w *writer) drop(id uint64) {
	if _, ok := w.dirty[id]; ok {
		delete(w.dirty, id)
		w.recycle(id)
		return
	}
	w.freed = append(w.freed, id)
}

func (w *writer) store(id uint64, page []byte) { w.dirty[id] = page }

func cloneBytes(b []byte) []byte { return append([]byte(nil), b...) }

// get looks up key in the tree rooted at root.
func get(src pageSource, l layout, root uint64, key []byte) ([]byte, bool, error) {
	id := root
	for depth := 0; id != 0; depth++ {
		if depth >= maxDepth {
			return nil, false, corrupt("tree deeper than %d levels", maxDepth)
		}
		page, err := src.page(id)
		if err != nil {
			return nil, false, err
		}
		switch typeOf(page) {
		case pageLeaf:
			i, found, err := searchLeaf(page, key)
			if err != nil || !found {
				return nil, false, err
			}
			n, _ := nodeCount(page)
			e, err := leafAt(page, i, n)
			if err != nil {
				return nil, false, err
			}
			v, err := valueOf(src, l.pageSize, e)
			return v, err == nil, err
		case pageBranch:
			i, n, err := childIndex(page, key)
			if err != nil {
				return nil, false, err
			}
			e, err := branchAt(page, i, n)
			if err != nil {
				return nil, false, err
			}
			id = e.child
		default:
			return nil, false, corrupt("tree page %d is a %s page", id, typeOf(page))
		}
	}
	return nil, false, nil
}

// put inserts or replaces key and returns the new root.
func (w *writer) put(root uint64, key, value []byte) (uint64, error) {
	if err := w.l.checkKey(key); err != nil {
		return 0, err
	}
	if len(value) > MaxValueSize {
		return 0, ErrValueTooLarge
	}
	e := leafEntry{key: cloneBytes(key), size: uint32(len(value))}
	if w.l.inline(key, len(value)) {
		e.value = cloneBytes(value)
	} else {
		first, err := w.writeOverflow(value)
		if err != nil {
			return 0, err
		}
		e.overflow = first
	}
	if root == 0 {
		id, err := w.alloc()
		if err != nil {
			return 0, err
		}
		w.store(id, encodeLeaf(w.l, []leafEntry{e}))
		return id, nil
	}
	res, err := w.insert(root, e, 0)
	if err != nil {
		return 0, err
	}
	return w.grow(res)
}

type insertResult struct {
	id        uint64
	max       []byte
	split     *branchEntry
	underflow bool // the node shrank below minFill and needs rebalancing
}

// grow adds a new root when the old one split.
func (w *writer) grow(res insertResult) (uint64, error) {
	if res.split == nil {
		return res.id, nil
	}
	id, err := w.alloc()
	if err != nil {
		return 0, err
	}
	w.store(id, encodeBranch(w.l, []branchEntry{{child: res.id, key: res.max}, *res.split}))
	return id, nil
}

func (w *writer) insert(id uint64, e leafEntry, depth int) (insertResult, error) {
	if depth >= maxDepth {
		return insertResult{}, corrupt("tree deeper than %d levels", maxDepth)
	}
	page, err := w.page(id)
	if err != nil {
		return insertResult{}, err
	}
	switch typeOf(page) {
	case pageLeaf:
		entries, err := decodeLeaf(page)
		if err != nil {
			return insertResult{}, err
		}
		i, found := searchEntries(len(entries), func(i int) []byte { return entries[i].key }, e.key)
		if found {
			if old := entries[i]; old.overflow != 0 {
				if err := w.freeOverflow(old); err != nil {
					return insertResult{}, err
				}
			}
			entries[i] = e
		} else {
			entries = append(entries, leafEntry{})
			copy(entries[i+1:], entries[i:])
			entries[i] = e
		}
		return w.writeLeaf(id, entries)
	case pageBranch:
		entries, err := decodeBranch(page)
		if err != nil {
			return insertResult{}, err
		}
		i := childEntry(entries, e.key)
		res, err := w.insert(entries[i].child, e, depth+1)
		if err != nil {
			return insertResult{}, err
		}
		entries[i] = branchEntry{child: res.id, key: res.max}
		if res.split != nil {
			entries = append(entries, branchEntry{})
			copy(entries[i+2:], entries[i+1:])
			entries[i+1] = *res.split
		} else if res.underflow && len(entries) > 1 {
			// Replacing a value or separator with a shorter one shrinks nodes too.
			if entries, err = w.rebalance(entries, i); err != nil {
				return insertResult{}, err
			}
		}
		return w.writeBranch(id, entries)
	default:
		return insertResult{}, corrupt("tree page %d is a %s page", id, typeOf(page))
	}
}

// writeLeaf stores entries as the new version of leaf id, splitting if needed.
func (w *writer) writeLeaf(id uint64, entries []leafEntry) (insertResult, error) {
	nid, err := w.rewrite(id)
	if err != nil {
		return insertResult{}, err
	}
	if leafBytes(entries) <= w.l.usable {
		w.store(nid, encodeLeaf(w.l, entries))
		return insertResult{id: nid, max: entries[len(entries)-1].key, underflow: leafBytes(entries) < w.l.minFill}, nil
	}
	s, err := splitPoint(len(entries), w.l.usable, func(i int) int { return leafCost(entries[i]) })
	if err != nil {
		return insertResult{}, err
	}
	rid, err := w.alloc()
	if err != nil {
		return insertResult{}, err
	}
	left, right := entries[:s], entries[s:]
	w.store(nid, encodeLeaf(w.l, left))
	w.store(rid, encodeLeaf(w.l, right))
	return insertResult{id: nid, max: left[len(left)-1].key, split: &branchEntry{child: rid, key: right[len(right)-1].key}}, nil
}

// writeBranch stores entries as the new version of branch id, splitting if needed.
func (w *writer) writeBranch(id uint64, entries []branchEntry) (insertResult, error) {
	nid, err := w.rewrite(id)
	if err != nil {
		return insertResult{}, err
	}
	if branchBytes(entries) <= w.l.usable {
		w.store(nid, encodeBranch(w.l, entries))
		return insertResult{id: nid, max: entries[len(entries)-1].key, underflow: branchBytes(entries) < w.l.minFill}, nil
	}
	s, err := splitPoint(len(entries), w.l.usable, func(i int) int { return branchCost(entries[i]) })
	if err != nil {
		return insertResult{}, err
	}
	rid, err := w.alloc()
	if err != nil {
		return insertResult{}, err
	}
	left, right := entries[:s], entries[s:]
	w.store(nid, encodeBranch(w.l, left))
	w.store(rid, encodeBranch(w.l, right))
	return insertResult{id: nid, max: left[len(left)-1].key, split: &branchEntry{child: rid, key: right[len(right)-1].key}}, nil
}

// splitPoint picks the split index that best balances bytes while keeping
// both halves within capacity.
func splitPoint(n, capacity int, cost func(int) int) (int, error) {
	total := 0
	for i := 0; i < n; i++ {
		total += cost(i)
	}
	best, bestGap := -1, 0
	left := 0
	for s := 1; s < n; s++ {
		left += cost(s - 1)
		right := total - left
		if left > capacity || right > capacity {
			continue
		}
		gap := left - right
		if gap < 0 {
			gap = -gap
		}
		if best < 0 || gap < bestGap {
			best, bestGap = s, gap
		}
	}
	if best < 0 {
		return 0, errSplit
	}
	return best, nil
}

func searchEntries(n int, key func(int) []byte, target []byte) (int, bool) {
	lo, hi := 0, n
	for lo < hi {
		mid := int(uint(lo+hi) >> 1)
		switch c := bytes.Compare(key(mid), target); {
		case c == 0:
			return mid, true
		case c < 0:
			lo = mid + 1
		default:
			hi = mid
		}
	}
	return lo, false
}

func childEntry(entries []branchEntry, key []byte) int {
	i, _ := searchEntries(len(entries), func(i int) []byte { return entries[i].key }, key)
	if i == len(entries) {
		return i - 1
	}
	return i
}

type deleteResult struct {
	found     bool
	empty     bool
	id        uint64
	max       []byte
	underflow bool
	split     *branchEntry
}

// del removes key and returns the new root.
func (w *writer) del(root uint64, key []byte) (uint64, bool, error) {
	if root == 0 {
		return 0, false, nil
	}
	res, err := w.remove(root, key, 0)
	if err != nil || !res.found {
		return root, false, err
	}
	if res.empty {
		return 0, true, nil
	}
	root, err = w.grow(insertResult{id: res.id, max: res.max, split: res.split})
	if err != nil {
		return 0, false, err
	}
	// A branch root with one child is replaced by that child.
	for {
		page, err := w.page(root)
		if err != nil {
			return 0, false, err
		}
		if typeOf(page) != pageBranch {
			return root, true, nil
		}
		n, err := nodeCount(page)
		if err != nil {
			return 0, false, err
		}
		if n != 1 {
			return root, true, nil
		}
		e, err := branchAt(page, 0, n)
		if err != nil {
			return 0, false, err
		}
		w.drop(root)
		root = e.child
	}
}

func (w *writer) remove(id uint64, key []byte, depth int) (deleteResult, error) {
	if depth >= maxDepth {
		return deleteResult{}, corrupt("tree deeper than %d levels", maxDepth)
	}
	page, err := w.page(id)
	if err != nil {
		return deleteResult{}, err
	}
	switch typeOf(page) {
	case pageLeaf:
		entries, err := decodeLeaf(page)
		if err != nil {
			return deleteResult{}, err
		}
		i, found := searchEntries(len(entries), func(i int) []byte { return entries[i].key }, key)
		if !found {
			return deleteResult{}, nil
		}
		if old := entries[i]; old.overflow != 0 {
			if err := w.freeOverflow(old); err != nil {
				return deleteResult{}, err
			}
		}
		entries = append(entries[:i], entries[i+1:]...)
		if len(entries) == 0 {
			w.drop(id)
			return deleteResult{found: true, empty: true}, nil
		}
		res, err := w.writeLeaf(id, entries)
		if err != nil {
			return deleteResult{}, err
		}
		return deleteResult{found: true, id: res.id, max: res.max, underflow: leafBytes(entries) < w.l.minFill}, nil
	case pageBranch:
		entries, err := decodeBranch(page)
		if err != nil {
			return deleteResult{}, err
		}
		i := childEntry(entries, key)
		child, err := w.remove(entries[i].child, key, depth+1)
		if err != nil || !child.found {
			return child, err
		}
		switch {
		case child.empty:
			entries = append(entries[:i], entries[i+1:]...)
		default:
			entries[i] = branchEntry{child: child.id, key: child.max}
			if child.split != nil {
				entries = append(entries, branchEntry{})
				copy(entries[i+2:], entries[i+1:])
				entries[i+1] = *child.split
			} else if child.underflow && len(entries) > 1 {
				if entries, err = w.rebalance(entries, i); err != nil {
					return deleteResult{}, err
				}
			}
		}
		if len(entries) == 0 {
			w.drop(id)
			return deleteResult{found: true, empty: true}, nil
		}
		// A shorter maximum can expose a longer separator, so deletion can
		// split a branch too.
		res, err := w.writeBranch(id, entries)
		if err != nil {
			return deleteResult{}, err
		}
		return deleteResult{found: true, id: res.id, max: res.max, split: res.split,
			underflow: res.split == nil && branchBytes(entries) < w.l.minFill}, nil
	default:
		return deleteResult{}, corrupt("tree page %d is a %s page", id, typeOf(page))
	}
}

// rebalance merges child i with a sibling, or redistributes their entries.
func (w *writer) rebalance(entries []branchEntry, i int) ([]branchEntry, error) {
	left := i - 1
	if i == 0 {
		left = 0
	}
	right := left + 1
	lp, err := w.page(entries[left].child)
	if err != nil {
		return nil, err
	}
	rp, err := w.page(entries[right].child)
	if err != nil {
		return nil, err
	}
	if typeOf(lp) != typeOf(rp) {
		return nil, corrupt("siblings %d and %d have different types", entries[left].child, entries[right].child)
	}

	var leftPage, rightPage []byte
	var leftMax, rightMax []byte
	merged := false
	switch typeOf(lp) {
	case pageLeaf:
		a, err := decodeLeaf(lp)
		if err != nil {
			return nil, err
		}
		b, err := decodeLeaf(rp)
		if err != nil {
			return nil, err
		}
		all := append(append([]leafEntry(nil), a...), b...)
		if leafBytes(all) <= w.l.usable {
			leftPage, leftMax, merged = encodeLeaf(w.l, all), all[len(all)-1].key, true
			break
		}
		s, err := splitPoint(len(all), w.l.usable, func(i int) int { return leafCost(all[i]) })
		if err != nil {
			return nil, err
		}
		leftPage, leftMax = encodeLeaf(w.l, all[:s]), all[s-1].key
		rightPage, rightMax = encodeLeaf(w.l, all[s:]), all[len(all)-1].key
	case pageBranch:
		a, err := decodeBranch(lp)
		if err != nil {
			return nil, err
		}
		b, err := decodeBranch(rp)
		if err != nil {
			return nil, err
		}
		all := append(append([]branchEntry(nil), a...), b...)
		if branchBytes(all) <= w.l.usable {
			leftPage, leftMax, merged = encodeBranch(w.l, all), all[len(all)-1].key, true
			break
		}
		s, err := splitPoint(len(all), w.l.usable, func(i int) int { return branchCost(all[i]) })
		if err != nil {
			return nil, err
		}
		leftPage, leftMax = encodeBranch(w.l, all[:s]), all[s-1].key
		rightPage, rightMax = encodeBranch(w.l, all[s:]), all[len(all)-1].key
	default:
		return nil, corrupt("tree page %d is a %s page", entries[left].child, typeOf(lp))
	}

	lid, err := w.rewrite(entries[left].child)
	if err != nil {
		return nil, err
	}
	w.store(lid, leftPage)
	entries[left] = branchEntry{child: lid, key: leftMax}
	if merged {
		w.drop(entries[right].child)
		return append(entries[:right], entries[right+1:]...), nil
	}
	rid, err := w.rewrite(entries[right].child)
	if err != nil {
		return nil, err
	}
	w.store(rid, rightPage)
	entries[right] = branchEntry{child: rid, key: rightMax}
	return entries, nil
}
