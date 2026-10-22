package storage

import (
	"bytes"
	"fmt"
)

// TreeStats summarizes a verified tree.
type TreeStats struct {
	Keys          int
	Depth         int // levels, 0 for an empty tree
	BranchPages   int
	LeafPages     int
	OverflowPages int
	// Underfull counts non-root nodes below the rebalancing threshold.
	Underfull int
	// Pages lists every page the tree owns, including overflow pages.
	Pages map[uint64]pageType
}

// verifyTree checks a tree's full structure: page types, strictly increasing
// keys, branch keys equal to their subtree maximum, uniform leaf depth,
// well-formed overflow chains, values stored inline exactly when they fit,
// and no page reached twice.
func verifyTree(src pageSource, l layout, root uint64) (TreeStats, error) {
	st := TreeStats{Pages: map[uint64]pageType{}}
	if root == 0 {
		return st, nil
	}
	v := treeVerifier{src: src, l: l, st: &st, leafDepth: -1}
	if _, err := v.node(root, 0, nil); err != nil {
		return TreeStats{}, err
	}
	st.Depth = v.leafDepth + 1
	return st, nil
}

type treeVerifier struct {
	src       pageSource
	l         layout
	st        *TreeStats
	leafDepth int
	last      []byte // largest key seen so far, in key order
}

func (v *treeVerifier) claim(id uint64, t pageType) error {
	if _, ok := v.st.Pages[id]; ok {
		return corrupt("page %d is reachable twice", id)
	}
	v.st.Pages[id] = t
	return nil
}

// node verifies the subtree at id and returns its largest key. lower is the
// exclusive lower bound from the parent, or nil.
func (v *treeVerifier) node(id uint64, depth int, lower []byte) ([]byte, error) {
	if depth >= maxDepth {
		return nil, corrupt("tree deeper than %d levels", maxDepth)
	}
	page, err := v.src.page(id)
	if err != nil {
		return nil, err
	}
	switch typeOf(page) {
	case pageLeaf:
		if err := v.claim(id, pageLeaf); err != nil {
			return nil, err
		}
		v.st.LeafPages++
		if v.leafDepth < 0 {
			v.leafDepth = depth
		} else if depth != v.leafDepth {
			return nil, corrupt("leaf %d at depth %d, others at %d", id, depth, v.leafDepth)
		}
		entries, err := decodeLeaf(page)
		if err != nil {
			return nil, err
		}
		if len(entries) == 0 {
			return nil, corrupt("leaf %d is empty", id)
		}
		for _, e := range entries {
			if v.last != nil && bytes.Compare(e.key, v.last) <= 0 {
				return nil, corrupt("leaf %d key %q not above previous %q", id, e.key, v.last)
			}
			if lower != nil && bytes.Compare(e.key, lower) <= 0 {
				return nil, corrupt("leaf %d key %q not above separator %q", id, e.key, lower)
			}
			if len(e.key) > v.l.maxKey {
				return nil, corrupt("leaf %d key of %d bytes exceeds limit %d", id, len(e.key), v.l.maxKey)
			}
			if inline := v.l.inline(e.key, int(e.size)); inline != (e.overflow == 0) {
				return nil, corrupt("leaf %d stores a %d-byte value %s", id, e.size, map[bool]string{true: "out of line", false: "inline"}[inline])
			}
			if e.overflow != 0 {
				var chain []uint64
				if err := walkOverflow(v.src, v.l.pageSize, e.overflow, e.size, func(oid uint64, _ []byte) {
					chain = append(chain, oid)
				}); err != nil {
					return nil, err
				}
				for _, oid := range chain {
					if err := v.claim(oid, pageOverflow); err != nil {
						return nil, err
					}
					v.st.OverflowPages++
				}
			}
			v.last = e.key
			v.st.Keys++
		}
		if leafBytes(entries) > v.l.usable {
			return nil, corrupt("leaf %d overfull", id)
		}
		if depth > 0 && leafBytes(entries) < v.l.minFill {
			v.st.Underfull++
		}
		return entries[len(entries)-1].key, nil
	case pageBranch:
		if err := v.claim(id, pageBranch); err != nil {
			return nil, err
		}
		v.st.BranchPages++
		entries, err := decodeBranch(page)
		if err != nil {
			return nil, err
		}
		if depth > 0 && branchBytes(entries) < v.l.minFill {
			v.st.Underfull++
		}
		prev := lower
		for i, e := range entries {
			if i > 0 && bytes.Compare(e.key, entries[i-1].key) <= 0 {
				return nil, corrupt("branch %d keys out of order at %d", id, i)
			}
			max, err := v.node(e.child, depth+1, prev)
			if err != nil {
				return nil, err
			}
			if !bytes.Equal(max, e.key) {
				return nil, corrupt("branch %d separator %q, child %d maximum %q", id, e.key, e.child, max)
			}
			prev = e.key
		}
		return entries[len(entries)-1].key, nil
	default:
		return nil, corrupt("tree page %d is a %s page", id, typeOf(page))
	}
}

func (st TreeStats) String() string {
	return fmt.Sprintf("%d keys, depth %d, %d branch, %d leaf, %d overflow pages",
		st.Keys, st.Depth, st.BranchPages, st.LeafPages, st.OverflowPages)
}
