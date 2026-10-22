package btree

import (
	"bytes"
	"fmt"
)

// ValidateTree loads and validates a complete routing graph before it is queried.
// Each page must have one parent, ordered disjoint keys, and consistent leaf depth.
func ValidateTree(root uint64, read func(uint64) ([]byte, error)) (map[uint64][]byte, error) {
	pages := make(map[uint64][]byte)
	leafDepth := -1
	var walk func(uint64, int) ([]byte, []byte, error)
	walk = func(id uint64, depth int) ([]byte, []byte, error) {
		bad := func(reason string) ([]byte, []byte, error) {
			return nil, nil, fmt.Errorf("%w: page %d: %s", ErrCorruptNode, id, reason)
		}
		if depth > 64 {
			return bad("excessive tree depth")
		}
		if _, exists := pages[id]; exists {
			return bad("cycle or multiple parents")
		}
		page, err := read(id)
		if err != nil {
			return nil, nil, err
		}
		pages[id] = page
		n, err := WrapNode(page)
		if err != nil {
			return nil, nil, err
		}
		if n.Type() != NodeTypeLeaf && n.Type() != NodeTypeInternal {
			return bad("unknown node type")
		}
		if int(n.Lower()) != nodeHeaderSize+n.Count()*nodeSlotSize || n.Upper() < n.Lower() || int(n.Upper()) > len(page) {
			return bad("invalid slot bounds")
		}
		if n.Count() == 0 && (depth != 0 || n.Type() != NodeTypeLeaf) {
			return bad("empty child or internal node")
		}
		used := make([]bool, len(page))
		var minimum, maximum []byte
		for i := 0; i < n.Count(); i++ {
			offset, err := n.cellOffset(i)
			if err != nil {
				return nil, nil, err
			}
			var key, childMin []byte
			var size int
			if n.Type() == NodeTypeLeaf {
				cell, err := n.LeafCell(i)
				if err != nil {
					return nil, nil, err
				}
				key, childMin, size = cell.Key, cell.Key, leafCellSize(cell.Key, cell.Value)
			} else {
				cell, err := n.InternalCell(i)
				if err != nil {
					return nil, nil, err
				}
				lo, hi, err := walk(cell.Child, depth+1)
				if err != nil {
					return nil, nil, err
				}
				if !bytes.Equal(cell.Key, hi) {
					return bad("separator differs from child maximum")
				}
				key, childMin, size = cell.Key, lo, internalCellSize(cell.Key)
			}
			for j := int(offset); j < int(offset)+size; j++ {
				if used[j] {
					return bad("overlapping cells")
				}
				used[j] = true
			}
			if i > 0 && bytes.Compare(maximum, childMin) >= 0 {
				return bad("overlapping or unordered key ranges")
			}
			if i == 0 {
				minimum = childMin
			}
			maximum = key
		}
		if n.Type() == NodeTypeLeaf {
			if leafDepth == -1 {
				leafDepth = depth
			}
			if depth != leafDepth {
				return bad("unequal leaf depths")
			}
		}
		return minimum, maximum, nil
	}
	if root != 0 {
		if _, _, err := walk(root, 0); err != nil {
			return nil, err
		}
	}
	return pages, nil
}

// ValidateSnapshot also rejects pages that cannot be reached from the root.
func ValidateSnapshot(s Snapshot) error {
	pages, err := ValidateTree(s.Root, func(id uint64) ([]byte, error) {
		page, ok := s.Pages[id]
		if !ok {
			return nil, ErrInvalidSnapshotPage
		}
		return page, nil
	})
	if err != nil {
		return err
	}
	if len(pages) != len(s.Pages) {
		return ErrInvalidSnapshotPage
	}
	return nil
}
