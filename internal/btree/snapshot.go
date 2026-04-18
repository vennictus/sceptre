package btree

import "errors"

var ErrInvalidSnapshotPage = errors.New("btree: invalid snapshot page")

// Snapshot captures the in-memory tree state in page-number form.
type Snapshot struct {
	Root     uint64
	NextPage uint64
	Pages    map[uint64][]byte
}

// Snapshot returns a deep copy of the tree's current page state.
func (t *Tree) Snapshot() Snapshot {
	pages := make(map[uint64][]byte, len(t.pages))
	for pageID, page := range t.pages {
		pages[pageID] = cloneBytes(page)
	}

	return Snapshot{
		Root:     t.root,
		NextPage: t.nextPage,
		Pages:    pages,
	}
}

// NewTreeFromSnapshot rebuilds a tree from a persisted page snapshot.
func NewTreeFromSnapshot(pageSize int, snapshot Snapshot) (*Tree, error) {
	tree, err := NewTree(pageSize)
	if err != nil {
		return nil, err
	}

	if snapshot.NextPage > 0 {
		tree.nextPage = snapshot.NextPage
	}
	tree.root = snapshot.Root
	tree.pages = make(map[uint64][]byte, len(snapshot.Pages))

	for pageID, page := range snapshot.Pages {
		if len(page) != pageSize {
			return nil, ErrInvalidSnapshotPage
		}
		if _, err := WrapNode(page); err != nil {
			return nil, err
		}
		tree.pages[pageID] = cloneBytes(page)
	}

	return tree, nil
}
