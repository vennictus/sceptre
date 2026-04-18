package btree

import (
	"errors"
	"fmt"
	"sort"
)

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

// RemapPageIDs rewrites the snapshot into a fresh append-only page range.
func (s Snapshot) RemapPageIDs(startPageID uint64) (Snapshot, error) {
	if s.Root == 0 {
		if len(s.Pages) != 0 {
			return Snapshot{}, ErrInvalidSnapshotPage
		}
		return Snapshot{
			Root:     0,
			NextPage: startPageID,
			Pages:    map[uint64][]byte{},
		}, nil
	}

	oldIDs := make([]uint64, 0, len(s.Pages))
	for pageID := range s.Pages {
		oldIDs = append(oldIDs, pageID)
	}
	sort.Slice(oldIDs, func(i, j int) bool {
		return oldIDs[i] < oldIDs[j]
	})

	mapping := make(map[uint64]uint64, len(oldIDs))
	nextPageID := startPageID
	for _, oldID := range oldIDs {
		mapping[oldID] = nextPageID
		nextPageID++
	}

	rootPageID, ok := mapping[s.Root]
	if !ok {
		return Snapshot{}, fmt.Errorf("%w: missing root page", ErrInvalidSnapshotPage)
	}

	pages := make(map[uint64][]byte, len(s.Pages))
	for _, oldID := range oldIDs {
		oldPage := s.Pages[oldID]
		node, err := WrapNode(oldPage)
		if err != nil {
			return Snapshot{}, err
		}

		newPageID := mapping[oldID]
		switch node.Type() {
		case NodeTypeLeaf:
			pages[newPageID] = cloneBytes(oldPage)
		case NodeTypeInternal:
			entries, err := node.internalEntries()
			if err != nil {
				return Snapshot{}, err
			}
			for i := range entries {
				childPageID, ok := mapping[entries[i].Child]
				if !ok {
					return Snapshot{}, fmt.Errorf("%w: missing child page", ErrInvalidSnapshotPage)
				}
				entries[i].Child = childPageID
			}

			page := make([]byte, len(oldPage))
			if _, err := buildInternalNode(page, entries); err != nil {
				return Snapshot{}, err
			}
			pages[newPageID] = page
		default:
			return Snapshot{}, ErrUnknownNodeType
		}
	}

	return Snapshot{
		Root:     rootPageID,
		NextPage: nextPageID,
		Pages:    pages,
	}, nil
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
