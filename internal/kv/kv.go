package kv

import (
	"sceptre/internal/btree"
	"sceptre/internal/pager"
)

// Options controls how the KV layer initializes its backing pager.
type Options struct {
	PageSize uint32
}

// KV ties the B+ tree state to the durable pager file.
type KV struct {
	pager *pager.Pager
	tree  *btree.Tree
}

// Open opens the durable pager and reconstructs the in-memory B+ tree state.
func Open(path string, opts Options) (*KV, error) {
	p, err := pager.Open(path, pager.Options{PageSize: opts.PageSize})
	if err != nil {
		return nil, err
	}

	tree, err := loadTree(p)
	if err != nil {
		p.Close()
		return nil, err
	}

	return &KV{
		pager: p,
		tree:  tree,
	}, nil
}

// Close closes the underlying pager file.
func (kv *KV) Close() error {
	if kv == nil || kv.pager == nil {
		return nil
	}
	return kv.pager.Close()
}

// Pager exposes the underlying durable pager.
func (kv *KV) Pager() *pager.Pager {
	return kv.pager
}

// Tree exposes the in-memory B+ tree wired to the pager state.
func (kv *KV) Tree() *btree.Tree {
	return kv.tree
}

func loadTree(p *pager.Pager) (*btree.Tree, error) {
	meta := p.Meta()
	pages := make(map[uint64][]byte)
	for pageID := uint64(pager.MetaPageCount); pageID < meta.PageCount; pageID++ {
		page, err := p.ReadPage(pageID)
		if err != nil {
			return nil, err
		}
		pages[pageID] = page
	}

	return btree.NewTreeFromSnapshot(int(meta.PageSize), btree.Snapshot{
		Root:     meta.RootPage,
		NextPage: meta.PageCount,
		Pages:    pages,
	})
}
