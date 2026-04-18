package kv

import (
	"fmt"
	"sceptre/internal/btree"
	"sceptre/internal/pager"
)

// Options controls how the KV layer initializes its backing pager.
type Options struct {
	PageSize uint32
}

// KV ties the B+ tree state to the durable pager file.
type KV struct {
	pager      *pager.Pager
	tree       *btree.Tree
	commitHook commitHook
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

// Get looks up a key from the current tree snapshot.
func (kv *KV) Get(key []byte) ([]byte, bool, error) {
	return kv.tree.Get(key)
}

// Set inserts or replaces a key/value pair and persists the updated tree.
func (kv *KV) Set(key, value []byte) error {
	previous := kv.tree.Snapshot()

	if _, err := kv.tree.Delete(key); err != nil {
		return kv.rollback(previous, err)
	}
	if err := kv.tree.Insert(key, value); err != nil {
		return kv.rollback(previous, err)
	}
	return kv.persistCommitted(previous)
}

// Del removes a key if it exists and persists the updated tree.
func (kv *KV) Del(key []byte) (bool, error) {
	previous := kv.tree.Snapshot()

	removed, err := kv.tree.Delete(key)
	if err != nil {
		return false, kv.rollback(previous, err)
	}
	if !removed {
		return false, nil
	}
	if err := kv.persistCommitted(previous); err != nil {
		return false, err
	}
	return true, nil
}

func loadTree(p *pager.Pager) (*btree.Tree, error) {
	meta := p.Meta()
	pages := make(map[uint64][]byte)
	if meta.RootPage != 0 {
		if err := loadReachablePages(p, meta.RootPage, pages); err != nil {
			return nil, err
		}
	}

	return btree.NewTreeFromSnapshot(int(meta.PageSize), btree.Snapshot{
		Root:     meta.RootPage,
		NextPage: meta.PageCount,
		Pages:    pages,
	})
}

func loadReachablePages(p *pager.Pager, pageID uint64, pages map[uint64][]byte) error {
	if _, ok := pages[pageID]; ok {
		return nil
	}

	page, err := p.ReadPage(pageID)
	if err != nil {
		return err
	}
	pages[pageID] = page

	node, err := btree.WrapNode(page)
	if err != nil {
		return err
	}
	switch node.Type() {
	case btree.NodeTypeLeaf:
		return nil
	case btree.NodeTypeInternal:
		for i := 0; i < node.Count(); i++ {
			cell, err := node.InternalCell(i)
			if err != nil {
				return err
			}
			if err := loadReachablePages(p, cell.Child, pages); err != nil {
				return err
			}
		}
		return nil
	default:
		return btree.ErrUnknownNodeType
	}
}

func (kv *KV) persist() error {
	snapshot := kv.tree.Snapshot()
	durable, err := snapshot.RemapPageIDs(kv.pager.Meta().PageCount)
	if err != nil {
		return err
	}

	for pageID, page := range durable.Pages {
		if err := kv.pager.WritePage(pageID, page); err != nil {
			return err
		}
	}
	if err := kv.runCommitHook(commitStagePagesWritten); err != nil {
		return err
	}
	if err := kv.pager.Sync(); err != nil {
		return err
	}
	if err := kv.runCommitHook(commitStagePagesSynced); err != nil {
		return err
	}

	meta := kv.pager.Meta()
	meta.RootPage = durable.Root
	meta.PageCount = durable.NextPage
	if err := kv.pager.PublishMeta(meta); err != nil {
		return err
	}
	return kv.runCommitHook(commitStageMetaPublished)
}

func (kv *KV) persistCommitted(previous btree.Snapshot) error {
	if err := kv.persist(); err != nil {
		if stage, ok := interruptedCommitStage(err); ok && stage == commitStageMetaPublished {
			return err
		}
		return kv.rollback(previous, err)
	}
	return nil
}

func (kv *KV) rollback(previous btree.Snapshot, cause error) error {
	restored, err := btree.NewTreeFromSnapshot(int(kv.pager.PageSize()), previous)
	if err != nil {
		return fmt.Errorf("%w: rollback failed: %v", cause, err)
	}
	kv.tree = restored
	return cause
}

func (kv *KV) runCommitHook(stage commitStage) error {
	if kv == nil || kv.commitHook == nil {
		return nil
	}
	return kv.commitHook(stage)
}
