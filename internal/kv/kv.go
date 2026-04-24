package kv

import (
	"fmt"
	"sceptre/internal/btree"
	"sceptre/internal/freelist"
	"sceptre/internal/pager"
	"sort"
)

// Options controls how the KV layer initializes its backing pager.
type Options struct {
	PageSize uint32
}

// KV ties the B+ tree state to the durable pager file.
type KV struct {
	pager      *pager.Pager
	tree       *btree.Tree
	free       freelist.State
	commitHook commitHook
}

// Mutation describes one KV update in an atomic Apply call.
type Mutation struct {
	Key    []byte
	Value  []byte
	Delete bool
}

// Put returns a mutation that stores key/value.
func Put(key, value []byte) Mutation {
	return Mutation{
		Key:   cloneBytes(key),
		Value: cloneBytes(value),
	}
}

// Delete returns a mutation that removes key.
func Delete(key []byte) Mutation {
	return Mutation{
		Key:    cloneBytes(key),
		Delete: true,
	}
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
	free, err := loadFreeList(p)
	if err != nil {
		p.Close()
		return nil, err
	}

	return &KV{
		pager: p,
		tree:  tree,
		free:  free,
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
	return kv.Apply([]Mutation{Put(key, value)})
}

// Apply atomically applies all mutations in one durable commit.
func (kv *KV) Apply(mutations []Mutation) error {
	if len(mutations) == 0 {
		return nil
	}

	previous := kv.tree.Snapshot()
	previousFree := kv.free.Clone()

	for _, mutation := range mutations {
		if mutation.Delete {
			if _, err := kv.tree.Delete(mutation.Key); err != nil {
				return kv.rollback(previous, previousFree, err)
			}
			continue
		}
		if _, err := kv.tree.Delete(mutation.Key); err != nil {
			return kv.rollback(previous, previousFree, err)
		}
		if err := kv.tree.Insert(mutation.Key, mutation.Value); err != nil {
			return kv.rollback(previous, previousFree, err)
		}
	}
	return kv.persistCommitted(previous, previousFree)
}

// Del removes a key if it exists and persists the updated tree.
func (kv *KV) Del(key []byte) (bool, error) {
	if _, ok, err := kv.Get(key); err != nil {
		return false, err
	} else if !ok {
		return false, nil
	}
	if err := kv.Apply([]Mutation{Delete(key)}); err != nil {
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

func loadFreeList(p *pager.Pager) (freelist.State, error) {
	return freelist.Load(p, p.Meta().FreeListPage)
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
		for i := 0; i < node.Count(); i++ {
			if _, err := node.LeafCell(i); err != nil {
				return err
			}
		}
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

func (kv *KV) persist(previous btree.Snapshot, previousFree freelist.State) error {
	alloc := freelist.NewAllocator(previousFree.FreePages, kv.pager.Meta().PageCount)

	snapshot := kv.tree.Snapshot()
	durable, err := snapshot.RemapPageIDsWithAllocator(alloc)
	if err != nil {
		return err
	}

	retiredPages := append(sortedPageIDs(previous.Pages), previousFree.PageIDs...)
	nextFree, err := freelist.Build(int(kv.pager.PageSize()), alloc, retiredPages)
	if err != nil {
		return err
	}

	for _, pageID := range sortedPageIDs(durable.Pages) {
		page := durable.Pages[pageID]
		if err := kv.pager.WritePage(pageID, page); err != nil {
			return err
		}
	}
	for _, pageID := range nextFree.PageIDs {
		page := nextFree.Pages[pageID]
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
	meta.FreeListPage = nextFree.HeadPage
	meta.PageCount = alloc.NextPageID()
	if err := kv.pager.PublishMeta(meta); err != nil {
		return err
	}
	if err := kv.installCommittedState(durable, nextFree); err != nil {
		return err
	}
	return kv.runCommitHook(commitStageMetaPublished)
}

func (kv *KV) persistCommitted(previous btree.Snapshot, previousFree freelist.State) error {
	if err := kv.persist(previous, previousFree); err != nil {
		if stage, ok := interruptedCommitStage(err); ok && stage == commitStageMetaPublished {
			return err
		}
		return kv.rollback(previous, previousFree, err)
	}
	return nil
}

func (kv *KV) rollback(previous btree.Snapshot, previousFree freelist.State, cause error) error {
	restored, err := btree.NewTreeFromSnapshot(int(kv.pager.PageSize()), previous)
	if err != nil {
		return fmt.Errorf("%w: rollback failed: %v", cause, err)
	}
	kv.tree = restored
	kv.free = previousFree.Clone()
	return cause
}

func (kv *KV) runCommitHook(stage commitStage) error {
	if kv == nil || kv.commitHook == nil {
		return nil
	}
	return kv.commitHook(stage)
}

func (kv *KV) installCommittedState(snapshot btree.Snapshot, free freelist.State) error {
	tree, err := btree.NewTreeFromSnapshot(int(kv.pager.PageSize()), snapshot)
	if err != nil {
		return err
	}
	kv.tree = tree
	kv.free = free.Clone()
	return nil
}

func sortedPageIDs(pages map[uint64][]byte) []uint64 {
	pageIDs := make([]uint64, 0, len(pages))
	for pageID := range pages {
		pageIDs = append(pageIDs, pageID)
	}
	sort.Slice(pageIDs, func(i, j int) bool {
		return pageIDs[i] < pageIDs[j]
	})
	return pageIDs
}

func cloneBytes(src []byte) []byte {
	return append([]byte(nil), src...)
}
