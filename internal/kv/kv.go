package kv

import (
	"errors"
	"fmt"
	"sort"

	"github.com/vennictus/sceptre/internal/btree"
	"github.com/vennictus/sceptre/internal/freelist"
	"github.com/vennictus/sceptre/internal/pager"
)

var (
	ErrCommitCommitted  = errors.New("kv: commit completed despite reported error")
	ErrRecoveryRequired = errors.New("kv: uncertain commit; close and reopen required")
)

// Options controls how the KV layer initializes its backing pager.
type Options struct {
	PageSize             uint32
	FailAfterCommitStage string
	IO                   *pager.IOHooks
}

// KV ties the B+ tree state to the durable pager file.
type KV struct {
	pager      *pager.Pager
	tree       *btree.Tree
	free       freelist.State
	commitHook commitHook
	failure    error
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
	p, err := pager.Open(path, pager.Options{PageSize: opts.PageSize, IO: opts.IO})
	if err != nil {
		return nil, err
	}
	if v := p.Meta().Version; v != pager.FormatV1 {
		p.Close()
		return nil, fmt.Errorf("%w: kv opens format %d files, found %d", pager.ErrUnsupportedVersion, pager.FormatV1, v)
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
	if err := validateFreeList(p.Meta(), tree.Snapshot().Pages, free); err != nil {
		p.Close()
		return nil, err
	}

	store := &KV{
		pager: p,
		tree:  tree,
		free:  free,
	}
	if opts.FailAfterCommitStage != "" {
		hook, err := commitHookForStage(opts.FailAfterCommitStage)
		if err != nil {
			p.Close()
			return nil, err
		}
		store.commitHook = hook
	}
	return store, nil
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
	if kv.failure != nil {
		return nil, false, kv.failure
	}
	return kv.tree.Get(key)
}

// Err reports whether an uncertain publication has made this handle unusable.
func (kv *KV) Err() error { return kv.failure }

// Set inserts or replaces a key/value pair and persists the updated tree.
func (kv *KV) Set(key, value []byte) error {
	return kv.Apply([]Mutation{Put(key, value)})
}

// Apply atomically applies all mutations in one durable commit.
func (kv *KV) Apply(mutations []Mutation) error {
	if kv.failure != nil {
		return kv.failure
	}
	if len(mutations) == 0 {
		return nil
	}

	previous := kv.tree.Snapshot()
	previousFree := kv.free.Clone()
	// Mutate a private working tree so existing iterators retain their snapshot.
	working, err := btree.NewTreeFromSnapshot(int(kv.pager.PageSize()), previous)
	if err != nil {
		return err
	}
	kv.tree = working

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
	pages, err := btree.ValidateTree(meta.RootPage, p.ReadPage)
	if err != nil {
		return nil, err
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

func (kv *KV) persist(previous btree.Snapshot, previousFree freelist.State) error {
	if err := validateFreeList(kv.pager.Meta(), previous.Pages, previousFree); err != nil {
		return err
	}
	protected := make(map[uint64]struct{})
	if older, ok := kv.pager.PreviousMeta(); ok {
		pages, err := btree.ValidateTree(older.RootPage, kv.pager.ReadPage)
		if err != nil {
			return fmt.Errorf("kv: invalid fallback tree: %w", err)
		}
		for id := range pages {
			protected[id] = struct{}{}
		}
		state, err := freelist.Load(kv.pager, older.FreeListPage)
		if err != nil {
			return fmt.Errorf("kv: invalid fallback freelist: %w", err)
		}
		for _, id := range state.PageIDs {
			protected[id] = struct{}{}
		}
	}
	var reusable, withheld []uint64
	for _, id := range previousFree.FreePages {
		if _, ok := protected[id]; ok {
			withheld = append(withheld, id)
		} else {
			reusable = append(reusable, id)
		}
	}
	alloc := freelist.NewAllocator(reusable, kv.pager.Meta().PageCount)

	snapshot := kv.tree.Snapshot()
	durable, err := snapshot.RemapPageIDsWithAllocator(alloc)
	if err != nil {
		return err
	}

	retiredPages := append(sortedPageIDs(previous.Pages), previousFree.PageIDs...)
	retiredPages = append(retiredPages, withheld...)
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
		return fmt.Errorf("%w: install committed state: %w", pager.ErrCommitUncertain, err)
	}
	if err := kv.runCommitHook(commitStageMetaPublished); err != nil {
		return fmt.Errorf("%w: %w", ErrCommitCommitted, err)
	}
	return nil
}

func (kv *KV) persistCommitted(previous btree.Snapshot, previousFree freelist.State) error {
	if err := kv.persist(previous, previousFree); err != nil {
		if errors.Is(err, ErrCommitCommitted) {
			return err
		}
		if errors.Is(err, pager.ErrCommitUncertain) {
			kv.failure = fmt.Errorf("%w: %w", ErrRecoveryRequired, err)
			return kv.failure
		}
		return kv.rollback(previous, previousFree, err)
	}
	return nil
}

func validateFreeList(meta pager.Meta, tree map[uint64][]byte, free freelist.State) error {
	seen := make(map[uint64]struct{})
	for _, ids := range [][]uint64{free.PageIDs, free.FreePages} {
		for _, id := range ids {
			if id < pager.MetaPageCount || id >= meta.PageCount {
				return fmt.Errorf("kv: freelist page %d out of bounds", id)
			}
			if _, ok := tree[id]; ok {
				return fmt.Errorf("kv: freelist page %d overlaps active tree", id)
			}
			if _, ok := seen[id]; ok {
				return fmt.Errorf("kv: duplicate freelist page %d", id)
			}
			seen[id] = struct{}{}
		}
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
