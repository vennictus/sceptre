package debug

import (
	"sceptre/internal/freelist"
	"sceptre/internal/kv"
	"sceptre/internal/pager"
)

type MetaInfo struct {
	Path         string
	PageSize     uint32
	RootPage     uint64
	FreeListPage uint64
	PageCount    uint64
	Generation   uint64
	ActiveSlot   int
}

type TreeEntry struct {
	Key   []byte
	Value []byte
}

type TreeInfo struct {
	RootPage  uint64
	PageCount uint64
	Entries   []TreeEntry
}

type FreeListInfo struct {
	HeadPage  uint64
	PageIDs   []uint64
	FreePages []uint64
}

// InspectMeta returns the active durable meta-page state.
func InspectMeta(path string) (MetaInfo, error) {
	p, err := pager.Open(path, pager.Options{})
	if err != nil {
		return MetaInfo{}, err
	}
	defer p.Close()

	meta := p.Meta()
	return MetaInfo{
		Path:         p.Path(),
		PageSize:     meta.PageSize,
		RootPage:     meta.RootPage,
		FreeListPage: meta.FreeListPage,
		PageCount:    meta.PageCount,
		Generation:   meta.Generation,
		ActiveSlot:   p.ActiveMetaSlot(),
	}, nil
}

// InspectTree returns an ordered view of all KV entries reachable from the root.
func InspectTree(path string) (TreeInfo, error) {
	store, err := kv.Open(path, kv.Options{})
	if err != nil {
		return TreeInfo{}, err
	}
	defer store.Close()

	info := TreeInfo{
		RootPage:  store.Pager().Meta().RootPage,
		PageCount: store.Pager().Meta().PageCount,
	}
	iter := store.Iterator()
	if err := iter.SeekFirst(); err != nil {
		return TreeInfo{}, err
	}
	for iter.Valid() {
		key, value, err := iter.Deref()
		if err != nil {
			return TreeInfo{}, err
		}
		info.Entries = append(info.Entries, TreeEntry{
			Key:   cloneBytes(key),
			Value: cloneBytes(value),
		})
		if err := iter.Next(); err != nil {
			return TreeInfo{}, err
		}
	}
	return info, nil
}

// InspectFreeList returns the current persisted freelist inventory.
func InspectFreeList(path string) (FreeListInfo, error) {
	p, err := pager.Open(path, pager.Options{})
	if err != nil {
		return FreeListInfo{}, err
	}
	defer p.Close()

	state, err := freelist.Load(p, p.Meta().FreeListPage)
	if err != nil {
		return FreeListInfo{}, err
	}
	return FreeListInfo{
		HeadPage:  state.HeadPage,
		PageIDs:   append([]uint64(nil), state.PageIDs...),
		FreePages: append([]uint64(nil), state.FreePages...),
	}, nil
}

func cloneBytes(src []byte) []byte {
	return append([]byte(nil), src...)
}
