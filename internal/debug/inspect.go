package debug

import (
	"fmt"
	"sceptre/internal/btree"
	"sceptre/internal/freelist"
	"sceptre/internal/kv"
	"sceptre/internal/pager"
	"sceptre/internal/table"
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

type TableInfo struct {
	Name       string
	Columns    []table.Column
	PrimaryKey []string
	Indexes    []table.IndexDef
	Rows       []table.Record
}

type IndexEntry struct {
	Values     table.Record
	PrimaryKey table.Record
}

type IndexInfo struct {
	Name    string
	Table   string
	Columns []string
	Entries []IndexEntry
}

type PageInfo struct {
	ID        uint64
	Kind      string
	Cells     int
	FreeBytes int
}

type PagesInfo struct {
	PageSize  uint32
	PageCount uint64
	Pages     []PageInfo
}

type PageCellInfo struct {
	Index int
	Child uint64
	Key   []byte
	Value []byte
}

type PageDetailInfo struct {
	ID         uint64
	Kind       string
	PageSize   uint32
	Cells      int
	FreeBytes  int
	Lower      uint16
	Upper      uint16
	Meta       *MetaInfo
	NextPage   uint64
	FreePages  []uint64
	BTreeCells []PageCellInfo
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

func InspectTable(path, tableName string) (TableInfo, error) {
	db, err := table.Open(path, table.Options{})
	if err != nil {
		return TableInfo{}, err
	}
	defer db.Close()

	def, ok, err := db.Table(tableName)
	if err != nil {
		return TableInfo{}, err
	}
	if !ok {
		return TableInfo{}, table.ErrTableNotFound
	}
	scanner, err := db.Scan(tableName, table.ScanBounds{})
	if err != nil {
		return TableInfo{}, err
	}

	info := TableInfo{
		Name:       def.Name,
		Columns:    append([]table.Column(nil), def.Columns...),
		PrimaryKey: append([]string(nil), def.PrimaryKey...),
		Indexes:    append([]table.IndexDef(nil), def.Indexes...),
	}
	for scanner.Valid() {
		row, err := scanner.Deref()
		if err != nil {
			return TableInfo{}, err
		}
		info.Rows = append(info.Rows, row)
		if err := scanner.Next(); err != nil {
			return TableInfo{}, err
		}
	}
	return info, nil
}

func InspectIndex(path, indexName string) (IndexInfo, error) {
	db, err := table.Open(path, table.Options{})
	if err != nil {
		return IndexInfo{}, err
	}
	defer db.Close()

	tables, err := db.Tables()
	if err != nil {
		return IndexInfo{}, err
	}
	for _, def := range tables {
		for _, index := range def.Indexes {
			if index.Name != indexName {
				continue
			}
			return inspectIndexForTable(db, def, index)
		}
	}
	return IndexInfo{}, table.ErrIndexNotFound
}

func inspectIndexForTable(db *table.DB, def table.TableDef, index table.IndexDef) (IndexInfo, error) {
	scanner, err := db.Scan(def.Name, table.ScanBounds{})
	if err != nil {
		return IndexInfo{}, err
	}
	info := IndexInfo{
		Name:    index.Name,
		Table:   def.Name,
		Columns: append([]string(nil), index.Columns...),
	}
	for scanner.Valid() {
		row, err := scanner.Deref()
		if err != nil {
			return IndexInfo{}, err
		}
		info.Entries = append(info.Entries, IndexEntry{
			Values:     projectRecord(index.Columns, row),
			PrimaryKey: projectRecord(def.PrimaryKey, row),
		})
		if err := scanner.Next(); err != nil {
			return IndexInfo{}, err
		}
	}
	return info, nil
}

func InspectPages(path string) (PagesInfo, error) {
	p, err := pager.Open(path, pager.Options{})
	if err != nil {
		return PagesInfo{}, err
	}
	defer p.Close()

	free, err := freelist.Load(p, p.Meta().FreeListPage)
	if err != nil {
		return PagesInfo{}, err
	}
	freelistPages := make(map[uint64]struct{}, len(free.PageIDs))
	for _, pageID := range free.PageIDs {
		freelistPages[pageID] = struct{}{}
	}
	freePages := make(map[uint64]struct{}, len(free.FreePages))
	for _, pageID := range free.FreePages {
		freePages[pageID] = struct{}{}
	}

	meta := p.Meta()
	info := PagesInfo{
		PageSize:  meta.PageSize,
		PageCount: meta.PageCount,
		Pages:     make([]PageInfo, 0, meta.PageCount),
	}
	for pageID := uint64(0); pageID < meta.PageCount; pageID++ {
		switch {
		case pageID < pager.MetaPageCount:
			kind := "meta"
			if int(pageID) == p.ActiveMetaSlot() {
				kind = "meta_active"
			}
			info.Pages = append(info.Pages, PageInfo{ID: pageID, Kind: kind})
		case pageID == meta.FreeListPage:
			info.Pages = append(info.Pages, PageInfo{ID: pageID, Kind: "freelist_head"})
		case hasPage(freelistPages, pageID):
			info.Pages = append(info.Pages, PageInfo{ID: pageID, Kind: "freelist"})
		case hasPage(freePages, pageID):
			info.Pages = append(info.Pages, PageInfo{ID: pageID, Kind: "free_page"})
		default:
			page, err := p.ReadPage(pageID)
			if err != nil {
				return PagesInfo{}, err
			}
			pageInfo := inspectBTreePage(pageID, page)
			info.Pages = append(info.Pages, pageInfo)
		}
	}
	return info, nil
}

func InspectPage(path string, pageID uint64) (PageDetailInfo, error) {
	p, err := pager.Open(path, pager.Options{})
	if err != nil {
		return PageDetailInfo{}, err
	}
	defer p.Close()

	meta := p.Meta()
	if pageID >= meta.PageCount {
		return PageDetailInfo{}, fmt.Errorf("page %d out of range", pageID)
	}

	pageKind, err := classifyPage(p, pageID)
	if err != nil {
		return PageDetailInfo{}, err
	}
	info := PageDetailInfo{
		ID:       pageID,
		Kind:     pageKind.Kind,
		PageSize: meta.PageSize,
	}

	page, err := p.ReadPage(pageID)
	if err != nil {
		return PageDetailInfo{}, err
	}

	switch pageKind.Kind {
	case "meta", "meta_active":
		decoded, err := pager.DecodeMeta(page)
		if err != nil {
			return PageDetailInfo{}, err
		}
		info.Meta = &MetaInfo{
			Path:         p.Path(),
			PageSize:     decoded.PageSize,
			RootPage:     decoded.RootPage,
			FreeListPage: decoded.FreeListPage,
			PageCount:    decoded.PageCount,
			Generation:   decoded.Generation,
			ActiveSlot:   p.ActiveMetaSlot(),
		}
	case "freelist_head", "freelist":
		nextPage, freePages, err := freelist.DecodePage(page)
		if err != nil {
			return PageDetailInfo{}, err
		}
		info.NextPage = nextPage
		info.FreePages = append([]uint64(nil), freePages...)
	case "btree_leaf", "btree_internal":
		node, err := btree.WrapNode(page)
		if err != nil {
			return PageDetailInfo{}, err
		}
		info.Cells = node.Count()
		info.FreeBytes = node.FreeSpace()
		info.Lower = node.Lower()
		info.Upper = node.Upper()
		for i := 0; i < node.Count(); i++ {
			if node.Type() == btree.NodeTypeLeaf {
				cell, err := node.LeafCell(i)
				if err != nil {
					return PageDetailInfo{}, err
				}
				info.BTreeCells = append(info.BTreeCells, PageCellInfo{
					Index: i,
					Key:   cloneBytes(cell.Key),
					Value: cloneBytes(cell.Value),
				})
				continue
			}
			cell, err := node.InternalCell(i)
			if err != nil {
				return PageDetailInfo{}, err
			}
			info.BTreeCells = append(info.BTreeCells, PageCellInfo{
				Index: i,
				Child: cell.Child,
				Key:   cloneBytes(cell.Key),
			})
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

func inspectBTreePage(pageID uint64, page []byte) PageInfo {
	node, err := btree.WrapNode(page)
	if err != nil {
		return PageInfo{ID: pageID, Kind: "unknown"}
	}
	switch node.Type() {
	case btree.NodeTypeLeaf:
		return PageInfo{ID: pageID, Kind: "btree_leaf", Cells: node.Count(), FreeBytes: node.FreeSpace()}
	case btree.NodeTypeInternal:
		return PageInfo{ID: pageID, Kind: "btree_internal", Cells: node.Count(), FreeBytes: node.FreeSpace()}
	default:
		return PageInfo{ID: pageID, Kind: fmt.Sprintf("unknown_type_%d", node.Type())}
	}
}

func classifyPage(p *pager.Pager, pageID uint64) (PageInfo, error) {
	meta := p.Meta()
	free, err := freelist.Load(p, meta.FreeListPage)
	if err != nil {
		return PageInfo{}, err
	}
	freelistPages := make(map[uint64]struct{}, len(free.PageIDs))
	for _, pageID := range free.PageIDs {
		freelistPages[pageID] = struct{}{}
	}
	freePages := make(map[uint64]struct{}, len(free.FreePages))
	for _, pageID := range free.FreePages {
		freePages[pageID] = struct{}{}
	}

	switch {
	case pageID < pager.MetaPageCount:
		kind := "meta"
		if int(pageID) == p.ActiveMetaSlot() {
			kind = "meta_active"
		}
		return PageInfo{ID: pageID, Kind: kind}, nil
	case pageID == meta.FreeListPage:
		return PageInfo{ID: pageID, Kind: "freelist_head"}, nil
	case hasPage(freelistPages, pageID):
		return PageInfo{ID: pageID, Kind: "freelist"}, nil
	case hasPage(freePages, pageID):
		return PageInfo{ID: pageID, Kind: "free_page"}, nil
	default:
		page, err := p.ReadPage(pageID)
		if err != nil {
			return PageInfo{}, err
		}
		return inspectBTreePage(pageID, page), nil
	}
}

func projectRecord(columns []string, record table.Record) table.Record {
	values := make(map[string]table.Value, len(columns))
	for _, column := range columns {
		values[column] = record.Values[column]
	}
	return table.NewRecord(values)
}

func hasPage(pages map[uint64]struct{}, pageID uint64) bool {
	_, ok := pages[pageID]
	return ok
}

func cloneBytes(src []byte) []byte {
	return append([]byte(nil), src...)
}
