package debug

import (
	"errors"

	"github.com/vennictus/sceptre/internal/kv"
	"github.com/vennictus/sceptre/internal/pager"
	"github.com/vennictus/sceptre/internal/storage"
	"github.com/vennictus/sceptre/internal/table"
)

type MetaInfo struct {
	Path          string
	FormatVersion uint32
	PageSize      uint32
	RootPage      uint64 // tree directory root (v2) or tree root (v1)
	DataRoot      uint64 // data tree root; 0 for v1 files
	FreeListPage  uint64
	PageCount     uint64
	Generation    uint64
	ActiveSlot    int
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
	Reusable  int // free pages no reader or fallback generation still needs
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
	Index     int
	Child     uint64
	Key       []byte
	Value     []byte
	ValueSize int
	Overflow  uint64 // first page of an out-of-line value
}

type PageDetailInfo struct {
	ID         uint64
	Kind       string
	PageSize   uint32
	TxID       uint64 // transaction that wrote the page
	FreedBy    uint64 // free pages: the transaction that freed it
	Cells      int
	FreeBytes  int
	DataBytes  int // overflow page payload
	Meta       *MetaInfo
	NextPage   uint64
	FreePages  []uint64
	BTreeCells []PageCellInfo
}

// pageKind maps engine page kinds to the names inspection output uses.
func pageKind(r storage.PageReport, freelistHead uint64) string {
	switch r.Kind {
	case storage.KindLeaf:
		switch r.Tree {
		case storage.TreeDirectory:
			return "directory_leaf"
		case storage.TreeFree:
			return "free_tree_leaf"
		}
		return "btree_leaf"
	case storage.KindBranch:
		switch r.Tree {
		case storage.TreeDirectory:
			return "directory_internal"
		case storage.TreeFree:
			return "free_tree_internal"
		}
		return "btree_internal"
	case storage.KindFreelist:
		if r.ID == freelistHead {
			return "freelist_head"
		}
		return "freelist"
	case storage.KindFree:
		return "free_page"
	default:
		return r.Kind
	}
}

// InspectMeta returns the active durable meta-page state of a v1 or v2 file.
func InspectMeta(path string) (MetaInfo, error) {
	store, err := kv.Open(path, kv.Options{})
	if errors.Is(err, kv.ErrUpgradeRequired) {
		p, err := pager.Open(path, pager.Options{})
		if err != nil {
			return MetaInfo{}, err
		}
		defer p.Close()
		return metaInfo(p, 0), nil
	}
	if err != nil {
		return MetaInfo{}, err
	}
	defer store.Close()
	return metaInfo(store.Pager(), store.Env().Info().DataRoot), nil
}

func metaInfo(p *pager.Pager, dataRoot uint64) MetaInfo {
	meta := p.Meta()
	return MetaInfo{
		Path:          p.Path(),
		FormatVersion: meta.Version,
		PageSize:      meta.PageSize,
		RootPage:      meta.RootPage,
		DataRoot:      dataRoot,
		FreeListPage:  meta.FreeListPage,
		PageCount:     meta.PageCount,
		Generation:    meta.Generation,
		ActiveSlot:    p.ActiveMetaSlot(),
	}
}

// InspectTree returns an ordered view of every KV entry.
func InspectTree(path string) (TreeInfo, error) {
	store, err := kv.Open(path, kv.Options{})
	if err != nil {
		return TreeInfo{}, err
	}
	defer store.Close()

	info := TreeInfo{RootPage: store.Env().Info().DataRoot, PageCount: store.Pager().Meta().PageCount}
	iter := store.Iterator()
	defer iter.Close()
	if err := iter.SeekFirst(); err != nil {
		return TreeInfo{}, err
	}
	for iter.Valid() {
		key, value, err := iter.Deref()
		if err != nil {
			return TreeInfo{}, err
		}
		info.Entries = append(info.Entries, TreeEntry{Key: key, Value: value})
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
	defer scanner.Close()

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
	defer scanner.Close()
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

// InspectPages classifies every page of the file.
func InspectPages(path string) (PagesInfo, error) {
	store, err := kv.Open(path, kv.Options{})
	if err != nil {
		return PagesInfo{}, err
	}
	defer store.Close()
	pages, err := store.Env().Pages()
	if err != nil {
		return PagesInfo{}, err
	}
	meta := store.Pager().Meta()
	info := PagesInfo{PageSize: meta.PageSize, PageCount: meta.PageCount, Pages: make([]PageInfo, 0, len(pages))}
	for _, p := range pages {
		info.Pages = append(info.Pages, PageInfo{ID: p.ID, Kind: pageKind(p, meta.FreeListPage), Cells: p.Cells, FreeBytes: p.FreeBytes})
	}
	return info, nil
}

// InspectPage decodes one page.
func InspectPage(path string, pageID uint64) (PageDetailInfo, error) {
	store, err := kv.Open(path, kv.Options{})
	if err != nil {
		return PageDetailInfo{}, err
	}
	defer store.Close()
	d, err := store.Env().InspectPage(pageID)
	if err != nil {
		return PageDetailInfo{}, err
	}
	meta := store.Pager().Meta()
	info := PageDetailInfo{
		ID: pageID, Kind: pageKind(d.PageReport, meta.FreeListPage), PageSize: meta.PageSize,
		TxID: d.TxID, FreedBy: d.FreedBy, Cells: d.PageReport.Cells, FreeBytes: d.FreeBytes,
		DataBytes: d.Data, NextPage: d.Next,
	}
	switch d.Kind {
	case storage.KindMeta, storage.KindMetaActive:
		m := metaInfo(store.Pager(), store.Env().Info().DataRoot)
		info.Meta = &m
	case storage.KindFreelist:
		for _, e := range d.Entries {
			info.FreePages = append(info.FreePages, e.Page)
		}
	}
	for _, c := range d.Cells {
		info.BTreeCells = append(info.BTreeCells, PageCellInfo{Index: c.Index, Child: c.Child, Key: c.Key, Value: c.Value, ValueSize: c.ValueSize, Overflow: c.Overflow})
	}
	return info, nil
}

// InspectFreeList returns the committed freelist inventory.
func InspectFreeList(path string) (FreeListInfo, error) {
	store, err := kv.Open(path, kv.Options{})
	if err != nil {
		return FreeListInfo{}, err
	}
	defer store.Close()
	rep, err := store.Env().Freelist()
	if err != nil {
		return FreeListInfo{}, err
	}
	info := FreeListInfo{HeadPage: rep.Head, PageIDs: rep.Pages, Reusable: rep.Reusable}
	for _, e := range rep.Entries {
		info.FreePages = append(info.FreePages, e.Page)
	}
	return info, nil
}

func projectRecord(columns []string, record table.Record) table.Record {
	values := make(map[string]table.Value, len(columns))
	for _, column := range columns {
		values[column] = record.Values[column]
	}
	return table.NewRecord(values)
}
