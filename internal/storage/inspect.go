package storage

import (
	"encoding/binary"
	"fmt"
	"sort"

	"github.com/vennictus/sceptre/internal/pager"
)

// Page kinds reported by inspection.
const (
	KindMeta       = "meta"
	KindMetaActive = "meta_active"
	KindBranch     = "branch"
	KindLeaf       = "leaf"
	KindOverflow   = "overflow"
	KindFreelist   = "freelist"
	KindFree       = "free"
)

// Tree names reported for tree and overflow pages.
const (
	TreeData      = "data"
	TreeDirectory = "directory"
	TreeFree      = "free"
)

// PageReport describes one physical page of the latest committed state.
type PageReport struct {
	ID        uint64
	Kind      string
	Tree      string // TreeData or TreeDirectory for tree and overflow pages
	TxID      uint64 // transaction that wrote the page; 0 for meta and free pages
	FreedBy   uint64 // free pages: the transaction that freed it (0: never used)
	Cells     int    // entries in a branch or leaf
	FreeBytes int    // unused bytes in a branch, leaf, overflow, or freelist page
}

// CellReport is one decoded branch or leaf entry.
type CellReport struct {
	Index     int
	Key       []byte
	Child     uint64 // branch entries
	Value     []byte // inline leaf values
	ValueSize int
	Overflow  uint64 // first page of an out-of-line value
}

// FreeEntry is one freelist record.
type FreeEntry struct {
	Page, FreedBy uint64
}

// PageDetail is a fully decoded page.
type PageDetail struct {
	PageReport
	Cells   []CellReport
	Next    uint64      // next page of an overflow or freelist chain
	Entries []FreeEntry // freelist page records
	Data    int         // overflow page payload bytes
}

// FreelistReport describes the committed freelist.
type FreelistReport struct {
	Head     uint64
	Pages    []uint64
	Entries  []FreeEntry // sorted by page
	Reusable int         // entries no reader or fallback state still needs
}

// inspection is a consistent view of page ownership, built under the writer slot.
type inspection struct {
	s         snapshot
	src       snapshotSource
	activeSlt int
	owner     map[uint64]PageReport
	chain     []uint64
	free      map[uint64]uint64
}

func (e *Env) inspect() (inspection, error) {
	e.writeMu.Lock()
	defer e.writeMu.Unlock()
	e.mu.Lock()
	if err := e.usable(); err != nil {
		e.mu.Unlock()
		return inspection{}, err
	}
	in := inspection{s: e.state, chain: e.chain, free: e.free.pages, activeSlt: e.p.ActiveMetaSlot()}
	e.mu.Unlock()
	in.src = e.source(in.s)
	in.owner = make(map[uint64]PageReport, in.s.pageCount)
	for _, tree := range []struct {
		name string
		root uint64
	}{{TreeDirectory, in.s.dirRoot}, {TreeData, in.s.dataRoot}, {TreeFree, in.s.freeRoot}} {
		st, err := verifyTree(in.src, e.l, tree.root)
		if err != nil {
			return inspection{}, fmt.Errorf("%s tree: %w", tree.name, err)
		}
		for id, t := range st.Pages {
			kind := KindLeaf
			switch t {
			case pageBranch:
				kind = KindBranch
			case pageOverflow:
				kind = KindOverflow
			}
			in.owner[id] = PageReport{ID: id, Kind: kind, Tree: tree.name}
		}
	}
	for _, id := range in.chain {
		in.owner[id] = PageReport{ID: id, Kind: KindFreelist}
	}
	for id, freedBy := range in.free {
		in.owner[id] = PageReport{ID: id, Kind: KindFree, FreedBy: freedBy}
	}
	return in, nil
}

func (e *Env) describe(in inspection, id uint64) (PageDetail, error) {
	if id < pager.MetaPageCount {
		kind := KindMeta
		if int(id) == in.activeSlt {
			kind = KindMetaActive
		}
		return PageDetail{PageReport: PageReport{ID: id, Kind: kind}}, nil
	}
	rep, ok := in.owner[id]
	if !ok {
		return PageDetail{}, fmt.Errorf("page %d belongs to nothing in a file of %d pages", id, in.s.pageCount)
	}
	d := PageDetail{PageReport: rep}
	if rep.Kind == KindFree {
		return d, nil // free pages may hold anything
	}
	page, err := in.src.page(id)
	if err != nil {
		return PageDetail{}, err
	}
	d.TxID = txIDOf(page)
	switch rep.Kind {
	case KindLeaf:
		entries, err := decodeLeaf(page)
		if err != nil {
			return PageDetail{}, err
		}
		d.FreeBytes = e.l.usable - leafBytes(entries)
		for i, en := range entries {
			d.Cells = append(d.Cells, CellReport{Index: i, Key: cloneBytes(en.key), Value: cloneBytes(en.value), ValueSize: int(en.size), Overflow: en.overflow})
		}
	case KindBranch:
		entries, err := decodeBranch(page)
		if err != nil {
			return PageDetail{}, err
		}
		d.FreeBytes = e.l.usable - branchBytes(entries)
		for i, en := range entries {
			d.Cells = append(d.Cells, CellReport{Index: i, Key: cloneBytes(en.key), Child: en.child})
		}
	case KindOverflow:
		d.Next = binary.LittleEndian.Uint64(page[overflowNextOff:])
		d.Data = int(binary.LittleEndian.Uint32(page[overflowLenOff:]))
		d.FreeBytes = overflowCapacity(e.l.pageSize) - d.Data
	case KindFreelist:
		d.Next = binary.LittleEndian.Uint64(page[freeNextOff:])
		count := int(binary.LittleEndian.Uint32(page[freeCountOff:]))
		for i := 0; i < count; i++ {
			at := freeEntriesOff + i*freeEntrySize
			d.Entries = append(d.Entries, FreeEntry{Page: binary.LittleEndian.Uint64(page[at:]), FreedBy: binary.LittleEndian.Uint64(page[at+8:])})
		}
		d.FreeBytes = (freelistCapacity(e.l.pageSize) - count) * freeEntrySize
	}
	d.PageReport.Cells = len(d.Cells)
	return d, nil
}

// Pages describes every page of the latest committed state, in page order.
func (e *Env) Pages() ([]PageReport, error) {
	in, err := e.inspect()
	if err != nil {
		return nil, err
	}
	out := make([]PageReport, 0, in.s.pageCount)
	for id := uint64(0); id < in.s.pageCount; id++ {
		d, err := e.describe(in, id)
		if err != nil {
			return nil, err
		}
		out = append(out, d.PageReport)
	}
	return out, nil
}

// InspectPage decodes one page of the latest committed state.
func (e *Env) InspectPage(id uint64) (PageDetail, error) {
	in, err := e.inspect()
	if err != nil {
		return PageDetail{}, err
	}
	if id >= in.s.pageCount {
		return PageDetail{}, fmt.Errorf("page %d out of range (file has %d pages)", id, in.s.pageCount)
	}
	return e.describe(in, id)
}

// Freelist describes the committed free pages. Head is the free tree root
// (format v3) or the freelist chain head (format v2 awaiting conversion), and
// Pages are the pages holding that bookkeeping.
func (e *Env) Freelist() (FreelistReport, error) {
	in, err := e.inspect()
	if err != nil {
		return FreelistReport{}, err
	}
	rep := FreelistReport{Reusable: e.Info().Reusable, Head: in.s.freeRoot}
	if in.chain != nil {
		rep.Head = in.chain[0]
	}
	for id, r := range in.owner {
		if r.Tree == TreeFree || r.Kind == KindFreelist {
			rep.Pages = append(rep.Pages, id)
		}
	}
	sort.Slice(rep.Pages, func(i, j int) bool { return rep.Pages[i] < rep.Pages[j] })
	for id, freedBy := range in.free {
		rep.Entries = append(rep.Entries, FreeEntry{Page: id, FreedBy: freedBy})
	}
	sort.Slice(rep.Entries, func(i, j int) bool { return rep.Entries[i].Page < rep.Entries[j].Page })
	return rep, nil
}
