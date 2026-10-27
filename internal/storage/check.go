package storage

import (
	"fmt"
	"sort"
)

// CheckReport describes a fully verified committed state.
type CheckReport struct {
	Format    uint32 // file format version
	TxID      uint64
	PageCount uint64
	Data      TreeStats
	Directory TreeStats
	FreeTree  TreeStats // the free tree; empty for a format v2 file not yet converted
	FreePages int
	// FreelistPages counts pages holding free-space bookkeeping: the free tree,
	// or for an unconverted format v2 file, its freelist chain.
	FreelistPages int
}

// Check verifies the latest committed state end to end: all trees in full,
// the on-disk free pages against the in-memory index, and an accounting of
// every page in the file. Each page must be exactly one of tree, overflow,
// free tree, freelist chain, or free. It holds the writer slot; readers
// continue.
func (e *Env) Check() (CheckReport, error) {
	e.writeMu.Lock()
	defer e.writeMu.Unlock()
	e.mu.Lock()
	if err := e.usable(); err != nil {
		e.mu.Unlock()
		return CheckReport{}, err
	}
	s := e.state
	free := e.free // stable: only commits change it, and Check holds the writer slot
	chain := e.chain
	head := e.p.Meta().FreeListPage
	format := e.p.Meta().Version
	e.mu.Unlock()

	src := e.source(s)
	rep := CheckReport{Format: format, TxID: s.txid, PageCount: s.pageCount, FreePages: len(free.pages)}
	var err error
	if rep.Directory, err = verifyTree(src, e.l, s.dirRoot); err != nil {
		return rep, fmt.Errorf("directory: %w", err)
	}
	if rep.Data, err = verifyTree(src, e.l, s.dataRoot); err != nil {
		return rep, fmt.Errorf("data tree: %w", err)
	}
	if rep.FreeTree, err = verifyTree(src, e.l, s.freeRoot); err != nil {
		return rep, fmt.Errorf("free tree: %w", err)
	}
	trees := 0
	for _, root := range []uint64{s.dataRoot, s.freeRoot} {
		if root != 0 {
			trees++
		}
	}
	if rep.Directory.Keys != trees {
		return rep, corrupt("directory holds %d trees, want %d", rep.Directory.Keys, trees)
	}

	// The free pages on disk must match the in-memory index exactly.
	var onDisk *freeIndex
	if chain != nil {
		pages, diskChain, err := loadFreelist(src, s.pageCount, head)
		if err != nil {
			return rep, fmt.Errorf("freelist: %w", err)
		}
		if len(diskChain) != len(chain) {
			return rep, corrupt("freelist chain on disk has %d pages, memory %d", len(diskChain), len(chain))
		}
		onDisk = legacyFreeIndex(pages, e.l)
		rep.FreelistPages = len(chain)
	} else {
		if head != 0 {
			return rep, corrupt("format v3 meta points at a freelist chain page %d", head)
		}
		if onDisk, err = loadFreeIndex(src, e.l, s.freeRoot, s.pageCount); err != nil {
			return rep, fmt.Errorf("free tree: %w", err)
		}
		rep.FreelistPages = len(rep.FreeTree.Pages)
	}
	if !onDisk.equal(free) {
		return rep, corrupt("free pages on disk (%d) differ from memory (%d)", len(onDisk.pages), len(free.pages))
	}

	owner := make(map[uint64]string, s.pageCount)
	claim := func(id uint64, what string) error {
		if id < 2 || id >= s.pageCount {
			return corrupt("%s page %d outside file of %d pages", what, id, s.pageCount)
		}
		if prev, ok := owner[id]; ok {
			return corrupt("page %d is both %s and %s", id, prev, what)
		}
		owner[id] = what
		return nil
	}
	for _, tree := range []struct {
		name string
		st   TreeStats
	}{{"directory", rep.Directory}, {"data", rep.Data}, {"free tree", rep.FreeTree}} {
		for id, t := range tree.st.Pages {
			if err := claim(id, tree.name+" "+t.String()); err != nil {
				return rep, err
			}
		}
	}
	for _, id := range chain {
		if err := claim(id, "freelist chain"); err != nil {
			return rep, err
		}
	}
	for id := range free.pages {
		if err := claim(id, "free"); err != nil {
			return rep, err
		}
	}
	var leaked []uint64
	for id := uint64(2); id < s.pageCount; id++ {
		if _, ok := owner[id]; !ok {
			leaked = append(leaked, id)
		}
	}
	if len(leaked) > 0 {
		sort.Slice(leaked, func(i, j int) bool { return leaked[i] < leaked[j] })
		return rep, corrupt("%d pages belong to nothing, first %d", len(leaked), leaked[0])
	}
	return rep, nil
}
