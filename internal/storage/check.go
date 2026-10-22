package storage

import (
	"fmt"
	"sort"
)

// CheckReport describes a fully verified committed state.
type CheckReport struct {
	TxID          uint64
	PageCount     uint64
	Data          TreeStats
	Directory     TreeStats
	FreePages     int
	FreelistPages int
}

// Check verifies the latest committed state end to end: both trees in full,
// the on-disk freelist against the in-memory one, and an accounting of every
// page in the file. Each page must be exactly one of tree, overflow,
// freelist, or free. It holds the writer slot; readers continue.
func (e *Env) Check() (CheckReport, error) {
	e.writeMu.Lock()
	defer e.writeMu.Unlock()
	e.mu.Lock()
	if err := e.usable(); err != nil {
		e.mu.Unlock()
		return CheckReport{}, err
	}
	s := e.state
	free := e.free
	freePages := e.freePages
	head := e.p.Meta().FreeListPage
	e.mu.Unlock()

	src := e.source(s)
	rep := CheckReport{TxID: s.txid, PageCount: s.pageCount, FreePages: len(free), FreelistPages: len(freePages)}
	var err error
	if rep.Directory, err = verifyTree(src, e.l, s.dirRoot); err != nil {
		return rep, fmt.Errorf("directory: %w", err)
	}
	if rep.Data, err = verifyTree(src, e.l, s.dataRoot); err != nil {
		return rep, fmt.Errorf("data tree: %w", err)
	}
	if s.dataRoot != 0 && rep.Directory.Keys != 1 {
		return rep, corrupt("directory holds %d trees, want 1", rep.Directory.Keys)
	}

	onDisk, diskPages, err := loadFreelist(src, s.pageCount, head)
	if err != nil {
		return rep, fmt.Errorf("freelist: %w", err)
	}
	if len(onDisk) != len(free) || len(diskPages) != len(freePages) {
		return rep, corrupt("freelist on disk has %d entries in %d pages, memory %d in %d", len(onDisk), len(diskPages), len(free), len(freePages))
	}
	for id, freedBy := range free {
		if got, ok := onDisk[id]; !ok || got != freedBy {
			return rep, corrupt("freelist entry for page %d differs between disk and memory", id)
		}
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
	}{{"directory", rep.Directory}, {"data", rep.Data}} {
		for id, t := range tree.st.Pages {
			if err := claim(id, tree.name+" "+t.String()); err != nil {
				return rep, err
			}
		}
	}
	for _, id := range freePages {
		if err := claim(id, "freelist"); err != nil {
			return rep, err
		}
	}
	for id := range free {
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
