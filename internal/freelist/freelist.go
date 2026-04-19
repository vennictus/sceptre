package freelist

import (
	"bytes"
	"encoding/binary"
	"errors"
)

const (
	pageMagic       = "SCFREE\x00\x00"
	pageMagicSize   = 8
	pageHeaderSize  = 24
	pageNextOffset  = 8
	pageCountOffset = 16
	pageEntryOffset = 24
	pageEntrySize   = 8
)

var (
	ErrInvalidPageSize  = errors.New("freelist: invalid page size")
	ErrPageTooSmall     = errors.New("freelist: page too small")
	ErrInvalidPageMagic = errors.New("freelist: invalid page magic")
	ErrPageCountTooHigh = errors.New("freelist: page entry count exceeds capacity")
	ErrPageLoop         = errors.New("freelist: linked pages contain a cycle")
)

// PageReader loads freelist pages by page ID.
type PageReader interface {
	ReadPage(pageID uint64) ([]byte, error)
}

// State captures the durable freelist root and page inventory.
type State struct {
	HeadPage  uint64
	PageIDs   []uint64
	FreePages []uint64
	Pages     map[uint64][]byte
}

// Clone returns a deep copy of the freelist state.
func (s State) Clone() State {
	pages := make(map[uint64][]byte, len(s.Pages))
	for pageID, page := range s.Pages {
		pages[pageID] = cloneBytes(page)
	}
	return State{
		HeadPage:  s.HeadPage,
		PageIDs:   append([]uint64(nil), s.PageIDs...),
		FreePages: append([]uint64(nil), s.FreePages...),
		Pages:     pages,
	}
}

// Allocator hands out page IDs from the current reusable pool before appending.
type Allocator struct {
	reusable []uint64
	nextPage uint64
}

// NewAllocator builds a page allocator from reusable page IDs and the current file tail.
func NewAllocator(reusable []uint64, nextPage uint64) *Allocator {
	return &Allocator{
		reusable: append([]uint64(nil), reusable...),
		nextPage: nextPage,
	}
}

// AllocatePage returns the next page ID to use for the new durable state.
func (a *Allocator) AllocatePage() uint64 {
	if len(a.reusable) > 0 {
		pageID := a.reusable[0]
		a.reusable = a.reusable[1:]
		return pageID
	}

	pageID := a.nextPage
	a.nextPage++
	return pageID
}

// NextPageID returns the first unallocated append-only page ID.
func (a *Allocator) NextPageID() uint64 {
	return a.nextPage
}

// RemainingPages returns the reusable page IDs that have not been consumed yet.
func (a *Allocator) RemainingPages() []uint64 {
	return append([]uint64(nil), a.reusable...)
}

// Load walks the on-disk freelist pages starting at headPage.
func Load(reader PageReader, headPage uint64) (State, error) {
	if headPage == 0 {
		return State{Pages: make(map[uint64][]byte)}, nil
	}

	state := State{
		HeadPage: headPage,
		Pages:    make(map[uint64][]byte),
	}
	visited := make(map[uint64]struct{})
	for pageID := headPage; pageID != 0; {
		if _, ok := visited[pageID]; ok {
			return State{}, ErrPageLoop
		}
		visited[pageID] = struct{}{}

		page, err := reader.ReadPage(pageID)
		if err != nil {
			return State{}, err
		}
		nextPage, freePages, err := DecodePage(page)
		if err != nil {
			return State{}, err
		}

		state.PageIDs = append(state.PageIDs, pageID)
		state.FreePages = append(state.FreePages, freePages...)
		state.Pages[pageID] = cloneBytes(page)
		pageID = nextPage
	}
	return state, nil
}

// Build encodes the next committed freelist state using the allocator's remaining pages.
func Build(pageSize int, alloc *Allocator, retiredPages []uint64) (State, error) {
	if pageSize < pageHeaderSize {
		return State{}, ErrInvalidPageSize
	}
	if alloc == nil {
		return State{}, ErrInvalidPageSize
	}

	reusableCount := len(alloc.reusable)
	candidateCount := reusableCount + len(retiredPages)
	pageCap := pageCapacity(pageSize)
	if pageCap == 0 && candidateCount > 0 {
		return State{}, ErrPageTooSmall
	}

	pageCount := 0
	for candidateCount-min(pageCount, reusableCount) > pageCount*pageCap {
		pageCount++
	}

	state := State{
		PageIDs: make([]uint64, 0, pageCount),
		Pages:   make(map[uint64][]byte, pageCount),
	}
	for i := 0; i < pageCount; i++ {
		state.PageIDs = append(state.PageIDs, alloc.AllocatePage())
	}

	state.FreePages = append(state.FreePages, alloc.RemainingPages()...)
	state.FreePages = append(state.FreePages, retiredPages...)

	for i, pageID := range state.PageIDs {
		start := i * pageCap
		end := start + pageCap
		if end > len(state.FreePages) {
			end = len(state.FreePages)
		}

		nextPage := uint64(0)
		if i+1 < len(state.PageIDs) {
			nextPage = state.PageIDs[i+1]
		}

		page, err := EncodePage(pageSize, nextPage, state.FreePages[start:end])
		if err != nil {
			return State{}, err
		}
		state.Pages[pageID] = page
	}
	if len(state.PageIDs) > 0 {
		state.HeadPage = state.PageIDs[0]
	}

	return state, nil
}

// EncodePage serializes a single freelist page.
func EncodePage(pageSize int, nextPage uint64, freePages []uint64) ([]byte, error) {
	if pageSize < pageHeaderSize {
		return nil, ErrInvalidPageSize
	}
	if len(freePages) > pageCapacity(pageSize) {
		return nil, ErrPageCountTooHigh
	}

	page := make([]byte, pageSize)
	copy(page[:pageMagicSize], pageMagic)
	binary.LittleEndian.PutUint64(page[pageNextOffset:], nextPage)
	binary.LittleEndian.PutUint32(page[pageCountOffset:], uint32(len(freePages)))
	for i, pageID := range freePages {
		offset := pageEntryOffset + i*pageEntrySize
		binary.LittleEndian.PutUint64(page[offset:], pageID)
	}
	return page, nil
}

// DecodePage parses a single freelist page.
func DecodePage(page []byte) (uint64, []uint64, error) {
	if len(page) < pageHeaderSize {
		return 0, nil, ErrPageTooSmall
	}
	if !bytes.Equal(page[:pageMagicSize], []byte(pageMagic)) {
		return 0, nil, ErrInvalidPageMagic
	}

	count := int(binary.LittleEndian.Uint32(page[pageCountOffset:]))
	if count > pageCapacity(len(page)) {
		return 0, nil, ErrPageCountTooHigh
	}

	freePages := make([]uint64, 0, count)
	for i := 0; i < count; i++ {
		offset := pageEntryOffset + i*pageEntrySize
		freePages = append(freePages, binary.LittleEndian.Uint64(page[offset:]))
	}
	return binary.LittleEndian.Uint64(page[pageNextOffset:]), freePages, nil
}

func pageCapacity(pageSize int) int {
	return (pageSize - pageHeaderSize) / pageEntrySize
}

func cloneBytes(src []byte) []byte {
	return append([]byte(nil), src...)
}

func min(left, right int) int {
	if left < right {
		return left
	}
	return right
}
