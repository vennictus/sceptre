package freelist

import (
	"errors"
	"testing"
)

func TestEncodeDecodePageRoundTrip(t *testing.T) {
	t.Parallel()

	page, err := EncodePage(512, 19, []uint64{4, 7, 11})
	if err != nil {
		t.Fatalf("EncodePage() error = %v", err)
	}

	nextPage, freePages, err := DecodePage(page)
	if err != nil {
		t.Fatalf("DecodePage() error = %v", err)
	}
	if nextPage != 19 {
		t.Fatalf("DecodePage() next = %d, want 19", nextPage)
	}
	if len(freePages) != 3 {
		t.Fatalf("DecodePage() len = %d, want 3", len(freePages))
	}
	want := []uint64{4, 7, 11}
	for i, pageID := range want {
		if freePages[i] != pageID {
			t.Fatalf("DecodePage() freePages[%d] = %d, want %d", i, freePages[i], pageID)
		}
	}
}

func TestDecodePageRejectsInvalidMagic(t *testing.T) {
	t.Parallel()

	page := make([]byte, 512)
	if _, _, err := DecodePage(page); !errors.Is(err, ErrInvalidPageMagic) {
		t.Fatalf("DecodePage() error = %v, want %v", err, ErrInvalidPageMagic)
	}
}

func TestBuildAndLoadRoundTrip(t *testing.T) {
	t.Parallel()

	alloc := NewAllocator([]uint64{9, 12, 15}, 20)
	state, err := Build(512, alloc, []uint64{31, 32, 33})
	if err != nil {
		t.Fatalf("Build() error = %v", err)
	}

	if state.HeadPage == 0 {
		t.Fatal("Build() head = 0, want non-zero")
	}
	if len(state.PageIDs) != 1 {
		t.Fatalf("Build() page count = %d, want 1", len(state.PageIDs))
	}
	if got := len(state.FreePages); got != 5 {
		t.Fatalf("Build() free page count = %d, want 5", got)
	}

	loaded, err := Load(pageMapReader(state.Pages), state.HeadPage)
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if len(loaded.PageIDs) != 1 {
		t.Fatalf("Load() page count = %d, want 1", len(loaded.PageIDs))
	}
	if len(loaded.FreePages) != len(state.FreePages) {
		t.Fatalf("Load() free page count = %d, want %d", len(loaded.FreePages), len(state.FreePages))
	}
	for i, pageID := range state.FreePages {
		if loaded.FreePages[i] != pageID {
			t.Fatalf("Load() freePages[%d] = %d, want %d", i, loaded.FreePages[i], pageID)
		}
	}
}

func TestBuildConsumesReusablePagesForMetadata(t *testing.T) {
	t.Parallel()

	alloc := NewAllocator([]uint64{5}, 10)
	state, err := Build(512, alloc, nil)
	if err != nil {
		t.Fatalf("Build() error = %v", err)
	}

	if state.HeadPage != 5 {
		t.Fatalf("Build() head = %d, want 5", state.HeadPage)
	}
	if len(state.FreePages) != 0 {
		t.Fatalf("Build() free page count = %d, want 0", len(state.FreePages))
	}
}

type pageMapReader map[uint64][]byte

func (r pageMapReader) ReadPage(pageID uint64) ([]byte, error) {
	page, ok := r[pageID]
	if !ok {
		return nil, errors.New("missing page")
	}
	return append([]byte(nil), page...), nil
}
