package pager

import (
	"errors"
	"path/filepath"
	"testing"
)

func TestOpenInitializesNewFileAndReopens(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "sceptre.db")

	p, err := Open(path, Options{PageSize: 512})
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}

	if got, want := p.PageSize(), uint32(512); got != want {
		t.Fatalf("PageSize() = %d, want %d", got, want)
	}
	if got, want := p.ActiveMetaSlot(), 1; got != want {
		t.Fatalf("ActiveMetaSlot() = %d, want %d", got, want)
	}
	if got, want := p.Meta().Generation, uint64(1); got != want {
		t.Fatalf("Meta().Generation = %d, want %d", got, want)
	}
	if got, want := p.Meta().PageCount, uint64(MetaPageCount); got != want {
		t.Fatalf("Meta().PageCount = %d, want %d", got, want)
	}

	if err := p.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	reopened, err := Open(path, Options{})
	if err != nil {
		t.Fatalf("Open() reopen error = %v", err)
	}
	defer reopened.Close()

	if got, want := reopened.PageSize(), uint32(512); got != want {
		t.Fatalf("reopened PageSize() = %d, want %d", got, want)
	}
	if got, want := reopened.ActiveMetaSlot(), 1; got != want {
		t.Fatalf("reopened ActiveMetaSlot() = %d, want %d", got, want)
	}
	if got, want := reopened.Meta().Generation, uint64(1); got != want {
		t.Fatalf("reopened Meta().Generation = %d, want %d", got, want)
	}
}

func TestOpenSelectsLatestValidMetaPage(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "sceptre.db")
	p := mustOpenPager(t, path)

	first := p.Meta()
	first.Generation = 2
	first.RootPage = 11
	first.FreeListPage = 13
	first.PageCount = 21
	mustWriteMetaPage(t, p, 0, first)

	second := p.Meta()
	second.Generation = 3
	second.RootPage = 17
	second.FreeListPage = 19
	second.PageCount = 25
	mustWriteMetaPage(t, p, 1, second)

	if err := p.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	reopened := mustReopenPager(t, path)
	defer reopened.Close()

	if got, want := reopened.ActiveMetaSlot(), 1; got != want {
		t.Fatalf("ActiveMetaSlot() = %d, want %d", got, want)
	}
	if got, want := reopened.Meta(), second; got != want {
		t.Fatalf("Meta() = %+v, want %+v", got, want)
	}
}

func TestOpenFallsBackWhenNewestMetaPageIsCorrupt(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "sceptre.db")
	p := mustOpenPager(t, path)

	oldest := p.Meta()
	oldest.Generation = 4
	oldest.RootPage = 23
	mustWriteMetaPage(t, p, 0, oldest)

	newest := p.Meta()
	newest.Generation = 5
	newest.RootPage = 29
	mustWriteMetaPage(t, p, 1, newest)
	mustCorruptMetaByte(t, p, 1, metaRootOffset)

	if err := p.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	reopened := mustReopenPager(t, path)
	defer reopened.Close()

	if got, want := reopened.ActiveMetaSlot(), 0; got != want {
		t.Fatalf("ActiveMetaSlot() = %d, want %d", got, want)
	}
	if got, want := reopened.Meta(), oldest; got != want {
		t.Fatalf("Meta() = %+v, want %+v", got, want)
	}
}

func TestOpenFailsWhenBothMetaPagesAreCorrupt(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "sceptre.db")
	p := mustOpenPager(t, path)
	mustCorruptMetaByte(t, p, 0, metaRootOffset)
	mustCorruptMetaByte(t, p, 1, metaRootOffset)

	if err := p.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	_, err := Open(path, Options{})
	if !errors.Is(err, ErrNoValidMetaPage) {
		t.Fatalf("Open() error = %v, want %v", err, ErrNoValidMetaPage)
	}
}

func mustOpenPager(t *testing.T, path string) *Pager {
	t.Helper()

	p, err := Open(path, Options{PageSize: 512})
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	return p
}

func mustReopenPager(t *testing.T, path string) *Pager {
	t.Helper()

	p, err := Open(path, Options{})
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	return p
}

func mustWriteMetaPage(t *testing.T, p *Pager, slot int, meta Meta) {
	t.Helper()

	if err := p.writeMetaPage(slot, meta); err != nil {
		t.Fatalf("writeMetaPage() error = %v", err)
	}
	if err := p.file.Sync(); err != nil {
		t.Fatalf("file.Sync() error = %v", err)
	}
}

func mustCorruptMetaByte(t *testing.T, p *Pager, slot int, byteOffset int) {
	t.Helper()

	offset := p.metaOffset(slot, p.PageSize()) + int64(byteOffset)
	buf := make([]byte, 1)
	if _, err := p.file.ReadAt(buf, offset); err != nil {
		t.Fatalf("ReadAt() error = %v", err)
	}
	buf[0] ^= 0xFF
	if _, err := p.file.WriteAt(buf, offset); err != nil {
		t.Fatalf("WriteAt() error = %v", err)
	}
	if err := p.file.Sync(); err != nil {
		t.Fatalf("file.Sync() error = %v", err)
	}
}
