package storage

import (
	"bytes"
	"errors"
	"fmt"
	"testing"

	"github.com/vennictus/sceptre/internal/pager"
)

func TestV2FixtureConvertsOnFirstCommit(t *testing.T) {
	t.Parallel()
	path := copyV2Fixture(t)
	e := openEnv(t, path, Options{})
	if v := e.Pager().Meta().Version; v != pager.FormatV2 {
		t.Fatalf("fixture opened as version %d", v)
	}
	chain := e.Info().FreelistPages
	if chain < 2 {
		t.Fatalf("fixture chain has %d pages", chain)
	}
	want := v2FixtureContents()
	commit(t, e, func(w *WriteTxn) { put(t, w, "after", []byte("conversion")) })
	want["after"] = []byte("conversion")

	if v := e.Pager().Meta().Version; v != pager.FormatV3 {
		t.Fatalf("version after the first commit = %d, want 3", v)
	}
	if info := e.Info(); info.FreelistPages != 0 {
		t.Fatalf("chain still tracked after conversion: %+v", info)
	}
	rep := mustCheck(t, e)
	if rep.FreeTree.Keys == 0 || rep.FreePages == 0 {
		t.Fatalf("no free tree after conversion: %+v", rep)
	}
	sameContents(t, contents(t, e), want)
	e.Close()

	e = openEnv(t, path, Options{})
	defer e.Close()
	if v := e.Pager().Meta().Version; v != pager.FormatV3 {
		t.Fatalf("reopened version = %d", v)
	}
	sameContents(t, contents(t, e), want)
	mustCheck(t, e)
	// The converted file keeps working, reusing its free pages.
	for i := 0; i < 20; i++ {
		commit(t, e, func(w *WriteTxn) { put(t, w, "after", bytes.Repeat([]byte{byte(i)}, 700)) })
	}
	mustCheck(t, e)
}

func TestV2ConversionInterruptedAtEachStage(t *testing.T) {
	t.Parallel()
	for _, stage := range CommitStages() {
		stage := stage
		t.Run(stage, func(t *testing.T) {
			t.Parallel()
			path := copyV2Fixture(t)
			fail := true
			e := openEnv(t, path, Options{CommitHook: func(s string) error {
				if fail && s == stage {
					return errors.New("crash")
				}
				return nil
			}})
			w, _ := e.BeginWrite()
			put(t, w, "after", []byte("conversion"))
			w.Commit()
			e.Close()

			e = openEnv(t, path, Options{})
			defer e.Close()
			want := v2FixtureContents()
			version := pager.FormatV2
			if stage == StageMetaPublished {
				version = pager.FormatV3
				want["after"] = []byte("conversion")
			}
			if v := e.Pager().Meta().Version; v != version {
				t.Fatalf("version after interruption at %s = %d, want %d", stage, v, version)
			}
			sameContents(t, contents(t, e), want)
			mustCheck(t, e)
			// A file left at v2 converts on its next commit.
			fail = false
			commit(t, e, func(w *WriteTxn) { put(t, w, "next", nil) })
			if v := e.Pager().Meta().Version; v != pager.FormatV3 {
				t.Fatalf("version after recovery commit = %d", v)
			}
			mustCheck(t, e)
		})
	}
}

func TestFreeTreeGrowsAndShrinksAcrossManyPages(t *testing.T) {
	t.Parallel()
	e := openEnv(t, t.TempDir()+"/db", Options{PageSize: 1024})
	defer e.Close()
	const n = 4000
	value := bytes.Repeat([]byte("v"), 300) // one overflow page per key at 1024-byte pages
	commit(t, e, func(w *WriteTxn) {
		for i := 0; i < n; i++ {
			put(t, w, fmt.Sprintf("key%05d", i), value)
		}
	})
	// One commit frees thousands of pages: the free tree needs many chunks and levels.
	commit(t, e, func(w *WriteTxn) {
		for i := 0; i < n; i++ {
			if _, err := w.Delete([]byte(fmt.Sprintf("key%05d", i))); err != nil {
				t.Fatal(err)
			}
		}
	})
	rep := mustCheck(t, e)
	if rep.FreeTree.Depth < 2 || rep.FreePages < n {
		t.Fatalf("free tree too small for the test: %+v", rep)
	}
	// Pages freed by a commit stay protected while the fallback meta generation
	// can reach them; one more commit moves the fallback past the delete.
	commit(t, e, func(w *WriteTxn) { put(t, w, "marker", nil) })
	grown := mustCheck(t, e).PageCount
	// Refill: new pages come from the free tree, which shrinks, and the file does not grow.
	for round := 0; round < 4; round++ {
		commit(t, e, func(w *WriteTxn) {
			for i := round * n / 4; i < (round+1)*n/4; i++ {
				put(t, w, fmt.Sprintf("new%05d", i), value)
			}
		})
		mustCheck(t, e)
	}
	if rep := mustCheck(t, e); rep.PageCount > grown {
		t.Fatalf("file grew while free pages were available: %d -> %d", grown, rep.PageCount)
	}
}

func TestNewFilesUseFormatV3(t *testing.T) {
	t.Parallel()
	e := openEnv(t, t.TempDir()+"/db", Options{})
	defer e.Close()
	if v := e.Pager().Meta().Version; v != pager.FormatV3 {
		t.Fatalf("new file version = %d", v)
	}
}
