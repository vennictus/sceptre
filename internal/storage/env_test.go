package storage

import (
	"bytes"
	"errors"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"testing"

	"github.com/vennictus/sceptre/internal/pager"
)

func openEnv(t *testing.T, path string, opts Options) *Env {
	t.Helper()
	e, err := Open(path, opts)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	return e
}

func mustCheck(t *testing.T, e *Env) CheckReport {
	t.Helper()
	rep, err := e.Check()
	if err != nil {
		t.Fatalf("Check: %v", err)
	}
	return rep
}

func commit(t *testing.T, e *Env, apply func(*WriteTxn)) {
	t.Helper()
	w, err := e.BeginWrite()
	if err != nil {
		t.Fatal(err)
	}
	apply(w)
	if err := w.Commit(); err != nil {
		t.Fatalf("Commit: %v", err)
	}
}

func put(t *testing.T, w *WriteTxn, k string, v []byte) {
	t.Helper()
	if err := w.Put([]byte(k), v); err != nil {
		t.Fatalf("Put %q: %v", k, err)
	}
}

// contents reads a full snapshot into a map.
func contents(t *testing.T, e *Env) map[string][]byte {
	t.Helper()
	r, err := e.BeginRead()
	if err != nil {
		t.Fatal(err)
	}
	defer r.Release()
	return scan(t, r.Cursor())
}

func scan(t *testing.T, c *Cursor) map[string][]byte {
	t.Helper()
	out := map[string][]byte{}
	var prev []byte
	for err := c.SeekFirst(); c.Valid(); err = c.Next() {
		if err != nil {
			t.Fatal(err)
		}
		k, _ := c.Key()
		if prev != nil && bytes.Compare(k, prev) <= 0 {
			t.Fatalf("scan out of order: %q after %q", k, prev)
		}
		v, err := c.Value()
		if err != nil {
			t.Fatal(err)
		}
		out[string(k)], prev = v, k
	}
	if c.Err() != nil {
		t.Fatal(c.Err())
	}
	return out
}

func sameContents(t *testing.T, got, want map[string][]byte) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("%d keys, want %d", len(got), len(want))
	}
	for k, v := range want {
		if !bytes.Equal(got[k], v) {
			t.Fatalf("key %q: %d bytes, want %d", k, len(got[k]), len(v))
		}
	}
}

func TestEnvPersistsAcrossReopen(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "db")
	e := openEnv(t, path, Options{})
	big := bytes.Repeat([]byte("overflow"), 3000)
	commit(t, e, func(w *WriteTxn) {
		put(t, w, "a", []byte("1"))
		put(t, w, "big", big)
		put(t, w, "empty", nil)
	})
	mustCheck(t, e)
	e.Close()

	e = openEnv(t, path, Options{})
	defer e.Close()
	sameContents(t, contents(t, e), map[string][]byte{"a": []byte("1"), "big": big, "empty": {}})
	if rep := mustCheck(t, e); rep.Data.OverflowPages == 0 {
		t.Fatalf("large value stored inline: %+v", rep.Data)
	}
}

func TestEnvRejectsV1FilesAndBadPageSizes(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	p, err := pager.Open(filepath.Join(dir, "v1.db"), pager.Options{Format: pager.FormatV1})
	if err != nil {
		t.Fatal(err)
	}
	p.Close()
	if _, err := Open(filepath.Join(dir, "v1.db"), Options{}); !errors.Is(err, ErrUpgradeRequired) {
		t.Fatalf("v1 file: %v", err)
	}
	if _, err := Open(filepath.Join(dir, "odd.db"), Options{PageSize: 3000}); !errors.Is(err, ErrInvalidPageSize) {
		t.Fatalf("page size 3000: %v", err)
	}
}

func TestEnvSnapshotIsolation(t *testing.T) {
	t.Parallel()
	e := openEnv(t, filepath.Join(t.TempDir(), "db"), Options{})
	defer e.Close()
	commit(t, e, func(w *WriteTxn) { put(t, w, "k", []byte("v1")) })

	r, _ := e.BeginRead()
	defer r.Release()
	w, _ := e.BeginWrite()
	put(t, w, "k", []byte("v2"))
	put(t, w, "new", []byte("x"))
	if v, _, _ := w.Get([]byte("k")); string(v) != "v2" {
		t.Fatalf("writer does not see its own write: %q", v)
	}
	if v, _, _ := r.Get([]byte("k")); string(v) != "v1" {
		t.Fatalf("reader sees uncommitted write: %q", v)
	}
	if err := w.Commit(); err != nil {
		t.Fatal(err)
	}
	if v, _, _ := r.Get([]byte("k")); string(v) != "v1" {
		t.Fatalf("old snapshot changed after commit: %q", v)
	}
	if _, ok, _ := r.Get([]byte("new")); ok {
		t.Fatal("old snapshot sees a later insert")
	}
	r2, _ := e.BeginRead()
	defer r2.Release()
	if v, _, _ := r2.Get([]byte("k")); string(v) != "v2" {
		t.Fatalf("new snapshot misses commit: %q", v)
	}
}

func TestEnvAbortDiscardsEverything(t *testing.T) {
	t.Parallel()
	e := openEnv(t, filepath.Join(t.TempDir(), "db"), Options{})
	defer e.Close()
	commit(t, e, func(w *WriteTxn) { put(t, w, "keep", []byte("1")) })
	before := e.Info()
	w, _ := e.BeginWrite()
	for i := 0; i < 500; i++ {
		put(t, w, fmt.Sprintf("tmp%04d", i), bytes.Repeat([]byte("z"), 300))
	}
	w.Abort()
	w.Abort() // repeated Abort is safe
	if err := w.Put([]byte("x"), nil); !errors.Is(err, ErrTxnClosed) {
		t.Fatalf("Put after Abort: %v", err)
	}
	sameContents(t, contents(t, e), map[string][]byte{"keep": []byte("1")})
	if after := e.Info(); after.TxID != before.TxID || after.PageCount != before.PageCount {
		t.Fatalf("abort changed state: %+v -> %+v", before, after)
	}
	// The writer slot is free again.
	commit(t, e, func(w *WriteTxn) { put(t, w, "next", nil) })
	mustCheck(t, e)
}

func TestEnvSteadyUpdatesReuseSpace(t *testing.T) {
	t.Parallel()
	e := openEnv(t, filepath.Join(t.TempDir(), "db"), Options{})
	defer e.Close()
	update := func(round int) {
		commit(t, e, func(w *WriteTxn) {
			for i := 0; i < 200; i++ {
				put(t, w, fmt.Sprintf("key%04d", i), []byte(fmt.Sprintf("round %d value %d", round, i)))
			}
		})
	}
	for round := 0; round < 10; round++ {
		update(round)
	}
	settled := e.Info().PageCount
	for round := 10; round < 200; round++ {
		update(round)
	}
	if grown := e.Info().PageCount; grown > settled+4 {
		t.Fatalf("file kept growing under steady updates: %d -> %d pages", settled, grown)
	}
	mustCheck(t, e)
}

func TestEnvReaderKeepsItsPagesAcrossManyCommits(t *testing.T) {
	t.Parallel()
	e := openEnv(t, filepath.Join(t.TempDir(), "db"), Options{CacheBytes: 1}) // tiny cache: reads hit disk
	defer e.Close()
	want := map[string][]byte{}
	commit(t, e, func(w *WriteTxn) {
		for i := 0; i < 300; i++ {
			k, v := fmt.Sprintf("key%04d", i), bytes.Repeat([]byte{byte(i)}, 100+i%2000)
			put(t, w, k, v)
			want[k] = v
		}
	})
	r, _ := e.BeginRead()
	for round := 0; round < 60; round++ {
		commit(t, e, func(w *WriteTxn) {
			for i := 0; i < 300; i += 7 {
				put(t, w, fmt.Sprintf("key%04d", (i+round)%300), []byte(fmt.Sprintf("r%d", round)))
			}
		})
	}
	if info := e.Info(); info.Readers != 1 || info.OldestReader != r.TxID() {
		t.Fatalf("reader table: %+v", info)
	}
	sameContents(t, scan(t, r.Cursor()), want) // every page of the old snapshot survived
	held := e.Info().PageCount
	r.Release()
	for round := 0; round < 60; round++ {
		commit(t, e, func(w *WriteTxn) {
			for i := 0; i < 300; i += 7 {
				put(t, w, fmt.Sprintf("key%04d", i), []byte("after"))
			}
		})
	}
	if e.Info().PageCount > held {
		t.Fatalf("pages held by the released reader were not reused: %d -> %d", held, e.Info().PageCount)
	}
	mustCheck(t, e)
}

// TestEnvFallbackStateStaysIntact interrupts a commit after it wrote its pages,
// then destroys the newest meta page, after many commits with page reuse. The
// previous state must be complete: the interrupted commit may only have
// reused pages that neither meta generation reaches.
func TestEnvFallbackStateStaysIntact(t *testing.T) {
	t.Parallel()
	for _, seed := range []int64{1, 2, 3} {
		path := filepath.Join(t.TempDir(), fmt.Sprintf("db%d", seed))
		rng := rand.New(rand.NewSource(seed))
		interrupt := false
		e := openEnv(t, path, Options{PageSize: 1024, CommitHook: func(stage string) error {
			if interrupt && stage == StagePagesSynced {
				return errors.New("crash after page writes")
			}
			return nil
		}})
		model := map[string][]byte{}
		var previous map[string][]byte
		for round := 0; round < 40; round++ {
			previous = cloneModel(model)
			commit(t, e, func(w *WriteTxn) { randomOps(t, rng, w, model, 40) })
		}
		interrupt = true
		w, _ := e.BeginWrite()
		randomOps(t, rng, w, cloneModel(model), 200)
		if err := w.Commit(); err == nil {
			t.Fatal("interrupted commit succeeded")
		}
		slot := e.Pager().ActiveMetaSlot()
		e.Close()

		f, err := os.OpenFile(path, os.O_RDWR, 0)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := f.WriteAt(make([]byte, 1024), int64(slot)*1024); err != nil {
			t.Fatal(err)
		}
		f.Close()

		e = openEnv(t, path, Options{})
		sameContents(t, contents(t, e), previous)
		mustCheck(t, e)
		// The fallback state accepts new commits.
		commit(t, e, func(w *WriteTxn) { randomOps(t, rng, w, previous, 40) })
		sameContents(t, contents(t, e), previous)
		mustCheck(t, e)
		e.Close()
	}
}

func cloneModel(m map[string][]byte) map[string][]byte {
	out := make(map[string][]byte, len(m))
	for k, v := range m {
		out[k] = v
	}
	return out
}

func randomOps(t *testing.T, rng *rand.Rand, w *WriteTxn, model map[string][]byte, n int) {
	t.Helper()
	for i := 0; i < n; i++ {
		k := fmt.Sprintf("k%0*d", 1+rng.Intn(30), rng.Intn(300))
		if rng.Intn(10) < 7 {
			v := randomValue(rng, w.e.l)
			put(t, w, k, v)
			model[k] = v
		} else {
			found, err := w.Delete([]byte(k))
			if err != nil {
				t.Fatal(err)
			}
			if _, ok := model[k]; ok != found {
				t.Fatalf("Delete %q found=%v model=%v", k, found, ok)
			}
			delete(model, k)
		}
	}
}

func TestEnvRandomizedAcrossCommitsAndReopens(t *testing.T) {
	t.Parallel()
	rng := rand.New(rand.NewSource(7))
	path := filepath.Join(t.TempDir(), "db")
	e := openEnv(t, path, Options{PageSize: 1024, CacheBytes: 32 * 1024})
	model := map[string][]byte{}
	for round := 0; round < 120; round++ {
		w, err := e.BeginWrite()
		if err != nil {
			t.Fatal(err)
		}
		scratch := cloneModel(model)
		randomOps(t, rng, w, scratch, 30)
		if rng.Intn(5) == 0 {
			w.Abort()
		} else {
			if err := w.Commit(); err != nil {
				t.Fatal(err)
			}
			model = scratch
		}
		if round%10 == 0 {
			e.Close()
			e = openEnv(t, path, Options{CacheBytes: 32 * 1024})
		}
		mustCheck(t, e)
		sameContents(t, contents(t, e), model)
	}
	e.Close()
}

func TestEnvDetectsCorruptPages(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "db")
	e := openEnv(t, path, Options{PageSize: 1024})
	commit(t, e, func(w *WriteTxn) {
		for i := 0; i < 200; i++ {
			put(t, w, fmt.Sprintf("key%04d", i), bytes.Repeat([]byte("v"), 50))
		}
	})
	leaf := uint64(0)
	for id, typ := range mustCheck(t, e).Data.Pages {
		if typ == pageLeaf {
			leaf = id
			break
		}
	}
	e.Close()

	f, _ := os.OpenFile(path, os.O_RDWR, 0)
	if _, err := f.WriteAt([]byte{0xAA}, int64(leaf)*1024+600); err != nil {
		t.Fatal(err)
	}
	f.Close()

	e = openEnv(t, path, Options{})
	defer e.Close()
	if _, err := e.Check(); !errors.Is(err, ErrPageCorrupt) {
		t.Fatalf("Check on corrupt page: %v", err)
	}
	r, _ := e.BeginRead()
	defer r.Release()
	c := r.Cursor()
	for err := c.SeekFirst(); c.Valid(); err = c.Next() {
		if err != nil {
			break
		}
		c.Value()
	}
	if !errors.Is(c.Err(), ErrPageCorrupt) {
		t.Fatalf("full scan over a corrupt page: %v", c.Err())
	}
}

func TestEnvRejectsPagesNewerThanSnapshot(t *testing.T) {
	t.Parallel()
	e := openEnv(t, filepath.Join(t.TempDir(), "db"), Options{})
	defer e.Close()
	commit(t, e, func(w *WriteTxn) { put(t, w, "a", []byte("1")) })
	commit(t, e, func(w *WriteTxn) { put(t, w, "a", []byte("2")) })
	s := e.Info()
	old := snapshot{txid: 1, pageCount: s.PageCount}
	if _, err := e.source(old).page(s.DataRoot); !errors.Is(err, ErrSnapshotPage) {
		t.Fatalf("page from transaction %d served to snapshot 1: %v", s.TxID, err)
	}
}

func TestEnvCommitInterruptedAtEachStage(t *testing.T) {
	t.Parallel()
	for _, stage := range CommitStages() {
		stage := stage
		t.Run(stage, func(t *testing.T) {
			t.Parallel()
			path := filepath.Join(t.TempDir(), "db")
			fail := false
			e := openEnv(t, path, Options{CommitHook: func(s string) error {
				if fail && s == stage {
					return errors.New("interrupted")
				}
				return nil
			}})
			commit(t, e, func(w *WriteTxn) { put(t, w, "k", []byte("old")) })
			fail = true
			w, _ := e.BeginWrite()
			put(t, w, "k", []byte("new"))
			err := w.Commit()
			fail = false
			committed := stage == StageMetaPublished
			if committed != errors.Is(err, ErrCommitCommitted) || err == nil {
				t.Fatalf("commit error at %s: %v", stage, err)
			}
			want := "old"
			if committed {
				want = "new"
			}
			if v, _, _ := mustRead(t, e, "k"); string(v) != want {
				t.Fatalf("after interrupted commit: %q, want %q", v, want)
			}
			// A definite failure leaves the environment usable.
			commit(t, e, func(w *WriteTxn) { put(t, w, "after", nil) })
			mustCheck(t, e)
			e.Close()
			e = openEnv(t, path, Options{})
			if v, _, _ := mustRead(t, e, "k"); string(v) != want {
				t.Fatalf("after reopen: %q, want %q", v, want)
			}
			mustCheck(t, e)
			e.Close()
		})
	}
}

func mustRead(t *testing.T, e *Env, k string) ([]byte, bool, error) {
	t.Helper()
	r, err := e.BeginRead()
	if err != nil {
		t.Fatal(err)
	}
	defer r.Release()
	return r.Get([]byte(k))
}

func TestEnvUncertainMetaWriteRequiresReopen(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "db")
	failMeta := false
	hooks := &pager.IOHooks{WriteAt: func(f *os.File, b []byte, off int64) (int, error) {
		if failMeta && off < 2*int64(pager.DefaultPageSize) {
			return len(b) / 2, errors.New("torn meta write")
		}
		return f.WriteAt(b, off)
	}}
	e := openEnv(t, path, Options{IO: hooks})
	commit(t, e, func(w *WriteTxn) { put(t, w, "k", []byte("old")) })
	failMeta = true
	w, _ := e.BeginWrite()
	put(t, w, "k", []byte("new"))
	if err := w.Commit(); !errors.Is(err, ErrRecoveryRequired) {
		t.Fatalf("torn meta write: %v", err)
	}
	if _, err := e.BeginRead(); !errors.Is(err, ErrRecoveryRequired) {
		t.Fatalf("read after uncertain commit: %v", err)
	}
	if _, err := e.BeginWrite(); !errors.Is(err, ErrRecoveryRequired) {
		t.Fatalf("write after uncertain commit: %v", err)
	}
	e.Close()
	e = openEnv(t, path, Options{})
	defer e.Close()
	if v, _, _ := mustRead(t, e, "k"); string(v) != "old" {
		t.Fatalf("reopen after torn meta: %q", v)
	}
	mustCheck(t, e)
}

func TestEnvConcurrentReadersSeeConsistentSnapshots(t *testing.T) {
	t.Parallel()
	e := openEnv(t, filepath.Join(t.TempDir(), "db"), Options{CacheBytes: 64 * 1024})
	defer e.Close()
	const keys = 100
	write := func(round int) {
		commit(t, e, func(w *WriteTxn) {
			for i := 0; i < keys; i++ {
				put(t, w, fmt.Sprintf("k%03d", i), []byte(fmt.Sprintf("%08d", round)))
			}
		})
	}
	write(0)
	var wg sync.WaitGroup
	stop := make(chan struct{})
	errs := make(chan error, 8)
	for g := 0; g < 4; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				r, err := e.BeginRead()
				if err != nil {
					errs <- err
					return
				}
				seen := map[string]int{}
				c := r.Cursor()
				n := 0
				for err := c.SeekFirst(); c.Valid(); err = c.Next() {
					if err != nil {
						break
					}
					v, _ := c.Value()
					seen[string(v)]++
					n++
				}
				r.Release()
				if c.Err() != nil || n != keys || len(seen) != 1 {
					errs <- fmt.Errorf("inconsistent snapshot: %d keys, values %v, err %v", n, seen, c.Err())
					return
				}
			}
		}()
	}
	for round := 1; round <= 150; round++ {
		write(round)
	}
	close(stop)
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
	mustCheck(t, e)
}

func TestEnvDeleteEverythingThenReuse(t *testing.T) {
	t.Parallel()
	e := openEnv(t, filepath.Join(t.TempDir(), "db"), Options{PageSize: 1024})
	defer e.Close()
	var keys []string
	commit(t, e, func(w *WriteTxn) {
		for i := 0; i < 400; i++ {
			k := fmt.Sprintf("key%04d", i)
			keys = append(keys, k)
			put(t, w, k, bytes.Repeat([]byte("q"), 200))
		}
	})
	commit(t, e, func(w *WriteTxn) {
		for _, k := range keys {
			if _, err := w.Delete([]byte(k)); err != nil {
				t.Fatal(err)
			}
		}
	})
	if info := e.Info(); info.DataRoot != 0 {
		t.Fatalf("empty data tree keeps root %d", info.DataRoot)
	}
	rep := mustCheck(t, e)
	if rep.Directory.Keys != 0 {
		t.Fatalf("directory still lists the empty data tree: %+v", rep.Directory)
	}
	commit(t, e, func(w *WriteTxn) { put(t, w, "again", nil) })
	commit(t, e, func(w *WriteTxn) { put(t, w, "again2", nil) })
	mustCheck(t, e)
	got := contents(t, e)
	keysOf := func(m map[string][]byte) []string {
		var ks []string
		for k := range m {
			ks = append(ks, k)
		}
		sort.Strings(ks)
		return ks
	}
	if fmt.Sprint(keysOf(got)) != "[again again2]" {
		t.Fatalf("contents %v", keysOf(got))
	}
}
