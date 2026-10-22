package tx

import (
	"bytes"
	"errors"
	"fmt"
	"math/rand"
	"path/filepath"
	"sort"
	"testing"

	"github.com/vennictus/sceptre/internal/kv"
)

func TestApplyFailureIsAtomicAndScanIncludesPendingChanges(t *testing.T) {
	t.Parallel()
	store, err := kv.Open(filepath.Join(t.TempDir(), "db"), kv.Options{PageSize: 1024})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	if err := store.Set([]byte("b"), []byte("base")); err != nil {
		t.Fatal(err)
	}
	tx := Begin(store)
	defer tx.Abort()
	if err := tx.Apply([]kv.Mutation{kv.Put([]byte("a"), []byte("pending")), kv.Delete([]byte("b")), kv.Put([]byte("c"), []byte("last"))}); err != nil {
		t.Fatal(err)
	}
	if err := tx.Apply([]kv.Mutation{kv.Put([]byte("a"), []byte("bad")), kv.Put(bytes.Repeat([]byte("k"), 2000), []byte("oversized key"))}); err == nil {
		t.Fatal("expected atomic failure")
	}
	iter := tx.ScanIterator()
	if err := iter.SeekFirst(); err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"a:pending", "c:last"} {
		k, v, err := iter.Deref()
		if err != nil || string(k)+":"+string(v) != want {
			t.Fatal(string(k), string(v), err)
		}
		if err := iter.Next(); err != nil {
			t.Fatal(err)
		}
	}
	if iter.Valid() {
		t.Fatal("extra key")
	}
	if err := tx.Apply([]kv.Mutation{kv.Put([]byte("a"), []byte("new"))}); err != nil {
		t.Fatal(err)
	}
	if err := iter.SeekFirst(); err != nil {
		t.Fatal(err)
	}
	_, v, _ := iter.Deref()
	if string(v) != "pending" {
		t.Fatal("iterator changed")
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	_, ok, err := store.Get([]byte("b"))
	if err != nil || ok {
		t.Fatal(ok, err)
	}
	if err := tx.ScanIterator().SeekGE(nil); !errors.Is(err, ErrClosed) {
		t.Fatal(err)
	}
}

func TestManagedScanConflictsWithPhantomInsert(t *testing.T) {
	t.Parallel()
	store, err := kv.Open(filepath.Join(t.TempDir(), "db"), kv.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	m := NewManager(store)
	reader, writer := m.Begin(), m.Begin()
	defer reader.Abort()
	defer writer.Abort()
	if err := reader.ScanIterator().SeekGE([]byte("missing")); err != nil {
		t.Fatal(err)
	}
	if err := writer.Set([]byte("phantom"), []byte("value")); err != nil {
		t.Fatal(err)
	}
	if err := writer.Commit(); err != nil {
		t.Fatal(err)
	}
	if err := reader.Commit(); !errors.Is(err, ErrConflict) {
		t.Fatal("phantom was not detected", err)
	}
}

// TestScanMergesPendingWritesAgainstModel drives random pending puts and
// deletes over a committed snapshot and checks every scan and seek against a
// map of the expected view.
func TestScanMergesPendingWritesAgainstModel(t *testing.T) {
	t.Parallel()
	store, err := kv.Open(filepath.Join(t.TempDir(), "db"), kv.Options{PageSize: 1024})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	rng := rand.New(rand.NewSource(11))
	committed := map[string]string{}
	var base []kv.Mutation
	for i := 0; i < 300; i++ {
		k := fmt.Sprintf("k%03d", rng.Intn(400))
		v := fmt.Sprintf("base-%d", i)
		committed[k] = v
		base = append(base, kv.Put([]byte(k), []byte(v)))
	}
	if err := store.Apply(base); err != nil {
		t.Fatal(err)
	}
	for round := 0; round < 20; round++ {
		tx := Begin(store)
		view := map[string]string{}
		for k, v := range committed {
			view[k] = v
		}
		for i := 0; i < 80; i++ {
			k := fmt.Sprintf("k%03d", rng.Intn(400))
			if rng.Intn(3) == 0 {
				if err := tx.Apply([]kv.Mutation{kv.Delete([]byte(k))}); err != nil {
					t.Fatal(err)
				}
				delete(view, k)
			} else {
				v := fmt.Sprintf("pending-%d-%d", round, i)
				if rng.Intn(10) == 0 {
					v = "" // empty pending values are still present
				}
				if err := tx.Apply([]kv.Mutation{kv.Put([]byte(k), []byte(v))}); err != nil {
					t.Fatal(err)
				}
				view[k] = v
			}
		}
		keys := make([]string, 0, len(view))
		for k := range view {
			keys = append(keys, k)
		}
		sort.Strings(keys)

		it := tx.ScanIterator()
		if err := it.SeekFirst(); err != nil {
			t.Fatal(err)
		}
		for _, k := range keys {
			gk, gv, err := it.Deref()
			if err != nil || string(gk) != k || string(gv) != view[k] {
				t.Fatalf("round %d scan at %q: got %q=%q err=%v", round, k, gk, gv, err)
			}
			if err := it.Next(); err != nil {
				t.Fatal(err)
			}
		}
		if it.Valid() {
			k, _, _ := it.Deref()
			t.Fatalf("round %d scan has extra key %q", round, k)
		}
		for probe := 0; probe < 50; probe++ {
			target := fmt.Sprintf("k%03d", rng.Intn(410))
			if err := it.SeekGE([]byte(target)); err != nil {
				t.Fatal(err)
			}
			i := sort.SearchStrings(keys, target)
			if i == len(keys) {
				if it.Valid() {
					t.Fatalf("SeekGE(%s) valid past end", target)
				}
				continue
			}
			if gk, _, _ := it.Deref(); string(gk) != keys[i] {
				t.Fatalf("SeekGE(%s) = %s, want %s", target, gk, keys[i])
			}
		}
		it.Close()
		if rng.Intn(2) == 0 {
			if err := tx.Commit(); err != nil {
				t.Fatal(err)
			}
			committed = view
		} else {
			tx.Abort()
		}
		got := map[string]string{}
		snap := store.Iterator()
		for err := snap.SeekFirst(); snap.Valid(); err = snap.Next() {
			if err != nil {
				t.Fatal(err)
			}
			k, v, _ := snap.Deref()
			got[string(k)] = string(v)
		}
		snap.Close()
		if fmt.Sprint(got) != fmt.Sprint(committed) {
			t.Fatalf("round %d: committed state diverged", round)
		}
	}
}
