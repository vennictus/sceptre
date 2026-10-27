package kv

import (
	"errors"
	"path/filepath"
	"testing"
)

func TestSnapshotStoreReadsOneStateAndRejectsWrites(t *testing.T) {
	t.Parallel()
	store := mustOpenKV(t, filepath.Join(t.TempDir(), "db"))
	defer store.Close()
	if err := store.Apply([]Mutation{Put([]byte("a"), []byte("1")), Put([]byte("b"), []byte("2"))}); err != nil {
		t.Fatal(err)
	}
	snap, err := store.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	defer snap.Release()
	var s Store = snap

	if err := store.Set([]byte("a"), []byte("changed")); err != nil {
		t.Fatal(err)
	}
	if v, ok, err := s.Get([]byte("a")); err != nil || !ok || string(v) != "1" {
		t.Fatalf("snapshot sees a later write: %q %v %v", v, ok, err)
	}
	if err := s.Apply([]Mutation{Put([]byte("c"), nil)}); !errors.Is(err, ErrReadOnly) {
		t.Fatalf("Apply on a snapshot: %v", err)
	}

	scan := func() []string {
		it := s.ScanIterator()
		defer it.Close()
		var keys []string
		for err := it.SeekFirst(); it.Valid(); err = it.Next() {
			if err != nil {
				t.Fatal(err)
			}
			k, _, err := it.Deref()
			if err != nil {
				t.Fatal(err)
			}
			keys = append(keys, string(k))
		}
		return keys
	}
	// Closing a scan cursor leaves the snapshot usable for the next one.
	for i := 0; i < 2; i++ {
		if got := scan(); len(got) != 2 || got[0] != "a" || got[1] != "b" {
			t.Fatalf("scan %d = %v", i, got)
		}
	}
	if store.Env().Info().Readers != 1 {
		t.Fatalf("snapshot should hold exactly one reader: %+v", store.Env().Info())
	}
}
