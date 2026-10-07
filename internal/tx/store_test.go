package tx

import (
	"errors"
	"path/filepath"
	"testing"

	"github.com/vennictus/sceptre/internal/kv"
)

func TestApplyFailureIsAtomicAndScanIncludesPendingChanges(t *testing.T) {
	t.Parallel()
	store, err := kv.Open(filepath.Join(t.TempDir(), "db"), kv.Options{PageSize: 512})
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
	if err := tx.Apply([]kv.Mutation{kv.Put([]byte("a"), []byte("bad")), kv.Put([]byte("oversized"), make([]byte, 2000))}); err == nil {
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
