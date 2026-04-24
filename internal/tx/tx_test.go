package tx

import (
	"errors"
	"path/filepath"
	"sceptre/internal/kv"
	"testing"
)

func TestTxCommitPublishesBufferedWrites(t *testing.T) {
	t.Parallel()

	store := mustOpenKV(t)
	defer store.Close()

	tx := Begin(store)
	if err := tx.Set([]byte("alpha"), []byte("one")); err != nil {
		t.Fatalf("Set(alpha) error = %v", err)
	}
	if err := tx.Set([]byte("beta"), []byte("two")); err != nil {
		t.Fatalf("Set(beta) error = %v", err)
	}

	assertTxValue(t, tx, "alpha", "one", true)
	assertKVValue(t, store, "alpha", "", false)

	if err := tx.Commit(); err != nil {
		t.Fatalf("Commit() error = %v", err)
	}

	assertKVValue(t, store, "alpha", "one", true)
	assertKVValue(t, store, "beta", "two", true)
}

func TestTxAbortDiscardsBufferedWrites(t *testing.T) {
	t.Parallel()

	store := mustOpenKV(t)
	defer store.Close()

	tx := Begin(store)
	if err := tx.Set([]byte("alpha"), []byte("one")); err != nil {
		t.Fatalf("Set(alpha) error = %v", err)
	}
	tx.Abort()

	assertKVValue(t, store, "alpha", "", false)
	if err := tx.Set([]byte("beta"), []byte("two")); !errors.Is(err, ErrClosed) {
		t.Fatalf("Set() after Abort error = %v, want %v", err, ErrClosed)
	}
}

func TestTxDeleteSeesPendingWrites(t *testing.T) {
	t.Parallel()

	store := mustOpenKV(t)
	defer store.Close()
	if err := store.Set([]byte("alpha"), []byte("one")); err != nil {
		t.Fatalf("Set(alpha) error = %v", err)
	}

	tx := Begin(store)
	removed, err := tx.Del([]byte("alpha"))
	if err != nil {
		t.Fatalf("Del(alpha) error = %v", err)
	}
	if !removed {
		t.Fatal("Del(alpha) removed = false, want true")
	}
	assertTxValue(t, tx, "alpha", "", false)
	assertKVValue(t, store, "alpha", "one", true)

	if err := tx.Commit(); err != nil {
		t.Fatalf("Commit() error = %v", err)
	}
	assertKVValue(t, store, "alpha", "", false)
}

func TestManagedTxReadsStableSnapshot(t *testing.T) {
	t.Parallel()

	store := mustOpenKV(t)
	defer store.Close()
	if err := store.Set([]byte("alpha"), []byte("one")); err != nil {
		t.Fatalf("Set(alpha) error = %v", err)
	}

	manager := NewManager(store)
	reader := manager.Begin()
	writer := manager.Begin()
	if err := writer.Set([]byte("alpha"), []byte("two")); err != nil {
		t.Fatalf("writer.Set(alpha) error = %v", err)
	}
	if err := writer.Commit(); err != nil {
		t.Fatalf("writer.Commit() error = %v", err)
	}

	assertTxValue(t, reader, "alpha", "one", true)
	reader.Abort()
}

func TestManagedTxDetectsReadWriteConflict(t *testing.T) {
	t.Parallel()

	store := mustOpenKV(t)
	defer store.Close()
	if err := store.Set([]byte("alpha"), []byte("one")); err != nil {
		t.Fatalf("Set(alpha) error = %v", err)
	}

	manager := NewManager(store)
	first := manager.Begin()
	assertTxValue(t, first, "alpha", "one", true)

	second := manager.Begin()
	if err := second.Set([]byte("alpha"), []byte("two")); err != nil {
		t.Fatalf("second.Set(alpha) error = %v", err)
	}
	if err := second.Commit(); err != nil {
		t.Fatalf("second.Commit() error = %v", err)
	}

	if err := first.Set([]byte("beta"), []byte("derived")); err != nil {
		t.Fatalf("first.Set(beta) error = %v", err)
	}
	if err := first.Commit(); !errors.Is(err, ErrConflict) {
		t.Fatalf("first.Commit() error = %v, want %v", err, ErrConflict)
	}
	assertKVValue(t, store, "beta", "", false)
}

func TestManagedTxAllowsNonConflictingCommits(t *testing.T) {
	t.Parallel()

	store := mustOpenKV(t)
	defer store.Close()
	if err := store.Set([]byte("alpha"), []byte("one")); err != nil {
		t.Fatalf("Set(alpha) error = %v", err)
	}

	manager := NewManager(store)
	first := manager.Begin()
	assertTxValue(t, first, "alpha", "one", true)

	second := manager.Begin()
	if err := second.Set([]byte("beta"), []byte("two")); err != nil {
		t.Fatalf("second.Set(beta) error = %v", err)
	}
	if err := second.Commit(); err != nil {
		t.Fatalf("second.Commit() error = %v", err)
	}

	if err := first.Set([]byte("gamma"), []byte("three")); err != nil {
		t.Fatalf("first.Set(gamma) error = %v", err)
	}
	if err := first.Commit(); err != nil {
		t.Fatalf("first.Commit() error = %v", err)
	}

	assertKVValue(t, store, "beta", "two", true)
	assertKVValue(t, store, "gamma", "three", true)
}

func TestManagerTracksActiveSnapshots(t *testing.T) {
	t.Parallel()

	store := mustOpenKV(t)
	defer store.Close()

	manager := NewManager(store)
	first := manager.Begin()
	second := manager.Begin()

	if got, want := len(manager.ActiveSnapshots()), 2; got != want {
		t.Fatalf("ActiveSnapshots() len = %d, want %d", got, want)
	}

	first.Abort()
	if got, want := len(manager.ActiveSnapshots()), 1; got != want {
		t.Fatalf("ActiveSnapshots() len after abort = %d, want %d", got, want)
	}

	if err := second.Commit(); err != nil {
		t.Fatalf("second.Commit() error = %v", err)
	}
	if got := len(manager.ActiveSnapshots()); got != 0 {
		t.Fatalf("ActiveSnapshots() len after commit = %d, want 0", got)
	}
}

func mustOpenKV(t *testing.T) *kv.KV {
	t.Helper()

	store, err := kv.Open(filepath.Join(t.TempDir(), "sceptre.db"), kv.Options{PageSize: 512})
	if err != nil {
		t.Fatalf("kv.Open() error = %v", err)
	}
	return store
}

func assertTxValue(t *testing.T, tx *Tx, key, want string, wantOK bool) {
	t.Helper()

	value, ok, err := tx.Get([]byte(key))
	if err != nil {
		t.Fatalf("tx.Get(%q) error = %v", key, err)
	}
	if ok != wantOK {
		t.Fatalf("tx.Get(%q) ok = %v, want %v", key, ok, wantOK)
	}
	if wantOK && string(value) != want {
		t.Fatalf("tx.Get(%q) = %q, want %q", key, string(value), want)
	}
}

func assertKVValue(t *testing.T, store *kv.KV, key, want string, wantOK bool) {
	t.Helper()

	value, ok, err := store.Get([]byte(key))
	if err != nil {
		t.Fatalf("kv.Get(%q) error = %v", key, err)
	}
	if ok != wantOK {
		t.Fatalf("kv.Get(%q) ok = %v, want %v", key, ok, wantOK)
	}
	if wantOK && string(value) != want {
		t.Fatalf("kv.Get(%q) = %q, want %q", key, string(value), want)
	}
}
