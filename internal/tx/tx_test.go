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
