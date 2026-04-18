package kv

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestKVSetGetDelAcrossReopen(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "sceptre.db")
	store := mustOpenKV(t, path)

	if err := store.Set([]byte("alpha"), []byte("one")); err != nil {
		t.Fatalf("Set(alpha) error = %v", err)
	}
	if err := store.Set([]byte("beta"), []byte("two")); err != nil {
		t.Fatalf("Set(beta) error = %v", err)
	}
	if err := store.Set([]byte("alpha"), []byte("uno")); err != nil {
		t.Fatalf("Set(alpha overwrite) error = %v", err)
	}

	removed, err := store.Del([]byte("beta"))
	if err != nil {
		t.Fatalf("Del(beta) error = %v", err)
	}
	if !removed {
		t.Fatal("Del(beta) removed = false, want true")
	}

	if err := store.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	reopened := mustOpenKV(t, path)
	defer reopened.Close()

	assertKVValue(t, reopened, "alpha", "uno", true)
	assertKVValue(t, reopened, "beta", "", false)
}

func TestKVAtomicVisibilityFallsBackToPreviousMeta(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "sceptre.db")
	store := mustOpenKV(t, path)

	if err := store.Set([]byte("alpha"), []byte("v1")); err != nil {
		t.Fatalf("Set(alpha=v1) error = %v", err)
	}

	if err := store.Set([]byte("alpha"), []byte("v2")); err != nil {
		t.Fatalf("Set(alpha=v2) error = %v", err)
	}

	metaSlot := store.Pager().ActiveMetaSlot()
	pageSize := store.Pager().PageSize()
	dbPath := store.Pager().Path()

	if err := store.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	corruptMetaMagicByte(t, dbPath, metaSlot, pageSize)

	reopened := mustOpenKV(t, dbPath)
	defer reopened.Close()

	assertKVValue(t, reopened, "alpha", "v1", true)
}

func TestKVDeleteMissingKeyIsStable(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "sceptre.db")
	store := mustOpenKV(t, path)
	defer store.Close()

	removed, err := store.Del([]byte("missing"))
	if err != nil {
		t.Fatalf("Del(missing) error = %v", err)
	}
	if removed {
		t.Fatal("Del(missing) removed = true, want false")
	}

	assertKVValue(t, store, "missing", "", false)
}

func TestKVRecoveryAcrossPartialCommitBoundaries(t *testing.T) {
	stages := []struct {
		name      string
		stage     commitStage
		wantAlpha string
	}{
		{
			name:      "pages_written",
			stage:     commitStagePagesWritten,
			wantAlpha: "one",
		},
		{
			name:      "pages_synced",
			stage:     commitStagePagesSynced,
			wantAlpha: "one",
		},
		{
			name:      "meta_published",
			stage:     commitStageMetaPublished,
			wantAlpha: "uno",
		},
	}

	for _, tc := range stages {
		t.Run(tc.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "sceptre.db")
			store := mustOpenKV(t, path)

			if err := store.Set([]byte("alpha"), []byte("one")); err != nil {
				t.Fatalf("Set(alpha=one) error = %v", err)
			}
			if err := store.Set([]byte("beta"), []byte("two")); err != nil {
				t.Fatalf("Set(beta=two) error = %v", err)
			}

			store.commitHook = failAfterCommitStage(tc.stage)
			err := store.Set([]byte("alpha"), []byte("uno"))
			if !errors.Is(err, errCommitInterrupted) {
				t.Fatalf("Set(alpha=uno) error = %v, want %v", err, errCommitInterrupted)
			}

			if err := store.Close(); err != nil {
				t.Fatalf("Close() error = %v", err)
			}

			reopened := mustOpenKV(t, path)
			defer reopened.Close()

			assertKVValue(t, reopened, "alpha", tc.wantAlpha, true)
			assertKVValue(t, reopened, "beta", "two", true)
		})
	}
}

func TestKVDeleteRecoveryAcrossPartialCommitBoundaries(t *testing.T) {
	stages := []struct {
		name       string
		stage      commitStage
		wantBetaOK bool
	}{
		{
			name:       "pages_written",
			stage:      commitStagePagesWritten,
			wantBetaOK: true,
		},
		{
			name:       "pages_synced",
			stage:      commitStagePagesSynced,
			wantBetaOK: true,
		},
		{
			name:       "meta_published",
			stage:      commitStageMetaPublished,
			wantBetaOK: false,
		},
	}

	for _, tc := range stages {
		t.Run(tc.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "sceptre.db")
			store := mustOpenKV(t, path)

			if err := store.Set([]byte("alpha"), []byte("one")); err != nil {
				t.Fatalf("Set(alpha=one) error = %v", err)
			}
			if err := store.Set([]byte("beta"), []byte("two")); err != nil {
				t.Fatalf("Set(beta=two) error = %v", err)
			}

			store.commitHook = failAfterCommitStage(tc.stage)
			removed, err := store.Del([]byte("beta"))
			if !errors.Is(err, errCommitInterrupted) {
				t.Fatalf("Del(beta) error = %v, want %v", err, errCommitInterrupted)
			}
			if removed {
				t.Fatal("Del(beta) removed = true on interrupted commit, want false")
			}

			if err := store.Close(); err != nil {
				t.Fatalf("Close() error = %v", err)
			}

			reopened := mustOpenKV(t, path)
			defer reopened.Close()

			assertKVValue(t, reopened, "alpha", "one", true)
			assertKVValue(t, reopened, "beta", "two", tc.wantBetaOK)
		})
	}
}

func mustOpenKV(t *testing.T, path string) *KV {
	t.Helper()

	store, err := Open(path, Options{PageSize: 512})
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	return store
}

func assertKVValue(t *testing.T, store *KV, key, want string, wantOK bool) {
	t.Helper()

	value, ok, err := store.Get([]byte(key))
	if err != nil {
		t.Fatalf("Get(%q) error = %v", key, err)
	}
	if ok != wantOK {
		t.Fatalf("Get(%q) ok = %v, want %v", key, ok, wantOK)
	}
	if !wantOK {
		return
	}
	if got := string(value); got != want {
		t.Fatalf("Get(%q) value = %q, want %q", key, got, want)
	}
}

func corruptMetaMagicByte(t *testing.T, path string, slot int, pageSize uint32) {
	t.Helper()

	file, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		t.Fatalf("OpenFile() error = %v", err)
	}
	defer file.Close()

	offset := int64(slot) * int64(pageSize)
	buf := make([]byte, 1)
	if _, err := file.ReadAt(buf, offset); err != nil {
		t.Fatalf("ReadAt() error = %v", err)
	}
	buf[0] ^= 0xFF
	if _, err := file.WriteAt(buf, offset); err != nil {
		t.Fatalf("WriteAt() error = %v", err)
	}
	if err := file.Sync(); err != nil {
		t.Fatalf("Sync() error = %v", err)
	}
}
