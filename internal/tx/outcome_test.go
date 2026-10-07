package tx

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/vennictus/sceptre/internal/kv"
	"github.com/vennictus/sceptre/internal/pager"
)

func TestPublishedErrorRecordsConflictsAndClosesWriter(t *testing.T) {
	t.Parallel()
	for _, managed := range []bool{false, true} {
		t.Run(map[bool]string{false: "unmanaged", true: "managed"}[managed], func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "db")
			s, err := kv.Open(path, kv.Options{})
			if err != nil {
				t.Fatal(err)
			}
			if err := s.Set([]byte("x"), []byte("before")); err != nil {
				t.Fatal(err)
			}
			if err := s.Close(); err != nil {
				t.Fatal(err)
			}
			s, err = kv.Open(path, kv.Options{FailAfterCommitStage: "meta-published"})
			if err != nil {
				t.Fatal(err)
			}
			defer s.Close()
			m := NewManager(s)
			reader := m.Begin()
			assertTxValue(t, reader, "x", "before", true)
			writer := Begin(s)
			if managed {
				writer = m.Begin()
			}
			if err := writer.Set([]byte("x"), []byte("after")); err != nil {
				t.Fatal(err)
			}
			if err := writer.Commit(); !errors.Is(err, kv.ErrCommitCommitted) {
				t.Fatalf("commit error: %v", err)
			}
			if err := writer.Commit(); !errors.Is(err, ErrClosed) {
				t.Fatalf("writer still open: %v", err)
			}
			assertKVValue(t, s, "x", "after", true)
			if managed {
				if m.Version() != 1 || len(m.ActiveSnapshots()) != 1 {
					t.Fatalf("manager version=%d active=%v", m.Version(), m.ActiveSnapshots())
				}
				if err := reader.Set([]byte("y"), []byte("derived-from-before")); err != nil {
					t.Fatal(err)
				}
				if err := reader.Commit(); !errors.Is(err, ErrConflict) {
					t.Fatalf("stale reader committed: %v", err)
				}
				assertKVValue(t, s, "y", "", false)
			} else {
				reader.Abort()
			}
		})
	}
}

func TestUncertainPublicationStopsActiveTransactions(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "db")
	s, err := kv.Open(path, kv.Options{})
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Set([]byte("x"), []byte("before")); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	syncs := 0
	s, err = kv.Open(path, kv.Options{IO: &pager.IOHooks{Sync: func(f *os.File) error {
		syncs++
		if syncs == 2 {
			return errors.New("injected metadata sync error")
		}
		return f.Sync()
	}}})
	if err != nil {
		t.Fatal(err)
	}
	m := NewManager(s)
	reader, writer := m.Begin(), m.Begin()
	if err := writer.Set([]byte("x"), []byte("after")); err != nil {
		t.Fatal(err)
	}
	if err := writer.Commit(); !errors.Is(err, kv.ErrRecoveryRequired) {
		t.Fatalf("outcome: %v", err)
	}
	if err := writer.Commit(); !errors.Is(err, ErrClosed) {
		t.Fatalf("writer not closed: %v", err)
	}
	if _, _, err := reader.Get([]byte("x")); !errors.Is(err, kv.ErrRecoveryRequired) {
		t.Fatalf("stale read permitted: %v", err)
	}
	if err := reader.Set([]byte("y"), []byte("bad")); !errors.Is(err, kv.ErrRecoveryRequired) {
		t.Fatalf("stale write permitted: %v", err)
	}
	reader.Abort()
	if len(m.ActiveSnapshots()) != 0 {
		t.Fatal("active transaction leaked")
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s, err = kv.Open(path, kv.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	assertKVValue(t, s, "x", "after", true)
	assertKVValue(t, s, "y", "", false)
}

func TestExternalCommitCannotBypassManagedConflicts(t *testing.T) {
	t.Parallel()
	s := mustOpenKV(t)
	defer s.Close()
	if err := s.Set([]byte("x"), []byte("before")); err != nil {
		t.Fatal(err)
	}
	m := NewManager(s)
	reader := m.Begin()
	assertTxValue(t, reader, "x", "before", true)
	if err := s.Set([]byte("x"), []byte("outside")); err != nil {
		t.Fatal(err)
	}
	fresh := m.Begin() // Observes and records the external generation.
	assertTxValue(t, fresh, "x", "outside", true)
	if err := fresh.Set([]byte("z"), []byte("fresh")); err != nil {
		t.Fatal(err)
	}
	if err := fresh.Commit(); err != nil {
		t.Fatal(err)
	}
	if err := reader.Set([]byte("y"), []byte("stale")); err != nil {
		t.Fatal(err)
	}
	if err := reader.Commit(); !errors.Is(err, ErrConflict) {
		t.Fatalf("external commit bypassed conflict: %v", err)
	}
	assertKVValue(t, s, "y", "", false)
	if len(m.history) != 0 {
		t.Fatal("history retained after all snapshots closed")
	}
}

func TestHistoryRetainedOnlyForActiveSnapshots(t *testing.T) {
	t.Parallel()
	s := mustOpenKV(t)
	defer s.Close()
	m := NewManager(s)
	reader := m.Begin()
	for i := 0; i < 20; i++ {
		writer := m.Begin()
		if err := writer.Set([]byte("x"), []byte{byte(i)}); err != nil {
			t.Fatal(err)
		}
		if err := writer.Commit(); err != nil {
			t.Fatal(err)
		}
	}
	if len(m.history) != 20 {
		t.Fatalf("history needed by reader was dropped: %d", len(m.history))
	}
	reader.Abort()
	if len(m.history) != 0 {
		t.Fatal("history not pruned after oldest snapshot closed")
	}
	for i := 0; i < 20; i++ {
		writer := m.Begin()
		if err := writer.Set([]byte("x"), []byte{byte(i)}); err != nil {
			t.Fatal(err)
		}
		if err := writer.Commit(); err != nil {
			t.Fatal(err)
		}
		if len(m.history) != 0 {
			t.Fatal("history grows without readers")
		}
	}
}
