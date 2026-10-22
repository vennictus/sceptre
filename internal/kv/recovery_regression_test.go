package kv

import (
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"github.com/vennictus/sceptre/internal/pager"
)

func TestFallbackAfterRepeatedReuseAndInterruptedCommit(t *testing.T) {
	t.Parallel()
	for _, stage := range []string{"pages-written", "pages-synced"} {
		for _, commits := range []int{2, 7, 20} {
			t.Run(fmt.Sprintf("%s/%d", stage, commits), func(t *testing.T) {
				path := filepath.Join(t.TempDir(), "db")
				s := mustOpenKV(t, path)
				for i := 1; i <= commits; i++ {
					if err := s.Set([]byte("x"), []byte(fmt.Sprint(i))); err != nil {
						t.Fatal(err)
					}
				}
				slot, size := s.Pager().ActiveMetaSlot(), s.Pager().PageSize()
				if err := s.Close(); err != nil {
					t.Fatal(err)
				}
				s, err := Open(path, Options{FailAfterCommitStage: stage})
				if err != nil {
					t.Fatal(err)
				}
				if err := s.Set([]byte("x"), []byte("uncommitted")); err == nil {
					t.Fatal("expected interruption")
				}
				if err := s.Close(); err != nil {
					t.Fatal(err)
				}
				f, err := os.OpenFile(path, os.O_RDWR, 0)
				if err != nil {
					t.Fatal(err)
				}
				if _, err := f.WriteAt([]byte{0xff}, int64(slot)*int64(size)); err != nil {
					t.Fatal(err)
				}
				if err := f.Close(); err != nil {
					t.Fatal(err)
				}
				s = mustOpenKV(t, path)
				assertKVValue(t, s, "x", fmt.Sprint(commits-1), true)
				if err := s.Set([]byte("y"), []byte("after-fallback")); err != nil {
					t.Fatal(err)
				}
				if err := s.Close(); err != nil {
					t.Fatal(err)
				}
				s = mustOpenKV(t, path)
				defer s.Close()
				assertKVValue(t, s, "x", fmt.Sprint(commits-1), true)
				assertKVValue(t, s, "y", "after-fallback", true)
			})
		}
	}
}

func TestIteratorRetainsSnapshotThroughWritesAndFailures(t *testing.T) {
	t.Parallel()
	s := mustOpenKV(t, filepath.Join(t.TempDir(), "db"))
	defer s.Close()
	for i := 0; i < 50; i++ {
		if err := s.Set([]byte(fmt.Sprintf("%03d", i)), []byte("before")); err != nil {
			t.Fatal(err)
		}
	}
	it := s.Iterator()
	if err := it.SeekFirst(); err != nil {
		t.Fatal(err)
	}
	var writes []Mutation
	for i := 0; i < 50; i++ {
		writes = append(writes, Delete([]byte(fmt.Sprintf("%03d", i))))
	}
	for i := 50; i < 100; i++ {
		writes = append(writes, Put([]byte(fmt.Sprintf("%03d", i)), []byte("after")))
	}
	if err := s.Apply(writes); err != nil {
		t.Fatal(err)
	}
	s.setCommitHook(failAfterCommitStage(commitStagePagesSynced))
	if err := s.Set([]byte("failed"), []byte("value")); err == nil {
		t.Fatal("expected failure")
	}
	count := 0
	for it.Valid() {
		key, val, err := it.Deref()
		if err != nil || string(key) != fmt.Sprintf("%03d", count) || string(val) != "before" {
			t.Fatalf("snapshot key=%s value=%s err=%v", key, val, err)
		}
		count++
		if err := it.Next(); err != nil {
			t.Fatal(err)
		}
	}
	if count != 50 {
		t.Fatalf("read %d rows", count)
	}
	if err := it.SeekLast(); err != nil {
		t.Fatal(err)
	}
	key, _, err := it.Deref()
	if err != nil || string(key) != "049" {
		t.Fatalf("reverse snapshot: %s %v", key, err)
	}
}

func TestPhysicalIOFailuresPreserveOrRequireRecovery(t *testing.T) {
	t.Parallel()
	for _, kind := range []string{"data_short", "data_sync", "meta_short", "meta_sync"} {
		t.Run(kind, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "db")
			s := mustOpenKV(t, path)
			if err := s.Set([]byte("x"), []byte("before")); err != nil {
				t.Fatal(err)
			}
			if err := s.Close(); err != nil {
				t.Fatal(err)
			}
			fault := errors.New("injected sync failure")
			fired, syncs := false, 0
			hooks := &pager.IOHooks{
				WriteAt: func(f *os.File, data []byte, offset int64) (int, error) {
					meta := offset < 2*1024
					if !fired && ((kind == "data_short" && !meta) || (kind == "meta_short" && meta)) {
						fired = true
						// Write a real torn metadata prefix, not just a synthetic error.
						n, err := f.WriteAt(data[:32], offset)
						return n, err
					}
					return f.WriteAt(data, offset)
				},
				Sync: func(f *os.File) error {
					syncs++
					if !fired && ((kind == "data_sync" && syncs == 1) || (kind == "meta_sync" && syncs == 2)) {
						fired = true
						return fault
					}
					return f.Sync()
				},
			}
			s, err := Open(path, Options{IO: hooks})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { s.Close() })
			err = s.Set([]byte("x"), []byte("after"))
			if err == nil || !fired {
				t.Fatalf("missing fault: %v", err)
			}
			if kind == "meta_short" || kind == "meta_sync" {
				if !errors.Is(err, ErrRecoveryRequired) {
					t.Fatalf("missing uncertain outcome: %v", err)
				}
				if _, _, err := s.Get([]byte("x")); !errors.Is(err, ErrRecoveryRequired) {
					t.Fatalf("reads allowed: %v", err)
				}
				if err := s.Set([]byte("y"), []byte("forbidden")); !errors.Is(err, ErrRecoveryRequired) {
					t.Fatalf("writes allowed: %v", err)
				}
				if err := s.Iterator().SeekFirst(); !errors.Is(err, ErrRecoveryRequired) {
					t.Fatalf("iterator allowed: %v", err)
				}
			} else {
				if kind == "data_short" && !errors.Is(err, io.ErrShortWrite) {
					t.Fatalf("short write error: %v", err)
				}
				assertKVValue(t, s, "x", "before", true)
				if err := s.Set([]byte("y"), []byte("continued")); err != nil {
					t.Fatal(err)
				}
			}
			if err := s.Close(); err != nil {
				t.Fatal(err)
			}
			s = mustOpenKV(t, path)
			defer s.Close()
			want := "before"
			if kind == "meta_sync" {
				want = "after"
			}
			assertKVValue(t, s, "x", want, true)
			if err := s.Set([]byte("z"), []byte("recovered")); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestAbruptProcessExitReleasesOwnershipAndRecovers(t *testing.T) {
	if os.Getenv("SCEPTRE_CRASH_CHILD") == "1" {
		s, err := Open(os.Getenv("SCEPTRE_CRASH_DB"), Options{FailAfterCommitStage: os.Getenv("SCEPTRE_CRASH_STAGE")})
		if err != nil {
			os.Exit(2)
		}
		_ = s.Set([]byte("x"), []byte("after"))
		os.Exit(0) // Deliberately bypass cleanup to exercise OS ownership recovery.
	}
	t.Parallel()
	for _, stage := range []string{"", "pages-written", "pages-synced", "meta-published"} {
		t.Run(stage, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "db")
			s := mustOpenKV(t, path)
			if err := s.Set([]byte("x"), []byte("before")); err != nil {
				t.Fatal(err)
			}
			if err := s.Close(); err != nil {
				t.Fatal(err)
			}
			cmd := exec.Command(os.Args[0], "-test.run=^TestAbruptProcessExitReleasesOwnershipAndRecovers$")
			cmd.Env = append(os.Environ(), "SCEPTRE_CRASH_CHILD=1", "SCEPTRE_CRASH_DB="+path, "SCEPTRE_CRASH_STAGE="+stage)
			if out, err := cmd.CombinedOutput(); err != nil {
				t.Fatalf("child: %v %s", err, out)
			}
			s = mustOpenKV(t, path)
			defer s.Close()
			want := "before"
			if stage == "" || stage == "meta-published" {
				want = "after"
			}
			assertKVValue(t, s, "x", want, true)
			if err := s.Set([]byte("y"), []byte("restart")); err != nil {
				t.Fatal(err)
			}
		})
	}
}
