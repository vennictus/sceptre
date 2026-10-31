package storage

import (
	"errors"
	"math/rand"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/vennictus/sceptre/internal/pager"
)

// powerSim tracks what a disk would keep on power loss: everything written
// before the last sync, plus any subset of later writes, possibly torn.
type powerSim struct {
	mu      sync.Mutex
	durable []byte
	pending []simWrite
	syncs   int
}

type simWrite struct {
	off  int64
	data []byte
}

func (s *powerSim) hooks() *pager.IOHooks {
	return &pager.IOHooks{
		WriteAt: func(f *os.File, b []byte, off int64) (int, error) {
			s.mu.Lock()
			s.pending = append(s.pending, simWrite{off, append([]byte(nil), b...)})
			s.mu.Unlock()
			return f.WriteAt(b, off)
		},
		Sync: func(*os.File) error {
			s.mu.Lock()
			defer s.mu.Unlock()
			for _, w := range s.pending {
				s.durable = apply(s.durable, w.off, w.data)
			}
			s.pending = nil
			s.syncs++
			return nil
		},
	}
}

func apply(img []byte, off int64, data []byte) []byte {
	if end := int(off) + len(data); end > len(img) {
		img = append(img, make([]byte, end-len(img))...)
	}
	copy(img[off:], data)
	return img
}

// crash writes what survives a power loss to path.
func (s *powerSim) crash(t *testing.T, rng *rand.Rand, path string) {
	t.Helper()
	s.mu.Lock()
	img := append([]byte(nil), s.durable...)
	for _, w := range s.pending {
		switch rng.Intn(4) {
		case 0: // lost
		case 1: // torn
			img = apply(img, w.off, w.data[:rng.Intn(len(w.data)+1)])
		default:
			img = apply(img, w.off, w.data)
		}
	}
	s.mu.Unlock()
	if err := os.WriteFile(path, img, 0o600); err != nil {
		t.Fatal(err)
	}
}

func sameMap(a, b map[string][]byte) bool {
	if len(a) != len(b) {
		return false
	}
	for k, v := range a {
		if w, ok := b[k]; !ok || string(w) != string(v) {
			return false
		}
	}
	return true
}

// After every commit, a simulated power loss must reopen at a committed state:
// the latest under full durability, the latest or the one before under normal.
func TestEnvPowerLossKeepsACommittedState(t *testing.T) {
	t.Parallel()
	for _, dur := range []Durability{DurabilityFull, DurabilityNormal} {
		t.Run(dur.String(), func(t *testing.T) {
			t.Parallel()
			rng := rand.New(rand.NewSource(11))
			dir := t.TempDir()
			sim := &powerSim{}
			e := openEnv(t, filepath.Join(dir, "db"), Options{PageSize: 1024, CacheBytes: 32 * 1024, Durability: dur, IO: sim.hooks()})
			defer e.Close()
			history := []map[string][]byte{{}}
			lostLatest := 0
			for round := 0; round < 80; round++ {
				w, err := e.BeginWrite()
				if err != nil {
					t.Fatal(err)
				}
				next := cloneModel(history[len(history)-1])
				randomOps(t, rng, w, next, 25)
				if err := w.Commit(); err != nil {
					t.Fatal(err)
				}
				history = append(history, next)

				for trial := 0; trial < 3; trial++ {
					crashPath := filepath.Join(dir, "crash.db")
					sim.crash(t, rng, crashPath)
					c, err := Open(crashPath, Options{})
					if err != nil {
						t.Fatalf("round %d: reopen after power loss: %v", round, err)
					}
					mustCheck(t, c)
					got := contents(t, c)
					c.Close()
					switch {
					case sameMap(got, history[len(history)-1]):
					case dur == DurabilityNormal && sameMap(got, history[len(history)-2]):
						lostLatest++
					default:
						t.Fatalf("round %d: power loss reopened at an unexpected state (%d keys)", round, len(got))
					}
				}
			}
			if dur == DurabilityNormal && lostLatest == 0 {
				t.Fatal("normal durability never lost the latest commit; the test is not exercising it")
			}
		})
	}
}

func TestEnvNormalDurabilitySavesOneSyncPerCommit(t *testing.T) {
	t.Parallel()
	count := func(dur Durability) int {
		sim := &powerSim{}
		e := openEnv(t, filepath.Join(t.TempDir(), "db"), Options{Durability: dur, IO: sim.hooks()})
		defer e.Close()
		before := sim.syncs
		for i := 0; i < 10; i++ {
			commit(t, e, func(w *WriteTxn) { put(t, w, "k", []byte{byte(i)}) })
		}
		return sim.syncs - before
	}
	if full, normal := count(DurabilityFull), count(DurabilityNormal); full != 20 || normal != 10 {
		t.Fatalf("syncs for 10 commits: full %d (want 20), normal %d (want 10)", full, normal)
	}
}

func TestEnvSyncAndCloseMakeNormalCommitsDurable(t *testing.T) {
	t.Parallel()
	rng := rand.New(rand.NewSource(3))
	for _, finish := range []string{"sync", "close"} {
		dir := t.TempDir()
		sim := &powerSim{}
		e := openEnv(t, filepath.Join(dir, "db"), Options{Durability: DurabilityNormal, IO: sim.hooks()})
		commit(t, e, func(w *WriteTxn) { put(t, w, "a", []byte("1")) })
		commit(t, e, func(w *WriteTxn) { put(t, w, "b", []byte("2")) })
		var err error
		if finish == "sync" {
			err = e.Sync()
		} else {
			err = e.Close()
		}
		if err != nil {
			t.Fatalf("%s: %v", finish, err)
		}
		crashPath := filepath.Join(dir, "crash.db")
		sim.crash(t, rng, crashPath)
		c := openEnv(t, crashPath, Options{})
		if got := contents(t, c); len(got) != 2 {
			t.Fatalf("%s: %d keys after power loss, want 2", finish, len(got))
		}
		c.Close()
		e.Close()
	}
}

func TestEnvNormalPageSyncFailureRequiresReopen(t *testing.T) {
	t.Parallel()
	fail := false
	hooks := &pager.IOHooks{Sync: func(f *os.File) error {
		if fail {
			return errors.New("injected sync failure")
		}
		return f.Sync()
	}}
	e := openEnv(t, filepath.Join(t.TempDir(), "db"), Options{Durability: DurabilityNormal, IO: hooks})
	defer e.Close()
	commit(t, e, func(w *WriteTxn) { put(t, w, "a", []byte("1")) })
	fail = true
	w, err := e.BeginWrite()
	if err != nil {
		t.Fatal(err)
	}
	put(t, w, "b", []byte("2"))
	if err := w.Commit(); !errors.Is(err, ErrRecoveryRequired) {
		t.Fatalf("Commit error = %v, want ErrRecoveryRequired", err)
	}
	if err := e.Err(); !errors.Is(err, ErrRecoveryRequired) {
		t.Fatalf("Err = %v, want ErrRecoveryRequired", err)
	}
}

func TestParseDurability(t *testing.T) {
	t.Parallel()
	for _, d := range []Durability{DurabilityFull, DurabilityNormal} {
		got, err := ParseDurability(d.String())
		if err != nil || got != d {
			t.Fatalf("ParseDurability(%q) = %v, %v", d, got, err)
		}
	}
	if _, err := ParseDurability("off"); !errors.Is(err, ErrInvalidDurability) {
		t.Fatalf("ParseDurability(off) error = %v", err)
	}
	if _, err := Open(filepath.Join(t.TempDir(), "db"), Options{Durability: 9}); !errors.Is(err, ErrInvalidDurability) {
		t.Fatalf("Open with bad durability error = %v", err)
	}
}
