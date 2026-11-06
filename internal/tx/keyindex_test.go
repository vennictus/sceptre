package tx

import (
	"fmt"
	"math/rand"
	"path/filepath"
	"slices"
	"sort"
	"testing"

	"github.com/vennictus/sceptre/internal/kv"
)

func (x *keyIndex) flat() []string {
	var out []string
	for _, blk := range x.blocks {
		out = append(out, blk...)
	}
	return out
}

func TestKeyIndexAgainstModel(t *testing.T) {
	t.Parallel()
	rng := rand.New(rand.NewSource(3))
	var x keyIndex
	model := map[string]bool{}
	for op := 0; op < 60000; op++ {
		key := fmt.Sprintf("k%05d", rng.Intn(8000))
		if rng.Intn(4) == 0 {
			x.remove(key)
			delete(model, key)
		} else if !model[key] {
			x.insert(key)
			model[key] = true
		}
		if op%997 != 0 {
			continue
		}
		want := make([]string, 0, len(model))
		for k := range model {
			want = append(want, k)
		}
		sort.Strings(want)
		if got := x.flat(); !slices.Equal(got, want) {
			t.Fatalf("op %d: index holds %d keys, model %d (or order differs)", op, len(got), len(want))
		}
		for _, blk := range x.blocks {
			if len(blk) == 0 || len(blk) >= 2*keyBlock {
				t.Fatalf("op %d: block of %d keys", op, len(blk))
			}
		}
		for probe := 0; probe < 100; probe++ {
			target := fmt.Sprintf("k%05d", rng.Intn(8100))
			b, i := x.find(target)
			j := sort.SearchStrings(want, target)
			if (j == len(want)) != !x.valid(b, i) || j < len(want) && x.blocks[b][i] != want[j] {
				t.Fatalf("op %d: find(%s) disagrees with the model", op, target)
			}
		}
	}
	if len(x.blocks) < 20 {
		t.Fatalf("only %d blocks: the test never exercised splits", len(x.blocks))
	}
}

// openCursor is a cursor under test and the view it must keep reading: the
// transaction's state when it was opened.
type openCursor struct {
	c    kv.Cursor
	keys []string
	view map[string]string
	at   int // index into keys of the cursor's current key
}

// Cursors read pending writes as of when they were opened, through writes,
// savepoint rollbacks and splits of the key index while they stay open.
func TestCursorsKeepTheirViewWhileTheTxChanges(t *testing.T) {
	t.Parallel()
	store, err := kv.Open(filepath.Join(t.TempDir(), "db"), kv.Options{PageSize: 1024})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	rng := rand.New(rand.NewSource(5))
	var base []kv.Mutation
	committed := map[string]string{}
	for i := 0; i < 500; i++ {
		k := fmt.Sprintf("k%04d", rng.Intn(3000))
		committed[k] = "base"
		base = append(base, kv.Put([]byte(k), []byte("base")))
	}
	if err := store.Apply(base); err != nil {
		t.Fatal(err)
	}
	tx := Begin(store)
	defer tx.Abort()
	view := map[string]string{}
	for k, v := range committed {
		view[k] = v
	}
	var marks []int
	var saved []map[string]string
	var open []*openCursor
	clone := func(m map[string]string) map[string]string {
		out := make(map[string]string, len(m))
		for k, v := range m {
			out[k] = v
		}
		return out
	}
	for op := 0; op < 20000; op++ {
		switch r := rng.Intn(100); {
		case r < 55:
			k := fmt.Sprintf("k%04d", rng.Intn(3000))
			v := fmt.Sprintf("v%d", op)
			if err := tx.Set([]byte(k), []byte(v)); err != nil {
				t.Fatal(err)
			}
			view[k] = v
		case r < 70:
			k := fmt.Sprintf("k%04d", rng.Intn(3000))
			if _, err := tx.Del([]byte(k)); err != nil {
				t.Fatal(err)
			}
			delete(view, k)
		case r < 74:
			m, err := tx.Savepoint()
			if err != nil {
				t.Fatal(err)
			}
			marks, saved = append(marks, m), append(saved, clone(view))
		case r < 78 && len(marks) > 0:
			if err := tx.RollbackTo(marks[len(marks)-1]); err != nil {
				t.Fatal(err)
			}
			view = clone(saved[len(saved)-1])
		case r < 80 && len(marks) > 0:
			if err := tx.Release(); err != nil {
				t.Fatal(err)
			}
			marks, saved = marks[:len(marks)-1], saved[:len(saved)-1]
		case r < 84 && len(open) < 3:
			oc := &openCursor{c: tx.ScanIterator(), view: clone(view)}
			for k := range oc.view {
				oc.keys = append(oc.keys, k)
			}
			sort.Strings(oc.keys)
			target := fmt.Sprintf("k%04d", rng.Intn(3100))
			if err := oc.c.SeekGE([]byte(target)); err != nil {
				t.Fatal(err)
			}
			oc.at = sort.SearchStrings(oc.keys, target)
			open = append(open, oc)
		case r < 96 && len(open) > 0:
			oc := open[rng.Intn(len(open))]
			for step := 0; step < 1+rng.Intn(40); step++ {
				if oc.at == len(oc.keys) {
					if oc.c.Valid() {
						t.Fatalf("op %d: cursor valid past the end of its view", op)
					}
					break
				}
				k, v, err := oc.c.Deref()
				if err != nil || string(k) != oc.keys[oc.at] || string(v) != oc.view[oc.keys[oc.at]] {
					t.Fatalf("op %d: cursor at %q=%q (%v), its view has %q=%q", op, k, v, err, oc.keys[oc.at], oc.view[oc.keys[oc.at]])
				}
				if err := oc.c.Next(); err != nil {
					t.Fatal(err)
				}
				oc.at++
			}
		case len(open) > 0:
			i := rng.Intn(len(open))
			open[i].c.Close()
			open = slices.Delete(open, i, i+1)
		}
	}
	if len(tx.index.blocks) < 5 {
		t.Fatalf("only %d index blocks: the test never exercised splits", len(tx.index.blocks))
	}
}
