package tx

import (
	"errors"
	"fmt"
	"math/rand"
	"testing"
)

func cloneState(m map[string]string) map[string]string {
	out := make(map[string]string, len(m))
	for k, v := range m {
		out[k] = v
	}
	return out
}

// txView reads everything the transaction sees through its merged cursor.
func txView(t *testing.T, tx *Tx) map[string]string {
	t.Helper()
	out := map[string]string{}
	c := tx.ScanIterator()
	for err := c.SeekFirst(); c.Valid(); err = c.Next() {
		if err != nil {
			t.Fatal(err)
		}
		k, v, err := c.Deref()
		if err != nil {
			t.Fatal(err)
		}
		out[string(k)] = string(v)
	}
	return out
}

func sameState(t *testing.T, step int, got, want map[string]string) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("step %d: %d keys, want %d", step, len(got), len(want))
	}
	for k, v := range want {
		if got[k] != v {
			t.Fatalf("step %d: %q = %q, want %q", step, k, got[k], v)
		}
	}
}

// Random writes, savepoints, rollbacks and releases against a model stack;
// the committed result must match the model.
func TestSavepointsMatchModel(t *testing.T) {
	t.Parallel()
	for seed := int64(1); seed <= 20; seed++ {
		rng := rand.New(rand.NewSource(seed))
		store := mustOpenKV(t)
		base := map[string]string{}
		for i := 0; i < 20; i++ {
			k, v := fmt.Sprintf("k%02d", i), fmt.Sprintf("base%d", i)
			if err := store.Set([]byte(k), []byte(v)); err != nil {
				t.Fatal(err)
			}
			base[k] = v
		}
		tx := Begin(store)
		model := cloneState(base)
		type mark struct {
			pos   int
			state map[string]string
		}
		var stack []mark
		for step := 0; step < 300; step++ {
			k := fmt.Sprintf("k%02d", rng.Intn(30))
			switch r := rng.Intn(20); {
			case r < 10:
				v := fmt.Sprintf("v%d", step)
				if err := tx.Set([]byte(k), []byte(v)); err != nil {
					t.Fatal(err)
				}
				model[k] = v
			case r < 14:
				if _, err := tx.Del([]byte(k)); err != nil {
					t.Fatal(err)
				}
				delete(model, k)
			case r < 16:
				pos, err := tx.Savepoint()
				if err != nil {
					t.Fatal(err)
				}
				stack = append(stack, mark{pos, cloneState(model)})
			case r < 18 && len(stack) > 0:
				i := rng.Intn(len(stack))
				// Rolling back to an outer savepoint closes the inner ones.
				for len(stack) > i+1 {
					if err := tx.Release(); err != nil {
						t.Fatal(err)
					}
					stack = stack[:len(stack)-1]
				}
				if err := tx.RollbackTo(stack[i].pos); err != nil {
					t.Fatal(err)
				}
				model = cloneState(stack[i].state)
			case len(stack) > 0:
				if err := tx.Release(); err != nil {
					t.Fatal(err)
				}
				stack = stack[:len(stack)-1]
			}
			if step%25 == 0 {
				sameState(t, step, txView(t, tx), model)
			}
		}
		sameState(t, -1, txView(t, tx), model)
		if err := tx.Commit(); err != nil {
			t.Fatal(err)
		}
		got := map[string]string{}
		for i := 0; i < 30; i++ {
			k := fmt.Sprintf("k%02d", i)
			v, ok, err := store.Get([]byte(k))
			if err != nil {
				t.Fatal(err)
			}
			if ok {
				got[k] = string(v)
			}
		}
		sameState(t, -2, got, model)
		store.Close()
	}
}

func TestSavepointErrors(t *testing.T) {
	t.Parallel()
	store := mustOpenKV(t)
	defer store.Close()
	tx := Begin(store)
	if err := tx.RollbackTo(0); !errors.Is(err, ErrNoSavepoint) {
		t.Fatalf("RollbackTo without savepoint: %v", err)
	}
	if err := tx.Release(); !errors.Is(err, ErrNoSavepoint) {
		t.Fatalf("Release without savepoint: %v", err)
	}
	if _, err := tx.Savepoint(); err != nil {
		t.Fatal(err)
	}
	if err := tx.RollbackTo(5); !errors.Is(err, ErrNoSavepoint) {
		t.Fatalf("RollbackTo past the log: %v", err)
	}
	tx.Abort()
	if _, err := tx.Savepoint(); !errors.Is(err, ErrClosed) {
		t.Fatalf("Savepoint after Abort: %v", err)
	}
}

func TestNoUndoLogWithoutSavepoints(t *testing.T) {
	t.Parallel()
	store := mustOpenKV(t)
	defer store.Close()
	tx := Begin(store)
	defer tx.Abort()
	for i := 0; i < 100; i++ {
		tx.Set([]byte(fmt.Sprint(i)), []byte("v"))
	}
	if len(tx.undo) != 0 {
		t.Fatalf("undo log has %d entries without a savepoint", len(tx.undo))
	}
	tx.Savepoint()
	tx.Set([]byte("a"), []byte("v"))
	tx.Release()
	if tx.undo != nil {
		t.Fatal("undo log kept after the last savepoint was released")
	}
}
