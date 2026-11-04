package tx

import (
	"fmt"
	"math/rand"
	"slices"
	"sort"
	"testing"
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
