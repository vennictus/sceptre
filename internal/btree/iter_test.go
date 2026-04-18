package btree

import (
	"fmt"
	"testing"
)

func TestIteratorSeekFirstAndNext(t *testing.T) {
	t.Parallel()

	tree := mustBuildIterTree(t)
	it := tree.Iterator()

	if err := it.SeekFirst(); err != nil {
		t.Fatalf("SeekFirst() error = %v", err)
	}

	var got []string
	for it.Valid() {
		cell, err := it.Deref()
		if err != nil {
			t.Fatalf("Deref() error = %v", err)
		}
		got = append(got, string(cell.Key))
		if err := it.Next(); err != nil {
			t.Fatalf("Next() error = %v", err)
		}
	}

	want := iterKeys(24)
	assertKeySequence(t, got, want)
}

func TestIteratorSeekLastAndPrev(t *testing.T) {
	t.Parallel()

	tree := mustBuildIterTree(t)
	it := tree.Iterator()

	if err := it.SeekLast(); err != nil {
		t.Fatalf("SeekLast() error = %v", err)
	}

	var got []string
	for it.Valid() {
		cell, err := it.Deref()
		if err != nil {
			t.Fatalf("Deref() error = %v", err)
		}
		got = append(got, string(cell.Key))
		if err := it.Prev(); err != nil {
			t.Fatalf("Prev() error = %v", err)
		}
	}

	want := reverseStrings(iterKeys(24))
	assertKeySequence(t, got, want)
}

func TestIteratorSeekGEAndSeekLE(t *testing.T) {
	t.Parallel()

	tree := mustBuildIterTree(t)
	it := tree.Iterator()

	if err := it.SeekGE([]byte("005")); err != nil {
		t.Fatalf("SeekGE() error = %v", err)
	}
	assertIterCell(t, it, "005", "v005")

	if err := it.SeekGE([]byte("005a")); err != nil {
		t.Fatalf("SeekGE() error = %v", err)
	}
	assertIterCell(t, it, "006", "v006")

	if err := it.SeekGE([]byte("999")); err != nil {
		t.Fatalf("SeekGE() error = %v", err)
	}
	if it.Valid() {
		t.Fatal("iterator valid after seeking past end, want invalid")
	}

	if err := it.SeekLE([]byte("005")); err != nil {
		t.Fatalf("SeekLE() error = %v", err)
	}
	assertIterCell(t, it, "005", "v005")

	if err := it.SeekLE([]byte("005a")); err != nil {
		t.Fatalf("SeekLE() error = %v", err)
	}
	assertIterCell(t, it, "005", "v005")

	if err := it.SeekLE([]byte("000")); err != nil {
		t.Fatalf("SeekLE() error = %v", err)
	}
	assertIterCell(t, it, "000", "v000")

	if err := it.SeekLE([]byte("-1")); err != nil {
		t.Fatalf("SeekLE() error = %v", err)
	}
	if it.Valid() {
		t.Fatal("iterator valid after seeking before start, want invalid")
	}
}

func TestIteratorDerefInvalid(t *testing.T) {
	t.Parallel()

	tree, err := NewTree(64)
	if err != nil {
		t.Fatalf("NewTree() error = %v", err)
	}
	it := tree.Iterator()

	if err := it.SeekFirst(); err != nil {
		t.Fatalf("SeekFirst() error = %v", err)
	}
	if _, err := it.Deref(); err != ErrIteratorInvalid {
		t.Fatalf("Deref() error = %v, want %v", err, ErrIteratorInvalid)
	}
}

func TestIteratorSurvivesMultiLevelTraversal(t *testing.T) {
	t.Parallel()

	tree, err := NewTree(48)
	if err != nil {
		t.Fatalf("NewTree() error = %v", err)
	}
	for i := 0; i < 80; i++ {
		key := fmt.Sprintf("%03d", i)
		if err := tree.Insert([]byte(key), []byte("v"+key)); err != nil {
			t.Fatalf("Insert(%q) error = %v", key, err)
		}
	}

	it := tree.Iterator()
	if err := it.SeekGE([]byte("031")); err != nil {
		t.Fatalf("SeekGE() error = %v", err)
	}

	for i := 31; i < 80; i++ {
		key := fmt.Sprintf("%03d", i)
		assertIterCell(t, it, key, "v"+key)
		if err := it.Next(); err != nil {
			t.Fatalf("Next() error = %v", err)
		}
	}

	if it.Valid() {
		t.Fatal("iterator valid after exhausting forward traversal, want invalid")
	}

	if err := it.SeekLE([]byte("031")); err != nil {
		t.Fatalf("SeekLE() error = %v", err)
	}
	for i := 31; i >= 0; i-- {
		key := fmt.Sprintf("%03d", i)
		assertIterCell(t, it, key, "v"+key)
		if err := it.Prev(); err != nil {
			t.Fatalf("Prev() error = %v", err)
		}
	}
}

func mustBuildIterTree(t *testing.T) *Tree {
	t.Helper()

	tree, err := NewTree(48)
	if err != nil {
		t.Fatalf("NewTree() error = %v", err)
	}

	for i := 0; i < 24; i++ {
		key := fmt.Sprintf("%03d", i)
		if err := tree.Insert([]byte(key), []byte("v"+key)); err != nil {
			t.Fatalf("Insert(%q) error = %v", key, err)
		}
	}
	return tree
}

func assertIterCell(t *testing.T, it *Iterator, wantKey, wantValue string) {
	t.Helper()

	if !it.Valid() {
		t.Fatalf("iterator invalid, want key %q", wantKey)
	}
	cell, err := it.Deref()
	if err != nil {
		t.Fatalf("Deref() error = %v", err)
	}
	if got := string(cell.Key); got != wantKey {
		t.Fatalf("Deref().Key = %q, want %q", got, wantKey)
	}
	if got := string(cell.Value); got != wantValue {
		t.Fatalf("Deref().Value = %q, want %q", got, wantValue)
	}
}

func iterKeys(n int) []string {
	out := make([]string, n)
	for i := 0; i < n; i++ {
		out[i] = fmt.Sprintf("%03d", i)
	}
	return out
}

func reverseStrings(in []string) []string {
	out := append([]string(nil), in...)
	for i, j := 0, len(out)-1; i < j; i, j = i+1, j-1 {
		out[i], out[j] = out[j], out[i]
	}
	return out
}

func assertKeySequence(t *testing.T, got, want []string) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("sequence length = %d, want %d", len(got), len(want))
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("sequence[%d] = %q, want %q", i, got[i], want[i])
		}
	}
}
