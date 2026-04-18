package btree

import (
	"errors"
	"strconv"
	"testing"
)

func TestTreeInsertCreatesLeafRoot(t *testing.T) {
	t.Parallel()

	tree, err := NewTree(64)
	if err != nil {
		t.Fatalf("NewTree() error = %v", err)
	}

	if err := tree.Insert([]byte("b"), []byte("two")); err != nil {
		t.Fatalf("Insert() error = %v", err)
	}

	if got := tree.Root(); got == 0 {
		t.Fatal("Root() = 0, want non-zero root page")
	}

	root, err := tree.node(tree.Root())
	if err != nil {
		t.Fatalf("node() error = %v", err)
	}
	if got, want := root.Type(), NodeTypeLeaf; got != want {
		t.Fatalf("root.Type() = %v, want %v", got, want)
	}

	value, ok, err := tree.Get([]byte("b"))
	if err != nil {
		t.Fatalf("Get() error = %v", err)
	}
	if !ok {
		t.Fatal("Get() ok = false, want true")
	}
	if got, want := string(value), "two"; got != want {
		t.Fatalf("Get() value = %q, want %q", got, want)
	}
}

func TestTreeInsertSplitsLeafRoot(t *testing.T) {
	t.Parallel()

	tree, err := NewTree(48)
	if err != nil {
		t.Fatalf("NewTree() error = %v", err)
	}

	pairs := []struct {
		key string
		val string
	}{
		{"a", "one"},
		{"b", "two"},
		{"c", "three"},
		{"d", "four"},
	}

	for _, pair := range pairs {
		if err := tree.Insert([]byte(pair.key), []byte(pair.val)); err != nil {
			t.Fatalf("Insert(%q) error = %v", pair.key, err)
		}
	}

	root, err := tree.node(tree.Root())
	if err != nil {
		t.Fatalf("node() error = %v", err)
	}
	if got, want := root.Type(), NodeTypeInternal; got != want {
		t.Fatalf("root.Type() = %v, want %v", got, want)
	}
	if got, want := root.Count(), 2; got != want {
		t.Fatalf("root.Count() = %d, want %d", got, want)
	}

	for _, pair := range pairs {
		value, ok, err := tree.Get([]byte(pair.key))
		if err != nil {
			t.Fatalf("Get(%q) error = %v", pair.key, err)
		}
		if !ok {
			t.Fatalf("Get(%q) ok = false, want true", pair.key)
		}
		if got, want := string(value), pair.val; got != want {
			t.Fatalf("Get(%q) value = %q, want %q", pair.key, got, want)
		}
	}
}

func TestTreeInsertPropagatesSplitBeyondRoot(t *testing.T) {
	t.Parallel()

	tree, err := NewTree(48)
	if err != nil {
		t.Fatalf("NewTree() error = %v", err)
	}

	for i := 0; i < 16; i++ {
		key := strconv.Itoa(i)
		if err := tree.Insert([]byte(key), []byte("v"+key)); err != nil {
			t.Fatalf("Insert(%q) error = %v", key, err)
		}
	}

	root, err := tree.node(tree.Root())
	if err != nil {
		t.Fatalf("node() error = %v", err)
	}
	if got, want := root.Type(), NodeTypeInternal; got != want {
		t.Fatalf("root.Type() = %v, want %v", got, want)
	}

	firstChildRef, err := root.InternalCell(0)
	if err != nil {
		t.Fatalf("InternalCell(0) error = %v", err)
	}
	firstChild, err := tree.node(firstChildRef.Child)
	if err != nil {
		t.Fatalf("node() child error = %v", err)
	}
	if got, want := firstChild.Type(), NodeTypeInternal; got != want {
		t.Fatalf("first child type = %v, want %v", got, want)
	}

	for i := 0; i < 16; i++ {
		key := strconv.Itoa(i)
		value, ok, err := tree.Get([]byte(key))
		if err != nil {
			t.Fatalf("Get(%q) error = %v", key, err)
		}
		if !ok {
			t.Fatalf("Get(%q) ok = false, want true", key)
		}
		if got, want := string(value), "v"+key; got != want {
			t.Fatalf("Get(%q) value = %q, want %q", key, got, want)
		}
	}
}

func TestTreeInsertRejectsDuplicateKeys(t *testing.T) {
	t.Parallel()

	tree, err := NewTree(64)
	if err != nil {
		t.Fatalf("NewTree() error = %v", err)
	}

	if err := tree.Insert([]byte("dup"), []byte("one")); err != nil {
		t.Fatalf("Insert() first error = %v", err)
	}
	if err := tree.Insert([]byte("dup"), []byte("two")); !errors.Is(err, ErrDuplicateKey) {
		t.Fatalf("Insert() duplicate error = %v, want %v", err, ErrDuplicateKey)
	}
}
