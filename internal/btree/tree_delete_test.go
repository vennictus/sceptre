package btree

import (
	"fmt"
	"testing"
)

func TestTreeDeleteMissingKeyFromEmptyTree(t *testing.T) {
	t.Parallel()

	tree, err := NewTree(64)
	if err != nil {
		t.Fatalf("NewTree() error = %v", err)
	}

	removed, err := tree.Delete([]byte("missing"))
	if err != nil {
		t.Fatalf("Delete() error = %v", err)
	}
	if removed {
		t.Fatal("Delete() removed = true, want false")
	}
	if got := tree.Root(); got != 0 {
		t.Fatalf("Root() = %d, want 0", got)
	}
}

func TestTreeDeleteRemovesLeafKey(t *testing.T) {
	t.Parallel()

	tree, err := NewTree(64)
	if err != nil {
		t.Fatalf("NewTree() error = %v", err)
	}

	for _, key := range []string{"a", "b", "c"} {
		if err := tree.Insert([]byte(key), []byte("v"+key)); err != nil {
			t.Fatalf("Insert(%q) error = %v", key, err)
		}
	}

	removed, err := tree.Delete([]byte("b"))
	if err != nil {
		t.Fatalf("Delete() error = %v", err)
	}
	if !removed {
		t.Fatal("Delete() removed = false, want true")
	}

	if err := validateTreeStructure(tree); err != nil {
		t.Fatalf("validateTreeStructure() error = %v", err)
	}

	if value, ok, err := tree.Get([]byte("b")); err != nil {
		t.Fatalf("Get() error = %v", err)
	} else if ok {
		t.Fatalf("Get() = %q, want not found", string(value))
	}

	for _, key := range []string{"a", "c"} {
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

func TestTreeDeleteCollapsesRoot(t *testing.T) {
	t.Parallel()

	tree, err := NewTree(48)
	if err != nil {
		t.Fatalf("NewTree() error = %v", err)
	}

	for _, key := range []string{"a", "b", "c", "d"} {
		if err := tree.Insert([]byte(key), []byte("v"+key)); err != nil {
			t.Fatalf("Insert(%q) error = %v", key, err)
		}
	}

	for _, key := range []string{"a", "b", "c"} {
		removed, err := tree.Delete([]byte(key))
		if err != nil {
			t.Fatalf("Delete(%q) error = %v", key, err)
		}
		if !removed {
			t.Fatalf("Delete(%q) removed = false, want true", key)
		}
	}

	root, err := tree.node(tree.Root())
	if err != nil {
		t.Fatalf("node() error = %v", err)
	}
	if got, want := root.Type(), NodeTypeLeaf; got != want {
		t.Fatalf("root.Type() = %v, want %v", got, want)
	}
	if got, want := root.Count(), 1; got != want {
		t.Fatalf("root.Count() = %d, want %d", got, want)
	}

	value, ok, err := tree.Get([]byte("d"))
	if err != nil {
		t.Fatalf("Get() error = %v", err)
	}
	if !ok {
		t.Fatal("Get() ok = false, want true")
	}
	if got, want := string(value), "vd"; got != want {
		t.Fatalf("Get() value = %q, want %q", got, want)
	}
}

func TestTreeDeleteEmptiesTree(t *testing.T) {
	t.Parallel()

	tree, err := NewTree(64)
	if err != nil {
		t.Fatalf("NewTree() error = %v", err)
	}

	for _, key := range []string{"a", "b"} {
		if err := tree.Insert([]byte(key), []byte("v"+key)); err != nil {
			t.Fatalf("Insert(%q) error = %v", key, err)
		}
	}

	for _, key := range []string{"a", "b"} {
		removed, err := tree.Delete([]byte(key))
		if err != nil {
			t.Fatalf("Delete(%q) error = %v", key, err)
		}
		if !removed {
			t.Fatalf("Delete(%q) removed = false, want true", key)
		}
	}

	if got := tree.Root(); got != 0 {
		t.Fatalf("Root() = %d, want 0", got)
	}

	value, ok, err := tree.Get([]byte("a"))
	if err != nil {
		t.Fatalf("Get() error = %v", err)
	}
	if ok {
		t.Fatalf("Get() = %q, want not found", string(value))
	}
}

func TestTreeDeleteRepairsUnderflowAndPreservesLookups(t *testing.T) {
	t.Parallel()

	tree, err := NewTree(48)
	if err != nil {
		t.Fatalf("NewTree() error = %v", err)
	}

	expected := make(map[string]string, 18)
	for i := 0; i < 18; i++ {
		key := fmt.Sprintf("%03d", i)
		value := "v" + key
		if err := tree.Insert([]byte(key), []byte(value)); err != nil {
			t.Fatalf("Insert(%q) error = %v", key, err)
		}
		expected[key] = value
	}

	for _, key := range []string{"001", "002", "003", "004", "006", "007", "009", "010"} {
		removed, err := tree.Delete([]byte(key))
		if err != nil {
			t.Fatalf("Delete(%q) error = %v", key, err)
		}
		if !removed {
			t.Fatalf("Delete(%q) removed = false, want true", key)
		}
		delete(expected, key)

		if err := validateTreeStructure(tree); err != nil {
			t.Fatalf("validateTreeStructure() after deleting %q = %v", key, err)
		}
	}

	for key, want := range expected {
		value, ok, err := tree.Get([]byte(key))
		if err != nil {
			t.Fatalf("Get(%q) error = %v", key, err)
		}
		if !ok {
			t.Fatalf("Get(%q) ok = false, want true", key)
		}
		if got := string(value); got != want {
			t.Fatalf("Get(%q) = %q, want %q", key, got, want)
		}
	}
}
