package btree

import (
	"bytes"
	"errors"
	"strings"
	"testing"
)

func TestDeleteSplitsRootWhenReplacementSeparatorsGrow(t *testing.T) {
	pages := make(map[uint64][]byte)
	keys := []string{"a" + strings.Repeat("q", 170), "c" + strings.Repeat("q", 170), "d" + strings.Repeat("q", 170)}
	for i, key := range keys {
		page := make([]byte, 512)
		n, _ := NewNode(page, NodeTypeLeaf)
		if err := n.AppendLeafCell([]byte(key), bytes.Repeat([]byte("v"), 160)); err != nil {
			t.Fatal(err)
		}
		if i == 0 {
			if err := n.AppendLeafCell([]byte("b"), nil); err != nil {
				t.Fatal(err)
			}
		}
		pages[uint64(i+1)] = page
	}
	root := make([]byte, 512)
	n, _ := NewNode(root, NodeTypeInternal)
	for i, key := range []string{"b", keys[1], keys[2]} {
		if err := n.AppendInternalCell(uint64(i+1), []byte(key)); err != nil {
			t.Fatal(err)
		}
	}
	pages[4] = root
	tree, err := NewTreeFromSnapshot(512, Snapshot{Root: 4, NextPage: 5, Pages: pages})
	if err != nil {
		t.Fatal(err)
	}
	if removed, err := tree.Delete([]byte("b")); err != nil || !removed {
		t.Fatalf("delete: removed=%v err=%v", removed, err)
	}
	if tree.Root() == 4 {
		t.Fatal("expected separator growth to split the root")
	}
	if err := ValidateSnapshot(tree.Snapshot()); err != nil {
		t.Fatal(err)
	}
	for _, key := range keys {
		val, ok, err := tree.Get([]byte(key))
		if err != nil || !ok || len(val) != 160 {
			t.Fatalf("remaining key lookup: ok=%v err=%v", ok, err)
		}
	}
	for _, key := range keys {
		if removed, err := tree.Delete([]byte(key)); err != nil || !removed {
			t.Fatalf("subsequent delete: removed=%v err=%v", removed, err)
		}
		if err := ValidateSnapshot(tree.Snapshot()); err != nil {
			t.Fatal(err)
		}
	}
}

func TestValidateSnapshotRejectsOverlappingChildRanges(t *testing.T) {
	pages := make(map[uint64][]byte)
	for id, keys := range map[uint64][]string{1: {"a", "m"}, 2: {"b", "z"}} {
		page := make([]byte, 512)
		n, err := NewNode(page, NodeTypeLeaf)
		if err != nil {
			t.Fatal(err)
		}
		for _, key := range keys {
			if err := n.AppendLeafCell([]byte(key), nil); err != nil {
				t.Fatal(err)
			}
		}
		pages[id] = page
	}
	root := make([]byte, 512)
	n, err := NewNode(root, NodeTypeInternal)
	if err != nil {
		t.Fatal(err)
	}
	if err := n.AppendInternalCell(1, []byte("m")); err != nil {
		t.Fatal(err)
	}
	if err := n.AppendInternalCell(2, []byte("z")); err != nil {
		t.Fatal(err)
	}
	pages[3] = root
	if err := ValidateSnapshot(Snapshot{Root: 3, Pages: pages}); !errors.Is(err, ErrCorruptNode) {
		t.Fatalf("overlapping ranges accepted: %v", err)
	}
}

func FuzzValidateTreeNeverPanics(f *testing.F) {
	page := make([]byte, 512)
	n, _ := NewNode(page, NodeTypeLeaf)
	_ = n.AppendLeafCell([]byte("x"), []byte("value"))
	f.Add(page)
	f.Add([]byte{})
	f.Add(make([]byte, 8))
	f.Fuzz(func(t *testing.T, data []byte) {
		if len(data) > 65535 {
			return
		}
		_, _ = ValidateTree(1, func(id uint64) ([]byte, error) {
			if id != 1 {
				return nil, ErrInvalidSnapshotPage
			}
			return data, nil
		})
	})
}
