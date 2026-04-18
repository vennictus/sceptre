package btree

import (
	"bytes"
	"fmt"
	"math/rand"
	"testing"
)

func TestTreeRandomizedInsertAndLookup(t *testing.T) {
	t.Parallel()

	seeds := []int64{1, 7, 42, 2026}
	pageSizes := []int{48, 64, 96}

	for _, pageSize := range pageSizes {
		pageSize := pageSize
		for _, seed := range seeds {
			seed := seed
			t.Run(fmt.Sprintf("page=%d/seed=%d", pageSize, seed), func(t *testing.T) {
				t.Parallel()

				tree, err := NewTree(pageSize)
				if err != nil {
					t.Fatalf("NewTree() error = %v", err)
				}

				keys := shuffledKeys(seed, 128)
				expected := make(map[string]string, len(keys))

				for _, key := range keys {
					value := "value-" + key
					if err := tree.Insert([]byte(key), []byte(value)); err != nil {
						t.Fatalf("Insert(%q) error = %v", key, err)
					}
					expected[key] = value
				}

				if err := validateTreeStructure(tree); err != nil {
					t.Fatalf("validateTreeStructure() error = %v", err)
				}

				for key, want := range expected {
					got, ok, err := tree.Get([]byte(key))
					if err != nil {
						t.Fatalf("Get(%q) error = %v", key, err)
					}
					if !ok {
						t.Fatalf("Get(%q) ok = false, want true", key)
					}
					if got := string(got); got != want {
						t.Fatalf("Get(%q) = %q, want %q", key, got, want)
					}
				}

				for _, missing := range []string{"missing", "z999", "000-miss"} {
					got, ok, err := tree.Get([]byte(missing))
					if err != nil {
						t.Fatalf("Get(%q) error = %v", missing, err)
					}
					if ok {
						t.Fatalf("Get(%q) = %q, want not found", missing, string(got))
					}
				}
			})
		}
	}
}

type treeValidation struct {
	maxKey []byte
	depth  int
}

func validateTreeStructure(tree *Tree) error {
	if tree.Root() == 0 {
		return nil
	}
	_, err := validateNode(tree, tree.Root())
	return err
}

func validateNode(tree *Tree, pageID uint64) (treeValidation, error) {
	node, err := tree.node(pageID)
	if err != nil {
		return treeValidation{}, err
	}
	if node.Count() == 0 {
		return treeValidation{}, fmt.Errorf("page %d: empty node", pageID)
	}
	if node.Lower() > node.Upper() {
		return treeValidation{}, fmt.Errorf("page %d: lower bound exceeds upper bound", pageID)
	}

	switch node.Type() {
	case NodeTypeLeaf:
		return validateLeafNode(node, pageID)
	case NodeTypeInternal:
		return validateInternalNode(tree, node, pageID)
	default:
		return treeValidation{}, fmt.Errorf("page %d: unknown node type %d", pageID, node.Type())
	}
}

func validateLeafNode(node Node, pageID uint64) (treeValidation, error) {
	entries, err := node.leafEntries()
	if err != nil {
		return treeValidation{}, err
	}
	if leafEntriesSize(entries) > len(node.Bytes()) {
		return treeValidation{}, fmt.Errorf("page %d: leaf entries exceed page size", pageID)
	}

	for i := 1; i < len(entries); i++ {
		if bytes.Compare(entries[i-1].Key, entries[i].Key) >= 0 {
			return treeValidation{}, fmt.Errorf("page %d: leaf keys out of order", pageID)
		}
	}

	return treeValidation{
		maxKey: cloneBytes(entries[len(entries)-1].Key),
		depth:  1,
	}, nil
}

func validateInternalNode(tree *Tree, node Node, pageID uint64) (treeValidation, error) {
	entries, err := node.internalEntries()
	if err != nil {
		return treeValidation{}, err
	}
	if internalEntriesSize(entries) > len(node.Bytes()) {
		return treeValidation{}, fmt.Errorf("page %d: internal entries exceed page size", pageID)
	}

	var childDepth int
	for i, entry := range entries {
		childInfo, err := validateNode(tree, entry.Child)
		if err != nil {
			return treeValidation{}, err
		}

		if i == 0 {
			childDepth = childInfo.depth
		} else if childInfo.depth != childDepth {
			return treeValidation{}, fmt.Errorf("page %d: child depths differ", pageID)
		}

		if !bytes.Equal(entry.Key, childInfo.maxKey) {
			return treeValidation{}, fmt.Errorf("page %d: child max key metadata mismatch", pageID)
		}
		if i > 0 && bytes.Compare(entries[i-1].Key, entry.Key) >= 0 {
			return treeValidation{}, fmt.Errorf("page %d: internal keys out of order", pageID)
		}
	}

	return treeValidation{
		maxKey: cloneBytes(entries[len(entries)-1].Key),
		depth:  childDepth + 1,
	}, nil
}

func shuffledKeys(seed int64, n int) []string {
	keys := make([]string, n)
	for i := range n {
		keys[i] = fmt.Sprintf("%03d", i)
	}

	rng := rand.New(rand.NewSource(seed))
	rng.Shuffle(len(keys), func(i, j int) {
		keys[i], keys[j] = keys[j], keys[i]
	})
	return keys
}
