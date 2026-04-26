package debug

import (
	"github.com/vennictus/sceptre/internal/kv"
	"path/filepath"
	"testing"
)

func TestInspectMetaTreeAndFreeList(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "sceptre.db")
	store, err := kv.Open(path, kv.Options{PageSize: 512})
	if err != nil {
		t.Fatalf("kv.Open() error = %v", err)
	}
	if err := store.Set([]byte("alpha"), []byte("one")); err != nil {
		t.Fatalf("Set(alpha) error = %v", err)
	}
	if err := store.Set([]byte("beta"), []byte("two")); err != nil {
		t.Fatalf("Set(beta) error = %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	meta, err := InspectMeta(path)
	if err != nil {
		t.Fatalf("InspectMeta() error = %v", err)
	}
	if meta.PageSize != 512 {
		t.Fatalf("PageSize = %d, want 512", meta.PageSize)
	}
	if meta.RootPage == 0 {
		t.Fatal("RootPage = 0, want non-zero")
	}

	tree, err := InspectTree(path)
	if err != nil {
		t.Fatalf("InspectTree() error = %v", err)
	}
	if len(tree.Entries) != 2 {
		t.Fatalf("tree entry count = %d, want 2", len(tree.Entries))
	}
	if string(tree.Entries[0].Key) != "alpha" || string(tree.Entries[0].Value) != "one" {
		t.Fatalf("first tree entry = %+v", tree.Entries[0])
	}

	free, err := InspectFreeList(path)
	if err != nil {
		t.Fatalf("InspectFreeList() error = %v", err)
	}
	if free.HeadPage == 0 {
		t.Fatal("HeadPage = 0, want non-zero after overwrite")
	}
}
