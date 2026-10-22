package upgrade

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/vennictus/sceptre/internal/btree"
	"github.com/vennictus/sceptre/internal/kv"
	"github.com/vennictus/sceptre/internal/pager"
	"github.com/vennictus/sceptre/internal/storage"
	"github.com/vennictus/sceptre/internal/table"
)

// testdata/v1.db was written by the last v1 release of the sceptre CLI:
// users(id, name, age) with index users_age and 119 rows (id 13 deleted,
// id 7 updated to age 99), and notes(owner, seq, body) with a composite
// primary key and 39 rows (seq 5 deleted).
func copyFixture(t *testing.T) (string, []byte) {
	t.Helper()
	original, err := os.ReadFile(filepath.Join("testdata", "v1.db"))
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "app.db")
	if err := os.WriteFile(path, original, 0o600); err != nil {
		t.Fatal(err)
	}
	return path, original
}

func TestUpgradeRealV1Database(t *testing.T) {
	t.Parallel()
	path, original := copyFixture(t)
	if _, err := table.Open(path, table.Options{}); !errors.Is(err, kv.ErrUpgradeRequired) {
		t.Fatalf("opening v1 before upgrade: %v", err)
	}

	rep, err := Upgrade(path)
	if err != nil {
		t.Fatal(err)
	}
	if rep.Keys == 0 || rep.NewPageSize != 4096 || rep.Backup != path+".v1.bak" {
		t.Fatalf("report: %+v", rep)
	}
	backup, err := os.ReadFile(rep.Backup)
	if err != nil || !bytes.Equal(backup, original) {
		t.Fatalf("backup is not the untouched original: %v", err)
	}
	if _, err := os.Stat(path + ".upgrade"); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("temporary file left behind: %v", err)
	}

	db, err := table.Open(path, table.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	report, err := db.Check()
	if err != nil || !report.OK() {
		t.Fatalf("check after upgrade: %+v %v", report, err)
	}
	rows := map[string]int{}
	for _, tc := range report.Tables {
		rows[tc.Name] = tc.Rows
	}
	if rows["users"] != 119 || rows["notes"] != 39 {
		t.Fatalf("row counts after upgrade: %v", rows)
	}
	user7, ok, err := db.Get("users", table.NewRecord(map[string]table.Value{"id": table.Int64Value(7)}))
	if err != nil || !ok || user7.Values["age"].I64 != 99 || string(user7.Values["name"].Bytes) != "user-7" {
		t.Fatalf("updated row: %+v %v %v", user7, ok, err)
	}
	if _, ok, _ := db.Get("users", table.NewRecord(map[string]table.Value{"id": table.Int64Value(13)})); ok {
		t.Fatal("deleted row came back")
	}
	byAge, err := db.LookupIndex("users", "users_age", table.NewRecord(map[string]table.Value{"age": table.Int64Value(99)}))
	if err != nil || len(byAge) != 1 || byAge[0].Values["id"].I64 != 7 {
		t.Fatalf("index after upgrade: %v %v", byAge, err)
	}

	// The upgraded file is a normal v2 database: it takes new writes.
	if err := db.Insert("users", table.NewRecord(map[string]table.Value{
		"id": table.Int64Value(1000), "name": table.BytesValue(bytes.Repeat([]byte("n"), 20000)), "age": table.Int64Value(1),
	})); err != nil {
		t.Fatalf("write after upgrade: %v", err)
	}
}

func TestUpgradeRefusesCurrentFilesAndLeftovers(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	v2 := filepath.Join(dir, "v2.db")
	e, err := storage.Open(v2, storage.Options{})
	if err != nil {
		t.Fatal(err)
	}
	e.Close()
	if _, err := Upgrade(v2); !errors.Is(err, ErrAlreadyCurrent) {
		t.Fatalf("upgrade of a v2 file: %v", err)
	}

	path, original := copyFixture(t)
	temp, _ := Paths(path)
	if err := os.WriteFile(temp, []byte("stale"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := Upgrade(path); !errors.Is(err, ErrLeftover) {
		t.Fatalf("upgrade with a leftover temp file: %v", err)
	}
	if got, _ := os.ReadFile(path); !bytes.Equal(got, original) {
		t.Fatal("refused upgrade changed the original")
	}
}

func TestUpgradeFailureLeavesOriginalInPlace(t *testing.T) {
	t.Parallel()
	path, original := copyFixture(t)
	temp, backup := Paths(path)
	// A directory where the temporary file belongs makes the conversion fail
	// after the source is open, before anything is renamed.
	if err := os.Mkdir(temp, 0o700); err != nil {
		t.Fatal(err)
	}
	if _, err := Upgrade(path); err == nil {
		t.Fatal("upgrade succeeded despite an unwritable target")
	}
	if got, _ := os.ReadFile(path); !bytes.Equal(got, original) {
		t.Fatal("failed upgrade changed the original")
	}
	if _, err := os.Stat(backup); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("failed upgrade created a backup: %v", err)
	}
}

// writeV1 builds a v1 file the way the v1 engine committed one.
func writeV1(t *testing.T, path string, pageSize uint32, entries map[string][]byte) {
	t.Helper()
	p, err := pager.Open(path, pager.Options{Format: pager.FormatV1, PageSize: pageSize})
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	tree, err := btree.NewTree(int(pageSize))
	if err != nil {
		t.Fatal(err)
	}
	for k, v := range entries {
		if err := tree.Insert([]byte(k), v); err != nil {
			t.Fatal(err)
		}
	}
	snap, err := tree.Snapshot().RemapPageIDs(pager.MetaPageCount)
	if err != nil {
		t.Fatal(err)
	}
	for id, page := range snap.Pages {
		if err := p.WritePage(id, page); err != nil {
			t.Fatal(err)
		}
	}
	meta := p.Meta()
	meta.RootPage, meta.PageCount = snap.Root, snap.NextPage
	if err := p.PublishMeta(meta); err != nil {
		t.Fatal(err)
	}
}

func TestUpgradeMovesUnsupportedPageSizesToDefault(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "small.db")
	want := map[string][]byte{}
	for i := 0; i < 2000; i++ {
		want[fmt.Sprintf("key-%05d", i)] = []byte(fmt.Sprintf("value %d", i*i))
	}
	writeV1(t, path, 512, want)

	rep, err := Upgrade(path)
	if err != nil {
		t.Fatal(err)
	}
	if rep.OldPageSize != 512 || rep.NewPageSize != pager.DefaultPageSize || rep.Keys != len(want) {
		t.Fatalf("report: %+v", rep)
	}
	store, err := kv.Open(path, kv.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	it := store.Iterator()
	defer it.Close()
	n := 0
	for err := it.SeekFirst(); it.Valid(); err = it.Next() {
		if err != nil {
			t.Fatal(err)
		}
		k, v, _ := it.Deref()
		if !bytes.Equal(want[string(k)], v) {
			t.Fatalf("key %q: %q", k, v)
		}
		n++
	}
	if n != len(want) {
		t.Fatalf("%d keys after upgrade, want %d", n, len(want))
	}
}

func TestUpgradeMissingFileCreatesNothing(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "typo.db")
	if _, err := Upgrade(path); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("upgrade of a missing file: %v", err)
	}
	if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("upgrade created %s: %v", path, err)
	}
}
