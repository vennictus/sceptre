package storage

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

// testdata/v2.db was written by the format v2 engine (freelist stored as a
// page chain) with 1024-byte pages: v2FixtureKeys keys in one commit, then
// the first v2FixtureDeleted keys deleted in a second commit, leaving a
// freelist that spans several chain pages.
const (
	v2FixtureKeys    = 200
	v2FixtureDeleted = 150
)

func v2FixtureKey(i int) string { return fmt.Sprintf("key%03d", i) }

// v2FixtureValue sizes values from 300 to 499 bytes, past the inline limit of
// a 1024-byte page, so each key also owns an overflow page.
func v2FixtureValue(i int) []byte { return bytes.Repeat([]byte{byte(i)}, 300+i) }

// v2FixtureContents is the data the fixture holds.
func v2FixtureContents() map[string][]byte {
	want := map[string][]byte{}
	for i := v2FixtureDeleted; i < v2FixtureKeys; i++ {
		want[v2FixtureKey(i)] = v2FixtureValue(i)
	}
	return want
}

// copyV2Fixture copies the fixture so tests never modify the committed file.
func copyV2Fixture(t *testing.T) string {
	t.Helper()
	data, err := os.ReadFile(filepath.Join("testdata", "v2.db"))
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "v2.db")
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestV2FixtureHoldsItsData(t *testing.T) {
	t.Parallel()
	e := openEnv(t, copyV2Fixture(t), Options{})
	defer e.Close()
	sameContents(t, contents(t, e), v2FixtureContents())
	if rep := mustCheck(t, e); rep.FreelistPages < 2 {
		t.Fatalf("fixture freelist fits one page; it should span several: %+v", rep)
	}
}
