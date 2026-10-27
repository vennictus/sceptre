package storage

import "testing"

func TestLegacyFreeIndexGroupsByFreeingTransaction(t *testing.T) {
	l, _ := newLayout(1024)
	capacity := freeChunkCap(l)
	free := map[uint64]uint64{}
	for id := uint64(10); id < uint64(10+capacity+5); id++ {
		free[id] = 7 // more than one chunk freed by transaction 7
	}
	free[3], free[4] = 0, 2
	x := legacyFreeIndex(free, l)
	if len(x.pages) != len(free) {
		t.Fatalf("index lists %d pages, want %d", len(x.pages), len(free))
	}
	if r := x.recs[string(freeKey(7, 1))]; r == nil || len(r.pages) != 5 {
		t.Fatalf("second chunk of transaction 7: %+v", r)
	}
	if x.nextSeq(7) != 2 || x.nextSeq(0) != 1 || x.nextSeq(99) != 0 {
		t.Fatalf("nextSeq: %d %d %d", x.nextSeq(7), x.nextSeq(0), x.nextSeq(99))
	}
}

func TestFreeIndexEligibleOldestFirst(t *testing.T) {
	x := newFreeIndex()
	x.add(9, 0, []uint64{90})
	x.add(2, 1, []uint64{21})
	x.add(2, 0, []uint64{20})
	x.add(0, 0, []uint64{5})
	got := x.eligible(8)
	want := []string{string(freeKey(0, 0)), string(freeKey(2, 0)), string(freeKey(2, 1))}
	if len(got) != len(want) {
		t.Fatalf("eligible = %d records, want %d", len(got), len(want))
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("eligible[%d] out of order", i)
		}
	}
	if n := x.reusable(8); n != 3 {
		t.Fatalf("reusable = %d", n)
	}
}

// A page reused from one record and recycled into another must end up
// listed once, whatever order the edits are applied in.
func TestFreeIndexInstallMovesPagesBetweenRecords(t *testing.T) {
	x := newFreeIndex()
	x.add(0, 0, []uint64{5, 6})
	x.add(3, 0, []uint64{30})
	edits := map[string][]uint64{
		string(freeKey(0, 0)): {6},      // page 5 was reused
		string(freeKey(0, 1)): {5},      // then recycled into a new tag-0 record
		string(freeKey(3, 0)): nil,      // page 30 was reused
		string(freeKey(4, 0)): {40, 41}, // freed by this commit
	}
	x.install(edits, []uint64{5, 30})
	want := map[uint64]uint64{5: 0, 6: 0, 40: 4, 41: 4}
	if len(x.pages) != len(want) {
		t.Fatalf("pages = %v, want %v", x.pages, want)
	}
	for id, txid := range want {
		if got, ok := x.pages[id]; !ok || got != txid {
			t.Fatalf("page %d: %d %v, want %d", id, got, ok, txid)
		}
	}
	if _, ok := x.recs[string(freeKey(3, 0))]; ok {
		t.Fatal("emptied record still present")
	}
	y := newFreeIndex()
	y.add(0, 0, []uint64{6})
	y.add(0, 1, []uint64{5})
	y.add(4, 0, []uint64{40, 41})
	if !x.equal(y) {
		t.Fatal("installed index differs from the expected one")
	}
}
