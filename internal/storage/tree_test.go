package storage

import (
	"bytes"
	"errors"
	"fmt"
	"math/rand"
	"sort"
	"testing"
)

// memPages is a committed page set for exercising the tree without a file.
type memPages map[uint64][]byte

func (m memPages) page(id uint64) ([]byte, error) {
	p, ok := m[id]
	if !ok {
		return nil, fmt.Errorf("page %d is not committed", id)
	}
	return p, nil
}

// treeHarness commits writer results into memPages and checks that the live
// pages are exactly the pages reachable from the root: nothing leaked and
// nothing freed that is still referenced.
type treeHarness struct {
	t     *testing.T
	l     layout
	pages memPages
	next  uint64
	root  uint64
	model map[string][]byte
}

func newTreeHarness(t *testing.T, pageSize int) *treeHarness {
	t.Helper()
	l, err := newLayout(pageSize)
	if err != nil {
		t.Fatal(err)
	}
	return &treeHarness{t: t, l: l, pages: memPages{}, next: 2, model: map[string][]byte{}}
}

func (h *treeHarness) begin() *writer {
	var spare []uint64
	return &writer{
		src:   h.pages,
		l:     h.l,
		dirty: map[uint64][]byte{},
		alloc: func() (uint64, error) {
			if n := len(spare); n > 0 {
				id := spare[n-1]
				spare = spare[:n-1]
				return id, nil
			}
			id := h.next
			h.next++
			return id, nil
		},
		recycle: func(id uint64) { spare = append(spare, id) },
	}
}

func (h *treeHarness) commit(w *writer, root uint64) {
	h.t.Helper()
	for _, id := range w.freed {
		if _, ok := w.dirty[id]; ok {
			h.t.Fatalf("page %d both freed and written", id)
		}
		if _, ok := h.pages[id]; !ok {
			h.t.Fatalf("freed page %d was not committed", id)
		}
		delete(h.pages, id)
	}
	for id, p := range w.dirty {
		h.pages[id] = p
	}
	h.root = root
	h.check()
}

func (h *treeHarness) check() {
	h.t.Helper()
	st, err := verifyTree(h.pages, h.l, h.root)
	if err != nil {
		h.t.Fatalf("verify: %v", err)
	}
	if st.Underfull != 0 {
		h.t.Fatalf("%d underfull non-root nodes: %v", st.Underfull, st)
	}
	if st.Keys != len(h.model) {
		h.t.Fatalf("tree has %d keys, model %d", st.Keys, len(h.model))
	}
	if len(st.Pages) != len(h.pages) {
		for id := range h.pages {
			if _, ok := st.Pages[id]; !ok {
				h.t.Fatalf("page %d committed but unreachable (leak)", id)
			}
		}
	}
	// Full ordered scan matches the model.
	keys := h.sortedKeys()
	c := newCursor(h.pages, h.l, h.root)
	if err := c.SeekFirst(); err != nil {
		h.t.Fatal(err)
	}
	for _, k := range keys {
		if !c.Valid() {
			h.t.Fatalf("scan ended before %q", k)
		}
		ck, _ := c.Key()
		cv, err := c.Value()
		if err != nil {
			h.t.Fatal(err)
		}
		if string(ck) != k || !bytes.Equal(cv, h.model[k]) {
			h.t.Fatalf("scan at %q: got %q (%d bytes)", k, ck, len(cv))
		}
		if err := c.Next(); err != nil {
			h.t.Fatal(err)
		}
	}
	if c.Valid() {
		k, _ := c.Key()
		h.t.Fatalf("scan has extra key %q", k)
	}
}

func (h *treeHarness) sortedKeys() []string {
	keys := make([]string, 0, len(h.model))
	for k := range h.model {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

func (h *treeHarness) put(w *writer, root uint64, k string, v []byte) uint64 {
	h.t.Helper()
	root, err := w.put(root, []byte(k), v)
	if err != nil {
		h.t.Fatalf("put %q: %v", k, err)
	}
	h.model[k] = v
	return root
}

func (h *treeHarness) del(w *writer, root uint64, k string) uint64 {
	h.t.Helper()
	root, found, err := w.del(root, []byte(k))
	if err != nil {
		h.t.Fatalf("del %q: %v", k, err)
	}
	if _, ok := h.model[k]; ok != found {
		h.t.Fatalf("del %q found=%v, model has=%v", k, found, ok)
	}
	delete(h.model, k)
	return root
}

func TestLayoutLimits(t *testing.T) {
	for _, size := range []int{512, 1000, 3000, 131072} {
		if _, err := newLayout(size); !errors.Is(err, ErrInvalidPageSize) {
			t.Errorf("page size %d accepted", size)
		}
	}
	l, err := newLayout(4096)
	if err != nil {
		t.Fatal(err)
	}
	// Four maximal entries always fit in a node.
	if 4*l.maxEntry > l.usable {
		t.Fatalf("4 * %d > %d", l.maxEntry, l.usable)
	}
	big := bytes.Repeat([]byte("k"), l.maxKey)
	if err := l.checkKey(big); err != nil {
		t.Fatal(err)
	}
	if err := l.checkKey(append(big, 'x')); !errors.Is(err, ErrKeyTooLarge) {
		t.Fatalf("oversized key: %v", err)
	}
	if err := l.checkKey(nil); !errors.Is(err, ErrEmptyKey) {
		t.Fatalf("empty key: %v", err)
	}
	if branchCost(branchEntry{key: big}) > l.maxEntry || leafCost(leafEntry{key: big, overflow: 9}) > l.maxEntry {
		t.Fatal("maximal key cell exceeds a quarter page")
	}
}

func TestNodeRoundTrip(t *testing.T) {
	l, _ := newLayout(4096)
	leaves := []leafEntry{
		{key: []byte("a"), value: []byte("1"), size: 1},
		{key: []byte("b"), value: nil, size: 0},
		{key: []byte("c"), overflow: 77, size: 5000},
	}
	got, err := decodeLeaf(encodeLeaf(l, leaves))
	if err != nil {
		t.Fatal(err)
	}
	for i := range leaves {
		if !bytes.Equal(got[i].key, leaves[i].key) || !bytes.Equal(got[i].value, leaves[i].value) || got[i].overflow != leaves[i].overflow || got[i].size != leaves[i].size {
			t.Fatalf("leaf entry %d: %+v != %+v", i, got[i], leaves[i])
		}
	}
	branches := []branchEntry{{child: 5, key: []byte("m")}, {child: 9, key: []byte("z")}}
	gb, err := decodeBranch(encodeBranch(l, branches))
	if err != nil {
		t.Fatal(err)
	}
	if gb[0].child != 5 || string(gb[1].key) != "z" {
		t.Fatalf("branch: %+v", gb)
	}
}

func TestPageChecksumAndLocation(t *testing.T) {
	l, _ := newLayout(4096)
	page := encodeLeaf(l, []leafEntry{{key: []byte("a"), value: []byte("1"), size: 1}})
	seal(page, 42, 7)
	if err := verify(page, 42); err != nil {
		t.Fatal(err)
	}
	if txIDOf(page) != 7 {
		t.Fatalf("txid = %d", txIDOf(page))
	}
	if err := verify(page, 43); !errors.Is(err, ErrPageCorrupt) {
		t.Fatalf("misdirected page accepted: %v", err)
	}
	for _, at := range []int{0, offType, offPageID, offTxID, nodeSlotsOff, len(page) - 1} {
		flipped := append([]byte(nil), page...)
		flipped[at] ^= 0x10
		if err := verify(flipped, 42); !errors.Is(err, ErrPageCorrupt) {
			t.Errorf("flipped byte %d accepted: %v", at, err)
		}
	}
}

func TestDecodeRejectsMalformedNodes(t *testing.T) {
	l, _ := newLayout(4096)
	good := encodeLeaf(l, []leafEntry{{key: []byte("a"), value: []byte("1"), size: 1}})
	cases := map[string]func(p []byte){
		"huge count":    func(p []byte) { p[nodeCountOff], p[nodeCountOff+1] = 0xff, 0xff },
		"slot past end": func(p []byte) { p[nodeSlotsOff], p[nodeSlotsOff+1] = 0xff, 0xff },
		"bad flags":     func(p []byte) { p[nodeSlotsOff+2+2] = 9 },
		"value overrun": func(p []byte) { p[nodeSlotsOff+2+3], p[nodeSlotsOff+2+6] = 0xff, 0x7f },
		"wrong type":    func(p []byte) { p[offType] = byte(pageBranch) },
	}
	for name, mutate := range cases {
		p := append([]byte(nil), good...)
		mutate(p)
		if _, err := decodeLeaf(p); !errors.Is(err, ErrPageCorrupt) {
			t.Errorf("%s: %v", name, err)
		}
	}
}

func TestTreeSequentialInsertDeleteBothDirections(t *testing.T) {
	for _, reverse := range []bool{false, true} {
		h := newTreeHarness(t, 1024)
		w := h.begin()
		root := uint64(0)
		const n = 2000
		for i := 0; i < n; i++ {
			k := i
			if reverse {
				k = n - 1 - i
			}
			root = h.put(w, root, fmt.Sprintf("key-%06d", k), []byte(fmt.Sprintf("value-%d", k)))
		}
		h.commit(w, root)
		st, _ := verifyTree(h.pages, h.l, h.root)
		if st.Depth < 3 {
			t.Fatalf("expected a deep tree, got %v", st)
		}
		w = h.begin()
		for i := 0; i < n; i++ {
			root = h.del(w, root, fmt.Sprintf("key-%06d", i))
			if i%250 == 0 {
				h.commit(w, root)
				w = h.begin()
			}
		}
		h.commit(w, root)
		if h.root != 0 || len(h.pages) != 0 {
			t.Fatalf("empty tree keeps root %d and %d pages", h.root, len(h.pages))
		}
	}
}

func TestTreeRandomizedAgainstModel(t *testing.T) {
	for _, pageSize := range []int{1024, 4096} {
		for seed := int64(1); seed <= 6; seed++ {
			t.Run(fmt.Sprintf("page%d/seed%d", pageSize, seed), func(t *testing.T) {
				rng := rand.New(rand.NewSource(seed))
				h := newTreeHarness(t, pageSize)
				for round := 0; round < 40; round++ {
					w := h.begin()
					root := h.root
					for op := 0; op < 60; op++ {
						k := fmt.Sprintf("k%0*d", 1+rng.Intn(40), rng.Intn(400))
						switch r := rng.Intn(10); {
						case r < 6:
							root = h.put(w, root, k, randomValue(rng, h.l))
						default:
							root = h.del(w, root, k)
						}
					}
					h.commit(w, root)
				}
			})
		}
	}
}

// randomValue mixes empty, small, page-sized and multi-page values.
func randomValue(rng *rand.Rand, l layout) []byte {
	var n int
	switch rng.Intn(6) {
	case 0:
		n = 0
	case 1, 2:
		n = rng.Intn(64)
	case 3:
		n = l.maxEntry - 40 + rng.Intn(80) // straddles the inline limit
	case 4:
		n = rng.Intn(3 * l.pageSize)
	default:
		n = rng.Intn(16)
	}
	v := make([]byte, n)
	rng.Read(v)
	return v
}

func TestTreeLargeKeysForceDeepSplitsAndMerges(t *testing.T) {
	h := newTreeHarness(t, 1024)
	rng := rand.New(rand.NewSource(99))
	w := h.begin()
	root := uint64(0)
	var keys []string
	for i := 0; i < 600; i++ {
		k := fmt.Sprintf("%04d", i) + string(bytes.Repeat([]byte{'a' + byte(rng.Intn(26))}, rng.Intn(h.l.maxKey-4)))
		keys = append(keys, k)
		root = h.put(w, root, k, []byte{byte(i)})
	}
	h.commit(w, root)
	rng.Shuffle(len(keys), func(i, j int) { keys[i], keys[j] = keys[j], keys[i] })
	w = h.begin()
	for i, k := range keys {
		root = h.del(w, root, k)
		if i%37 == 0 {
			h.commit(w, root)
			w = h.begin()
		}
	}
	h.commit(w, root)
}

func TestTreeOverflowReplaceAndDeleteFreeChains(t *testing.T) {
	h := newTreeHarness(t, 1024)
	w := h.begin()
	big := bytes.Repeat([]byte("x"), 10*1024)
	root := h.put(w, 0, "big", big)
	h.commit(w, root)
	st, _ := verifyTree(h.pages, h.l, h.root)
	if want := overflowPages(1024, len(big)); st.OverflowPages != want {
		t.Fatalf("overflow pages = %d, want %d", st.OverflowPages, want)
	}

	w = h.begin()
	root = h.put(w, h.root, "big", []byte("small now"))
	h.commit(w, root) // check() proves the old chain was freed
	if st, _ := verifyTree(h.pages, h.l, h.root); st.OverflowPages != 0 {
		t.Fatalf("chain survived replacement: %v", st)
	}

	w = h.begin()
	root = h.put(w, h.root, "big", big)
	root = h.put(w, root, "big", append(big, 'y')) // replace a chain written in the same transaction
	h.commit(w, root)
	w = h.begin()
	root = h.del(w, h.root, "big")
	h.commit(w, root)
	if len(h.pages) != 0 {
		t.Fatalf("%d pages remain after deleting the only key", len(h.pages))
	}
}

func TestTreeRejectsInvalidKeysAndValues(t *testing.T) {
	h := newTreeHarness(t, 1024)
	w := h.begin()
	if _, err := w.put(0, nil, []byte("v")); !errors.Is(err, ErrEmptyKey) {
		t.Fatalf("empty key: %v", err)
	}
	if _, err := w.put(0, bytes.Repeat([]byte("k"), h.l.maxKey+1), nil); !errors.Is(err, ErrKeyTooLarge) {
		t.Fatalf("large key: %v", err)
	}
	if len(w.dirty) != 0 {
		t.Fatal("rejected writes left dirty pages")
	}
}

func TestCursorSeeksAgainstModel(t *testing.T) {
	h := newTreeHarness(t, 1024)
	w := h.begin()
	root := uint64(0)
	for i := 0; i < 500; i += 2 { // even numbers only
		root = h.put(w, root, fmt.Sprintf("%05d", i), []byte{byte(i)})
	}
	h.commit(w, root)
	c := newCursor(h.pages, h.l, h.root)
	for probe := -1; probe <= 501; probe++ {
		key := []byte(fmt.Sprintf("%05d", probe))
		if probe < 0 {
			key = []byte("")
		}
		if err := c.SeekGE(key); err != nil {
			t.Fatal(err)
		}
		ge := probe + (probe & 1)
		if probe < 0 {
			ge = 0
		}
		if ge >= 500 {
			if c.Valid() {
				t.Fatalf("SeekGE(%d) valid past end", probe)
			}
		} else if k, _ := c.Key(); string(k) != fmt.Sprintf("%05d", ge) {
			t.Fatalf("SeekGE(%d) = %s", probe, k)
		}

		if err := c.SeekLE(key); err != nil {
			t.Fatal(err)
		}
		le := probe - (probe & 1)
		if probe > 498 {
			le = 498
		}
		if probe < 0 {
			if c.Valid() {
				t.Fatalf("SeekLE(%d) valid before start", probe)
			}
		} else if k, _ := c.Key(); string(k) != fmt.Sprintf("%05d", le) {
			t.Fatalf("SeekLE(%d) = %s", probe, k)
		}
	}
	// Walking backwards from the end visits every key.
	if err := c.SeekLast(); err != nil {
		t.Fatal(err)
	}
	for i := 498; i >= 0; i -= 2 {
		if k, _ := c.Key(); string(k) != fmt.Sprintf("%05d", i) {
			t.Fatalf("Prev walk at %d: %s", i, k)
		}
		c.Prev()
	}
	if c.Valid() {
		t.Fatal("cursor valid before first key")
	}
	if _, err := c.Key(); !errors.Is(err, ErrCursorInvalid) {
		t.Fatalf("Key on invalid cursor: %v", err)
	}
}

func TestOldSnapshotStaysReadableAfterCopyOnWrite(t *testing.T) {
	h := newTreeHarness(t, 1024)
	w := h.begin()
	root := uint64(0)
	for i := 0; i < 300; i++ {
		root = h.put(w, root, fmt.Sprintf("%04d", i), []byte("old"))
	}
	h.commit(w, root)
	oldRoot := h.root
	snapshot := memPages{}
	for id, p := range h.pages {
		snapshot[id] = p
	}

	w = h.begin()
	root = h.root
	for i := 0; i < 300; i += 3 {
		root = h.put(w, root, fmt.Sprintf("%04d", i), []byte("new"))
	}
	// Committed pages are never modified in place.
	for id, p := range snapshot {
		if !bytes.Equal(p, h.pages[id]) {
			t.Fatalf("committed page %d modified by a write transaction", id)
		}
	}
	h.commit(w, root)
	v, ok, err := get(snapshot, h.l, oldRoot, []byte("0003"))
	if err != nil || !ok || string(v) != "old" {
		t.Fatalf("old snapshot read = %q %v %v", v, ok, err)
	}
}
