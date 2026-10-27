package storage

import (
	"bytes"
	"errors"
	"math/rand"
	"reflect"
	"sort"
	"testing"
)

func TestFreeKeyRoundTripAndOrder(t *testing.T) {
	t.Parallel()
	k := freeKey(42, 7)
	txid, seq, err := parseFreeKey(k)
	if err != nil {
		t.Fatalf("parseFreeKey() error = %v", err)
	}
	if txid != 42 || seq != 7 {
		t.Fatalf("parseFreeKey() = (%d, %d), want (42, 7)", txid, seq)
	}
	if bytes.Compare(freeKey(1, 5), freeKey(2, 0)) >= 0 {
		t.Fatal("free key (1,5) does not sort before (2,0)")
	}
}

func TestParseFreeKeyRejectsWrongLength(t *testing.T) {
	t.Parallel()
	for _, n := range []int{11, 13} {
		if _, _, err := parseFreeKey(make([]byte, n)); !errors.Is(err, ErrPageCorrupt) {
			t.Fatalf("parseFreeKey(%d bytes) error = %v, want ErrPageCorrupt", n, err)
		}
	}
}

func TestFreePagesRoundTrip(t *testing.T) {
	t.Parallel()
	ids := []uint64{3, 9, 1 << 40}
	got, err := decodeFreePages(encodeFreePages(ids))
	if err != nil {
		t.Fatalf("decodeFreePages() error = %v", err)
	}
	if !reflect.DeepEqual(got, ids) {
		t.Fatalf("decodeFreePages() = %v, want %v", got, ids)
	}
}

func TestDecodeFreePagesRejectsMalformed(t *testing.T) {
	t.Parallel()
	cases := map[string][]byte{
		"length 7":      make([]byte, 7),
		"empty":         {},
		"non-ascending": encodeFreePages([]uint64{5, 5}),
		"descending":    encodeFreePages([]uint64{9, 2}),
	}
	for name, v := range cases {
		if _, err := decodeFreePages(v); !errors.Is(err, ErrPageCorrupt) {
			t.Fatalf("%s: decodeFreePages error = %v, want ErrPageCorrupt", name, err)
		}
	}
}

func TestFreeChunkCapFitsInlineExactly(t *testing.T) {
	t.Parallel()
	for _, size := range []int{1024, 4096} {
		l, err := newLayout(size)
		if err != nil {
			t.Fatal(err)
		}
		c := freeChunkCap(l)
		if c < 1 {
			t.Fatalf("page size %d: freeChunkCap = %d", size, c)
		}
		if !l.inline(freeKey(1, 1), c*8) {
			t.Fatalf("page size %d: %d ids should be inline", size, c)
		}
		if l.inline(freeKey(1, 1), (c+1)*8) {
			t.Fatalf("page size %d: %d ids should not be inline", size, c+1)
		}
	}
}

func TestChunkFreePagesSplitsSortedWithinCap(t *testing.T) {
	t.Parallel()
	for _, size := range []int{1024, 4096} {
		l, err := newLayout(size)
		if err != nil {
			t.Fatal(err)
		}
		cap := freeChunkCap(l)

		ids := make([]uint64, 300)
		for i := range ids {
			ids[i] = uint64(i + 1)
		}
		rand.New(rand.NewSource(int64(size))).Shuffle(len(ids), func(i, j int) {
			ids[i], ids[j] = ids[j], ids[i]
		})
		original := append([]uint64(nil), ids...)

		chunks := chunkFreePages(ids, cap)
		var union []uint64
		for _, chunk := range chunks {
			if len(chunk) == 0 || len(chunk) > cap {
				t.Fatalf("page size %d: chunk length %d, cap %d", size, len(chunk), cap)
			}
			if !sort.SliceIsSorted(chunk, func(i, j int) bool { return chunk[i] < chunk[j] }) {
				t.Fatalf("page size %d: chunk not ascending: %v", size, chunk)
			}
			union = append(union, chunk...)
		}
		sort.Slice(union, func(i, j int) bool { return union[i] < union[j] })
		if !reflect.DeepEqual(union, sortedCopy(original)) {
			t.Fatalf("page size %d: union of chunks differs from input", size)
		}
		if !reflect.DeepEqual(ids, original) {
			t.Fatalf("page size %d: chunkFreePages modified its input", size)
		}
	}
}

func TestChunkFreePagesEmptyIsNil(t *testing.T) {
	t.Parallel()
	if got := chunkFreePages(nil, 10); got != nil {
		t.Fatalf("chunkFreePages(nil) = %v, want nil", got)
	}
}

func sortedCopy(ids []uint64) []uint64 {
	out := append([]uint64(nil), ids...)
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}
