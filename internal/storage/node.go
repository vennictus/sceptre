package storage

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
)

// Leaf and branch pages share one layout after the page header:
//
//	24  uint16      entry count
//	26  uint16[n]   offset of each cell, in key order
//	..  cells, packed after the slot array
//
// Leaf cell:   keyLen uint16 | flags uint8 | valueLen uint32 | key | value
//
//	(flags bit 0 set: the value is in an overflow chain and the
//	cell holds its first page ID as uint64 instead)
//
// Branch cell: child uint64 | keyLen uint16 | key
//
// A branch key is the largest key in its child's subtree.
const (
	nodeCountOff     = pageHeaderSize
	nodeSlotsOff     = pageHeaderSize + 2
	slotSize         = 2
	leafCellHeader   = 7
	branchCellHeader = 10
	overflowRefSize  = 8

	flagOverflow = 1

	// MinPageSize and MaxPageSize bound v2 page sizes. Slot offsets are uint16.
	MinPageSize = 1024
	MaxPageSize = 65536
)

var (
	// ErrKeyTooLarge reports a key above the layout's inline key limit.
	ErrKeyTooLarge = errors.New("storage: key too large")
	// ErrValueTooLarge reports a value above MaxValueSize.
	ErrValueTooLarge = errors.New("storage: value too large")
	// ErrEmptyKey reports a zero-length key, which the engine does not store.
	ErrEmptyKey = errors.New("storage: empty key")
	// ErrInvalidPageSize reports a page size outside MinPageSize..MaxPageSize.
	ErrInvalidPageSize = fmt.Errorf("storage: page size must be a power of two from %d to %d", MinPageSize, MaxPageSize)
)

// MaxValueSize bounds a single value (1 GiB).
const MaxValueSize = 1 << 30

// layout holds the size rules derived from a page size. Every entry costs at
// most a quarter of a page, so any overfull node splits into two that fit.
type layout struct {
	pageSize int
	usable   int // bytes for slots and cells in a node
	maxEntry int // largest slot + cell
	maxKey   int
	minFill  int // a non-root node below this many bytes is rebalanced
}

func newLayout(pageSize int) (layout, error) {
	if pageSize < MinPageSize || pageSize > MaxPageSize || pageSize&(pageSize-1) != 0 {
		return layout{}, ErrInvalidPageSize
	}
	usable := pageSize - nodeSlotsOff
	maxEntry := usable / 4
	return layout{
		pageSize: pageSize,
		usable:   usable,
		maxEntry: maxEntry,
		maxKey:   maxEntry - slotSize - leafCellHeader - overflowRefSize,
		minFill:  usable / 4,
	}, nil
}

type leafEntry struct {
	key      []byte
	value    []byte // inline value; nil when overflow != 0
	overflow uint64 // first overflow page, or 0 for an inline value
	size     uint32 // value length in bytes
}

type branchEntry struct {
	child uint64
	key   []byte // largest key in the child's subtree
}

func leafCost(e leafEntry) int {
	n := slotSize + leafCellHeader + len(e.key)
	if e.overflow != 0 {
		return n + overflowRefSize
	}
	return n + len(e.value)
}

func branchCost(e branchEntry) int { return slotSize + branchCellHeader + len(e.key) }

// inline reports whether a value can be stored inside its leaf cell.
func (l layout) inline(key []byte, valueLen int) bool {
	return slotSize+leafCellHeader+len(key)+valueLen <= l.maxEntry
}

func (l layout) checkKey(key []byte) error {
	switch {
	case len(key) == 0:
		return ErrEmptyKey
	case len(key) > l.maxKey:
		return fmt.Errorf("%w: %d bytes, limit %d", ErrKeyTooLarge, len(key), l.maxKey)
	}
	return nil
}

func leafBytes(entries []leafEntry) int {
	n := 0
	for _, e := range entries {
		n += leafCost(e)
	}
	return n
}

func branchBytes(entries []branchEntry) int {
	n := 0
	for _, e := range entries {
		n += branchCost(e)
	}
	return n
}

func encodeLeaf(l layout, entries []leafEntry) []byte {
	page := newPage(l.pageSize, pageLeaf)
	binary.LittleEndian.PutUint16(page[nodeCountOff:], uint16(len(entries)))
	at := nodeSlotsOff + slotSize*len(entries)
	for i, e := range entries {
		binary.LittleEndian.PutUint16(page[nodeSlotsOff+slotSize*i:], uint16(at))
		binary.LittleEndian.PutUint16(page[at:], uint16(len(e.key)))
		binary.LittleEndian.PutUint32(page[at+3:], e.size)
		if e.overflow != 0 {
			page[at+2] = flagOverflow
		}
		at += leafCellHeader
		at += copy(page[at:], e.key)
		if e.overflow != 0 {
			binary.LittleEndian.PutUint64(page[at:], e.overflow)
			at += overflowRefSize
		} else {
			at += copy(page[at:], e.value)
		}
	}
	return page
}

func encodeBranch(l layout, entries []branchEntry) []byte {
	page := newPage(l.pageSize, pageBranch)
	binary.LittleEndian.PutUint16(page[nodeCountOff:], uint16(len(entries)))
	at := nodeSlotsOff + slotSize*len(entries)
	for i, e := range entries {
		binary.LittleEndian.PutUint16(page[nodeSlotsOff+slotSize*i:], uint16(at))
		binary.LittleEndian.PutUint64(page[at:], e.child)
		binary.LittleEndian.PutUint16(page[at+8:], uint16(len(e.key)))
		at += branchCellHeader
		at += copy(page[at:], e.key)
	}
	return page
}

func corrupt(format string, args ...any) error {
	return fmt.Errorf("%w: "+format, append([]any{ErrPageCorrupt}, args...)...)
}

func nodeCount(page []byte) (int, error) {
	n := int(binary.LittleEndian.Uint16(page[nodeCountOff:]))
	if nodeSlotsOff+slotSize*n > len(page) {
		return 0, corrupt("entry count %d exceeds page", n)
	}
	return n, nil
}

func cellOffset(page []byte, i, n int) (int, error) {
	at := int(binary.LittleEndian.Uint16(page[nodeSlotsOff+slotSize*i:]))
	if at < nodeSlotsOff+slotSize*n || at >= len(page) {
		return 0, corrupt("cell %d offset %d out of bounds", i, at)
	}
	return at, nil
}

// leafAt decodes entry i of a leaf. Slices reference the page.
func leafAt(page []byte, i, n int) (leafEntry, error) {
	at, err := cellOffset(page, i, n)
	if err != nil {
		return leafEntry{}, err
	}
	if at+leafCellHeader > len(page) {
		return leafEntry{}, corrupt("leaf cell %d header out of bounds", i)
	}
	keyLen := int(binary.LittleEndian.Uint16(page[at:]))
	flags := page[at+2]
	size := binary.LittleEndian.Uint32(page[at+3:])
	keyStart := at + leafCellHeader
	keyEnd := keyStart + keyLen
	if keyLen == 0 || keyEnd > len(page) {
		return leafEntry{}, corrupt("leaf cell %d key out of bounds", i)
	}
	e := leafEntry{key: page[keyStart:keyEnd], size: size}
	switch flags {
	case 0:
		if keyEnd+int(size) > len(page) {
			return leafEntry{}, corrupt("leaf cell %d value out of bounds", i)
		}
		e.value = page[keyEnd : keyEnd+int(size)]
	case flagOverflow:
		if keyEnd+overflowRefSize > len(page) {
			return leafEntry{}, corrupt("leaf cell %d overflow reference out of bounds", i)
		}
		e.overflow = binary.LittleEndian.Uint64(page[keyEnd:])
		if e.overflow == 0 || size > MaxValueSize {
			return leafEntry{}, corrupt("leaf cell %d has invalid overflow reference", i)
		}
	default:
		return leafEntry{}, corrupt("leaf cell %d has unknown flags %d", i, flags)
	}
	return e, nil
}

// branchAt decodes entry i of a branch. The key references the page.
func branchAt(page []byte, i, n int) (branchEntry, error) {
	at, err := cellOffset(page, i, n)
	if err != nil {
		return branchEntry{}, err
	}
	if at+branchCellHeader > len(page) {
		return branchEntry{}, corrupt("branch cell %d header out of bounds", i)
	}
	child := binary.LittleEndian.Uint64(page[at:])
	keyLen := int(binary.LittleEndian.Uint16(page[at+8:]))
	keyStart := at + branchCellHeader
	if child == 0 || keyLen == 0 || keyStart+keyLen > len(page) {
		return branchEntry{}, corrupt("branch cell %d is malformed", i)
	}
	return branchEntry{child: child, key: page[keyStart : keyStart+keyLen]}, nil
}

func decodeLeaf(page []byte) ([]leafEntry, error) {
	if typeOf(page) != pageLeaf {
		return nil, corrupt("expected leaf, found %s", typeOf(page))
	}
	n, err := nodeCount(page)
	if err != nil {
		return nil, err
	}
	entries := make([]leafEntry, n)
	for i := range entries {
		if entries[i], err = leafAt(page, i, n); err != nil {
			return nil, err
		}
	}
	return entries, nil
}

func decodeBranch(page []byte) ([]branchEntry, error) {
	if typeOf(page) != pageBranch {
		return nil, corrupt("expected branch, found %s", typeOf(page))
	}
	n, err := nodeCount(page)
	if err != nil {
		return nil, err
	}
	if n == 0 {
		return nil, corrupt("empty branch")
	}
	entries := make([]branchEntry, n)
	for i := range entries {
		if entries[i], err = branchAt(page, i, n); err != nil {
			return nil, err
		}
	}
	return entries, nil
}

// searchLeaf returns the index of key, or where it would be inserted.
func searchLeaf(page []byte, key []byte) (int, bool, error) {
	n, err := nodeCount(page)
	if err != nil {
		return 0, false, err
	}
	lo, hi := 0, n
	for lo < hi {
		mid := int(uint(lo+hi) >> 1)
		k, err := leafKeyAt(page, mid, n)
		if err != nil {
			return 0, false, err
		}
		switch c := bytes.Compare(k, key); {
		case c == 0:
			return mid, true, nil
		case c < 0:
			lo = mid + 1
		default:
			hi = mid
		}
	}
	return lo, false, nil
}

// childIndex returns the child whose subtree holds key: the first entry whose
// maximum is at least key, or the last entry when key is beyond them all.
func childIndex(page []byte, key []byte) (int, int, error) {
	n, err := nodeCount(page)
	if err != nil {
		return 0, 0, err
	}
	if n == 0 {
		return 0, 0, corrupt("empty branch")
	}
	lo, hi := 0, n-1
	for lo < hi {
		mid := int(uint(lo+hi) >> 1)
		k, err := branchKeyAt(page, mid, n)
		if err != nil {
			return 0, 0, err
		}
		if bytes.Compare(k, key) < 0 {
			lo = mid + 1
		} else {
			hi = mid
		}
	}
	return lo, n, nil
}

// leafKeyAt returns key i of a leaf without decoding the rest of the cell.
// Binary search probes use it; the chosen entry is decoded with leafAt.
func leafKeyAt(page []byte, i, n int) ([]byte, error) {
	at, err := cellOffset(page, i, n)
	if err != nil {
		return nil, err
	}
	if at+leafCellHeader > len(page) {
		return nil, corrupt("leaf cell %d header out of bounds", i)
	}
	keyLen := int(binary.LittleEndian.Uint16(page[at:]))
	start := at + leafCellHeader
	if keyLen == 0 || start+keyLen > len(page) {
		return nil, corrupt("leaf cell %d key out of bounds", i)
	}
	return page[start : start+keyLen], nil
}

// branchKeyAt returns key i of a branch without decoding its child pointer.
func branchKeyAt(page []byte, i, n int) ([]byte, error) {
	at, err := cellOffset(page, i, n)
	if err != nil {
		return nil, err
	}
	if at+branchCellHeader > len(page) {
		return nil, corrupt("branch cell %d header out of bounds", i)
	}
	keyLen := int(binary.LittleEndian.Uint16(page[at+8:]))
	start := at + branchCellHeader
	if keyLen == 0 || start+keyLen > len(page) {
		return nil, corrupt("branch cell %d key out of bounds", i)
	}
	return page[start : start+keyLen], nil
}
