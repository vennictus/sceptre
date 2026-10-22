package storage

import "encoding/binary"

// Overflow page layout after the header:
//
//	24 uint64  next page in the chain, or 0
//	32 uint32  bytes of data on this page
//	36 data
const (
	overflowNextOff = pageHeaderSize
	overflowLenOff  = pageHeaderSize + 8
	overflowDataOff = pageHeaderSize + 12
)

func overflowCapacity(pageSize int) int { return pageSize - overflowDataOff }

func overflowPages(pageSize, size int) int {
	c := overflowCapacity(pageSize)
	return (size + c - 1) / c
}

// writeOverflow stores value in a new chain and returns its first page.
func (w *writer) writeOverflow(value []byte) (uint64, error) {
	count := overflowPages(w.l.pageSize, len(value))
	ids := make([]uint64, count)
	for i := range ids {
		id, err := w.alloc()
		if err != nil {
			return 0, err
		}
		ids[i] = id
	}
	capacity := overflowCapacity(w.l.pageSize)
	for i, id := range ids {
		page := newPage(w.l.pageSize, pageOverflow)
		if i+1 < count {
			binary.LittleEndian.PutUint64(page[overflowNextOff:], ids[i+1])
		}
		n := copy(page[overflowDataOff:], value[i*capacity:])
		binary.LittleEndian.PutUint32(page[overflowLenOff:], uint32(n))
		w.dirty[id] = page
	}
	return ids[0], nil
}

// walkOverflow visits each page of a chain holding size bytes, checking the
// chain's shape so a corrupt chain cannot loop or overrun.
func walkOverflow(src pageSource, pageSize int, first uint64, size uint32, visit func(id uint64, data []byte)) error {
	want := overflowPages(pageSize, int(size))
	capacity := overflowCapacity(pageSize)
	remaining := int(size)
	id := first
	for i := 0; i < want; i++ {
		if id == 0 {
			return corrupt("overflow chain from page %d ends after %d of %d pages", first, i, want)
		}
		page, err := src.page(id)
		if err != nil {
			return err
		}
		if typeOf(page) != pageOverflow {
			return corrupt("overflow chain from page %d reaches %s page %d", first, typeOf(page), id)
		}
		n := int(binary.LittleEndian.Uint32(page[overflowLenOff:]))
		expect := capacity
		if remaining < capacity {
			expect = remaining
		}
		if n != expect {
			return corrupt("overflow page %d holds %d bytes, want %d", id, n, expect)
		}
		visit(id, page[overflowDataOff:overflowDataOff+n])
		remaining -= n
		id = binary.LittleEndian.Uint64(page[overflowNextOff:])
	}
	if id != 0 {
		return corrupt("overflow chain from page %d continues past %d pages", first, want)
	}
	return nil
}

func readOverflow(src pageSource, pageSize int, first uint64, size uint32) ([]byte, error) {
	value := make([]byte, 0, size)
	err := walkOverflow(src, pageSize, first, size, func(_ uint64, data []byte) {
		value = append(value, data...)
	})
	if err != nil {
		return nil, err
	}
	return value, nil
}

// freeOverflow releases every page of a chain this transaction no longer needs.
func (w *writer) freeOverflow(e leafEntry) error {
	var ids []uint64
	if err := walkOverflow(w, w.l.pageSize, e.overflow, e.size, func(id uint64, _ []byte) {
		ids = append(ids, id)
	}); err != nil {
		return err
	}
	for _, id := range ids {
		w.drop(id)
	}
	return nil
}

// valueOf returns an entry's value, reading its overflow chain if needed.
func valueOf(src pageSource, pageSize int, e leafEntry) ([]byte, error) {
	if e.overflow == 0 {
		return append([]byte(nil), e.value...), nil
	}
	return readOverflow(src, pageSize, e.overflow, e.size)
}
