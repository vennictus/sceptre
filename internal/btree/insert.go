package btree

import "errors"

var (
	ErrDuplicateKey     = errors.New("btree: duplicate key")
	ErrInsertOutOfRange = errors.New("btree: insert position out of range")
)

// InsertLeaf inserts a key/value pair while preserving leaf key order.
func (n Node) InsertLeaf(key, value []byte) error {
	if n.Type() != NodeTypeLeaf {
		return ErrNodeTypeMismatch
	}

	index, found, err := n.Search(key)
	if err != nil {
		return err
	}
	if found {
		return ErrDuplicateKey
	}

	offset, err := n.reserveCellAt(index, leafCellSize(key, value))
	if err != nil {
		return err
	}

	binaryPutLeafCell(n.page[offset:], key, value)
	return nil
}

func (n Node) reserveCellAt(index, cellSize int) (uint16, error) {
	if index < 0 || index > n.Count() {
		return 0, ErrInsertOutOfRange
	}

	required := nodeSlotSize + cellSize
	if required > n.FreeSpace() {
		return 0, ErrNodeFull
	}

	newUpper := n.Upper() - uint16(cellSize)
	n.shiftSlotsRight(index)
	n.setSlot(index, newUpper)
	n.setCount(n.Count() + 1)
	n.setLower(n.Lower() + nodeSlotSize)
	n.setUpper(newUpper)
	return newUpper, nil
}

func (n Node) shiftSlotsRight(index int) {
	if index == n.Count() {
		return
	}

	start := nodeHeaderSize + index*nodeSlotSize
	end := nodeHeaderSize + n.Count()*nodeSlotSize
	copy(n.page[start+nodeSlotSize:end+nodeSlotSize], n.page[start:end])
}

func binaryPutLeafCell(dst []byte, key, value []byte) {
	putUint16(dst[0:], uint16(len(key)))
	putUint16(dst[2:], uint16(len(value)))
	copy(dst[leafCellHeaderSize:], key)
	copy(dst[leafCellHeaderSize+len(key):], value)
}
