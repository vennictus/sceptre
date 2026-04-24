package btree

import (
	"encoding/binary"
	"errors"
	"math"
)

// NodeType identifies the physical layout stored in a B+ tree page.
type NodeType uint16

const (
	NodeTypeUnknown NodeType = iota
	NodeTypeInternal
	NodeTypeLeaf
)

const (
	nodeHeaderSize         = 8
	nodeSlotSize           = 2
	leafCellHeaderSize     = 4
	internalCellHeaderSize = 10

	headerTypeOffset  = 0
	headerCountOffset = 2
	headerLowerOffset = 4
	headerUpperOffset = 6
)

var (
	ErrPageTooSmall     = errors.New("btree: page too small")
	ErrPageTooLarge     = errors.New("btree: page too large")
	ErrUnknownNodeType  = errors.New("btree: unknown node type")
	ErrNodeFull         = errors.New("btree: node is full")
	ErrCellOutOfRange   = errors.New("btree: cell index out of range")
	ErrNodeTypeMismatch = errors.New("btree: node type mismatch")
	ErrCorruptNode      = errors.New("btree: corrupt node")
)

// Node wraps the raw bytes of a single slotted B+ tree page.
type Node struct {
	page []byte
}

// LeafCell is the decoded payload stored in a leaf page.
//
// Key and Value reference the underlying page bytes.
type LeafCell struct {
	Key   []byte
	Value []byte
}

// InternalCell is the decoded payload stored in an internal page.
//
// Key references the underlying page bytes.
type InternalCell struct {
	Child uint64
	Key   []byte
}

// NewNode clears the provided page and formats it as an empty node.
func NewNode(page []byte, nodeType NodeType) (Node, error) {
	node, err := WrapNode(page)
	if err != nil {
		return Node{}, err
	}
	if nodeType != NodeTypeLeaf && nodeType != NodeTypeInternal {
		return Node{}, ErrUnknownNodeType
	}

	clear(page)
	node.setType(nodeType)
	node.setCount(0)
	node.setLower(nodeHeaderSize)
	node.setUpper(uint16(len(page)))
	return node, nil
}

// WrapNode validates that the page is large enough to hold a node header.
func WrapNode(page []byte) (Node, error) {
	switch {
	case len(page) < nodeHeaderSize:
		return Node{}, ErrPageTooSmall
	case len(page) > math.MaxUint16:
		return Node{}, ErrPageTooLarge
	default:
		return Node{page: page}, nil
	}
}

// Bytes returns the raw page bytes backing the node.
func (n Node) Bytes() []byte {
	return n.page
}

// Type returns the physical node type.
func (n Node) Type() NodeType {
	return NodeType(binary.LittleEndian.Uint16(n.page[headerTypeOffset:]))
}

// Count returns the number of cells tracked in the slot directory.
func (n Node) Count() int {
	return int(binary.LittleEndian.Uint16(n.page[headerCountOffset:]))
}

// Lower returns the first free byte after the slot directory.
func (n Node) Lower() uint16 {
	return binary.LittleEndian.Uint16(n.page[headerLowerOffset:])
}

// Upper returns the first byte used by cell payloads at the end of the page.
func (n Node) Upper() uint16 {
	return binary.LittleEndian.Uint16(n.page[headerUpperOffset:])
}

// FreeSpace returns the number of bytes currently available for new cells.
func (n Node) FreeSpace() int {
	return int(n.Upper()) - int(n.Lower())
}

// AppendLeafCell appends a leaf cell to the node's slot directory and payload area.
func (n Node) AppendLeafCell(key, value []byte) error {
	if n.Type() != NodeTypeLeaf {
		return ErrNodeTypeMismatch
	}

	offset, err := n.reserveCellAt(n.Count(), leafCellSize(key, value))
	if err != nil {
		return err
	}

	binaryPutLeafCell(n.page[offset:], key, value)
	return nil
}

// AppendInternalCell appends an internal cell to the node's slot directory and payload area.
func (n Node) AppendInternalCell(child uint64, key []byte) error {
	if n.Type() != NodeTypeInternal {
		return ErrNodeTypeMismatch
	}

	offset, err := n.reserveCellAt(n.Count(), internalCellSize(key))
	if err != nil {
		return err
	}

	binaryPutInternalCell(n.page[offset:], child, key)
	return nil
}

// LeafCell decodes the leaf payload stored at the given cell index.
func (n Node) LeafCell(index int) (LeafCell, error) {
	if n.Type() != NodeTypeLeaf {
		return LeafCell{}, ErrNodeTypeMismatch
	}

	offset, err := n.cellOffset(index)
	if err != nil {
		return LeafCell{}, err
	}

	start := int(offset)
	if start+leafCellHeaderSize > len(n.page) {
		return LeafCell{}, ErrCorruptNode
	}

	keyLen := int(binary.LittleEndian.Uint16(n.page[start:]))
	valLen := int(binary.LittleEndian.Uint16(n.page[start+2:]))
	keyStart := start + leafCellHeaderSize
	keyEnd := keyStart + keyLen
	valEnd := keyEnd + valLen
	if keyEnd > len(n.page) || valEnd > len(n.page) {
		return LeafCell{}, ErrCorruptNode
	}

	return LeafCell{
		Key:   n.page[keyStart:keyEnd],
		Value: n.page[keyEnd:valEnd],
	}, nil
}

// InternalCell decodes the internal payload stored at the given cell index.
func (n Node) InternalCell(index int) (InternalCell, error) {
	if n.Type() != NodeTypeInternal {
		return InternalCell{}, ErrNodeTypeMismatch
	}

	offset, err := n.cellOffset(index)
	if err != nil {
		return InternalCell{}, err
	}

	start := int(offset)
	if start+internalCellHeaderSize > len(n.page) {
		return InternalCell{}, ErrCorruptNode
	}

	keyLen := int(binary.LittleEndian.Uint16(n.page[start+8:]))
	keyStart := start + internalCellHeaderSize
	keyEnd := keyStart + keyLen
	if keyEnd > len(n.page) {
		return InternalCell{}, ErrCorruptNode
	}

	return InternalCell{
		Child: binary.LittleEndian.Uint64(n.page[start:]),
		Key:   n.page[keyStart:keyEnd],
	}, nil
}

func (n Node) cellOffset(index int) (uint16, error) {
	count := n.Count()
	if index < 0 || index >= count {
		return 0, ErrCellOutOfRange
	}

	slotEnd := nodeHeaderSize + count*nodeSlotSize
	lower := int(n.Lower())
	upper := int(n.Upper())
	if slotEnd > len(n.page) || lower < slotEnd || lower > len(n.page) || upper < lower || upper > len(n.page) {
		return 0, ErrCorruptNode
	}

	start := nodeHeaderSize + index*nodeSlotSize
	if start+nodeSlotSize > len(n.page) {
		return 0, ErrCorruptNode
	}

	offset := binary.LittleEndian.Uint16(n.page[start:])
	if int(offset) < upper || int(offset) >= len(n.page) {
		return 0, ErrCorruptNode
	}
	return offset, nil
}

func (n Node) setSlot(index int, offset uint16) {
	start := nodeHeaderSize + index*nodeSlotSize
	binary.LittleEndian.PutUint16(n.page[start:], offset)
}

func (n Node) setType(nodeType NodeType) {
	binary.LittleEndian.PutUint16(n.page[headerTypeOffset:], uint16(nodeType))
}

func (n Node) setCount(count int) {
	binary.LittleEndian.PutUint16(n.page[headerCountOffset:], uint16(count))
}

func (n Node) setLower(lower uint16) {
	binary.LittleEndian.PutUint16(n.page[headerLowerOffset:], lower)
}

func (n Node) setUpper(upper uint16) {
	binary.LittleEndian.PutUint16(n.page[headerUpperOffset:], upper)
}

func leafCellSize(key, value []byte) int {
	return leafCellHeaderSize + len(key) + len(value)
}

func internalCellSize(key []byte) int {
	return internalCellHeaderSize + len(key)
}

func binaryPutInternalCell(dst []byte, child uint64, key []byte) {
	putUint64(dst[0:], child)
	putUint16(dst[8:], uint16(len(key)))
	copy(dst[internalCellHeaderSize:], key)
}

func putUint16(dst []byte, value uint16) {
	binary.LittleEndian.PutUint16(dst, value)
}

func putUint64(dst []byte, value uint64) {
	binary.LittleEndian.PutUint64(dst, value)
}
