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

	cellSize := leafCellSize(key, value)
	offset, err := n.reserveCell(cellSize)
	if err != nil {
		return err
	}

	binary.LittleEndian.PutUint16(n.page[offset:], uint16(len(key)))
	binary.LittleEndian.PutUint16(n.page[offset+2:], uint16(len(value)))
	copy(n.page[offset+leafCellHeaderSize:], key)
	copy(n.page[offset+leafCellHeaderSize+uint16(len(key)):], value)
	return nil
}

// AppendInternalCell appends an internal cell to the node's slot directory and payload area.
func (n Node) AppendInternalCell(child uint64, key []byte) error {
	if n.Type() != NodeTypeInternal {
		return ErrNodeTypeMismatch
	}

	cellSize := internalCellSize(key)
	offset, err := n.reserveCell(cellSize)
	if err != nil {
		return err
	}

	binary.LittleEndian.PutUint64(n.page[offset:], child)
	binary.LittleEndian.PutUint16(n.page[offset+8:], uint16(len(key)))
	copy(n.page[offset+internalCellHeaderSize:], key)
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

	keyLen := binary.LittleEndian.Uint16(n.page[offset:])
	valLen := binary.LittleEndian.Uint16(n.page[offset+2:])
	keyStart := offset + leafCellHeaderSize
	keyEnd := keyStart + keyLen
	valEnd := keyEnd + valLen
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

	keyLen := binary.LittleEndian.Uint16(n.page[offset+8:])
	keyStart := offset + internalCellHeaderSize
	keyEnd := keyStart + keyLen
	return InternalCell{
		Child: binary.LittleEndian.Uint64(n.page[offset:]),
		Key:   n.page[keyStart:keyEnd],
	}, nil
}

func (n Node) reserveCell(cellSize int) (uint16, error) {
	required := nodeSlotSize + cellSize
	if required > n.FreeSpace() {
		return 0, ErrNodeFull
	}

	newUpper := n.Upper() - uint16(cellSize)
	slotIndex := n.Count()
	n.setSlot(slotIndex, newUpper)
	n.setCount(slotIndex + 1)
	n.setLower(n.Lower() + nodeSlotSize)
	n.setUpper(newUpper)
	return newUpper, nil
}

func (n Node) cellOffset(index int) (uint16, error) {
	if index < 0 || index >= n.Count() {
		return 0, ErrCellOutOfRange
	}
	start := nodeHeaderSize + index*nodeSlotSize
	return binary.LittleEndian.Uint16(n.page[start:]), nil
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
