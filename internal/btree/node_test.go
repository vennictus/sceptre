package btree

import (
	"errors"
	"testing"
)

func TestNewNodeInitializesLeafHeader(t *testing.T) {
	t.Parallel()

	page := make([]byte, 128)
	for i := range page {
		page[i] = 0xFF
	}

	node, err := NewNode(page, NodeTypeLeaf)
	if err != nil {
		t.Fatalf("NewNode() error = %v", err)
	}

	if got, want := node.Type(), NodeTypeLeaf; got != want {
		t.Fatalf("Type() = %v, want %v", got, want)
	}
	if got, want := node.Count(), 0; got != want {
		t.Fatalf("Count() = %d, want %d", got, want)
	}
	if got, want := node.Lower(), uint16(nodeHeaderSize); got != want {
		t.Fatalf("Lower() = %d, want %d", got, want)
	}
	if got, want := node.Upper(), uint16(len(page)); got != want {
		t.Fatalf("Upper() = %d, want %d", got, want)
	}
	if got, want := node.FreeSpace(), len(page)-nodeHeaderSize; got != want {
		t.Fatalf("FreeSpace() = %d, want %d", got, want)
	}
}

func TestNewNodeRejectsInvalidInputs(t *testing.T) {
	t.Parallel()

	if _, err := NewNode(make([]byte, nodeHeaderSize-1), NodeTypeLeaf); !errors.Is(err, ErrPageTooSmall) {
		t.Fatalf("NewNode() error = %v, want %v", err, ErrPageTooSmall)
	}

	if _, err := NewNode(make([]byte, 64), NodeTypeUnknown); !errors.Is(err, ErrUnknownNodeType) {
		t.Fatalf("NewNode() error = %v, want %v", err, ErrUnknownNodeType)
	}
}

func TestLeafCellRoundTrip(t *testing.T) {
	t.Parallel()

	node, err := NewNode(make([]byte, 128), NodeTypeLeaf)
	if err != nil {
		t.Fatalf("NewNode() error = %v", err)
	}

	if err := node.AppendLeafCell([]byte("alpha"), []byte("one")); err != nil {
		t.Fatalf("AppendLeafCell() error = %v", err)
	}
	if err := node.AppendLeafCell([]byte("beta"), []byte("two")); err != nil {
		t.Fatalf("AppendLeafCell() error = %v", err)
	}

	if got, want := node.Count(), 2; got != want {
		t.Fatalf("Count() = %d, want %d", got, want)
	}
	if got, want := node.Lower(), uint16(nodeHeaderSize+2*nodeSlotSize); got != want {
		t.Fatalf("Lower() = %d, want %d", got, want)
	}

	first, err := node.LeafCell(0)
	if err != nil {
		t.Fatalf("LeafCell(0) error = %v", err)
	}
	if got, want := string(first.Key), "alpha"; got != want {
		t.Fatalf("LeafCell(0).Key = %q, want %q", got, want)
	}
	if got, want := string(first.Value), "one"; got != want {
		t.Fatalf("LeafCell(0).Value = %q, want %q", got, want)
	}

	second, err := node.LeafCell(1)
	if err != nil {
		t.Fatalf("LeafCell(1) error = %v", err)
	}
	if got, want := string(second.Key), "beta"; got != want {
		t.Fatalf("LeafCell(1).Key = %q, want %q", got, want)
	}
	if got, want := string(second.Value), "two"; got != want {
		t.Fatalf("LeafCell(1).Value = %q, want %q", got, want)
	}
}

func TestInternalCellRoundTrip(t *testing.T) {
	t.Parallel()

	node, err := NewNode(make([]byte, 128), NodeTypeInternal)
	if err != nil {
		t.Fatalf("NewNode() error = %v", err)
	}

	if err := node.AppendInternalCell(41, []byte("alpha")); err != nil {
		t.Fatalf("AppendInternalCell() error = %v", err)
	}
	if err := node.AppendInternalCell(42, []byte("beta")); err != nil {
		t.Fatalf("AppendInternalCell() error = %v", err)
	}

	first, err := node.InternalCell(0)
	if err != nil {
		t.Fatalf("InternalCell(0) error = %v", err)
	}
	if got, want := first.Child, uint64(41); got != want {
		t.Fatalf("InternalCell(0).Child = %d, want %d", got, want)
	}
	if got, want := string(first.Key), "alpha"; got != want {
		t.Fatalf("InternalCell(0).Key = %q, want %q", got, want)
	}

	second, err := node.InternalCell(1)
	if err != nil {
		t.Fatalf("InternalCell(1) error = %v", err)
	}
	if got, want := second.Child, uint64(42); got != want {
		t.Fatalf("InternalCell(1).Child = %d, want %d", got, want)
	}
	if got, want := string(second.Key), "beta"; got != want {
		t.Fatalf("InternalCell(1).Key = %q, want %q", got, want)
	}
}

func TestNodeTypeMismatchErrors(t *testing.T) {
	t.Parallel()

	leaf, err := NewNode(make([]byte, 64), NodeTypeLeaf)
	if err != nil {
		t.Fatalf("NewNode() error = %v", err)
	}
	internal, err := NewNode(make([]byte, 64), NodeTypeInternal)
	if err != nil {
		t.Fatalf("NewNode() error = %v", err)
	}

	if err := leaf.AppendInternalCell(1, []byte("x")); !errors.Is(err, ErrNodeTypeMismatch) {
		t.Fatalf("AppendInternalCell() error = %v, want %v", err, ErrNodeTypeMismatch)
	}
	if _, err := leaf.InternalCell(0); !errors.Is(err, ErrNodeTypeMismatch) {
		t.Fatalf("InternalCell() error = %v, want %v", err, ErrNodeTypeMismatch)
	}
	if err := internal.AppendLeafCell([]byte("x"), []byte("y")); !errors.Is(err, ErrNodeTypeMismatch) {
		t.Fatalf("AppendLeafCell() error = %v, want %v", err, ErrNodeTypeMismatch)
	}
	if _, err := internal.LeafCell(0); !errors.Is(err, ErrNodeTypeMismatch) {
		t.Fatalf("LeafCell() error = %v, want %v", err, ErrNodeTypeMismatch)
	}
}

func TestCellLookupOutOfRange(t *testing.T) {
	t.Parallel()

	leaf, err := NewNode(make([]byte, 64), NodeTypeLeaf)
	if err != nil {
		t.Fatalf("NewNode() error = %v", err)
	}
	if _, err := leaf.LeafCell(0); !errors.Is(err, ErrCellOutOfRange) {
		t.Fatalf("LeafCell() error = %v, want %v", err, ErrCellOutOfRange)
	}

	internal, err := NewNode(make([]byte, 64), NodeTypeInternal)
	if err != nil {
		t.Fatalf("NewNode() error = %v", err)
	}
	if _, err := internal.InternalCell(-1); !errors.Is(err, ErrCellOutOfRange) {
		t.Fatalf("InternalCell() error = %v, want %v", err, ErrCellOutOfRange)
	}
}

func TestAppendLeafCellReturnsNodeFull(t *testing.T) {
	t.Parallel()

	node, err := NewNode(make([]byte, 18), NodeTypeLeaf)
	if err != nil {
		t.Fatalf("NewNode() error = %v", err)
	}

	if err := node.AppendLeafCell([]byte("a"), []byte("b")); err != nil {
		t.Fatalf("AppendLeafCell() first insert error = %v", err)
	}
	if err := node.AppendLeafCell([]byte("c"), []byte("d")); !errors.Is(err, ErrNodeFull) {
		t.Fatalf("AppendLeafCell() second insert error = %v, want %v", err, ErrNodeFull)
	}
}
