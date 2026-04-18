package btree

import (
	"errors"
	"math"
)

var (
	ErrInvalidPageSize = errors.New("btree: invalid page size")
	ErrSplitFailed     = errors.New("btree: could not split node")
)

// Tree is an in-memory B+ tree backed by slotted pages.
//
// Page identifiers are stable within the lifetime of the tree and mirror the
// page-number approach the durable pager will use later.
type Tree struct {
	pageSize int
	root     uint64
	nextPage uint64
	pages    map[uint64][]byte
}

type leafEntry struct {
	Key   []byte
	Value []byte
}

type internalEntry struct {
	Child uint64
	Key   []byte
}

type splitResult struct {
	PageID uint64
	MaxKey []byte
}

type deleteResult struct {
	removed   bool
	empty     bool
	maxKey    []byte
	underflow bool
}

// NewTree creates an empty in-memory tree with a fixed page size.
func NewTree(pageSize int) (*Tree, error) {
	switch {
	case pageSize < nodeHeaderSize+nodeSlotSize+leafCellHeaderSize+2:
		return nil, ErrInvalidPageSize
	case pageSize > math.MaxUint16:
		return nil, ErrInvalidPageSize
	default:
		return &Tree{
			pageSize: pageSize,
			nextPage: 1,
			pages:    make(map[uint64][]byte),
		}, nil
	}
}

// Root returns the current root page ID, or zero when the tree is empty.
func (t *Tree) Root() uint64 {
	return t.root
}

// Get looks up a key starting from the current root.
func (t *Tree) Get(key []byte) ([]byte, bool, error) {
	if t.root == 0 {
		return nil, false, nil
	}
	return t.getFrom(t.root, key)
}

// Insert adds a key/value pair, splitting pages and growing the root as needed.
func (t *Tree) Insert(key, value []byte) error {
	if t.root == 0 {
		rootID, node, err := t.allocateNode(NodeTypeLeaf)
		if err != nil {
			return err
		}
		if err := node.InsertLeaf(key, value); err != nil {
			return err
		}
		t.root = rootID
		return nil
	}

	leftMax, rightSplit, err := t.insertRecursive(t.root, key, value)
	if err != nil {
		return err
	}
	if rightSplit == nil {
		return nil
	}

	rootID, root, err := t.allocateNode(NodeTypeInternal)
	if err != nil {
		return err
	}
	if err := root.AppendInternalCell(t.root, leftMax); err != nil {
		return err
	}
	if err := root.AppendInternalCell(rightSplit.PageID, rightSplit.MaxKey); err != nil {
		return err
	}
	t.root = rootID
	return nil
}

// Delete removes a key if it exists.
func (t *Tree) Delete(key []byte) (bool, error) {
	if t.root == 0 {
		return false, nil
	}

	result, err := t.deleteRecursive(t.root, key, true)
	if err != nil || !result.removed {
		return result.removed, err
	}
	if result.empty {
		delete(t.pages, t.root)
		t.root = 0
		return true, nil
	}

	root, err := t.node(t.root)
	if err != nil {
		return false, err
	}
	if root.Type() == NodeTypeInternal && root.Count() == 1 {
		entry, err := root.InternalCell(0)
		if err != nil {
			return false, err
		}
		oldRoot := t.root
		t.root = entry.Child
		delete(t.pages, oldRoot)
	}

	return true, nil
}

func (t *Tree) getFrom(pageID uint64, key []byte) ([]byte, bool, error) {
	node, err := t.node(pageID)
	if err != nil {
		return nil, false, err
	}

	switch node.Type() {
	case NodeTypeLeaf:
		index, found, err := node.Search(key)
		if err != nil || !found {
			return nil, false, err
		}
		cell, err := node.LeafCell(index)
		if err != nil {
			return nil, false, err
		}
		return cloneBytes(cell.Value), true, nil
	case NodeTypeInternal:
		index, err := node.childIndexForKey(key)
		if err != nil {
			return nil, false, err
		}
		cell, err := node.InternalCell(index)
		if err != nil {
			return nil, false, err
		}
		return t.getFrom(cell.Child, key)
	default:
		return nil, false, ErrUnknownNodeType
	}
}

func (t *Tree) deleteRecursive(pageID uint64, key []byte, isRoot bool) (deleteResult, error) {
	node, err := t.node(pageID)
	if err != nil {
		return deleteResult{}, err
	}

	switch node.Type() {
	case NodeTypeLeaf:
		return t.deleteFromLeaf(pageID, node, key, isRoot)
	case NodeTypeInternal:
		return t.deleteFromInternal(pageID, node, key, isRoot)
	default:
		return deleteResult{}, ErrUnknownNodeType
	}
}

func (t *Tree) insertRecursive(pageID uint64, key, value []byte) ([]byte, *splitResult, error) {
	node, err := t.node(pageID)
	if err != nil {
		return nil, nil, err
	}

	switch node.Type() {
	case NodeTypeLeaf:
		return t.insertIntoLeaf(pageID, node, key, value)
	case NodeTypeInternal:
		return t.insertIntoInternal(pageID, node, key, value)
	default:
		return nil, nil, ErrUnknownNodeType
	}
}

func (t *Tree) deleteFromLeaf(pageID uint64, node Node, key []byte, isRoot bool) (deleteResult, error) {
	entries, err := node.leafEntries()
	if err != nil {
		return deleteResult{}, err
	}

	index, found, err := node.Search(key)
	if err != nil {
		return deleteResult{}, err
	}
	if !found {
		return leafDeleteResult(entries, false, isRoot, t.minNodeBytes()), nil
	}

	entries = removeLeafEntry(entries, index)
	if len(entries) == 0 {
		if _, err := NewNode(t.pages[pageID], NodeTypeLeaf); err != nil {
			return deleteResult{}, err
		}
		return deleteResult{removed: true, empty: true}, nil
	}

	if _, err := buildLeafNode(t.pages[pageID], entries); err != nil {
		return deleteResult{}, err
	}
	return leafDeleteResult(entries, true, isRoot, t.minNodeBytes()), nil
}

func (t *Tree) deleteFromInternal(pageID uint64, node Node, key []byte, isRoot bool) (deleteResult, error) {
	entries, err := node.internalEntries()
	if err != nil {
		return deleteResult{}, err
	}

	childIndex, err := node.childIndexForKey(key)
	if err != nil {
		return deleteResult{}, err
	}

	childResult, err := t.deleteRecursive(entries[childIndex].Child, key, false)
	if err != nil {
		return deleteResult{}, err
	}
	if !childResult.removed {
		return internalDeleteResult(entries, false, isRoot, t.minNodeBytes()), nil
	}

	if childResult.empty {
		delete(t.pages, entries[childIndex].Child)
		entries = removeInternalEntry(entries, childIndex)
	} else {
		entries[childIndex].Key = cloneBytes(childResult.maxKey)
		if childResult.underflow && len(entries) > 1 {
			entries, err = t.fixChildUnderflow(entries, childIndex)
			if err != nil {
				return deleteResult{}, err
			}
		}
	}

	if len(entries) == 0 {
		if _, err := NewNode(t.pages[pageID], NodeTypeInternal); err != nil {
			return deleteResult{}, err
		}
		return deleteResult{removed: true, empty: true}, nil
	}

	if _, err := buildInternalNode(t.pages[pageID], entries); err != nil {
		return deleteResult{}, err
	}
	return internalDeleteResult(entries, true, isRoot, t.minNodeBytes()), nil
}

func (t *Tree) insertIntoLeaf(pageID uint64, node Node, key, value []byte) ([]byte, *splitResult, error) {
	entries, err := node.leafEntries()
	if err != nil {
		return nil, nil, err
	}

	index, found, err := node.Search(key)
	if err != nil {
		return nil, nil, err
	}
	if found {
		return nil, nil, ErrDuplicateKey
	}

	entries = insertLeafEntry(entries, index, leafEntry{
		Key:   cloneBytes(key),
		Value: cloneBytes(value),
	})

	if leafEntriesSize(entries) <= t.pageSize {
		if _, err := buildLeafNode(t.pages[pageID], entries); err != nil {
			return nil, nil, err
		}
		return cloneBytes(entries[len(entries)-1].Key), nil, nil
	}

	splitIndex, err := splitLeafEntries(entries, t.pageSize)
	if err != nil {
		return nil, nil, err
	}

	leftEntries := entries[:splitIndex]
	rightEntries := entries[splitIndex:]
	if _, err := buildLeafNode(t.pages[pageID], leftEntries); err != nil {
		return nil, nil, err
	}

	rightID, rightNode, err := t.allocateNode(NodeTypeLeaf)
	if err != nil {
		return nil, nil, err
	}
	if _, err := buildLeafNode(rightNode.page, rightEntries); err != nil {
		return nil, nil, err
	}

	return cloneBytes(leftEntries[len(leftEntries)-1].Key), &splitResult{
		PageID: rightID,
		MaxKey: cloneBytes(rightEntries[len(rightEntries)-1].Key),
	}, nil
}

func (t *Tree) fixChildUnderflow(entries []internalEntry, childIndex int) ([]internalEntry, error) {
	if childIndex > 0 {
		return t.rebalanceChildren(entries, childIndex-1, childIndex)
	}
	return t.rebalanceChildren(entries, childIndex, childIndex+1)
}

func (t *Tree) rebalanceChildren(entries []internalEntry, leftIndex, rightIndex int) ([]internalEntry, error) {
	leftNode, err := t.node(entries[leftIndex].Child)
	if err != nil {
		return nil, err
	}
	rightNode, err := t.node(entries[rightIndex].Child)
	if err != nil {
		return nil, err
	}
	if leftNode.Type() != rightNode.Type() {
		return nil, ErrNodeTypeMismatch
	}

	switch leftNode.Type() {
	case NodeTypeLeaf:
		leftEntries, err := leftNode.leafEntries()
		if err != nil {
			return nil, err
		}
		rightEntries, err := rightNode.leafEntries()
		if err != nil {
			return nil, err
		}
		return t.rebalanceLeafChildren(entries, leftIndex, rightIndex, leftEntries, rightEntries)
	case NodeTypeInternal:
		leftEntries, err := leftNode.internalEntries()
		if err != nil {
			return nil, err
		}
		rightEntries, err := rightNode.internalEntries()
		if err != nil {
			return nil, err
		}
		return t.rebalanceInternalChildren(entries, leftIndex, rightIndex, leftEntries, rightEntries)
	default:
		return nil, ErrUnknownNodeType
	}
}

func (t *Tree) rebalanceLeafChildren(parent []internalEntry, leftIndex, rightIndex int, leftEntries, rightEntries []leafEntry) ([]internalEntry, error) {
	combined := make([]leafEntry, 0, len(leftEntries)+len(rightEntries))
	combined = append(combined, leftEntries...)
	combined = append(combined, rightEntries...)

	leftPageID := parent[leftIndex].Child
	rightPageID := parent[rightIndex].Child
	if leafEntriesSize(combined) <= t.pageSize {
		if _, err := buildLeafNode(t.pages[leftPageID], combined); err != nil {
			return nil, err
		}
		delete(t.pages, rightPageID)
		parent[leftIndex].Key = cloneBytes(combined[len(combined)-1].Key)
		return removeInternalEntry(parent, rightIndex), nil
	}

	splitIndex, err := chooseRebalanceSplit(len(combined), t.minNodeBytes(), t.pageSize, func(index int) (left, right int) {
		return leafEntriesSize(combined[:index]), leafEntriesSize(combined[index:])
	})
	if err != nil {
		return nil, err
	}

	leftEntries = combined[:splitIndex]
	rightEntries = combined[splitIndex:]
	if _, err := buildLeafNode(t.pages[leftPageID], leftEntries); err != nil {
		return nil, err
	}
	if _, err := buildLeafNode(t.pages[rightPageID], rightEntries); err != nil {
		return nil, err
	}
	parent[leftIndex].Key = cloneBytes(leftEntries[len(leftEntries)-1].Key)
	parent[rightIndex].Key = cloneBytes(rightEntries[len(rightEntries)-1].Key)
	return parent, nil
}

func (t *Tree) rebalanceInternalChildren(parent []internalEntry, leftIndex, rightIndex int, leftEntries, rightEntries []internalEntry) ([]internalEntry, error) {
	combined := make([]internalEntry, 0, len(leftEntries)+len(rightEntries))
	combined = append(combined, leftEntries...)
	combined = append(combined, rightEntries...)

	leftPageID := parent[leftIndex].Child
	rightPageID := parent[rightIndex].Child
	if internalEntriesSize(combined) <= t.pageSize {
		if _, err := buildInternalNode(t.pages[leftPageID], combined); err != nil {
			return nil, err
		}
		delete(t.pages, rightPageID)
		parent[leftIndex].Key = cloneBytes(combined[len(combined)-1].Key)
		return removeInternalEntry(parent, rightIndex), nil
	}

	splitIndex, err := chooseRebalanceSplit(len(combined), t.minNodeBytes(), t.pageSize, func(index int) (left, right int) {
		return internalEntriesSize(combined[:index]), internalEntriesSize(combined[index:])
	})
	if err != nil {
		return nil, err
	}

	leftEntries = combined[:splitIndex]
	rightEntries = combined[splitIndex:]
	if _, err := buildInternalNode(t.pages[leftPageID], leftEntries); err != nil {
		return nil, err
	}
	if _, err := buildInternalNode(t.pages[rightPageID], rightEntries); err != nil {
		return nil, err
	}
	parent[leftIndex].Key = cloneBytes(leftEntries[len(leftEntries)-1].Key)
	parent[rightIndex].Key = cloneBytes(rightEntries[len(rightEntries)-1].Key)
	return parent, nil
}

func (t *Tree) insertIntoInternal(pageID uint64, node Node, key, value []byte) ([]byte, *splitResult, error) {
	entries, err := node.internalEntries()
	if err != nil {
		return nil, nil, err
	}

	childIndex, err := node.childIndexForKey(key)
	if err != nil {
		return nil, nil, err
	}

	leftMax, rightSplit, err := t.insertRecursive(entries[childIndex].Child, key, value)
	if err != nil {
		return nil, nil, err
	}

	entries[childIndex].Key = cloneBytes(leftMax)
	if rightSplit != nil {
		entries = insertInternalEntry(entries, childIndex+1, internalEntry{
			Child: rightSplit.PageID,
			Key:   cloneBytes(rightSplit.MaxKey),
		})
	}

	if internalEntriesSize(entries) <= t.pageSize {
		if _, err := buildInternalNode(t.pages[pageID], entries); err != nil {
			return nil, nil, err
		}
		return cloneBytes(entries[len(entries)-1].Key), nil, nil
	}

	splitIndex, err := splitInternalEntries(entries, t.pageSize)
	if err != nil {
		return nil, nil, err
	}

	leftEntries := entries[:splitIndex]
	rightEntries := entries[splitIndex:]
	if _, err := buildInternalNode(t.pages[pageID], leftEntries); err != nil {
		return nil, nil, err
	}

	rightID, rightNode, err := t.allocateNode(NodeTypeInternal)
	if err != nil {
		return nil, nil, err
	}
	if _, err := buildInternalNode(rightNode.page, rightEntries); err != nil {
		return nil, nil, err
	}

	return cloneBytes(leftEntries[len(leftEntries)-1].Key), &splitResult{
		PageID: rightID,
		MaxKey: cloneBytes(rightEntries[len(rightEntries)-1].Key),
	}, nil
}

func (t *Tree) allocateNode(nodeType NodeType) (uint64, Node, error) {
	pageID := t.nextPage
	t.nextPage++

	page := make([]byte, t.pageSize)
	node, err := NewNode(page, nodeType)
	if err != nil {
		return 0, Node{}, err
	}
	t.pages[pageID] = page
	return pageID, node, nil
}

func (t *Tree) node(pageID uint64) (Node, error) {
	page, ok := t.pages[pageID]
	if !ok {
		return Node{}, ErrCellOutOfRange
	}
	return WrapNode(page)
}

func (t *Tree) minNodeBytes() int {
	return t.pageSize / 4
}

func (n Node) childIndexForKey(key []byte) (int, error) {
	if n.Type() != NodeTypeInternal {
		return 0, ErrNodeTypeMismatch
	}
	if n.Count() == 0 {
		return 0, ErrCellOutOfRange
	}

	index, _, err := n.Search(key)
	if err != nil {
		return 0, err
	}
	if index >= n.Count() {
		return n.Count() - 1, nil
	}
	return index, nil
}

func (n Node) leafEntries() ([]leafEntry, error) {
	entries := make([]leafEntry, 0, n.Count())
	for i := 0; i < n.Count(); i++ {
		cell, err := n.LeafCell(i)
		if err != nil {
			return nil, err
		}
		entries = append(entries, leafEntry{
			Key:   cloneBytes(cell.Key),
			Value: cloneBytes(cell.Value),
		})
	}
	return entries, nil
}

func (n Node) internalEntries() ([]internalEntry, error) {
	entries := make([]internalEntry, 0, n.Count())
	for i := 0; i < n.Count(); i++ {
		cell, err := n.InternalCell(i)
		if err != nil {
			return nil, err
		}
		entries = append(entries, internalEntry{
			Child: cell.Child,
			Key:   cloneBytes(cell.Key),
		})
	}
	return entries, nil
}

func buildLeafNode(page []byte, entries []leafEntry) (Node, error) {
	node, err := NewNode(page, NodeTypeLeaf)
	if err != nil {
		return Node{}, err
	}
	for _, entry := range entries {
		if err := node.AppendLeafCell(entry.Key, entry.Value); err != nil {
			return Node{}, err
		}
	}
	return node, nil
}

func buildInternalNode(page []byte, entries []internalEntry) (Node, error) {
	node, err := NewNode(page, NodeTypeInternal)
	if err != nil {
		return Node{}, err
	}
	for _, entry := range entries {
		if err := node.AppendInternalCell(entry.Child, entry.Key); err != nil {
			return Node{}, err
		}
	}
	return node, nil
}

func insertLeafEntry(entries []leafEntry, index int, entry leafEntry) []leafEntry {
	entries = append(entries, leafEntry{})
	copy(entries[index+1:], entries[index:])
	entries[index] = entry
	return entries
}

func removeLeafEntry(entries []leafEntry, index int) []leafEntry {
	copy(entries[index:], entries[index+1:])
	return entries[:len(entries)-1]
}

func insertInternalEntry(entries []internalEntry, index int, entry internalEntry) []internalEntry {
	entries = append(entries, internalEntry{})
	copy(entries[index+1:], entries[index:])
	entries[index] = entry
	return entries
}

func removeInternalEntry(entries []internalEntry, index int) []internalEntry {
	copy(entries[index:], entries[index+1:])
	return entries[:len(entries)-1]
}

func splitLeafEntries(entries []leafEntry, pageSize int) (int, error) {
	return chooseSplitIndex(len(entries), func(index int) bool {
		return leafEntriesSize(entries[:index]) <= pageSize &&
			leafEntriesSize(entries[index:]) <= pageSize
	})
}

func splitInternalEntries(entries []internalEntry, pageSize int) (int, error) {
	return chooseSplitIndex(len(entries), func(index int) bool {
		return internalEntriesSize(entries[:index]) <= pageSize &&
			internalEntriesSize(entries[index:]) <= pageSize
	})
}

func chooseSplitIndex(total int, fits func(index int) bool) (int, error) {
	bestIndex := 0
	bestDistance := total
	target := total / 2

	for i := 1; i < total; i++ {
		if !fits(i) {
			continue
		}
		distance := target - i
		if distance < 0 {
			distance = -distance
		}
		if distance < bestDistance {
			bestDistance = distance
			bestIndex = i
		}
	}

	if bestIndex == 0 {
		return 0, ErrSplitFailed
	}
	return bestIndex, nil
}

func chooseRebalanceSplit(total, minBytes, pageSize int, sizes func(index int) (left, right int)) (int, error) {
	bestIndex := 0
	bestScore := math.MaxInt
	bestPenalty := math.MaxInt

	for i := 1; i < total; i++ {
		leftSize, rightSize := sizes(i)
		if leftSize > pageSize || rightSize > pageSize {
			continue
		}

		penalty := 0
		if leftSize < minBytes {
			penalty += minBytes - leftSize
		}
		if rightSize < minBytes {
			penalty += minBytes - rightSize
		}

		score := leftSize - rightSize
		if score < 0 {
			score = -score
		}

		if penalty < bestPenalty || (penalty == bestPenalty && score < bestScore) {
			bestPenalty = penalty
			bestScore = score
			bestIndex = i
		}
	}

	if bestIndex == 0 {
		return 0, ErrSplitFailed
	}
	return bestIndex, nil
}

func leafEntriesSize(entries []leafEntry) int {
	size := nodeHeaderSize + len(entries)*nodeSlotSize
	for _, entry := range entries {
		size += leafCellSize(entry.Key, entry.Value)
	}
	return size
}

func internalEntriesSize(entries []internalEntry) int {
	size := nodeHeaderSize + len(entries)*nodeSlotSize
	for _, entry := range entries {
		size += internalCellSize(entry.Key)
	}
	return size
}

func cloneBytes(src []byte) []byte {
	return append([]byte(nil), src...)
}

func leafDeleteResult(entries []leafEntry, removed, isRoot bool, minBytes int) deleteResult {
	result := deleteResult{removed: removed}
	if len(entries) == 0 {
		result.empty = true
		return result
	}
	result.maxKey = cloneBytes(entries[len(entries)-1].Key)
	result.underflow = !isRoot && leafEntriesSize(entries) < minBytes
	return result
}

func internalDeleteResult(entries []internalEntry, removed, isRoot bool, minBytes int) deleteResult {
	result := deleteResult{removed: removed}
	if len(entries) == 0 {
		result.empty = true
		return result
	}
	result.maxKey = cloneBytes(entries[len(entries)-1].Key)
	result.underflow = !isRoot && internalEntriesSize(entries) < minBytes
	return result
}
