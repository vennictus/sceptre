package btree

import "errors"

var ErrIteratorInvalid = errors.New("btree: iterator is not positioned on a valid cell")

type iterFrame struct {
	pageID uint64
	index  int
}

// Iterator walks leaf cells in key order using a root-to-leaf path.
type Iterator struct {
	tree *Tree
	path []iterFrame
}

// Iterator returns a new iterator bound to the tree.
func (t *Tree) Iterator() *Iterator {
	return &Iterator{tree: t}
}

// Valid reports whether the iterator currently references a leaf cell.
func (it *Iterator) Valid() bool {
	return len(it.path) > 0
}

// SeekFirst positions the iterator at the smallest key.
func (it *Iterator) SeekFirst() error {
	it.reset()
	if it.tree == nil || it.tree.root == 0 {
		return nil
	}
	return it.descendExtreme(it.tree.root, true)
}

// SeekLast positions the iterator at the largest key.
func (it *Iterator) SeekLast() error {
	it.reset()
	if it.tree == nil || it.tree.root == 0 {
		return nil
	}
	return it.descendExtreme(it.tree.root, false)
}

// SeekGE positions the iterator at the first key greater than or equal to key.
func (it *Iterator) SeekGE(key []byte) error {
	it.reset()
	if it.tree == nil || it.tree.root == 0 {
		return nil
	}
	return it.seek(it.tree.root, key, true)
}

// SeekLE positions the iterator at the last key less than or equal to key.
func (it *Iterator) SeekLE(key []byte) error {
	it.reset()
	if it.tree == nil || it.tree.root == 0 {
		return nil
	}
	return it.seek(it.tree.root, key, false)
}

// Next advances the iterator to the next leaf cell.
func (it *Iterator) Next() error {
	if !it.Valid() {
		return nil
	}

	leaf, err := it.leafNode()
	if err != nil {
		return err
	}
	last := len(it.path) - 1
	if it.path[last].index+1 < leaf.Count() {
		it.path[last].index++
		return nil
	}

	for depth := len(it.path) - 2; depth >= 0; depth-- {
		parent, err := it.tree.node(it.path[depth].pageID)
		if err != nil {
			return err
		}
		if it.path[depth].index+1 >= parent.Count() {
			continue
		}

		it.path[depth].index++
		child, err := parent.InternalCell(it.path[depth].index)
		if err != nil {
			return err
		}
		it.path = it.path[:depth+1]
		return it.descendExtreme(child.Child, true)
	}

	it.reset()
	return nil
}

// Prev moves the iterator to the previous leaf cell.
func (it *Iterator) Prev() error {
	if !it.Valid() {
		return nil
	}

	last := len(it.path) - 1
	if it.path[last].index > 0 {
		it.path[last].index--
		return nil
	}

	for depth := len(it.path) - 2; depth >= 0; depth-- {
		if it.path[depth].index == 0 {
			continue
		}

		parent, err := it.tree.node(it.path[depth].pageID)
		if err != nil {
			return err
		}
		it.path[depth].index--
		child, err := parent.InternalCell(it.path[depth].index)
		if err != nil {
			return err
		}
		it.path = it.path[:depth+1]
		return it.descendExtreme(child.Child, false)
	}

	it.reset()
	return nil
}

// Deref returns the current key/value pair.
func (it *Iterator) Deref() (LeafCell, error) {
	if !it.Valid() {
		return LeafCell{}, ErrIteratorInvalid
	}

	leaf, err := it.leafNode()
	if err != nil {
		return LeafCell{}, err
	}
	cell, err := leaf.LeafCell(it.path[len(it.path)-1].index)
	if err != nil {
		return LeafCell{}, err
	}
	return LeafCell{
		Key:   cloneBytes(cell.Key),
		Value: cloneBytes(cell.Value),
	}, nil
}

func (it *Iterator) seek(pageID uint64, key []byte, greaterEqual bool) error {
	node, err := it.tree.node(pageID)
	if err != nil {
		return err
	}

	switch node.Type() {
	case NodeTypeInternal:
		index, err := node.childIndexForKey(key)
		if err != nil {
			return err
		}
		it.path = append(it.path, iterFrame{pageID: pageID, index: index})
		child, err := node.InternalCell(index)
		if err != nil {
			return err
		}
		return it.seek(child.Child, key, greaterEqual)
	case NodeTypeLeaf:
		index, found, err := node.Search(key)
		if err != nil {
			return err
		}
		switch {
		case found:
			it.path = append(it.path, iterFrame{pageID: pageID, index: index})
			return nil
		case greaterEqual && index < node.Count():
			it.path = append(it.path, iterFrame{pageID: pageID, index: index})
			return nil
		case !greaterEqual && index > 0:
			it.path = append(it.path, iterFrame{pageID: pageID, index: index - 1})
			return nil
		case node.Count() == 0:
			it.reset()
			return nil
		case greaterEqual:
			it.path = append(it.path, iterFrame{pageID: pageID, index: node.Count() - 1})
			return it.Next()
		default:
			it.path = append(it.path, iterFrame{pageID: pageID, index: 0})
			return it.Prev()
		}
	default:
		return ErrUnknownNodeType
	}
}

func (it *Iterator) descendExtreme(pageID uint64, leftmost bool) error {
	for {
		node, err := it.tree.node(pageID)
		if err != nil {
			return err
		}

		switch node.Type() {
		case NodeTypeInternal:
			index := 0
			if !leftmost {
				index = node.Count() - 1
			}
			it.path = append(it.path, iterFrame{pageID: pageID, index: index})
			child, err := node.InternalCell(index)
			if err != nil {
				return err
			}
			pageID = child.Child
		case NodeTypeLeaf:
			index := 0
			if !leftmost {
				index = node.Count() - 1
			}
			it.path = append(it.path, iterFrame{pageID: pageID, index: index})
			return nil
		default:
			return ErrUnknownNodeType
		}
	}
}

func (it *Iterator) leafNode() (Node, error) {
	if !it.Valid() {
		return Node{}, ErrIteratorInvalid
	}
	return it.tree.node(it.path[len(it.path)-1].pageID)
}

func (it *Iterator) reset() {
	it.path = nil
}
