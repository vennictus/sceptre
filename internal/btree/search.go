package btree

import "bytes"

// Search returns the slot position for the provided key.
//
// If found is true, index identifies the matching cell. Otherwise, index is the
// insertion position that preserves sorted order.
func (n Node) Search(key []byte) (index int, found bool, err error) {
	switch n.Type() {
	case NodeTypeLeaf:
		return n.search(key, n.leafKey)
	case NodeTypeInternal:
		return n.search(key, n.internalKey)
	default:
		return 0, false, ErrUnknownNodeType
	}
}

func (n Node) search(key []byte, keyAt func(int) ([]byte, error)) (index int, found bool, err error) {
	low := 0
	high := n.Count()

	for low < high {
		mid := low + (high-low)/2
		midKey, err := keyAt(mid)
		if err != nil {
			return 0, false, err
		}

		switch cmp := bytes.Compare(midKey, key); {
		case cmp < 0:
			low = mid + 1
		case cmp > 0:
			high = mid
		default:
			return mid, true, nil
		}
	}

	return low, false, nil
}

func (n Node) leafKey(index int) ([]byte, error) {
	cell, err := n.LeafCell(index)
	if err != nil {
		return nil, err
	}
	return cell.Key, nil
}

func (n Node) internalKey(index int) ([]byte, error) {
	cell, err := n.InternalCell(index)
	if err != nil {
		return nil, err
	}
	return cell.Key, nil
}
