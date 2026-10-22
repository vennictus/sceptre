package storage

import "errors"

// ErrCursorInvalid reports access to a cursor that is not on an entry.
var ErrCursorInvalid = errors.New("storage: cursor is not positioned on an entry")

type frame struct {
	page []byte
	i, n int
}

// Cursor walks one tree in key order. The pages it reads are immutable, so
// a cursor's view never changes. It is not safe for concurrent use.
type Cursor struct {
	src   pageSource
	l     layout
	root  uint64
	stack []frame
	err   error

	// The decoded entry at the current position, so Key and Value decode once.
	cur   leafEntry
	curOK bool
}

func newCursor(src pageSource, l layout, root uint64) *Cursor {
	return &Cursor{src: src, l: l, root: root}
}

func errCursor(err error) *Cursor { return &Cursor{err: err} }

// Valid reports whether the cursor is on an entry.
func (c *Cursor) Valid() bool { return c.err == nil && len(c.stack) > 0 }

// Err returns the error that stopped the cursor, if any.
func (c *Cursor) Err() error { return c.err }

func (c *Cursor) fail(err error) error {
	c.curOK = false
	c.err = err
	c.stack = c.stack[:0]
	return err
}

func (c *Cursor) load(id uint64) (frame, error) {
	if len(c.stack) >= maxDepth {
		return frame{}, corrupt("tree deeper than %d levels", maxDepth)
	}
	page, err := c.src.page(id)
	if err != nil {
		return frame{}, err
	}
	if t := typeOf(page); t != pageLeaf && t != pageBranch {
		return frame{}, corrupt("tree page %d is a %s page", id, t)
	}
	n, err := nodeCount(page)
	if err != nil {
		return frame{}, err
	}
	if n == 0 {
		return frame{}, corrupt("tree page %d is empty", id)
	}
	return frame{page: page, n: n}, nil
}

// descend pushes pages from id down to a leaf, taking the first or last child.
func (c *Cursor) descend(id uint64, last bool) error {
	for {
		f, err := c.load(id)
		if err != nil {
			return c.fail(err)
		}
		if last {
			f.i = f.n - 1
		}
		c.stack = append(c.stack, f)
		if typeOf(f.page) == pageLeaf {
			return nil
		}
		e, err := branchAt(f.page, f.i, f.n)
		if err != nil {
			return c.fail(err)
		}
		id = e.child
	}
}

func (c *Cursor) reset() bool {
	c.curOK = false
	c.stack = c.stack[:0]
	return c.err == nil && c.root != 0
}

// SeekFirst moves to the smallest key.
func (c *Cursor) SeekFirst() error {
	if !c.reset() {
		return c.err
	}
	return c.descend(c.root, false)
}

// SeekLast moves to the largest key.
func (c *Cursor) SeekLast() error {
	if !c.reset() {
		return c.err
	}
	return c.descend(c.root, true)
}

// seek descends toward key and returns whether the leaf holds it exactly.
func (c *Cursor) seek(key []byte) (bool, error) {
	id := c.root
	for {
		f, err := c.load(id)
		if err != nil {
			return false, c.fail(err)
		}
		if typeOf(f.page) == pageLeaf {
			i, found, err := searchLeaf(f.page, key)
			if err != nil {
				return false, c.fail(err)
			}
			f.i = i
			c.stack = append(c.stack, f)
			return found, nil
		}
		i, _, err := childIndex(f.page, key)
		if err != nil {
			return false, c.fail(err)
		}
		f.i = i
		c.stack = append(c.stack, f)
		e, err := branchAt(f.page, f.i, f.n)
		if err != nil {
			return false, c.fail(err)
		}
		id = e.child
	}
}

// SeekGE moves to the first key at or after key.
func (c *Cursor) SeekGE(key []byte) error {
	if !c.reset() {
		return c.err
	}
	if _, err := c.seek(key); err != nil {
		return err
	}
	if top := &c.stack[len(c.stack)-1]; top.i == top.n {
		top.i--
		return c.Next()
	}
	return nil
}

// SeekLE moves to the last key at or before key.
func (c *Cursor) SeekLE(key []byte) error {
	if !c.reset() {
		return c.err
	}
	found, err := c.seek(key)
	if err != nil || found {
		return err
	}
	return c.Prev()
}

// Next moves to the following key; past the last key the cursor is invalid.
func (c *Cursor) Next() error {
	if !c.Valid() {
		return c.err
	}
	c.curOK = false
	for len(c.stack) > 0 {
		top := &c.stack[len(c.stack)-1]
		if top.i+1 < top.n {
			top.i++
			if typeOf(top.page) == pageLeaf {
				return nil
			}
			e, err := branchAt(top.page, top.i, top.n)
			if err != nil {
				return c.fail(err)
			}
			return c.descend(e.child, false)
		}
		c.stack = c.stack[:len(c.stack)-1]
	}
	return nil
}

// Prev moves to the preceding key; before the first key the cursor is invalid.
func (c *Cursor) Prev() error {
	if !c.Valid() {
		return c.err
	}
	c.curOK = false
	for len(c.stack) > 0 {
		top := &c.stack[len(c.stack)-1]
		if top.i > 0 {
			top.i--
			if typeOf(top.page) == pageLeaf {
				return nil
			}
			e, err := branchAt(top.page, top.i, top.n)
			if err != nil {
				return c.fail(err)
			}
			return c.descend(e.child, true)
		}
		c.stack = c.stack[:len(c.stack)-1]
	}
	return nil
}

func (c *Cursor) entry() (leafEntry, error) {
	if !c.Valid() {
		if c.err != nil {
			return leafEntry{}, c.err
		}
		return leafEntry{}, ErrCursorInvalid
	}
	if c.curOK {
		return c.cur, nil
	}
	top := c.stack[len(c.stack)-1]
	e, err := leafAt(top.page, top.i, top.n)
	if err != nil {
		return leafEntry{}, err
	}
	c.cur, c.curOK = e, true
	return e, nil
}

// Key returns a copy of the current key.
func (c *Cursor) Key() ([]byte, error) {
	e, err := c.entry()
	if err != nil {
		return nil, err
	}
	return cloneBytes(e.key), nil
}

// Value returns a copy of the current value, reading overflow pages if needed.
func (c *Cursor) Value() ([]byte, error) {
	e, err := c.entry()
	if err != nil {
		return nil, err
	}
	v, err := valueOf(c.src, c.l.pageSize, e)
	if err != nil {
		return nil, c.fail(err)
	}
	return v, nil
}

// ValueSize returns the current value's length without reading overflow pages.
func (c *Cursor) ValueSize() (int, error) {
	e, err := c.entry()
	return int(e.size), err
}
