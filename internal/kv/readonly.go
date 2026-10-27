package kv

import "errors"

// ErrReadOnly reports a write attempted through a read-only snapshot.
var ErrReadOnly = errors.New("kv: snapshot is read-only")

// Snapshot is also a Store, so table and SQL code can run against one
// committed state without the writer: reads see the snapshot, and writes
// return ErrReadOnly.
var _ Store = (*Snapshot)(nil)

// Apply rejects every mutation; a snapshot never changes.
func (s *Snapshot) Apply([]Mutation) error { return ErrReadOnly }

// ScanIterator returns an ordered cursor over the snapshot. Closing it does
// not release the snapshot, which belongs to whoever took it.
func (s *Snapshot) ScanIterator() Cursor { return &Iterator{c: s.Cursor()} }
