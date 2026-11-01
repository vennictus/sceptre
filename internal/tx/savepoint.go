package tx

import "errors"

// ErrNoSavepoint reports a savepoint that is not open.
var ErrNoSavepoint = errors.New("tx: savepoint is not open")

type undoEntry struct {
	key     string
	prev    pendingMutation
	existed bool
}

// Savepoint marks the transaction's current writes. RollbackTo the returned
// mark discards writes made after it. Savepoints nest; release them in
// reverse order with Release.
func (tx *Tx) Savepoint() (int, error) {
	if err := tx.ensureOpen(); err != nil {
		return 0, err
	}
	tx.marks++
	return len(tx.undo), nil
}

// RollbackTo discards writes made after mark and keeps the savepoint open.
// Reads stay recorded for conflict detection.
func (tx *Tx) RollbackTo(mark int) error {
	if err := tx.ensureOpen(); err != nil {
		return err
	}
	if tx.marks == 0 || mark < 0 || mark > len(tx.undo) {
		return ErrNoSavepoint
	}
	for i := len(tx.undo) - 1; i >= mark; i-- {
		u := tx.undo[i]
		if u.existed {
			tx.pending[u.key] = u.prev
			continue
		}
		// The write added this key, so it is the last one in order.
		delete(tx.pending, u.key)
		delete(tx.writes, u.key)
		tx.order = tx.order[:len(tx.order)-1]
	}
	clear(tx.undo[mark:])
	tx.undo = tx.undo[:mark]
	return nil
}

// Release closes the innermost savepoint, keeping its writes. With no open
// savepoints left, the undo log is dropped.
func (tx *Tx) Release() error {
	if err := tx.ensureOpen(); err != nil {
		return err
	}
	if tx.marks == 0 {
		return ErrNoSavepoint
	}
	tx.marks--
	if tx.marks == 0 {
		tx.undo = nil
	}
	return nil
}
