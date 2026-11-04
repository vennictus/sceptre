package table

import "github.com/vennictus/sceptre/internal/tx"

// Atomic runs fn so that all of its writes commit together or not at all.
//
// On a DB opened directly over a KV store, each table operation would
// otherwise commit on its own, so a statement that writes in several steps
// (an upsert row by row, a column and its unique index) could leave part of
// its work behind. Atomic buffers fn's writes in one transaction and commits
// them once, after fn succeeds. On a transactional DB fn runs directly: the
// caller's transaction, and its savepoints, own atomicity.
func (db *DB) Atomic(fn func(*DB) error) error {
	if db == nil || db.kv == nil {
		return fn(db)
	}
	inner := tx.Begin(db.kv)
	if err := fn(NewTransactional(inner)); err != nil {
		inner.Abort()
		return err
	}
	if err := inner.Commit(); err != nil {
		inner.Abort()
		return err
	}
	return nil
}
