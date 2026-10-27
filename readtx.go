package sceptre

import (
	"context"
	"sync"

	"github.com/vennictus/sceptre/internal/kv"
	"github.com/vennictus/sceptre/internal/sql"
	"github.com/vennictus/sceptre/internal/table"
)

// ReadTx is a read-only transaction on one committed snapshot. It never waits
// for the execution gate: it runs alongside other readers and alongside the
// write transaction, and it sees neither that transaction's pending changes
// nor anything committed after it began. Close it when done; an open ReadTx
// keeps the pages of its snapshot from being reused.
//
// Methods are safe for concurrent calls; calls on one ReadTx run one at a time.
type ReadTx struct {
	db     *DB
	snap   *kv.Snapshot
	tables *table.DB

	mu     sync.Mutex
	closed bool
}

// BeginRead starts a read-only transaction on the latest committed state.
func (db *DB) BeginRead(ctx context.Context) (*ReadTx, error) {
	if ctx == nil {
		return nil, executionError(ErrInvalidOptions)
	}
	if err := ctx.Err(); err != nil {
		return nil, executionError(err)
	}
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.closed {
		return nil, executionError(ErrClosed)
	}
	snap, err := db.store.Snapshot()
	if err != nil {
		return nil, executionError(err)
	}
	return &ReadTx{db: db, snap: snap, tables: table.NewTransactional(snap)}, nil
}

func (r *ReadTx) ready(ctx context.Context) error {
	if r.closed {
		return ErrTxClosed
	}
	if ctx == nil {
		return ErrInvalidOptions
	}
	return ctx.Err()
}

// Execute runs a SELECT on the snapshot. Any other statement fails with
// ErrReadOnly (code read_only) and leaves the ReadTx usable.
func (r *ReadTx) Execute(ctx context.Context, input string, args ...Value) (Result, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if err := r.ready(ctx); err != nil {
		return Result{}, executionError(err)
	}
	if err := r.db.enterRead(); err != nil {
		return Result{}, executionError(err)
	}
	defer r.db.reads.Done()
	stmt, err := r.db.statement(input, args)
	if err != nil {
		return Result{}, executionError(err)
	}
	if _, ok := stmt.(*sql.SelectStmt); !ok {
		return Result{}, executionError(ErrReadOnly)
	}
	res, err := sql.ExecuteStatement(r.tables, stmt)
	return res, executionError(err)
}

// Explain returns the planned access path for a statement on the snapshot.
func (r *ReadTx) Explain(ctx context.Context, input string, args ...Value) (Plan, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if err := r.ready(ctx); err != nil {
		return Plan{}, executionError(err)
	}
	if err := r.db.enterRead(); err != nil {
		return Plan{}, executionError(err)
	}
	defer r.db.reads.Done()
	stmt, err := r.db.statement(input, args)
	if err != nil {
		return Plan{}, executionError(err)
	}
	p, err := sql.ExplainStatement(r.tables, stmt)
	return p, executionError(err)
}

// Tables lists the snapshot's table definitions.
func (r *ReadTx) Tables(ctx context.Context) ([]TableDef, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if err := r.ready(ctx); err != nil {
		return nil, executionError(err)
	}
	if err := r.db.enterRead(); err != nil {
		return nil, executionError(err)
	}
	defer r.db.reads.Done()
	defs, err := r.tables.Tables()
	return defs, executionError(err)
}

// Close ends the transaction and releases its snapshot. Repeated calls are safe.
func (r *ReadTx) Close() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if !r.closed {
		r.closed = true
		r.snap.Release()
	}
	return nil
}
