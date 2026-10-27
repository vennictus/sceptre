// Package sceptre provides a serialized, durable SQL database for Go applications.
package sceptre

import (
	"context"
	"errors"
	"sync"

	"github.com/vennictus/sceptre/internal/kv"
	"github.com/vennictus/sceptre/internal/sql"
	"github.com/vennictus/sceptre/internal/table"
	"github.com/vennictus/sceptre/internal/tx"
)

// Value is an explicitly typed integer or byte string. NULL is not supported.
type Value = table.Value
type Type = table.Type

const (
	TypeInt64 = table.TypeInt64
	TypeBytes = table.TypeBytes
)

func Int64(v int64) Value  { return table.Int64Value(v) }
func Bytes(v []byte) Value { return table.BytesValue(v) }

type Result = sql.Result
type TableDef = table.TableDef
type Column = table.Column
type IndexDef = table.IndexDef
type Plan = sql.Plan
type CheckReport = table.CheckReport

var (
	ErrClosed           = errors.New("sceptre: database is closed")
	ErrTxClosed         = errors.New("sceptre: transaction is closed")
	ErrInvalidOptions   = errors.New("sceptre: invalid options")
	ErrLimit            = errors.New("sceptre: request limit exceeded")
	ErrCommitCommitted  = kv.ErrCommitCommitted
	ErrRecoveryRequired = kv.ErrRecoveryRequired
	// ErrReadOnly reports a write statement in a read-only transaction.
	ErrReadOnly = kv.ErrReadOnly
)

// Options controls opening and the maximum SELECT rows returned per request.
// MaxRows defaults to 1000; explicit SQL limits are capped to this bound.
type Options struct {
	PageSize uint32
	MaxRows  int
}

// DB owns a database file. All public operations are safe for concurrent calls.
// One transaction at a time owns the execution path, including read operations.
// A DB must be created with Open and must not be copied after use.
type DB struct {
	mu      sync.Mutex
	store   *kv.KV
	gate    chan struct{}
	done    chan struct{}
	active  *Tx
	closed  bool
	maxRows int
}

// Open acquires exclusive file ownership and recovers the latest valid state.
func Open(path string, opts Options) (*DB, error) {
	if opts.MaxRows < 0 || opts.MaxRows > 100000 {
		return nil, executionError(ErrInvalidOptions)
	}
	if opts.MaxRows == 0 {
		opts.MaxRows = 1000
	}
	store, err := kv.Open(path, kv.Options{PageSize: opts.PageSize})
	if err != nil {
		return nil, executionError(err)
	}
	db := &DB{store: store, gate: make(chan struct{}, 1), done: make(chan struct{}), maxRows: opts.MaxRows}
	db.gate <- struct{}{}
	return db, nil
}

// Begin waits for exclusive execution ownership. Context cancellation stops
// waiting; once begun, the caller must Commit or Rollback to release ownership.
func (db *DB) Begin(ctx context.Context) (*Tx, error) {
	if ctx == nil {
		return nil, executionError(ErrInvalidOptions)
	}
	if err := ctx.Err(); err != nil {
		return nil, executionError(err)
	}
	select {
	case <-ctx.Done():
		return nil, executionError(ctx.Err())
	case <-db.done:
		return nil, executionError(ErrClosed)
	case <-db.gate:
	}
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.closed {
		db.gate <- struct{}{}
		return nil, executionError(ErrClosed)
	}
	if err := ctx.Err(); err != nil {
		db.gate <- struct{}{}
		return nil, executionError(err)
	}
	if err := db.store.Err(); err != nil {
		db.gate <- struct{}{}
		return nil, executionError(err)
	}
	inner := tx.Begin(db.store)
	t := &Tx{db: db, inner: inner, tables: table.NewTransactional(inner)}
	db.active = t
	return t, nil
}

// Close aborts an open transaction, wakes waiting requests, and releases the file.
// It waits for any currently executing operation to finish. Repeated Close is safe.
func (db *DB) Close() error {
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.closed {
		return nil
	}
	db.closed = true
	close(db.done)
	if db.active != nil {
		db.active.finish()
	}
	return executionError(db.store.Close())
}

// Execute runs one statement in an automatic transaction. On commit failure,
// inspect Error.Code: committed and uncertain outcomes require different handling.
func (db *DB) Execute(ctx context.Context, input string, args ...Value) (Result, error) {
	t, err := db.Begin(ctx)
	if err != nil {
		return Result{}, err
	}
	defer t.Rollback()
	result, err := t.Execute(ctx, input, args...)
	if err != nil {
		return Result{}, err
	}
	return result, t.Commit(ctx)
}

func (db *DB) Tables(ctx context.Context) ([]TableDef, error) {
	t, err := db.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer t.Rollback()
	return t.Tables(ctx)
}

func (db *DB) Explain(ctx context.Context, input string, args ...Value) (Plan, error) {
	t, err := db.Begin(ctx)
	if err != nil {
		return Plan{}, err
	}
	defer t.Rollback()
	return t.Explain(ctx, input, args...)
}

func (db *DB) Check(ctx context.Context) (CheckReport, error) {
	t, err := db.Begin(ctx)
	if err != nil {
		return CheckReport{}, err
	}
	defer t.Rollback()
	db.mu.Lock()
	defer db.mu.Unlock()
	if err := t.ready(ctx); err != nil {
		return CheckReport{}, executionError(err)
	}
	r, err := table.New(db.store).Check()
	return r, executionError(err)
}

// Tx buffers schema, row, and index changes until one durable Commit. Methods
// are safe for concurrent calls, but their ordering is the order of execution.
// Failed statements leave prior successful statements intact and Tx usable.
type Tx struct {
	db     *DB
	inner  *tx.Tx
	tables *table.DB
	closed bool
}

func (t *Tx) ready(ctx context.Context) error {
	if t.closed {
		return ErrTxClosed
	}
	if t.db.closed {
		return ErrClosed
	}
	if ctx == nil {
		return ErrInvalidOptions
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	return t.db.store.Err()
}

func (t *Tx) statement(input string, args []Value) (sql.Statement, error) {
	if len(input) > 65536 || len(args) > 256 {
		return nil, ErrLimit
	}
	stmt, err := sql.ParseParameters(input, args)
	if err != nil {
		return nil, err
	}
	if s, ok := stmt.(*sql.SelectStmt); ok {
		if s.Limit == nil || *s.Limit > int64(t.db.maxRows) {
			n := int64(t.db.maxRows)
			s.Limit = &n
		}
	}
	return stmt, nil
}

// Execute checks context before execution. In-flight engine work is not yet
// interruptible. A cancellation after execution begins cannot undo a commit.
func (t *Tx) Execute(ctx context.Context, input string, args ...Value) (Result, error) {
	t.db.mu.Lock()
	defer t.db.mu.Unlock()
	if err := t.ready(ctx); err != nil {
		return Result{}, executionError(err)
	}
	stmt, err := t.statement(input, args)
	if err != nil {
		return Result{}, executionError(err)
	}
	r, err := sql.ExecuteStatement(t.tables, stmt)
	return r, executionError(err)
}

func (t *Tx) Tables(ctx context.Context) ([]TableDef, error) {
	t.db.mu.Lock()
	defer t.db.mu.Unlock()
	if err := t.ready(ctx); err != nil {
		return nil, executionError(err)
	}
	r, err := t.tables.Tables()
	return r, executionError(err)
}

func (t *Tx) Explain(ctx context.Context, input string, args ...Value) (Plan, error) {
	t.db.mu.Lock()
	defer t.db.mu.Unlock()
	if err := t.ready(ctx); err != nil {
		return Plan{}, executionError(err)
	}
	stmt, err := t.statement(input, args)
	if err != nil {
		return Plan{}, executionError(err)
	}
	r, err := sql.ExplainStatement(t.tables, stmt)
	return r, executionError(err)
}

// Commit ends the transaction on every outcome, including cancellation or
// definitely aborted storage errors. Retrying requires a new transaction.
func (t *Tx) Commit(ctx context.Context) error {
	t.db.mu.Lock()
	defer t.db.mu.Unlock()
	if t.closed {
		return executionError(ErrTxClosed)
	}
	defer t.finish()
	if err := t.ready(ctx); err != nil {
		return executionError(err)
	}
	return executionError(t.inner.Commit())
}

// Rollback discards buffered work. Repeated Rollback is safe.
func (t *Tx) Rollback() error {
	t.db.mu.Lock()
	defer t.db.mu.Unlock()
	if !t.closed {
		t.finish()
	}
	return nil
}

// finish is called only while holding the database mutex.
func (t *Tx) finish() {
	t.inner.Abort()
	t.closed = true
	t.db.active = nil
	t.db.gate <- struct{}{}
}
