// Package sceptre provides a serialized, durable SQL database for Go applications.
package sceptre

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/vennictus/sceptre/internal/kv"
	"github.com/vennictus/sceptre/internal/sql"
	"github.com/vennictus/sceptre/internal/storage"
	"github.com/vennictus/sceptre/internal/table"
	"github.com/vennictus/sceptre/internal/tx"
)

// Value is an explicitly typed integer or byte string. NULL is not supported.
type Value = table.Value
type Type = table.Type

const (
	TypeInt64 = table.TypeInt64
	TypeBytes = table.TypeBytes
	TypeText  = table.TypeText
	// TypeNull is the type of Null(); columns are never of this type.
	TypeNull = table.TypeNull
)

func Int64(v int64) Value  { return table.Int64Value(v) }
func Bytes(v []byte) Value { return table.BytesValue(v) }

// Text returns a text value; it must be valid UTF-8.
func Text(v string) Value { return table.TextValue(v) }

// Null returns the SQL NULL, as a binding for any nullable column. Results
// report NULL with Value.IsNull.
func Null() Value { return table.NullValue() }

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
	// ErrTransactionState reports a transaction statement that does not fit
	// the current state, such as BEGIN inside a transaction.
	ErrTransactionState = errors.New("sceptre: statement does not fit the transaction state")
	// ErrNoSavepoint reports ROLLBACK TO or RELEASE of a savepoint that is not open.
	ErrNoSavepoint = errors.New("sceptre: no such savepoint")
)

// Durability says when a commit reaches durable storage.
type Durability = storage.Durability

const (
	// DurabilityFull (the default) makes every commit durable before Commit
	// returns.
	DurabilityFull = storage.DurabilityFull
	// DurabilityNormal skips one of a commit's two syncs. Power loss can lose
	// the latest commit (never an earlier one) and never corrupts the file; a
	// process crash loses nothing. DB.Sync and DB.Close make it durable.
	DurabilityNormal = storage.DurabilityNormal
)

// ParseDurability accepts "full" and "normal".
func ParseDurability(s string) (Durability, error) {
	d, err := storage.ParseDurability(s)
	if err != nil {
		return 0, fmt.Errorf("%w: %w", ErrInvalidOptions, err)
	}
	return d, nil
}

// Options controls opening and the maximum SELECT rows returned per request.
// MaxRows defaults to 1000; explicit SQL limits are capped to this bound.
type Options struct {
	PageSize   uint32
	MaxRows    int
	Durability Durability
}

// DB owns a database file. All public operations are safe for concurrent calls.
// Write transactions own the execution path one at a time. Reads (SELECT
// through Execute, Tables, Explain, and ReadTx) run on snapshots alongside
// them and each other. A DB must be created with Open and must not be copied
// after use.
type DB struct {
	mu      sync.Mutex
	store   *kv.KV
	gate    chan struct{}
	done    chan struct{}
	active  *Tx
	closed  bool
	maxRows int
	reads   sync.WaitGroup // read operations in flight; Close waits for them
}

// Open acquires exclusive file ownership and recovers the latest valid state.
func Open(path string, opts Options) (*DB, error) {
	if opts.MaxRows < 0 || opts.MaxRows > 100000 || opts.Durability > DurabilityNormal {
		return nil, executionError(ErrInvalidOptions)
	}
	if opts.MaxRows == 0 {
		opts.MaxRows = 1000
	}
	store, err := kv.Open(path, kv.Options{PageSize: opts.PageSize, Durability: opts.Durability})
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
// It waits for any currently executing operation, including reads on
// snapshots, to finish; later operations fail with closed. Repeated Close is safe.
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
	// Read operations never take db.mu while running, so waiting here is safe,
	// and new ones block on db.mu until they can see closed.
	db.reads.Wait()
	return executionError(db.store.Close())
}

// Sync makes every returned commit durable. Under DurabilityFull they already
// are; under DurabilityNormal it syncs the latest commit. It waits for an open
// transaction to finish.
func (db *DB) Sync(ctx context.Context) error {
	t, err := db.Begin(ctx)
	if err != nil {
		return err
	}
	defer t.Rollback()
	return executionError(db.store.Sync())
}

// enterRead registers a read operation, failing once the database is closed.
func (db *DB) enterRead() error {
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.closed {
		return ErrClosed
	}
	db.reads.Add(1)
	return nil
}

// Execute runs one statement in an automatic transaction. A SELECT reads the
// latest committed state on a snapshot and never waits for an open write
// transaction. Other statements wait for exclusive execution. On commit
// failure, inspect Error.Code: committed and uncertain outcomes require
// different handling.
func (db *DB) Execute(ctx context.Context, input string, args ...Value) (Result, error) {
	switch kindOf(input, args) {
	case kindControl:
		return Result{}, executionError(fmt.Errorf("%w: use a Session or DB.Begin for transaction statements", ErrTransactionState))
	case kindQuery:
		r, err := db.BeginRead(ctx)
		if err != nil {
			return Result{}, err
		}
		defer r.Close()
		return r.Execute(ctx, input, args...)
	}
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

// IsQuery reports whether input is a statement that only reads (a SELECT),
// parsed with its bindings as Execute parses it. DB.Execute runs such
// statements on a snapshot without waiting for an open write transaction.
// Input that does not parse is not a query.
func IsQuery(input string, args ...Value) bool { return kindOf(input, args) == kindQuery }

type statementKind int

const (
	kindWrite   statementKind = iota // anything else, including input that does not parse
	kindQuery                        // SELECT
	kindControl                      // BEGIN, COMMIT, ROLLBACK, SAVEPOINT, RELEASE
)

// kindOf classifies input, parsing with its bindings as execution does.
// Statements that fail to parse take the write path, which reports the same
// error.
func kindOf(input string, args []Value) statementKind {
	stmt, err := sql.ParseParameters(input, args)
	switch {
	case err != nil:
		return kindWrite
	case sql.IsTransactionControl(stmt):
		return kindControl
	}
	if _, ok := stmt.(*sql.SelectStmt); ok {
		return kindQuery
	}
	return kindWrite
}

// Tables lists table definitions from the latest committed state, without
// waiting for an open write transaction.
func (db *DB) Tables(ctx context.Context) ([]TableDef, error) {
	r, err := db.BeginRead(ctx)
	if err != nil {
		return nil, err
	}
	defer r.Close()
	return r.Tables(ctx)
}

// Explain plans a statement against the latest committed state, without
// waiting for an open write transaction.
func (db *DB) Explain(ctx context.Context, input string, args ...Value) (Plan, error) {
	r, err := db.BeginRead(ctx)
	if err != nil {
		return Plan{}, err
	}
	defer r.Close()
	return r.Explain(ctx, input, args...)
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
	db         *DB
	inner      *tx.Tx
	tables     *table.DB
	closed     bool
	savepoints []savepoint // open savepoints, innermost last
}

type savepoint struct {
	name string
	mark int
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

// statement parses one statement with the request bounds and caps SELECT output.
func (db *DB) statement(input string, args []Value) (sql.Statement, error) {
	if len(input) > 65536 || len(args) > 256 {
		return nil, ErrLimit
	}
	stmt, err := sql.ParseParameters(input, args)
	if err != nil {
		return nil, err
	}
	if s, ok := stmt.(*sql.SelectStmt); ok {
		if s.Limit == nil || *s.Limit > int64(db.maxRows) {
			n := int64(db.maxRows)
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
	stmt, err := t.db.statement(input, args)
	if err != nil {
		return Result{}, executionError(err)
	}
	if sql.IsTransactionControl(stmt) {
		return Result{}, executionError(t.control(stmt))
	}
	r, err := sql.ExecuteStatement(t.tables, stmt)
	return r, executionError(err)
}

// control runs SAVEPOINT, RELEASE and ROLLBACK TO. BEGIN, COMMIT and ROLLBACK
// belong to the caller: Tx.Commit and Tx.Rollback, or a Session.
func (t *Tx) control(stmt sql.Statement) error {
	switch stmt := stmt.(type) {
	case *sql.SavepointStmt:
		mark, err := t.inner.Savepoint()
		if err != nil {
			return err
		}
		t.savepoints = append(t.savepoints, savepoint{stmt.Name, mark})
		return nil
	case *sql.ReleaseStmt:
		i := t.findSavepoint(stmt.Name)
		if i < 0 {
			return fmt.Errorf("%w: %s", ErrNoSavepoint, stmt.Name)
		}
		return t.closeSavepoints(i)
	case *sql.RollbackStmt:
		if stmt.Savepoint == "" {
			break
		}
		i := t.findSavepoint(stmt.Savepoint)
		if i < 0 {
			return fmt.Errorf("%w: %s", ErrNoSavepoint, stmt.Savepoint)
		}
		// Inner savepoints close; this one stays open, as in SQL.
		if err := t.closeSavepoints(i + 1); err != nil {
			return err
		}
		return t.inner.RollbackTo(t.savepoints[i].mark)
	}
	return fmt.Errorf("%w: a transaction is already open; use Tx.Commit or Tx.Rollback", ErrTransactionState)
}

// findSavepoint returns the innermost open savepoint with name, or -1.
func (t *Tx) findSavepoint(name string) int {
	for i := len(t.savepoints) - 1; i >= 0; i-- {
		if t.savepoints[i].name == name {
			return i
		}
	}
	return -1
}

// closeSavepoints releases savepoint i and every one inside it.
func (t *Tx) closeSavepoints(i int) error {
	for len(t.savepoints) > i {
		if err := t.inner.Release(); err != nil {
			return err
		}
		t.savepoints = t.savepoints[:len(t.savepoints)-1]
	}
	return nil
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
	stmt, err := t.db.statement(input, args)
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
