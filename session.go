package sceptre

import (
	"context"
	"fmt"
	"sync"

	"github.com/vennictus/sceptre/internal/sql"
)

// Session runs SQL the way a database connection does, including BEGIN,
// COMMIT, ROLLBACK, SAVEPOINT, RELEASE and ROLLBACK TO. Outside BEGIN each
// statement runs like DB.Execute. Inside, statements run in one Tx that holds
// exclusive execution until COMMIT or ROLLBACK, so keep it short.
//
// A failed statement inside a transaction leaves it usable, as with Tx. A
// Session is safe for concurrent calls, which run one at a time. Close rolls
// back an open transaction.
type Session struct {
	db *DB
	mu sync.Mutex
	tx *Tx
}

// Session returns a new session with no transaction open.
func (db *DB) Session() *Session { return &Session{db: db} }

// InTransaction reports whether BEGIN has opened a transaction that COMMIT or
// ROLLBACK has not yet ended.
func (s *Session) InTransaction() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.tx != nil
}

// Execute runs one statement. A COMMIT error follows Tx.Commit: inspect
// Error.Code for committed and uncertain outcomes. The transaction ends on
// every COMMIT outcome.
func (s *Session) Execute(ctx context.Context, input string, args ...Value) (Result, error) {
	return s.ExecuteChecked(ctx, nil, input, args...)
}

// ExecuteChecked is Execute with a last say over a statement's result, as
// for DB.ExecuteChecked and Tx.ExecuteChecked: an error from accept undoes
// the statement. Transaction control statements do not call accept.
func (s *Session) ExecuteChecked(ctx context.Context, accept func(Result) error, input string, args ...Value) (Result, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if kindOf(input, args) != kindControl {
		if s.tx != nil {
			return s.tx.ExecuteChecked(ctx, accept, input, args...)
		}
		return s.db.ExecuteChecked(ctx, accept, input, args...)
	}
	stmt, err := s.db.statement(input, args)
	if err != nil {
		return Result{}, executionError(err)
	}
	switch stmt := stmt.(type) {
	case *sql.BeginStmt:
		if s.tx != nil {
			return Result{}, executionError(fmt.Errorf("%w: a transaction is already open", ErrTransactionState))
		}
		t, err := s.db.Begin(ctx)
		if err != nil {
			return Result{}, err
		}
		s.tx = t
		return Result{}, nil
	case *sql.CommitStmt:
		if s.tx == nil {
			return Result{}, executionError(fmt.Errorf("%w: no transaction is open", ErrTransactionState))
		}
		t := s.tx
		s.tx = nil
		return Result{}, t.Commit(ctx)
	case *sql.RollbackStmt:
		if stmt.Savepoint != "" {
			break
		}
		if s.tx == nil {
			return Result{}, executionError(fmt.Errorf("%w: no transaction is open", ErrTransactionState))
		}
		t := s.tx
		s.tx = nil
		return Result{}, t.Rollback()
	}
	// SAVEPOINT, RELEASE and ROLLBACK TO.
	if s.tx == nil {
		return Result{}, executionError(fmt.Errorf("%w: savepoints need an open transaction", ErrTransactionState))
	}
	return s.tx.Execute(ctx, input, args...)
}

// Close rolls back an open transaction. The session stays usable.
func (s *Session) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.tx != nil {
		s.tx.Rollback()
		s.tx = nil
	}
	return nil
}
