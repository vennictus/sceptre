package sceptre

import (
	"context"
	"errors"

	"github.com/vennictus/sceptre/internal/btree"
	"github.com/vennictus/sceptre/internal/pager"
	"github.com/vennictus/sceptre/internal/sql"
	"github.com/vennictus/sceptre/internal/table"
)

// Code is a stable category; error messages provide detail and may change.
type Code string

const (
	CodeClosed            Code = "closed"
	CodeTransactionClosed Code = "transaction_closed"
	CodeCanceled          Code = "canceled"
	CodeInvalid           Code = "invalid"
	CodeParse             Code = "parse"
	CodeParameter         Code = "parameter"
	CodeConstraint        Code = "constraint"
	CodeNotFound          Code = "not_found"
	CodeLimit             Code = "limit"
	CodeStorage           Code = "storage"
	CodeCommitted         Code = "committed"
	CodeUncertain         Code = "uncertain"
)

// Error preserves its underlying error for errors.Is and errors.As.
type Error struct {
	Code  Code
	Cause error
}

func (e *Error) Error() string { return string(e.Code) + ": " + e.Cause.Error() }
func (e *Error) Unwrap() error { return e.Cause }

func executionError(err error) error {
	if err == nil {
		return nil
	}
	var existing *Error
	if errors.As(err, &existing) {
		return err
	}
	code := CodeStorage
	switch {
	case errors.Is(err, ErrRecoveryRequired):
		code = CodeUncertain
	case errors.Is(err, ErrCommitCommitted):
		code = CodeCommitted
	case errors.Is(err, ErrClosed):
		code = CodeClosed
	case errors.Is(err, ErrTxClosed):
		code = CodeTransactionClosed
	case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
		code = CodeCanceled
	case errors.Is(err, ErrLimit):
		code = CodeLimit
	case errors.Is(err, btree.ErrNodeFull), errors.Is(err, btree.ErrSplitFailed):
		code = CodeLimit
	case errors.Is(err, sql.ErrParameter):
		code = CodeParameter
	case errors.Is(err, sql.ErrParse):
		code = CodeParse
	case errors.Is(err, table.ErrRowExists), errors.Is(err, table.ErrTableExists), errors.Is(err, table.ErrIndexExists):
		code = CodeConstraint
	case errors.Is(err, table.ErrRowNotFound), errors.Is(err, table.ErrTableNotFound), errors.Is(err, table.ErrIndexNotFound):
		code = CodeNotFound
	case errors.Is(err, ErrInvalidOptions), errors.Is(err, pager.ErrInvalidPageSize), errors.Is(err, btree.ErrInvalidPageSize), errors.Is(err, sql.ErrExec), errors.Is(err, table.ErrInvalidRecord), errors.Is(err, table.ErrInvalidValue), errors.Is(err, table.ErrInvalidTableDef), errors.Is(err, table.ErrInvalidIndexDef):
		code = CodeInvalid
	}
	return &Error{Code: code, Cause: err}
}
