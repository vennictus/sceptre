package sql

import (
	"errors"
	"fmt"

	"github.com/vennictus/sceptre/internal/table"
)

// ErrCheck reports a row that makes a CHECK constraint false.
var ErrCheck = errors.New("sql: check constraint violated")

// checkStrings validates CHECK expressions against def and returns their
// stored form.
func checkStrings(def table.TableDef, exprs []Expr) ([]string, error) {
	out := make([]string, 0, len(exprs))
	for _, expr := range exprs {
		if err := validateWhere(def, expr); err != nil {
			return nil, fmt.Errorf("check %s: %w", FormatExpr(expr), err)
		}
		out = append(out, FormatExpr(expr))
	}
	return out, nil
}

// checkRow fails when row makes any of the table's checks false. A check
// that is unknown, because of NULL, passes, as in standard SQL.
func checkRow(def table.TableDef, row table.Record) error {
	for _, text := range def.Checks {
		expr, err := ParseExpr(text)
		if err != nil {
			return err
		}
		r, err := evalExpr(expr, row)
		if err != nil {
			return err
		}
		if r.isBool && !r.unknown && !r.bool {
			return fmt.Errorf("%w: %s", ErrCheck, text)
		}
	}
	return nil
}

// checksReferencing returns the checks that mention column.
func checksReferencing(def table.TableDef, column string) []string {
	var out []string
	for _, text := range def.Checks {
		expr, err := ParseExpr(text)
		if err == nil && exprMentions(expr, column) {
			out = append(out, text)
		}
	}
	return out
}

// renameInChecks rewrites references to a renamed column.
func renameInChecks(checks []string, from, to string) ([]string, error) {
	out := make([]string, 0, len(checks))
	for _, text := range checks {
		expr, err := ParseExpr(text)
		if err != nil {
			return nil, err
		}
		renameIdents(expr, from, to)
		out = append(out, FormatExpr(expr))
	}
	return out, nil
}

func exprMentions(expr Expr, column string) bool {
	found := false
	walkExpr(expr, func(e Expr) {
		if id, ok := e.(*IdentExpr); ok && id.Name == column {
			found = true
		}
	})
	return found
}

func renameIdents(expr Expr, from, to string) {
	walkExpr(expr, func(e Expr) {
		if id, ok := e.(*IdentExpr); ok && id.Name == from {
			id.Name = to
		}
	})
}

func walkExpr(expr Expr, visit func(Expr)) {
	if expr == nil {
		return
	}
	visit(expr)
	switch e := expr.(type) {
	case *BinaryExpr:
		walkExpr(e.Left, visit)
		walkExpr(e.Right, visit)
	case *IsNullExpr:
		walkExpr(e.Expr, visit)
	case *CastExpr:
		walkExpr(e.Expr, visit)
	}
}
