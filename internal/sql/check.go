package sql

import (
	"encoding/hex"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

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
		out = append(out, StoreExpr(expr))
	}
	return out, nil
}

// StoreExpr writes expr in the form CHECK constraints are stored in. Unlike
// FormatExpr, which is for display, it is lossless: ParseExpr returns an
// expression that evaluates the same on every row. Identifiers are always
// quoted, so a column named like a keyword (today's or a later release's)
// stays a column; bytes that are not valid UTF-8 are written X'..', so the
// JSON catalog keeps them exactly; timestamps keep their type; and floats
// always read back as floats.
func StoreExpr(expr Expr) string {
	var b strings.Builder
	storeExpr(&b, expr)
	return b.String()
}

func storeExpr(b *strings.Builder, expr Expr) {
	switch e := expr.(type) {
	case *IdentExpr:
		b.WriteString(quoteIdent(e.Name))
	case *Literal:
		b.WriteString(storeLiteral(*e))
	case *BinaryExpr:
		b.WriteByte('(')
		storeExpr(b, e.Left)
		b.WriteString(" " + strings.ToUpper(e.Op) + " ")
		storeExpr(b, e.Right)
		b.WriteByte(')')
	case *CastExpr:
		b.WriteString("CAST(")
		storeExpr(b, e.Expr)
		b.WriteString(" AS " + e.Type + ")")
	case *IsNullExpr:
		b.WriteByte('(')
		storeExpr(b, e.Expr)
		if e.Not {
			b.WriteString(" IS NOT NULL)")
		} else {
			b.WriteString(" IS NULL)")
		}
	default:
		// Unreachable for parsed expressions; fail loudly on reparse.
		b.WriteString("<unknown expression>")
	}
}

func quoteIdent(name string) string {
	return `"` + strings.ReplaceAll(name, `"`, `""`) + `"`
}

func storeLiteral(l Literal) string {
	switch l.Kind {
	case LiteralString:
		if !utf8.ValidString(l.String) {
			return "X'" + hex.EncodeToString([]byte(l.String)) + "'"
		}
		return "'" + strings.ReplaceAll(l.String, "'", "''") + "'"
	case LiteralFloat:
		s := strconv.FormatFloat(l.Float, 'g', -1, 64)
		if !strings.ContainsAny(s, ".e") {
			s += ".0"
		}
		return s
	case LiteralTimestamp:
		t := time.UnixMicro(l.Int).UTC()
		if y := t.Year(); y < 0 || y > 9999 {
			// RFC 3339 has four-digit years; out of that range, keep the
			// exact microseconds.
			return fmt.Sprintf("CAST(%d AS timestamp)", l.Int)
		}
		return "TIMESTAMP '" + t.Format(time.RFC3339Nano) + "'"
	default:
		return FormatLiteral(l)
	}
}

// parsedChecks caches stored CHECK text by its parsed expression, so rows
// are checked without reparsing. Cached expressions are never mutated:
// evaluation only reads them, and renameInChecks parses its own copy.
var parsedChecks = struct {
	sync.Mutex
	m map[string]Expr
}{m: make(map[string]Expr)}

const maxParsedChecks = 4096

func parsedCheck(text string) (Expr, error) {
	parsedChecks.Lock()
	expr, ok := parsedChecks.m[text]
	parsedChecks.Unlock()
	if ok {
		return expr, nil
	}
	expr, err := ParseExpr(text)
	if err != nil {
		return nil, fmt.Errorf("stored check %s: %w", text, err)
	}
	parsedChecks.Lock()
	if len(parsedChecks.m) >= maxParsedChecks {
		clear(parsedChecks.m)
	}
	parsedChecks.m[text] = expr
	parsedChecks.Unlock()
	return expr, nil
}

// checkRow fails when row makes any of the table's checks false. A check
// that is unknown, because of NULL, passes, as in standard SQL.
func checkRow(def table.TableDef, row table.Record) error {
	for _, text := range def.Checks {
		expr, err := parsedCheck(text)
		if err != nil {
			return err
		}
		r, err := evalExpr(expr, row)
		if err != nil {
			return err
		}
		if r.isBool && !r.unknown && !r.bool {
			return fmt.Errorf("%w: %s", ErrCheck, displayCheck(text))
		}
	}
	return nil
}

// displayCheck turns stored check text back into the readable form.
func displayCheck(text string) string {
	expr, err := parsedCheck(text)
	if err != nil {
		return text
	}
	return FormatExpr(expr)
}

// checksReferencing returns the checks that mention column, for display.
func checksReferencing(def table.TableDef, column string) []string {
	var out []string
	for _, text := range def.Checks {
		expr, err := parsedCheck(text)
		if err == nil && exprMentions(expr, column) {
			out = append(out, FormatExpr(expr))
		}
	}
	return out
}

// renameInChecks rewrites references to a renamed column and validates the
// result against the renamed table, so a rename never leaves a CHECK that
// cannot run.
func renameInChecks(renamed table.TableDef, from, to string) ([]string, error) {
	exprs := make([]Expr, 0, len(renamed.Checks))
	for _, text := range renamed.Checks {
		expr, err := ParseExpr(text) // a private copy: renameIdents mutates it
		if err != nil {
			return nil, fmt.Errorf("stored check %s: %w", text, err)
		}
		renameIdents(expr, from, to)
		exprs = append(exprs, expr)
	}
	return checkStrings(renamed, exprs)
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
