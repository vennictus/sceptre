package sql

import (
	"fmt"

	"github.com/vennictus/sceptre/internal/table"
)

// Expression types distinguish row values from boolean predicates. Validation
// happens before planning so semantic errors cannot depend on candidate rows.
type expressionType struct {
	value   table.Type
	boolean bool
}

func validateWhere(def table.TableDef, expr Expr) error {
	if expr == nil {
		return nil
	}
	typ, err := validateExpr(def, expr)
	if err != nil {
		return err
	}
	if !typ.boolean {
		return fmt.Errorf("%w: where expression is not boolean", ErrExec)
	}
	return nil
}

func validateExpr(def table.TableDef, expr Expr) (expressionType, error) {
	switch expr := expr.(type) {
	case *IdentExpr:
		if expr == nil {
			break
		}
		typ, ok := columnType(def, expr.Name)
		if !ok {
			return expressionType{}, fmt.Errorf("%w: unknown column %s", ErrExec, expr.Name)
		}
		return expressionType{value: typ}, nil
	case *Literal:
		if expr == nil {
			break
		}
		switch expr.Kind {
		case LiteralInt:
			return expressionType{value: table.TypeInt64}, nil
		case LiteralString:
			return expressionType{value: table.TypeBytes}, nil
		}
	case *BinaryExpr:
		if expr == nil {
			break
		}
		left, err := validateExpr(def, expr.Left)
		if err != nil {
			return expressionType{}, err
		}
		right, err := validateExpr(def, expr.Right)
		if err != nil {
			return expressionType{}, err
		}
		switch expr.Op {
		case "and", "or":
			if !left.boolean || !right.boolean {
				return expressionType{}, fmt.Errorf("%w: logical operands must be boolean", ErrExec)
			}
		case "=", "!=", "<>", "<", "<=", ">", ">=":
			if left.boolean || right.boolean {
				return expressionType{}, fmt.Errorf("%w: comparison operands must be values", ErrExec)
			}
			if left.value != right.value {
				return expressionType{}, fmt.Errorf("%w: type mismatch", ErrExec)
			}
		default:
			return expressionType{}, fmt.Errorf("%w: unsupported operator %s", ErrExec, expr.Op)
		}
		return expressionType{boolean: true}, nil
	}
	return expressionType{}, fmt.Errorf("%w: invalid expression", ErrExec)
}

func validateSelect(def table.TableDef, stmt *SelectStmt) ([]string, error) {
	if stmt.Limit != nil && *stmt.Limit < 0 {
		return nil, fmt.Errorf("%w: limit must be non-negative", ErrExec)
	}
	if stmt.Offset != nil && *stmt.Offset < 0 {
		return nil, fmt.Errorf("%w: offset must be non-negative", ErrExec)
	}
	return selectColumns(def, stmt)
}

func validateAssignments(def table.TableDef, assignments []Assignment) (map[string]table.Value, error) {
	if len(assignments) == 0 {
		return nil, fmt.Errorf("%w: update requires assignments", ErrExec)
	}
	values := make(map[string]table.Value, len(assignments))
	for _, assignment := range assignments {
		if _, exists := values[assignment.Column]; exists {
			return nil, fmt.Errorf("%w: duplicate assignment to %s", ErrExec, assignment.Column)
		}
		typ, ok := columnType(def, assignment.Column)
		if !ok {
			return nil, fmt.Errorf("%w: unknown column %s", ErrExec, assignment.Column)
		}
		for _, key := range def.PrimaryKey {
			if assignment.Column == key {
				return nil, fmt.Errorf("%w: updating primary-key column %s is unsupported", ErrExec, key)
			}
		}
		value, err := literalToValue(assignment.Value, typ)
		if err != nil {
			return nil, err
		}
		values[assignment.Column] = value
	}
	return values, nil
}
