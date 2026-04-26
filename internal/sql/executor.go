package sql

import (
	"bytes"
	"errors"
	"fmt"

	"github.com/vennictus/sceptre/internal/table"
)

var ErrExec = errors.New("sql: execution error")

type Result struct {
	Columns      []string
	Rows         [][]table.Value
	RowsAffected int
}

// Execute parses and runs one SQL statement against db.
func Execute(db *table.DB, input string) (Result, error) {
	stmt, err := Parse(input)
	if err != nil {
		return Result{}, err
	}
	return ExecuteStatement(db, stmt)
}

// ExecuteStatement runs a parsed statement against db.
func ExecuteStatement(db *table.DB, stmt Statement) (Result, error) {
	switch stmt := stmt.(type) {
	case *CreateTableStmt:
		return executeCreateTable(db, stmt)
	case *CreateIndexStmt:
		return executeCreateIndex(db, stmt)
	case *InsertStmt:
		return executeInsert(db, stmt)
	case *SelectStmt:
		return executeSelect(db, stmt)
	case *UpdateStmt:
		return executeUpdate(db, stmt)
	case *DeleteStmt:
		return executeDelete(db, stmt)
	default:
		return Result{}, fmt.Errorf("%w: unknown statement", ErrExec)
	}
}

func executeCreateTable(db *table.DB, stmt *CreateTableStmt) (Result, error) {
	def := table.TableDef{
		Name:       stmt.Name,
		Columns:    make([]table.Column, 0, len(stmt.Columns)),
		PrimaryKey: append([]string(nil), stmt.PrimaryKey...),
	}
	for _, column := range stmt.Columns {
		valueType, err := sqlType(column.Type)
		if err != nil {
			return Result{}, err
		}
		def.Columns = append(def.Columns, table.Column{Name: column.Name, Type: valueType})
	}
	if err := db.CreateTable(def); err != nil {
		return Result{}, err
	}
	return Result{}, nil
}

func executeCreateIndex(db *table.DB, stmt *CreateIndexStmt) (Result, error) {
	err := db.CreateIndex(stmt.Table, table.IndexDef{
		Name:    stmt.Name,
		Columns: append([]string(nil), stmt.Columns...),
	})
	return Result{}, err
}

func executeInsert(db *table.DB, stmt *InsertStmt) (Result, error) {
	def, ok, err := db.Table(stmt.Table)
	if err != nil {
		return Result{}, err
	}
	if !ok {
		return Result{}, table.ErrTableNotFound
	}

	values := make(map[string]table.Value, len(stmt.Columns))
	for i, column := range stmt.Columns {
		valueType, ok := columnType(def, column)
		if !ok {
			return Result{}, fmt.Errorf("%w: unknown column %s", ErrExec, column)
		}
		value, err := literalToValue(stmt.Values[i], valueType)
		if err != nil {
			return Result{}, err
		}
		values[column] = value
	}
	if err := db.Insert(stmt.Table, table.NewRecord(values)); err != nil {
		return Result{}, err
	}
	return Result{RowsAffected: 1}, nil
}

func executeSelect(db *table.DB, stmt *SelectStmt) (Result, error) {
	plan, def, err := planQuery(db, "select", stmt.Table, stmt.Where)
	if err != nil {
		return Result{}, err
	}

	columns, err := selectColumns(def, stmt)
	if err != nil {
		return Result{}, err
	}
	rows, err := candidateRows(db, def, plan)
	if err != nil {
		return Result{}, err
	}

	offset := int64(0)
	if stmt.Offset != nil {
		offset = *stmt.Offset
	}
	limit := int64(-1)
	if stmt.Limit != nil {
		limit = *stmt.Limit
	}

	result := Result{Columns: columns}
	for _, row := range rows {
		matches, err := evalWhere(stmt.Where, row)
		if err != nil {
			return Result{}, err
		}
		if matches {
			if offset > 0 {
				offset--
			} else if limit != 0 {
				result.Rows = append(result.Rows, projectRow(row, columns))
				if limit > 0 {
					limit--
				}
			}
		}
	}
	result.RowsAffected = len(result.Rows)
	return result, nil
}

func executeUpdate(db *table.DB, stmt *UpdateStmt) (Result, error) {
	plan, def, err := planQuery(db, "update", stmt.Table, stmt.Where)
	if err != nil {
		return Result{}, err
	}
	rows, err := candidateRows(db, def, plan)
	if err != nil {
		return Result{}, err
	}

	var matchesRows []table.Record
	for _, row := range rows {
		matches, err := evalWhere(stmt.Where, row)
		if err != nil {
			return Result{}, err
		}
		if matches {
			matchesRows = append(matchesRows, row)
		}
	}

	for _, row := range matchesRows {
		updated := table.NewRecord(row.Values)
		for _, assignment := range stmt.Assignments {
			valueType, ok := columnType(def, assignment.Column)
			if !ok {
				return Result{}, fmt.Errorf("%w: unknown column %s", ErrExec, assignment.Column)
			}
			value, err := literalToValue(assignment.Value, valueType)
			if err != nil {
				return Result{}, err
			}
			updated.Values[assignment.Column] = value
		}
		if err := db.Update(stmt.Table, updated); err != nil {
			return Result{}, err
		}
	}
	return Result{RowsAffected: len(matchesRows)}, nil
}

func executeDelete(db *table.DB, stmt *DeleteStmt) (Result, error) {
	plan, def, err := planQuery(db, "delete", stmt.Table, stmt.Where)
	if err != nil {
		return Result{}, err
	}
	rows, err := candidateRows(db, def, plan)
	if err != nil {
		return Result{}, err
	}

	var matchesRows []table.Record
	for _, row := range rows {
		matches, err := evalWhere(stmt.Where, row)
		if err != nil {
			return Result{}, err
		}
		if matches {
			matchesRows = append(matchesRows, row)
		}
	}

	for _, row := range matchesRows {
		removed, err := db.Delete(stmt.Table, primaryKeyRecord(def, row))
		if err != nil {
			return Result{}, err
		}
		if !removed {
			return Result{}, table.ErrRowNotFound
		}
	}
	return Result{RowsAffected: len(matchesRows)}, nil
}

type evalResult struct {
	value  table.Value
	bool   bool
	isBool bool
}

func evalWhere(expr Expr, row table.Record) (bool, error) {
	if expr == nil {
		return true, nil
	}
	value, err := evalExpr(expr, row)
	if err != nil {
		return false, err
	}
	if !value.isBool {
		return false, fmt.Errorf("%w: where expression is not boolean", ErrExec)
	}
	return value.bool, nil
}

func evalExpr(expr Expr, row table.Record) (evalResult, error) {
	switch expr := expr.(type) {
	case *IdentExpr:
		value, ok := row.Values[expr.Name]
		if !ok {
			return evalResult{}, fmt.Errorf("%w: unknown column %s", ErrExec, expr.Name)
		}
		return evalResult{value: value}, nil
	case *Literal:
		return evalResult{value: literalUntyped(*expr)}, nil
	case *BinaryExpr:
		if expr.Op == "and" || expr.Op == "or" {
			left, err := evalExpr(expr.Left, row)
			if err != nil {
				return evalResult{}, err
			}
			right, err := evalExpr(expr.Right, row)
			if err != nil {
				return evalResult{}, err
			}
			if !left.isBool || !right.isBool {
				return evalResult{}, fmt.Errorf("%w: logical operands must be boolean", ErrExec)
			}
			if expr.Op == "and" {
				return evalResult{bool: left.bool && right.bool, isBool: true}, nil
			}
			return evalResult{bool: left.bool || right.bool, isBool: true}, nil
		}
		left, err := evalExpr(expr.Left, row)
		if err != nil {
			return evalResult{}, err
		}
		right, err := evalExpr(expr.Right, row)
		if err != nil {
			return evalResult{}, err
		}
		result, err := compareValues(left.value, right.value, expr.Op)
		if err != nil {
			return evalResult{}, err
		}
		return evalResult{bool: result, isBool: true}, nil
	default:
		return evalResult{}, fmt.Errorf("%w: unknown expression", ErrExec)
	}
}

func compareValues(left, right table.Value, op string) (bool, error) {
	if left.Type != right.Type {
		return false, fmt.Errorf("%w: type mismatch", ErrExec)
	}
	var cmp int
	switch left.Type {
	case table.TypeInt64:
		switch {
		case left.I64 < right.I64:
			cmp = -1
		case left.I64 > right.I64:
			cmp = 1
		}
	case table.TypeBytes:
		cmp = bytes.Compare(left.Bytes, right.Bytes)
	default:
		return false, fmt.Errorf("%w: unsupported value type", ErrExec)
	}
	switch op {
	case "=":
		return cmp == 0, nil
	case "!=", "<>":
		return cmp != 0, nil
	case "<":
		return cmp < 0, nil
	case "<=":
		return cmp <= 0, nil
	case ">":
		return cmp > 0, nil
	case ">=":
		return cmp >= 0, nil
	default:
		return false, fmt.Errorf("%w: unsupported operator %s", ErrExec, op)
	}
}

func literalUntyped(literal Literal) table.Value {
	switch literal.Kind {
	case LiteralInt:
		return table.Int64Value(literal.Int)
	case LiteralString:
		return table.BytesValue([]byte(literal.String))
	default:
		return table.Value{}
	}
}

func literalToValue(literal Literal, valueType table.Type) (table.Value, error) {
	switch valueType {
	case table.TypeInt64:
		if literal.Kind != LiteralInt {
			return table.Value{}, fmt.Errorf("%w: expected int64 literal", ErrExec)
		}
		return table.Int64Value(literal.Int), nil
	case table.TypeBytes:
		if literal.Kind != LiteralString {
			return table.Value{}, fmt.Errorf("%w: expected bytes literal", ErrExec)
		}
		return table.BytesValue([]byte(literal.String)), nil
	default:
		return table.Value{}, fmt.Errorf("%w: unsupported column type", ErrExec)
	}
}

func sqlType(name string) (table.Type, error) {
	switch name {
	case "bytes", "string", "text":
		return table.TypeBytes, nil
	case "int64", "int":
		return table.TypeInt64, nil
	default:
		return 0, fmt.Errorf("%w: unsupported type %s", ErrExec, name)
	}
}

func selectColumns(def table.TableDef, stmt *SelectStmt) ([]string, error) {
	if stmt.AllColumns {
		columns := make([]string, 0, len(def.Columns))
		for _, column := range def.Columns {
			columns = append(columns, column.Name)
		}
		return columns, nil
	}
	for _, name := range stmt.Columns {
		if _, ok := columnType(def, name); !ok {
			return nil, fmt.Errorf("%w: unknown column %s", ErrExec, name)
		}
	}
	return append([]string(nil), stmt.Columns...), nil
}

func columnType(def table.TableDef, name string) (table.Type, bool) {
	for _, column := range def.Columns {
		if column.Name == name {
			return column.Type, true
		}
	}
	return 0, false
}

func projectRow(row table.Record, columns []string) []table.Value {
	out := make([]table.Value, 0, len(columns))
	for _, column := range columns {
		out = append(out, row.Values[column])
	}
	return out
}

func candidateRows(db *table.DB, def table.TableDef, plan Plan) ([]table.Record, error) {
	switch plan.Access {
	case AccessPrimaryKeyLookup:
		key, err := lookupRecord(plan.Lookup)
		if err != nil {
			return nil, err
		}
		row, ok, err := db.Get(def.Name, key)
		if err != nil {
			return nil, err
		}
		if !ok {
			return nil, nil
		}
		return []table.Record{row}, nil
	case AccessSecondaryIndexLookup:
		key, err := lookupRecord(plan.Lookup)
		if err != nil {
			return nil, err
		}
		return db.LookupIndex(def.Name, plan.Index, key)
	case AccessPrimaryKeyRange:
		bounds, err := scanBoundsFromPlan(def, plan)
		if err != nil {
			return nil, err
		}
		return scanRows(db, def.Name, bounds)
	default:
		return scanRows(db, def.Name, table.ScanBounds{})
	}
}

func scanRows(db *table.DB, tableName string, bounds table.ScanBounds) ([]table.Record, error) {
	scanner, err := db.Scan(tableName, bounds)
	if err != nil {
		return nil, err
	}
	var rows []table.Record
	for scanner.Valid() {
		row, err := scanner.Deref()
		if err != nil {
			return nil, err
		}
		rows = append(rows, row)
		if err := scanner.Next(); err != nil {
			return nil, err
		}
	}
	return rows, nil
}

func lookupRecord(conditions []Condition) (table.Record, error) {
	values := make(map[string]table.Value, len(conditions))
	for _, condition := range conditions {
		values[condition.Column] = literalUntyped(condition.Literal)
	}
	return table.NewRecord(values), nil
}

func scanBoundsFromPlan(def table.TableDef, plan Plan) (table.ScanBounds, error) {
	var bounds table.ScanBounds
	if plan.Lower != nil {
		bounds.Lower = &table.Bound{
			Key:       table.NewRecord(map[string]table.Value{plan.Lower.Column: literalUntyped(plan.Lower.Literal)}),
			Inclusive: plan.Lower.Op == ">=",
		}
	}
	if plan.Upper != nil {
		bounds.Upper = &table.Bound{
			Key:       table.NewRecord(map[string]table.Value{plan.Upper.Column: literalUntyped(plan.Upper.Literal)}),
			Inclusive: plan.Upper.Op == "<=",
		}
	}
	return bounds, nil
}

func primaryKeyRecord(def table.TableDef, row table.Record) table.Record {
	values := make(map[string]table.Value, len(def.PrimaryKey))
	for _, name := range def.PrimaryKey {
		values[name] = row.Values[name]
	}
	return table.NewRecord(values)
}
