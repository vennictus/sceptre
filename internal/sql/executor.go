package sql

import (
	"errors"
	"fmt"
	"slices"
	"time"
	"unicode/utf8"

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
	case *DropTableStmt:
		return executeDropTable(db, stmt)
	case *DropIndexStmt:
		return executeDropIndex(db, stmt)
	case *AlterTableStmt:
		return executeAlterTable(db, stmt)
	case *BeginStmt, *CommitStmt, *RollbackStmt, *SavepointStmt, *ReleaseStmt:
		return Result{}, fmt.Errorf("%w: transaction control is handled by the caller", ErrExec)
	default:
		return Result{}, fmt.Errorf("%w: unknown statement", ErrExec)
	}
}

func executeCreateTable(db *table.DB, stmt *CreateTableStmt) (Result, error) {
	if stmt.IfNotExists {
		if _, ok, err := db.Table(stmt.Name); err != nil || ok {
			return Result{}, err
		}
	}
	def := table.TableDef{
		Name:       stmt.Name,
		Columns:    make([]table.Column, 0, len(stmt.Columns)),
		PrimaryKey: append([]string(nil), stmt.PrimaryKey...),
	}
	checks := append([]Expr(nil), stmt.Checks...)
	for _, column := range stmt.Columns {
		c, err := tableColumn(column, slices.Contains(stmt.PrimaryKey, column.Name))
		if err != nil {
			return Result{}, err
		}
		def.Columns = append(def.Columns, c)
		checks = append(checks, column.Checks...)
	}
	stored, err := checkStrings(def, checks)
	if err != nil {
		return Result{}, err
	}
	def.Checks = stored
	if err := db.CreateTable(def); err != nil {
		return Result{}, err
	}
	for _, column := range stmt.Columns {
		if !column.Unique {
			continue
		}
		index := table.IndexDef{Name: stmt.Name + "_" + column.Name + "_key", Columns: []string{column.Name}, Unique: true}
		if err := db.CreateIndex(stmt.Name, index); err != nil {
			return Result{}, err
		}
	}
	return Result{}, nil
}

// tableColumn converts a column definition. Columns accept NULL unless NOT
// NULL or part of the primary key.
func tableColumn(column ColumnDef, primaryKey bool) (table.Column, error) {
	typ, err := sqlType(column.Type)
	if err != nil {
		return table.Column{}, err
	}
	c := table.Column{Name: column.Name, Type: typ, Nullable: !column.NotNull && !primaryKey, DefaultNow: column.DefaultNow, AutoIncrement: column.AutoInc}
	if column.Default != nil {
		v, err := literalToValue(*column.Default, typ)
		if err != nil {
			return table.Column{}, err
		}
		c.Default = &v
	}
	return c, nil
}

// fillAutoIncrement numbers rows that omit the table's AUTOINCREMENT column.
func fillAutoIncrement(db *table.DB, def table.TableDef, rows []map[string]table.Value) error {
	var column string
	for _, c := range def.Columns {
		if c.AutoIncrement {
			column = c.Name
		}
	}
	if column == "" {
		return nil
	}
	var missing []map[string]table.Value
	for _, row := range rows {
		if _, ok := row[column]; !ok {
			missing = append(missing, row)
		}
	}
	if len(missing) == 0 {
		return nil
	}
	ids, err := db.NextIDs(def.Name, len(missing))
	if err != nil {
		return err
	}
	for i, row := range missing {
		row[column] = table.Int64Value(ids[i])
	}
	return nil
}

// fillDefaults gives every column an insert omitted its default: the
// statement's time, a stored default, or NULL.
func fillDefaults(def table.TableDef, values map[string]table.Value, now time.Time) error {
	for _, column := range def.Columns {
		if _, ok := values[column.Name]; ok {
			continue
		}
		switch {
		case column.DefaultNow:
			values[column.Name] = table.TimestampValue(now)
		case column.Default != nil:
			values[column.Name] = *column.Default
		case column.Nullable:
			values[column.Name] = table.NullValue()
		default:
			return fmt.Errorf("%w: column %s needs a value", ErrExec, column.Name)
		}
	}
	return nil
}

func executeCreateIndex(db *table.DB, stmt *CreateIndexStmt) (Result, error) {
	if stmt.IfNotExists {
		if _, ok, err := db.Index(stmt.Table, stmt.Name); err != nil || ok {
			return Result{}, err
		}
	}
	err := db.CreateIndex(stmt.Table, table.IndexDef{
		Name:    stmt.Name,
		Columns: append([]string(nil), stmt.Columns...),
		Unique:  stmt.Unique,
	})
	return Result{}, err
}

func executeInsert(db *table.DB, stmt *InsertStmt) (Result, error) {
	if len(stmt.Columns) == 0 || len(stmt.Columns) != len(stmt.Values) {
		return Result{}, fmt.Errorf("%w: column/value count mismatch", ErrExec)
	}
	def, ok, err := db.Table(stmt.Table)
	if err != nil {
		return Result{}, err
	}
	if !ok {
		return Result{}, table.ErrTableNotFound
	}

	values := make(map[string]table.Value, len(stmt.Columns))
	for i, column := range stmt.Columns {
		if _, exists := values[column]; exists {
			return Result{}, fmt.Errorf("%w: duplicate insert column %s", ErrExec, column)
		}
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
	if err := fillAutoIncrement(db, def, []map[string]table.Value{values}); err != nil {
		return Result{}, err
	}
	if err := fillDefaults(def, values, time.Now()); err != nil {
		return Result{}, err
	}
	if err := checkRow(def, table.NewRecord(values)); err != nil {
		return Result{}, err
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

	columns, err := validateSelect(def, stmt)
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
	assignments, err := validateAssignments(def, stmt.Assignments)
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

	updates := make([]table.Record, 0, len(matchesRows))
	for _, row := range matchesRows {
		updated := table.NewRecord(row.Values)
		for column, value := range assignments {
			updated.Values[column] = value
		}
		if err := checkRow(def, updated); err != nil {
			return Result{}, err
		}
		updates = append(updates, updated)
	}
	if err := db.UpdateMany(stmt.Table, updates); err != nil {
		return Result{}, err
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

	keys := make([]table.Record, 0, len(matchesRows))
	for _, row := range matchesRows {
		keys = append(keys, primaryKeyRecord(def, row))
	}
	removed, err := db.DeleteMany(stmt.Table, keys)
	if err != nil {
		return Result{}, err
	}
	return Result{RowsAffected: removed}, nil
}

// evalResult is a value or a three-valued boolean: true, false, or unknown
// (any comparison involving NULL).
type evalResult struct {
	value   table.Value
	bool    bool
	unknown bool
	isBool  bool
}

// evalWhere keeps a row only when the predicate is true, not unknown.
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
	return value.bool && !value.unknown, nil
}

// logic applies SQL's three-valued AND or OR.
func logic(op string, left, right evalResult) evalResult {
	out := evalResult{isBool: true}
	isFalse := func(r evalResult) bool { return !r.unknown && !r.bool }
	isTrue := func(r evalResult) bool { return !r.unknown && r.bool }
	switch op {
	case "and":
		switch {
		case isFalse(left) || isFalse(right):
		case left.unknown || right.unknown:
			out.unknown = true
		default:
			out.bool = true
		}
	default: // or
		switch {
		case isTrue(left) || isTrue(right):
			out.bool = true
		case left.unknown || right.unknown:
			out.unknown = true
		}
	}
	return out
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
			return logic(expr.Op, left, right), nil
		}
		left, err := evalExpr(expr.Left, row)
		if err != nil {
			return evalResult{}, err
		}
		right, err := evalExpr(expr.Right, row)
		if err != nil {
			return evalResult{}, err
		}
		if left.value.IsNull() || right.value.IsNull() {
			return evalResult{unknown: true, isBool: true}, nil
		}
		// A literal takes the type of the value it is compared with.
		if lit, ok := expr.Left.(*Literal); ok {
			if left.value, err = literalToValue(*lit, right.value.Type); err != nil {
				return evalResult{}, err
			}
		}
		if lit, ok := expr.Right.(*Literal); ok {
			if right.value, err = literalToValue(*lit, left.value.Type); err != nil {
				return evalResult{}, err
			}
		}
		result, err := compareValues(left.value, right.value, expr.Op)
		if err != nil {
			return evalResult{}, err
		}
		return evalResult{bool: result, isBool: true}, nil
	case *CastExpr:
		to, err := sqlType(expr.Type)
		if err != nil {
			return evalResult{}, err
		}
		operand, err := evalExpr(expr.Expr, row)
		if err != nil {
			return evalResult{}, err
		}
		if operand.isBool {
			return evalResult{}, fmt.Errorf("%w: CAST needs a value", ErrExec)
		}
		v := operand.value
		if lit, ok := expr.Expr.(*Literal); ok && lit.Kind == LiteralString {
			v = table.TextValue(lit.String) // a string literal casts as text
		}
		v, err = castValue(v, to)
		if err != nil {
			return evalResult{}, err
		}
		return evalResult{value: v}, nil
	case *IsNullExpr:
		operand, err := evalExpr(expr.Expr, row)
		if err != nil {
			return evalResult{}, err
		}
		if operand.isBool {
			return evalResult{}, fmt.Errorf("%w: IS NULL needs a value", ErrExec)
		}
		return evalResult{bool: operand.value.IsNull() != expr.Not, isBool: true}, nil
	default:
		return evalResult{}, fmt.Errorf("%w: unknown expression", ErrExec)
	}
}

func compareValues(left, right table.Value, op string) (bool, error) {
	if left.Type != right.Type && !(left.Type.HasBytes() && right.Type.HasBytes()) {
		return false, fmt.Errorf("%w: type mismatch", ErrExec)
	}
	cmp := table.Compare(left, right)
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
	case LiteralNull:
		return table.NullValue()
	case LiteralFloat:
		return table.Float64Value(literal.Float)
	case LiteralBool:
		return table.BoolValue(literal.Bool)
	case LiteralTimestamp:
		return table.Value{Type: table.TypeTimestamp, I64: literal.Int}
	default:
		return table.Value{}
	}
}

// literalToValue converts a literal to a column type. NULL converts to any
// type; the table layer rejects it for columns that do not accept NULL.
func literalToValue(literal Literal, valueType table.Type) (table.Value, error) {
	if literal.Kind == LiteralNull {
		return table.NullValue(), nil
	}
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
	case table.TypeText:
		if literal.Kind != LiteralString {
			return table.Value{}, fmt.Errorf("%w: expected text literal", ErrExec)
		}
		if !utf8.ValidString(literal.String) {
			return table.Value{}, fmt.Errorf("%w: text must be valid UTF-8", table.ErrInvalidValue)
		}
		return table.TextValue(literal.String), nil
	case table.TypeFloat64:
		switch literal.Kind {
		case LiteralFloat:
			return table.Float64Value(literal.Float), nil
		case LiteralInt:
			return table.Float64Value(float64(literal.Int)), nil
		}
		return table.Value{}, fmt.Errorf("%w: expected float64 literal", ErrExec)
	case table.TypeBool:
		if literal.Kind != LiteralBool {
			return table.Value{}, fmt.Errorf("%w: expected TRUE or FALSE", ErrExec)
		}
		return table.BoolValue(literal.Bool), nil
	case table.TypeTimestamp:
		switch literal.Kind {
		case LiteralTimestamp:
			return table.Value{Type: table.TypeTimestamp, I64: literal.Int}, nil
		case LiteralString:
			t, err := parseTimestamp(literal.String)
			if err != nil {
				return table.Value{}, err
			}
			return table.TimestampValue(t), nil
		}
		return table.Value{}, fmt.Errorf("%w: expected timestamp literal", ErrExec)
	default:
		return table.Value{}, fmt.Errorf("%w: unsupported column type", ErrExec)
	}
}

func sqlType(name string) (table.Type, error) {
	switch name {
	case "bytes", "blob":
		return table.TypeBytes, nil
	case "text", "string", "varchar":
		return table.TypeText, nil
	case "int64", "int", "integer", "bigint":
		return table.TypeInt64, nil
	case "float64", "float", "double", "real":
		return table.TypeFloat64, nil
	case "bool", "boolean":
		return table.TypeBool, nil
	case "timestamp":
		return table.TypeTimestamp, nil
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
		key, err := lookupRecord(def, plan.Lookup)
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
		key, err := lookupRecord(def, plan.Lookup)
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
	defer scanner.Close()
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

func lookupRecord(def table.TableDef, conditions []Condition) (table.Record, error) {
	values := make(map[string]table.Value, len(conditions))
	for _, condition := range conditions {
		value, err := conditionValue(def, condition)
		if err != nil {
			return table.Record{}, err
		}
		values[condition.Column] = value
	}
	return table.NewRecord(values), nil
}

// conditionValue converts a condition's literal to its column's type.
func conditionValue(def table.TableDef, condition Condition) (table.Value, error) {
	typ, ok := columnType(def, condition.Column)
	if !ok {
		return table.Value{}, fmt.Errorf("%w: unknown column %s", ErrExec, condition.Column)
	}
	return literalToValue(condition.Literal, typ)
}

func scanBoundsFromPlan(def table.TableDef, plan Plan) (table.ScanBounds, error) {
	var bounds table.ScanBounds
	if plan.Lower != nil {
		v, err := conditionValue(def, *plan.Lower)
		if err != nil {
			return bounds, err
		}
		bounds.Lower = &table.Bound{Key: table.NewRecord(map[string]table.Value{plan.Lower.Column: v}), Inclusive: plan.Lower.Op == ">="}
	}
	if plan.Upper != nil {
		v, err := conditionValue(def, *plan.Upper)
		if err != nil {
			return bounds, err
		}
		bounds.Upper = &table.Bound{Key: table.NewRecord(map[string]table.Value{plan.Upper.Column: v}), Inclusive: plan.Upper.Op == "<="}
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
