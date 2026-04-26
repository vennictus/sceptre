package sql

import (
	"bytes"
	"fmt"
	"strings"

	"github.com/vennictus/sceptre/internal/table"
)

type AccessPath string

const (
	AccessTableScan            AccessPath = "table_scan"
	AccessPrimaryKeyLookup     AccessPath = "primary_key_lookup"
	AccessPrimaryKeyRange      AccessPath = "primary_key_range"
	AccessSecondaryIndexLookup AccessPath = "secondary_index_lookup"
)

type Condition struct {
	Column  string
	Op      string
	Literal Literal
}

type Plan struct {
	Statement string
	Table     string
	Access    AccessPath
	Index     string
	Lookup    []Condition
	Lower     *Condition
	Upper     *Condition
	Residual  Expr
	Limit     *int64
	Offset    *int64
}

type termInfo struct {
	expr Expr
	cond *Condition
}

// Explain parses a statement and returns the chosen access path.
func Explain(db *table.DB, input string) (Plan, error) {
	stmt, err := Parse(input)
	if err != nil {
		return Plan{}, err
	}
	return ExplainStatement(db, stmt)
}

// ExplainStatement returns the chosen access path for a parsed statement.
func ExplainStatement(db *table.DB, stmt Statement) (Plan, error) {
	switch stmt := stmt.(type) {
	case *SelectStmt:
		plan, _, err := planQuery(db, "select", stmt.Table, stmt.Where)
		if err != nil {
			return Plan{}, err
		}
		plan.Limit = stmt.Limit
		plan.Offset = stmt.Offset
		return plan, nil
	case *UpdateStmt:
		plan, _, err := planQuery(db, "update", stmt.Table, stmt.Where)
		return plan, err
	case *DeleteStmt:
		plan, _, err := planQuery(db, "delete", stmt.Table, stmt.Where)
		return plan, err
	default:
		return Plan{}, fmt.Errorf("%w: explain only supports select, update, and delete", ErrExec)
	}
}

func planQuery(db *table.DB, statement, tableName string, where Expr) (Plan, table.TableDef, error) {
	def, ok, err := db.Table(tableName)
	if err != nil {
		return Plan{}, table.TableDef{}, err
	}
	if !ok {
		return Plan{}, table.TableDef{}, table.ErrTableNotFound
	}

	terms := analyzeTerms(where)
	plan := Plan{
		Statement: statement,
		Table:     tableName,
		Access:    AccessTableScan,
		Residual:  where,
	}

	if lookup, used := planPrimaryKeyLookup(def, terms); len(lookup) > 0 {
		plan.Access = AccessPrimaryKeyLookup
		plan.Lookup = lookup
		plan.Residual = residualExpr(terms, used)
		return plan, def, nil
	}

	if indexName, lookup, used := planIndexLookup(def, terms); indexName != "" {
		plan.Access = AccessSecondaryIndexLookup
		plan.Index = indexName
		plan.Lookup = lookup
		plan.Residual = residualExpr(terms, used)
		return plan, def, nil
	}

	if lower, upper, used := planPrimaryKeyRange(def, terms); lower != nil || upper != nil {
		plan.Access = AccessPrimaryKeyRange
		plan.Lower = lower
		plan.Upper = upper
		plan.Residual = residualExpr(terms, used)
		return plan, def, nil
	}

	return plan, def, nil
}

func analyzeTerms(expr Expr) []termInfo {
	parts := splitAnd(expr)
	terms := make([]termInfo, 0, len(parts))
	for _, part := range parts {
		cond, ok := normalizeCondition(part)
		if ok {
			terms = append(terms, termInfo{expr: part, cond: &cond})
			continue
		}
		terms = append(terms, termInfo{expr: part})
	}
	return terms
}

func splitAnd(expr Expr) []Expr {
	if expr == nil {
		return nil
	}
	binary, ok := expr.(*BinaryExpr)
	if !ok || binary.Op != "and" {
		return []Expr{expr}
	}
	left := splitAnd(binary.Left)
	right := splitAnd(binary.Right)
	return append(left, right...)
}

func normalizeCondition(expr Expr) (Condition, bool) {
	binary, ok := expr.(*BinaryExpr)
	if !ok {
		return Condition{}, false
	}
	if binary.Op == "and" || binary.Op == "or" {
		return Condition{}, false
	}

	if ident, ok := binary.Left.(*IdentExpr); ok {
		literal, ok := exprLiteral(binary.Right)
		if !ok {
			return Condition{}, false
		}
		return Condition{Column: ident.Name, Op: binary.Op, Literal: literal}, true
	}
	if ident, ok := binary.Right.(*IdentExpr); ok {
		literal, ok := exprLiteral(binary.Left)
		if !ok {
			return Condition{}, false
		}
		return Condition{Column: ident.Name, Op: flipComparison(binary.Op), Literal: literal}, true
	}
	return Condition{}, false
}

func exprLiteral(expr Expr) (Literal, bool) {
	literal, ok := expr.(*Literal)
	if !ok {
		return Literal{}, false
	}
	return *literal, true
}

func flipComparison(op string) string {
	switch op {
	case "<":
		return ">"
	case "<=":
		return ">="
	case ">":
		return "<"
	case ">=":
		return "<="
	default:
		return op
	}
}

func planPrimaryKeyLookup(def table.TableDef, terms []termInfo) ([]Condition, map[int]struct{}) {
	used := make(map[int]struct{}, len(def.PrimaryKey))
	lookup := make([]Condition, 0, len(def.PrimaryKey))
	for _, column := range def.PrimaryKey {
		index := -1
		var condition Condition
		for i, term := range terms {
			if term.cond == nil || term.cond.Column != column || term.cond.Op != "=" {
				continue
			}
			index = i
			condition = *term.cond
			break
		}
		if index == -1 {
			return nil, nil
		}
		used[index] = struct{}{}
		lookup = append(lookup, condition)
	}
	return lookup, used
}

func planIndexLookup(def table.TableDef, terms []termInfo) (string, []Condition, map[int]struct{}) {
	bestName := ""
	var bestLookup []Condition
	var bestUsed map[int]struct{}
	bestColumns := 0

	for _, index := range def.Indexes {
		used := make(map[int]struct{}, len(index.Columns))
		lookup := make([]Condition, 0, len(index.Columns))
		valid := true
		for _, column := range index.Columns {
			found := -1
			var condition Condition
			for i, term := range terms {
				if term.cond == nil || term.cond.Column != column || term.cond.Op != "=" {
					continue
				}
				found = i
				condition = *term.cond
				break
			}
			if found == -1 {
				valid = false
				break
			}
			used[found] = struct{}{}
			lookup = append(lookup, condition)
		}
		if !valid || len(index.Columns) < bestColumns {
			continue
		}
		bestName = index.Name
		bestLookup = lookup
		bestUsed = used
		bestColumns = len(index.Columns)
	}

	return bestName, bestLookup, bestUsed
}

func planPrimaryKeyRange(def table.TableDef, terms []termInfo) (*Condition, *Condition, map[int]struct{}) {
	if len(def.PrimaryKey) != 1 {
		return nil, nil, nil
	}
	column := def.PrimaryKey[0]
	valueType, ok := tableColumnType(def, column)
	if !ok {
		return nil, nil, nil
	}

	var lower *Condition
	var upper *Condition
	used := make(map[int]struct{})
	lowerIndex := -1
	upperIndex := -1

	for i, term := range terms {
		if term.cond == nil || term.cond.Column != column {
			continue
		}
		switch term.cond.Op {
		case ">", ">=":
			if lower == nil || compareLowerBound(valueType, *term.cond, *lower) > 0 {
				lower = cloneCondition(*term.cond)
				lowerIndex = i
			}
		case "<", "<=":
			if upper == nil || compareUpperBound(valueType, *term.cond, *upper) < 0 {
				upper = cloneCondition(*term.cond)
				upperIndex = i
			}
		}
	}

	if lower == nil && upper == nil {
		return nil, nil, nil
	}
	if lowerIndex >= 0 {
		used[lowerIndex] = struct{}{}
	}
	if upperIndex >= 0 {
		used[upperIndex] = struct{}{}
	}
	return lower, upper, used
}

func compareLowerBound(valueType table.Type, left, right Condition) int {
	leftValue, err := literalToValue(left.Literal, valueType)
	if err != nil {
		return 0
	}
	rightValue, err := literalToValue(right.Literal, valueType)
	if err != nil {
		return 0
	}
	switch valueType {
	case table.TypeInt64:
		switch {
		case leftValue.I64 < rightValue.I64:
			return -1
		case leftValue.I64 > rightValue.I64:
			return 1
		}
	case table.TypeBytes:
		return bytes.Compare(leftValue.Bytes, rightValue.Bytes)
	}

	if left.Op == right.Op {
		return 0
	}
	if left.Op == ">" {
		return 1
	}
	return -1
}

func compareUpperBound(valueType table.Type, left, right Condition) int {
	leftValue, err := literalToValue(left.Literal, valueType)
	if err != nil {
		return 0
	}
	rightValue, err := literalToValue(right.Literal, valueType)
	if err != nil {
		return 0
	}
	switch valueType {
	case table.TypeInt64:
		switch {
		case leftValue.I64 < rightValue.I64:
			return -1
		case leftValue.I64 > rightValue.I64:
			return 1
		}
	case table.TypeBytes:
		return bytes.Compare(leftValue.Bytes, rightValue.Bytes)
	}

	if left.Op == right.Op {
		return 0
	}
	if left.Op == "<" {
		return -1
	}
	return 1
}

func residualExpr(terms []termInfo, used map[int]struct{}) Expr {
	if len(terms) == 0 {
		return nil
	}
	var residual []Expr
	for i, term := range terms {
		if _, ok := used[i]; ok {
			continue
		}
		residual = append(residual, term.expr)
	}
	if len(residual) == 0 {
		return nil
	}
	out := residual[0]
	for i := 1; i < len(residual); i++ {
		out = &BinaryExpr{Left: out, Op: "and", Right: residual[i]}
	}
	return out
}

func cloneCondition(condition Condition) *Condition {
	out := condition
	return &out
}

func FormatExpr(expr Expr) string {
	if expr == nil {
		return "none"
	}
	switch expr := expr.(type) {
	case *IdentExpr:
		return expr.Name
	case *Literal:
		return FormatLiteral(*expr)
	case *BinaryExpr:
		left := FormatExpr(expr.Left)
		right := FormatExpr(expr.Right)
		if expr.Op == "and" || expr.Op == "or" {
			return fmt.Sprintf("(%s %s %s)", left, strings.ToUpper(expr.Op), right)
		}
		return fmt.Sprintf("%s %s %s", left, expr.Op, right)
	default:
		return "<expr>"
	}
}

func FormatLiteral(literal Literal) string {
	switch literal.Kind {
	case LiteralInt:
		return fmt.Sprintf("%d", literal.Int)
	case LiteralString:
		return fmt.Sprintf("'%s'", strings.ReplaceAll(literal.String, "'", "''"))
	default:
		return "<literal>"
	}
}

func formatCondition(condition Condition) string {
	return fmt.Sprintf("%s %s %s", condition.Column, condition.Op, FormatLiteral(condition.Literal))
}

func tableColumnType(def table.TableDef, name string) (table.Type, bool) {
	for _, column := range def.Columns {
		if column.Name == name {
			return column.Type, true
		}
	}
	return 0, false
}
