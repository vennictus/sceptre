package sql

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
)

var ErrParse = errors.New("sql: parse error")

type parser struct {
	tokens []token
	pos    int
}

// Parse parses one SQL statement.
func Parse(input string) (Statement, error) {
	tokens, err := lex(input)
	if err != nil {
		return nil, err
	}
	return parseTokens(tokens)
}

func parseTokens(tokens []token) (Statement, error) {
	p := &parser{tokens: tokens}
	stmt, err := p.parseStatement()
	if err != nil {
		return nil, err
	}
	if !p.at(tokenEOF) {
		return nil, p.errorf("unexpected token %q", p.peek().text)
	}
	return stmt, nil
}

func (p *parser) parseStatement() (Statement, error) {
	switch {
	case p.matchKeyword("drop"):
		return p.parseDrop()
	case p.matchKeyword("alter"):
		return p.parseAlterTable()
	case p.matchKeyword("create"):
		if p.matchKeyword("table") {
			return p.parseCreateTable()
		}
		if p.matchKeyword("index") {
			return p.parseCreateIndex()
		}
		if p.matchKeyword("unique") {
			if err := p.expectKeyword("index"); err != nil {
				return nil, err
			}
			stmt, err := p.parseCreateIndex()
			if err != nil {
				return nil, err
			}
			stmt.(*CreateIndexStmt).Unique = true
			return stmt, nil
		}
	case p.matchKeyword("insert"):
		return p.parseInsert()
	case p.matchKeyword("select"):
		return p.parseSelect()
	case p.matchKeyword("update"):
		return p.parseUpdate()
	case p.matchKeyword("delete"):
		return p.parseDelete()
	case p.matchKeyword("begin"):
		p.matchTransactionWord()
		return &BeginStmt{}, nil
	case p.matchKeyword("commit"):
		p.matchTransactionWord()
		return &CommitStmt{}, nil
	case p.matchKeyword("rollback"):
		p.matchTransactionWord()
		if !p.matchKeyword("to") {
			return &RollbackStmt{}, nil
		}
		p.matchKeyword("savepoint")
		name, err := p.expectIdent()
		if err != nil {
			return nil, err
		}
		return &RollbackStmt{Savepoint: strings.ToLower(name)}, nil
	case p.matchKeyword("savepoint"):
		name, err := p.expectIdent()
		if err != nil {
			return nil, err
		}
		return &SavepointStmt{Name: strings.ToLower(name)}, nil
	case p.matchKeyword("release"):
		p.matchKeyword("savepoint")
		name, err := p.expectIdent()
		if err != nil {
			return nil, err
		}
		return &ReleaseStmt{Name: strings.ToLower(name)}, nil
	}
	return nil, p.errorf("expected statement")
}

// matchTransactionWord skips the optional TRANSACTION or WORK noise word.
func (p *parser) matchTransactionWord() {
	if !p.matchKeyword("transaction") {
		p.matchKeyword("work")
	}
}

// matchIfExists reads IF EXISTS, or IF NOT EXISTS when not is set.
func (p *parser) matchIf(not bool) (bool, error) {
	if !p.matchKeyword("if") {
		return false, nil
	}
	if not {
		if err := p.expectKeyword("not"); err != nil {
			return false, err
		}
	}
	return true, p.expectKeyword("exists")
}

func (p *parser) parseDrop() (Statement, error) {
	isTable := p.matchKeyword("table")
	if !isTable {
		if err := p.expectKeyword("index"); err != nil {
			return nil, err
		}
	}
	ifExists, err := p.matchIf(false)
	if err != nil {
		return nil, err
	}
	name, err := p.expectIdent()
	if err != nil {
		return nil, err
	}
	if isTable {
		return &DropTableStmt{Name: name, IfExists: ifExists}, nil
	}
	stmt := &DropIndexStmt{Name: name, IfExists: ifExists}
	if p.matchKeyword("on") {
		if stmt.Table, err = p.expectIdent(); err != nil {
			return nil, err
		}
	}
	return stmt, nil
}

func (p *parser) parseAlterTable() (Statement, error) {
	if err := p.expectKeyword("table"); err != nil {
		return nil, err
	}
	name, err := p.expectIdent()
	if err != nil {
		return nil, err
	}
	stmt := &AlterTableStmt{Table: name}
	switch {
	case p.matchKeyword("add"):
		p.matchKeyword("column")
		column := ColumnDef{}
		if column.Name, err = p.expectIdent(); err != nil {
			return nil, err
		}
		typeName, err := p.expectIdent()
		if err != nil {
			return nil, err
		}
		column.Type = strings.ToLower(typeName)
		if err := p.parseColumnConstraints(&column); err != nil {
			return nil, err
		}
		stmt.Add = &column
	case p.matchKeyword("drop"):
		p.matchKeyword("column")
		if stmt.Drop, err = p.expectIdent(); err != nil {
			return nil, err
		}
	case p.matchKeyword("rename"):
		if p.matchKeyword("to") {
			if stmt.RenameTo, err = p.expectIdent(); err != nil {
				return nil, err
			}
			break
		}
		p.matchKeyword("column")
		if stmt.RenameFrom, err = p.expectIdent(); err != nil {
			return nil, err
		}
		if err := p.expectKeyword("to"); err != nil {
			return nil, err
		}
		if stmt.RenameTo, err = p.expectIdent(); err != nil {
			return nil, err
		}
	default:
		return nil, p.errorf("expected ADD, DROP or RENAME")
	}
	return stmt, nil
}

func (p *parser) parseCreateTable() (Statement, error) {
	ifNotExists, err := p.matchIf(true)
	if err != nil {
		return nil, err
	}
	name, err := p.expectIdent()
	if err != nil {
		return nil, err
	}
	if err := p.expect(tokenLParen); err != nil {
		return nil, err
	}

	var columns []ColumnDef
	var primaryKey []string
	var checks []Expr
	for {
		if p.matchKeyword("check") {
			expr, err := p.parseParenExpr()
			if err != nil {
				return nil, err
			}
			checks = append(checks, expr)
		} else if p.matchKeyword("primary") {
			if err := p.expectKeyword("key"); err != nil {
				return nil, err
			}
			names, err := p.parseIdentListInParens()
			if err != nil {
				return nil, err
			}
			primaryKey = names
		} else {
			columnName, err := p.expectIdent()
			if err != nil {
				return nil, err
			}
			typeName, err := p.expectIdent()
			if err != nil {
				return nil, err
			}
			column := ColumnDef{Name: columnName, Type: strings.ToLower(typeName)}
			if err := p.parseColumnConstraints(&column); err != nil {
				return nil, err
			}
			columns = append(columns, column)
		}

		if p.match(tokenComma) {
			continue
		}
		break
	}
	if err := p.expect(tokenRParen); err != nil {
		return nil, err
	}
	for _, c := range columns {
		if !c.PrimaryKey {
			continue
		}
		if primaryKey != nil {
			return nil, p.errorf("primary key declared twice")
		}
		primaryKey = []string{c.Name}
	}
	return &CreateTableStmt{Name: name, Columns: columns, PrimaryKey: primaryKey, IfNotExists: ifNotExists, Checks: checks}, nil
}

// parseParenExpr reads ( expr ).
func (p *parser) parseParenExpr() (Expr, error) {
	if err := p.expect(tokenLParen); err != nil {
		return nil, err
	}
	expr, err := p.parseExpr()
	if err != nil {
		return nil, err
	}
	return expr, p.expect(tokenRParen)
}

// ParseExpr parses one boolean or value expression, as stored for CHECK.
func ParseExpr(input string) (Expr, error) {
	tokens, err := lex(input)
	if err != nil {
		return nil, err
	}
	p := &parser{tokens: tokens}
	expr, err := p.parseExpr()
	if err != nil {
		return nil, err
	}
	if !p.at(tokenEOF) {
		return nil, p.errorf("unexpected token %q", p.peek().text)
	}
	return expr, nil
}

// parseColumnConstraints reads NOT NULL, NULL and UNIQUE in any order.
func (p *parser) parseColumnConstraints(column *ColumnDef) error {
	for {
		switch {
		case p.matchKeyword("not"):
			if err := p.expectKeyword("null"); err != nil {
				return err
			}
			column.NotNull = true
		case p.matchKeyword("null"):
		case p.matchKeyword("unique"):
			column.Unique = true
		case p.matchKeyword("primary"):
			if err := p.expectKeyword("key"); err != nil {
				return err
			}
			column.PrimaryKey = true
		case p.matchKeyword("autoincrement"):
			column.AutoInc = true
		case p.matchKeyword("check"):
			expr, err := p.parseParenExpr()
			if err != nil {
				return err
			}
			column.Checks = append(column.Checks, expr)
		case p.matchKeyword("default"):
			if p.matchKeyword("current_timestamp") {
				column.DefaultNow = true
				continue
			}
			literal, err := p.expectLiteral()
			if err != nil {
				return err
			}
			column.Default = &literal
		default:
			return nil
		}
	}
}

func (p *parser) parseCreateIndex() (Statement, error) {
	ifNotExists, err := p.matchIf(true)
	if err != nil {
		return nil, err
	}
	name, err := p.expectIdent()
	if err != nil {
		return nil, err
	}
	if err := p.expectKeyword("on"); err != nil {
		return nil, err
	}
	table, err := p.expectIdent()
	if err != nil {
		return nil, err
	}
	columns, err := p.parseIdentListInParens()
	if err != nil {
		return nil, err
	}
	return &CreateIndexStmt{Name: name, Table: table, Columns: columns, IfNotExists: ifNotExists}, nil
}

func (p *parser) parseInsert() (Statement, error) {
	if err := p.expectKeyword("into"); err != nil {
		return nil, err
	}
	table, err := p.expectIdent()
	if err != nil {
		return nil, err
	}
	stmt := &InsertStmt{Table: table}
	if p.at(tokenLParen) {
		if stmt.Columns, err = p.parseIdentListInParens(); err != nil {
			return nil, err
		}
	}
	if p.matchKeyword("select") {
		sel, err := p.parseSelect()
		if err != nil {
			return nil, err
		}
		stmt.Select = sel.(*SelectStmt)
		return p.finishInsert(stmt)
	}
	if err := p.expectKeyword("values"); err != nil {
		return nil, err
	}
	for {
		row, err := p.parseLiteralListInParens()
		if err != nil {
			return nil, err
		}
		if stmt.Columns != nil && len(row) != len(stmt.Columns) {
			return nil, p.errorf("column/value count mismatch")
		}
		if len(stmt.Rows) > 0 && len(row) != len(stmt.Rows[0]) {
			return nil, p.errorf("rows have different lengths")
		}
		stmt.Rows = append(stmt.Rows, row)
		if !p.match(tokenComma) {
			return p.finishInsert(stmt)
		}
	}
}

// finishInsert reads INSERT's optional ON CONFLICT and RETURNING clauses.
func (p *parser) finishInsert(stmt *InsertStmt) (Statement, error) {
	var err error
	if stmt.OnConflict, err = p.parseOnConflict(); err != nil {
		return nil, err
	}
	if stmt.Returning, err = p.parseReturning(); err != nil {
		return nil, err
	}
	return stmt, nil
}

func (p *parser) parseSelect() (Statement, error) {
	stmt := &SelectStmt{}
	if p.match(tokenStar) {
		stmt.AllColumns = true
	} else {
		columns, err := p.parseIdentList()
		if err != nil {
			return nil, err
		}
		stmt.Columns = columns
	}
	if err := p.expectKeyword("from"); err != nil {
		return nil, err
	}
	table, err := p.expectIdent()
	if err != nil {
		return nil, err
	}
	stmt.Table = table
	if p.matchKeyword("where") {
		where, err := p.parseExpr()
		if err != nil {
			return nil, err
		}
		stmt.Where = where
	}
	if p.matchKeyword("limit") {
		limit, err := p.expectNumber()
		if err != nil {
			return nil, err
		}
		if limit < 0 {
			return nil, p.errorf("limit must be non-negative")
		}
		stmt.Limit = &limit
	}
	if p.matchKeyword("offset") {
		offset, err := p.expectNumber()
		if err != nil {
			return nil, err
		}
		if offset < 0 {
			return nil, p.errorf("offset must be non-negative")
		}
		stmt.Offset = &offset
	}
	return stmt, nil
}

func (p *parser) parseUpdate() (Statement, error) {
	table, err := p.expectIdent()
	if err != nil {
		return nil, err
	}
	if err := p.expectKeyword("set"); err != nil {
		return nil, err
	}
	assignments, err := p.parseAssignments(false)
	if err != nil {
		return nil, err
	}
	stmt := &UpdateStmt{Table: table, Assignments: assignments}
	if p.matchKeyword("where") {
		where, err := p.parseExpr()
		if err != nil {
			return nil, err
		}
		stmt.Where = where
	}
	if stmt.Returning, err = p.parseReturning(); err != nil {
		return nil, err
	}
	return stmt, nil
}

// parseAssignments reads col = literal, ...; with excluded set, a value may
// also be excluded.col.
func (p *parser) parseAssignments(excluded bool) ([]Assignment, error) {
	var out []Assignment
	for {
		column, err := p.expectIdent()
		if err != nil {
			return nil, err
		}
		if err := p.expect(tokenEqual); err != nil {
			return nil, err
		}
		a := Assignment{Column: column}
		if excluded && p.at(tokenIdent) && !p.peek().quoted && strings.EqualFold(p.peek().text, "excluded") && p.tokens[p.pos+1].kind == tokenDot {
			p.pos += 2
			if a.Excluded, err = p.expectIdent(); err != nil {
				return nil, err
			}
		} else if a.Value, err = p.expectLiteral(); err != nil {
			return nil, err
		}
		out = append(out, a)
		if !p.match(tokenComma) {
			return out, nil
		}
	}
}

// parseReturning reads an optional RETURNING * or RETURNING columns.
func (p *parser) parseReturning() (*Returning, error) {
	if !p.matchKeyword("returning") {
		return nil, nil
	}
	if p.match(tokenStar) {
		return &Returning{All: true}, nil
	}
	columns, err := p.parseIdentList()
	if err != nil {
		return nil, err
	}
	return &Returning{Columns: columns}, nil
}

// parseOnConflict reads an optional ON CONFLICT clause.
func (p *parser) parseOnConflict() (*OnConflict, error) {
	if !p.matchKeyword("on") {
		return nil, nil
	}
	if err := p.expectKeyword("conflict"); err != nil {
		return nil, err
	}
	out := &OnConflict{}
	if p.at(tokenLParen) {
		columns, err := p.parseIdentListInParens()
		if err != nil {
			return nil, err
		}
		out.Columns = columns
	}
	if err := p.expectKeyword("do"); err != nil {
		return nil, err
	}
	if p.matchKeyword("nothing") {
		out.DoNothing = true
		return out, nil
	}
	if err := p.expectKeyword("update"); err != nil {
		return nil, err
	}
	if out.Columns == nil {
		return nil, p.errorf("ON CONFLICT DO UPDATE needs conflict columns")
	}
	if err := p.expectKeyword("set"); err != nil {
		return nil, err
	}
	set, err := p.parseAssignments(true)
	if err != nil {
		return nil, err
	}
	out.Set = set
	return out, nil
}

func (p *parser) parseDelete() (Statement, error) {
	if err := p.expectKeyword("from"); err != nil {
		return nil, err
	}
	table, err := p.expectIdent()
	if err != nil {
		return nil, err
	}
	stmt := &DeleteStmt{Table: table}
	if p.matchKeyword("where") {
		where, err := p.parseExpr()
		if err != nil {
			return nil, err
		}
		stmt.Where = where
	}
	if stmt.Returning, err = p.parseReturning(); err != nil {
		return nil, err
	}
	return stmt, nil
}

func (p *parser) parseExpr() (Expr, error) {
	return p.parseOr()
}

func (p *parser) parseOr() (Expr, error) {
	left, err := p.parseAnd()
	if err != nil {
		return nil, err
	}
	for p.matchKeyword("or") {
		right, err := p.parseAnd()
		if err != nil {
			return nil, err
		}
		left = &BinaryExpr{Left: left, Op: "or", Right: right}
	}
	return left, nil
}

func (p *parser) parseAnd() (Expr, error) {
	left, err := p.parseComparison()
	if err != nil {
		return nil, err
	}
	for p.matchKeyword("and") {
		right, err := p.parseComparison()
		if err != nil {
			return nil, err
		}
		left = &BinaryExpr{Left: left, Op: "and", Right: right}
	}
	return left, nil
}

func (p *parser) parseComparison() (Expr, error) {
	left, err := p.parsePrimary()
	if err != nil {
		return nil, err
	}
	if p.matchKeyword("is") {
		not := p.matchKeyword("not")
		if err := p.expectKeyword("null"); err != nil {
			return nil, err
		}
		return &IsNullExpr{Expr: left, Not: not}, nil
	}
	if !p.isComparison() {
		return left, nil
	}
	op := p.next().text
	right, err := p.parsePrimary()
	if err != nil {
		return nil, err
	}
	return &BinaryExpr{Left: left, Op: op, Right: right}, nil
}

func (p *parser) parsePrimary() (Expr, error) {
	if p.match(tokenLParen) {
		expr, err := p.parseExpr()
		if err != nil {
			return nil, err
		}
		if err := p.expect(tokenRParen); err != nil {
			return nil, err
		}
		return expr, nil
	}
	if p.at(tokenIdent) && !p.peek().quoted && strings.EqualFold(p.peek().text, "cast") && p.tokens[p.pos+1].kind == tokenLParen {
		p.pos += 2
		operand, err := p.parseExpr()
		if err != nil {
			return nil, err
		}
		if err := p.expectKeyword("as"); err != nil {
			return nil, err
		}
		typeName, err := p.expectIdent()
		if err != nil {
			return nil, err
		}
		if err := p.expect(tokenRParen); err != nil {
			return nil, err
		}
		return &CastExpr{Expr: operand, Type: strings.ToLower(typeName)}, nil
	}
	if p.at(tokenIdent) && !p.atNull() && !p.atBool() && !p.atTimestampLiteral() {
		name := p.next().text
		return &IdentExpr{Name: name}, nil
	}
	literal, err := p.expectLiteral()
	if err != nil {
		return nil, err
	}
	return &literal, nil
}

func (p *parser) parseIdentListInParens() ([]string, error) {
	if err := p.expect(tokenLParen); err != nil {
		return nil, err
	}
	names, err := p.parseIdentList()
	if err != nil {
		return nil, err
	}
	if err := p.expect(tokenRParen); err != nil {
		return nil, err
	}
	return names, nil
}

func (p *parser) parseIdentList() ([]string, error) {
	first, err := p.expectIdent()
	if err != nil {
		return nil, err
	}
	names := []string{first}
	for p.match(tokenComma) {
		name, err := p.expectIdent()
		if err != nil {
			return nil, err
		}
		names = append(names, name)
	}
	return names, nil
}

func (p *parser) parseLiteralListInParens() ([]Literal, error) {
	if err := p.expect(tokenLParen); err != nil {
		return nil, err
	}
	first, err := p.expectLiteral()
	if err != nil {
		return nil, err
	}
	values := []Literal{first}
	for p.match(tokenComma) {
		value, err := p.expectLiteral()
		if err != nil {
			return nil, err
		}
		values = append(values, value)
	}
	if err := p.expect(tokenRParen); err != nil {
		return nil, err
	}
	return values, nil
}

func (p *parser) expectLiteral() (Literal, error) {
	switch p.peek().kind {
	case tokenNumber:
		value, err := p.expectNumber()
		if err != nil {
			return Literal{}, err
		}
		return Literal{Kind: LiteralInt, Int: value}, nil
	case tokenString:
		return Literal{Kind: LiteralString, String: p.next().text}, nil
	case tokenFloat:
		f, err := strconv.ParseFloat(p.next().text, 64)
		if err != nil {
			return Literal{}, fmt.Errorf("%w: invalid number", ErrParse)
		}
		return Literal{Kind: LiteralFloat, Float: f}, nil
	case tokenTimestamp:
		micros, err := strconv.ParseInt(p.next().text, 10, 64)
		if err != nil {
			return Literal{}, fmt.Errorf("%w: invalid timestamp binding", ErrParse)
		}
		return Literal{Kind: LiteralTimestamp, Int: micros}, nil
	default:
		if p.atTimestampLiteral() {
			p.next()
			t, err := parseTimestamp(p.next().text)
			if err != nil {
				return Literal{}, fmt.Errorf("%w: %v", ErrParse, err)
			}
			return Literal{Kind: LiteralTimestamp, Int: t.UnixMicro()}, nil
		}
		if p.atNull() {
			p.next()
			return Literal{Kind: LiteralNull}, nil
		}
		if p.atBool() {
			return Literal{Kind: LiteralBool, Bool: strings.EqualFold(p.next().text, "true")}, nil
		}
		return Literal{}, p.errorf("expected literal")
	}
}

// atBool reports whether the next token is TRUE or FALSE: a keyword or a binding.
func (p *parser) atBool() bool {
	tok := p.peek()
	return tok.kind == tokenBool || tok.kind == tokenIdent && !tok.quoted && (strings.EqualFold(tok.text, "true") || strings.EqualFold(tok.text, "false"))
}

// atNull reports whether the next token is NULL: the keyword or a binding.
func (p *parser) atNull() bool {
	tok := p.peek()
	return tok.kind == tokenNull || tok.kind == tokenIdent && !tok.quoted && strings.EqualFold(tok.text, "null")
}

// atTimestampLiteral reports whether the next tokens are TIMESTAMP '...'.
func (p *parser) atTimestampLiteral() bool {
	tok := p.peek()
	return tok.kind == tokenIdent && !tok.quoted && strings.EqualFold(tok.text, "timestamp") &&
		p.pos+1 < len(p.tokens) && p.tokens[p.pos+1].kind == tokenString
}

func (p *parser) expectIdent() (string, error) {
	if !p.at(tokenIdent) {
		return "", p.errorf("expected identifier")
	}
	return p.next().text, nil
}

func (p *parser) expectNumber() (int64, error) {
	if !p.at(tokenNumber) {
		return 0, p.errorf("expected number")
	}
	value, err := strconv.ParseInt(p.next().text, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("%w: invalid integer", ErrParse)
	}
	return value, nil
}

func (p *parser) expectKeyword(keyword string) error {
	if !p.matchKeyword(keyword) {
		return p.errorf("expected %s", keyword)
	}
	return nil
}

func (p *parser) expect(kind tokenKind) error {
	if !p.match(kind) {
		return p.errorf("expected %v", kind)
	}
	return nil
}

func (p *parser) matchKeyword(keyword string) bool {
	if !p.at(tokenIdent) || p.peek().quoted || !strings.EqualFold(p.peek().text, keyword) {
		return false
	}
	p.pos++
	return true
}

func (p *parser) match(kind tokenKind) bool {
	if !p.at(kind) {
		return false
	}
	p.pos++
	return true
}

func (p *parser) at(kind tokenKind) bool {
	return p.peek().kind == kind
}

func (p *parser) peek() token {
	return p.tokens[p.pos]
}

func (p *parser) next() token {
	tok := p.tokens[p.pos]
	p.pos++
	return tok
}

func (p *parser) isComparison() bool {
	switch p.peek().kind {
	case tokenEqual, tokenNotEqual, tokenLess, tokenLessEqual, tokenGreater, tokenGreaterEqual:
		return true
	default:
		return false
	}
}

func (p *parser) errorf(format string, args ...any) error {
	return fmt.Errorf("%w: "+format, append([]any{ErrParse}, args...)...)
}
