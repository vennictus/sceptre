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
	case p.matchKeyword("create"):
		if p.matchKeyword("table") {
			return p.parseCreateTable()
		}
		if p.matchKeyword("index") {
			return p.parseCreateIndex()
		}
	case p.matchKeyword("insert"):
		return p.parseInsert()
	case p.matchKeyword("select"):
		return p.parseSelect()
	case p.matchKeyword("update"):
		return p.parseUpdate()
	case p.matchKeyword("delete"):
		return p.parseDelete()
	}
	return nil, p.errorf("expected statement")
}

func (p *parser) parseCreateTable() (Statement, error) {
	name, err := p.expectIdent()
	if err != nil {
		return nil, err
	}
	if err := p.expect(tokenLParen); err != nil {
		return nil, err
	}

	var columns []ColumnDef
	var primaryKey []string
	for {
		if p.matchKeyword("primary") {
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
			columns = append(columns, ColumnDef{Name: columnName, Type: strings.ToLower(typeName)})
		}

		if p.match(tokenComma) {
			continue
		}
		break
	}
	if err := p.expect(tokenRParen); err != nil {
		return nil, err
	}
	return &CreateTableStmt{Name: name, Columns: columns, PrimaryKey: primaryKey}, nil
}

func (p *parser) parseCreateIndex() (Statement, error) {
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
	return &CreateIndexStmt{Name: name, Table: table, Columns: columns}, nil
}

func (p *parser) parseInsert() (Statement, error) {
	if err := p.expectKeyword("into"); err != nil {
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
	if err := p.expectKeyword("values"); err != nil {
		return nil, err
	}
	values, err := p.parseLiteralListInParens()
	if err != nil {
		return nil, err
	}
	if len(columns) != len(values) {
		return nil, p.errorf("column/value count mismatch")
	}
	return &InsertStmt{Table: table, Columns: columns, Values: values}, nil
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
		stmt.Limit = &limit
	}
	if p.matchKeyword("offset") {
		offset, err := p.expectNumber()
		if err != nil {
			return nil, err
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
	var assignments []Assignment
	for {
		column, err := p.expectIdent()
		if err != nil {
			return nil, err
		}
		if err := p.expect(tokenEqual); err != nil {
			return nil, err
		}
		value, err := p.expectLiteral()
		if err != nil {
			return nil, err
		}
		assignments = append(assignments, Assignment{Column: column, Value: value})
		if !p.match(tokenComma) {
			break
		}
	}
	stmt := &UpdateStmt{Table: table, Assignments: assignments}
	if p.matchKeyword("where") {
		where, err := p.parseExpr()
		if err != nil {
			return nil, err
		}
		stmt.Where = where
	}
	return stmt, nil
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
	if p.at(tokenIdent) {
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
	default:
		return Literal{}, p.errorf("expected literal")
	}
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
	if !p.at(tokenIdent) || !strings.EqualFold(p.peek().text, keyword) {
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
