package sql

// Statement is implemented by all parsed SQL statements.
type Statement interface {
	statementNode()
}

type ColumnDef struct {
	Name string
	Type string
}

type CreateTableStmt struct {
	Name       string
	Columns    []ColumnDef
	PrimaryKey []string
}

func (*CreateTableStmt) statementNode() {}

type CreateIndexStmt struct {
	Name    string
	Table   string
	Columns []string
}

func (*CreateIndexStmt) statementNode() {}

type InsertStmt struct {
	Table   string
	Columns []string
	Values  []Literal
}

func (*InsertStmt) statementNode() {}

type SelectStmt struct {
	AllColumns bool
	Columns    []string
	Table      string
	Where      Expr
	Limit      *int64
	Offset     *int64
}

func (*SelectStmt) statementNode() {}

type Assignment struct {
	Column string
	Value  Literal
}

type UpdateStmt struct {
	Table       string
	Assignments []Assignment
	Where       Expr
}

func (*UpdateStmt) statementNode() {}

type DeleteStmt struct {
	Table string
	Where Expr
}

func (*DeleteStmt) statementNode() {}

// Expr is implemented by parsed expression nodes.
type Expr interface {
	exprNode()
}

type IdentExpr struct {
	Name string
}

func (*IdentExpr) exprNode() {}

type LiteralKind int

const (
	LiteralInt LiteralKind = iota + 1
	LiteralString
)

type Literal struct {
	Kind   LiteralKind
	Int    int64
	String string
}

func (*Literal) exprNode() {}

type BinaryExpr struct {
	Left  Expr
	Op    string
	Right Expr
}

func (*BinaryExpr) exprNode() {}
