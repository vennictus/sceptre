package sql

// Statement is implemented by all parsed SQL statements.
type Statement interface {
	statementNode()
}

type ColumnDef struct {
	Name    string
	Type    string
	NotNull bool
	Unique  bool
}

type CreateTableStmt struct {
	Name        string
	Columns     []ColumnDef
	PrimaryKey  []string
	IfNotExists bool
}

func (*CreateTableStmt) statementNode() {}

type CreateIndexStmt struct {
	Name        string
	Table       string
	Columns     []string
	Unique      bool
	IfNotExists bool
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
	LiteralNull
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

// IsNullExpr is expr IS NULL, or IS NOT NULL when Not is set.
type IsNullExpr struct {
	Expr Expr
	Not  bool
}

func (*IsNullExpr) exprNode() {}

// DropTableStmt is DROP TABLE [IF EXISTS] name.
type DropTableStmt struct {
	Name     string
	IfExists bool
}

func (*DropTableStmt) statementNode() {}

// DropIndexStmt is DROP INDEX [IF EXISTS] name [ON table]. Without ON, the
// name must identify one index across all tables.
type DropIndexStmt struct {
	Name     string
	Table    string
	IfExists bool
}

func (*DropIndexStmt) statementNode() {}

// AlterTableStmt is ALTER TABLE name followed by exactly one action: ADD
// [COLUMN] def, DROP [COLUMN] name, RENAME [COLUMN] old TO new, or RENAME TO
// new.
type AlterTableStmt struct {
	Table      string
	Add        *ColumnDef
	Drop       string
	RenameFrom string // with RenameTo: rename a column
	RenameTo   string // alone: rename the table
}

func (*AlterTableStmt) statementNode() {}

// BeginStmt is BEGIN [TRANSACTION | WORK].
type BeginStmt struct{}

func (*BeginStmt) statementNode() {}

// CommitStmt is COMMIT [TRANSACTION | WORK].
type CommitStmt struct{}

func (*CommitStmt) statementNode() {}

// RollbackStmt is ROLLBACK [TRANSACTION | WORK] [TO [SAVEPOINT] name]. An
// empty Savepoint rolls back the whole transaction.
type RollbackStmt struct {
	Savepoint string
}

func (*RollbackStmt) statementNode() {}

// SavepointStmt is SAVEPOINT name.
type SavepointStmt struct {
	Name string
}

func (*SavepointStmt) statementNode() {}

// ReleaseStmt is RELEASE [SAVEPOINT] name.
type ReleaseStmt struct {
	Name string
}

func (*ReleaseStmt) statementNode() {}

// IsTransactionControl reports whether stmt begins, ends, or marks a
// transaction rather than reading or changing data.
func IsTransactionControl(stmt Statement) bool {
	switch stmt.(type) {
	case *BeginStmt, *CommitStmt, *RollbackStmt, *SavepointStmt, *ReleaseStmt:
		return true
	}
	return false
}
