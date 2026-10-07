package sql

import (
	"errors"
	"testing"
)

func FuzzParse(f *testing.F) {
	for _, input := range []string{
		"select * from t where id >= -9223372036854775808 limit 0",
		"insert into t (id,body) values (1,'a''b')",
		"update t set body = 'x' where id = 2 or (id = 1 and body != 'y')",
		"create table t (id int64,body bytes,primary key (id))",
		"create index body_idx on t (body)",
		"delete from t", "", "select (", "select * from t limit -1",
	} {
		f.Add(input)
	}
	f.Fuzz(func(t *testing.T, input string) {
		if len(input) > 8192 {
			return
		}
		stmt, err := Parse(input)
		if err != nil {
			if stmt != nil || !errors.Is(err, ErrParse) {
				t.Fatalf("inconsistent parse failure: %T %v", stmt, err)
			}
			return
		}
		switch stmt.(type) {
		case *CreateTableStmt, *CreateIndexStmt, *InsertStmt, *SelectStmt, *UpdateStmt, *DeleteStmt:
		default:
			t.Fatalf("successful parse returned unexpected statement %T", stmt)
		}
	})
}
