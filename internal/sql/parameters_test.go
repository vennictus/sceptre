package sql

import (
	"bytes"
	"errors"
	"testing"

	"github.com/vennictus/sceptre/internal/table"
)

func TestParameterTokenBinding(t *testing.T) {
	t.Parallel()
	stmt, err := ParseParameters("insert into t (id, body) values (?, ?)", []table.Value{table.Int64Value(-1), table.BytesValue([]byte("' ?\x00\xff; SELECT"))})
	if err != nil {
		t.Fatal(err)
	}
	insert := stmt.(*InsertStmt)
	if insert.Values[0].Int != -1 || insert.Values[1].String != "' ?\x00\xff; SELECT" {
		t.Fatal(insert)
	}
	stmt, err = ParseParameters("select * from t where body='?' and id=? LIMIT ? OFFSET ?", []table.Value{table.Int64Value(3), table.Int64Value(2), table.Int64Value(1)})
	if err != nil {
		t.Fatal(err)
	}
	selectStmt := stmt.(*SelectStmt)
	if *selectStmt.Limit != 2 || *selectStmt.Offset != 1 {
		t.Fatal(selectStmt)
	}
	for _, values := range [][]table.Value{nil, {table.Int64Value(1), table.Int64Value(2)}, {{Type: 0}}} {
		if _, err := ParseParameters("select * from t where id=?", values); !errors.Is(err, ErrParameter) {
			t.Fatal(err)
		}
	}
}

func FuzzParameterBytes(f *testing.F) {
	for _, v := range [][]byte{[]byte("text"), []byte("' OR id=1;"), {0, 255, '?', '\''}} {
		f.Add(v)
	}
	f.Fuzz(func(t *testing.T, v []byte) {
		if len(v) > 8192 {
			t.Skip()
		}
		stmt, err := ParseParameters("insert into t (body) values (?)", []table.Value{table.BytesValue(v)})
		if err != nil {
			t.Fatal(err)
		}
		got := stmt.(*InsertStmt).Values[0]
		if got.Kind != LiteralString || !bytes.Equal([]byte(got.String), v) {
			t.Fatal("binding changed bytes or grammar")
		}
	})
}
