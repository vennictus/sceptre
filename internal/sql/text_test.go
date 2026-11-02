package sql

import (
	"errors"
	"testing"

	"github.com/vennictus/sceptre/internal/table"
)

func mustExec(t *testing.T, db *table.DB, q string, args ...table.Value) Result {
	t.Helper()
	stmt, err := ParseParameters(q, args)
	if err != nil {
		t.Fatalf("%s: %v", q, err)
	}
	r, err := ExecuteStatement(db, stmt)
	if err != nil {
		t.Fatalf("%s: %v", q, err)
	}
	return r
}

func TestTextColumns(t *testing.T) {
	t.Parallel()
	db := mustOpenSQLTable(t)
	defer db.Close()
	mustExec(t, db, "create table posts (slug text, title text, body bytes, primary key (slug))")
	mustExec(t, db, "create index posts_title on posts (title)")
	mustExec(t, db, "insert into posts (slug, title, body) values ('hello', 'Héllo wörld', 'raw')")
	mustExec(t, db, "insert into posts (slug, title, body) values (?, ?, ?)", table.TextValue("next"), table.TextValue("Next"), table.BytesValue([]byte{0, 0xff}))

	def, _, err := db.Table("posts")
	if err != nil || def.Columns[0].Type != table.TypeText || def.Columns[2].Type != table.TypeBytes {
		t.Fatalf("column types = %+v, %v", def.Columns, err)
	}
	r := mustExec(t, db, "select title from posts where slug = 'hello'")
	if len(r.Rows) != 1 || r.Rows[0][0].Type != table.TypeText || string(r.Rows[0][0].Bytes) != "Héllo wörld" {
		t.Fatalf("text round trip = %+v", r.Rows)
	}
	if r := mustExec(t, db, "select slug from posts where title = 'Next'"); len(r.Rows) != 1 {
		t.Fatalf("index lookup on text = %+v", r.Rows)
	}
	if r := mustExec(t, db, "select slug from posts where slug > 'hello'"); len(r.Rows) != 1 || string(r.Rows[0][0].Bytes) != "next" {
		t.Fatalf("text range = %+v", r.Rows)
	}
	// A bytes binding is an untyped string, accepted when it is valid UTF-8.
	mustExec(t, db, "insert into posts (slug, title, body) values (?, 't', 'b')", table.BytesValue([]byte("from-bytes")))

	for _, bad := range []struct {
		q    string
		args []table.Value
	}{
		{"insert into posts (slug, title, body) values (?, 't', 'b')", []table.Value{table.BytesValue([]byte{0xff})}},
		{"insert into posts (slug, title, body) values ('x', ?, 'b')", []table.Value{table.TextValue("\xff")}},
		{"select slug from posts where title = body", nil},
		{"select slug from posts where title = 1", nil},
	} {
		stmt, err := ParseParameters(bad.q, bad.args)
		if err == nil {
			_, err = ExecuteStatement(db, stmt)
		}
		if err == nil {
			t.Fatalf("%s accepted", bad.q)
		}
		if !errors.Is(err, ErrExec) && !errors.Is(err, table.ErrInvalidValue) {
			t.Fatalf("%s: unexpected error %v", bad.q, err)
		}
	}
}

func TestTextTableRejectsInvalidUTF8(t *testing.T) {
	t.Parallel()
	db := mustOpenSQLTable(t)
	defer db.Close()
	mustExec(t, db, "create table notes (id int64, body text, primary key (id))")
	err := db.Insert("notes", table.NewRecord(map[string]table.Value{"id": table.Int64Value(1), "body": table.TextValue("\xc3")}))
	if !errors.Is(err, table.ErrInvalidValue) {
		t.Fatalf("invalid UTF-8 text insert: %v", err)
	}
}
