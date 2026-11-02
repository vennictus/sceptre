package sceptre

import "testing"

func TestNullThroughThePublicAPI(t *testing.T) {
	t.Parallel()
	db, _ := openTestDB(t, Options{})
	execOK(t, db, "create table notes (id int64, body text, primary key (id))")
	execOK(t, db, "insert into notes (id, body) values (?, ?)", Int64(1), Null())
	execOK(t, db, "insert into notes (id, body) values (?, ?)", Int64(2), Text("hi"))
	res := execOK(t, db, "select id, body from notes where body is null")
	if len(res.Rows) != 1 || res.Rows[0][0].I64 != 1 || !res.Rows[0][1].IsNull() || res.Rows[0][1].Type != TypeNull {
		t.Fatalf("NULL row = %+v", res.Rows)
	}
	if _, err := db.Execute(background, "insert into notes (id, body) values (?, 'x')", Null()); codeOf(err) != CodeInvalid {
		t.Fatalf("NULL primary key: %v", err)
	}
	defs, err := db.Tables(background)
	if err != nil || defs[0].Columns[0].Nullable || !defs[0].Columns[1].Nullable {
		t.Fatalf("Tables nullability = %+v %v", defs, err)
	}
}
