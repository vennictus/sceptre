package sql

import (
	"errors"
	"testing"
)

func TestQuotedIdentifiersAndTypedLiterals(t *testing.T) {
	t.Parallel()
	db := mustOpenSQLTable(t)
	defer db.Close()
	mustExec(t, db, `create table "my posts" ("select" int64 primary key, "true" text, at timestamp, raw bytes)`)
	mustExec(t, db, `insert into "my posts" values (1, 'a', timestamp '2026-11-05 10:00:00', X'ff00')`)
	r := mustExec(t, db, `select "select", raw from "my posts" where "true" = 'a' and at = TIMESTAMP '2026-11-05T10:00:00Z'`)
	if len(r.Rows) != 1 || string(r.Rows[0][1].Bytes) != "\xff\x00" {
		t.Fatalf("rows = %+v", r.Rows)
	}
	for _, q := range []string{`select * from ""`, `select * from "open`, `select X'f' from t`, `select X'zz' from t`, `insert into "my posts" values (2, 'b', timestamp 'nope', X'')`} {
		if _, err := Execute(db, q); !errors.Is(err, ErrParse) {
			t.Errorf("%s: %v, want a parse error", q, err)
		}
	}
}
