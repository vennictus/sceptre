package sql

import (
	"errors"
	"testing"
	"time"

	"github.com/vennictus/sceptre/internal/table"
)

func TestColumnDefaults(t *testing.T) {
	t.Parallel()
	db := mustOpenSQLTable(t)
	defer db.Close()
	mustExec(t, db, "create table posts (id int64, state text not null default 'draft', views int64 default 0, pinned bool default false, created timestamp default current_timestamp, note text default null, primary key (id))")
	before := time.Now().Add(-time.Second)
	mustExec(t, db, "insert into posts (id) values (1)")
	mustExec(t, db, "insert into posts (id, state, views) values (2, 'live', 7)")
	r := mustExec(t, db, "select state, views, pinned, created, note from posts where id = 1")
	row := r.Rows[0]
	if string(row[0].Bytes) != "draft" || row[1].I64 != 0 || row[2].Bool || !row[4].IsNull() {
		t.Fatalf("defaults = %+v", row)
	}
	if created := row[3].Time(); created.Before(before) || created.After(time.Now().Add(time.Second)) {
		t.Fatalf("CURRENT_TIMESTAMP default = %v", created)
	}
	wantIDs(t, db, "select id from posts where state = 'live' and views = 7", 2)

	for _, q := range []string{
		"create table bad (id int64, n int64 default 'x', primary key (id))",
		"create table bad (id int64, n text not null default null, primary key (id))",
		"create table bad (id int64, n text default current_timestamp, primary key (id))",
	} {
		if err := execErr(t, db, q); err == nil {
			t.Fatalf("%s accepted", q)
		}
	}
}

func TestAddColumnWithDefault(t *testing.T) {
	t.Parallel()
	db := mustOpenSQLTable(t)
	defer db.Close()
	mustExec(t, db, "create table posts (id int64, primary key (id))")
	mustExec(t, db, "insert into posts (id) values (1)")
	mustExec(t, db, "alter table posts add column state text not null default 'draft'")
	mustExec(t, db, "alter table posts add column updated timestamp not null default current_timestamp")
	r := mustExec(t, db, "select state, updated from posts where id = 1")
	if string(r.Rows[0][0].Bytes) != "draft" || r.Rows[0][1].IsNull() {
		t.Fatalf("existing row after ADD COLUMN DEFAULT = %+v", r.Rows)
	}
	wantIDs(t, db, "select id from posts where state = 'draft'", 1)
	mustExec(t, db, "insert into posts (id, state) values (2, 'live')")
	wantIDs(t, db, "select id from posts where state = 'draft'", 1)
	if err := execErr(t, db, "insert into posts (id, state) values (3, NULL)"); !errors.Is(err, table.ErrInvalidValue) {
		t.Fatalf("NULL into NOT NULL DEFAULT column: %v", err)
	}
	if err := execErr(t, db, "alter table posts add column body text not null"); !errors.Is(err, ErrExec) {
		t.Fatalf("ADD COLUMN NOT NULL without DEFAULT: %v", err)
	}
	if rep, err := db.Check(); err != nil || !rep.OK() {
		t.Fatalf("check: %+v %v", rep, err)
	}
}
