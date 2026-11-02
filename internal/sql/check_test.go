package sql

import (
	"errors"
	"testing"
)

func TestCheckConstraints(t *testing.T) {
	t.Parallel()
	db := mustOpenSQLTable(t)
	defer db.Close()
	mustExec(t, db, "create table posts (id int64, views int64 check (views >= 0), state text not null default 'draft' check (state = 'draft' or state = 'live'), rating float64, check (rating is null or rating <= 5.0), primary key (id))")
	def, _, _ := db.Table("posts")
	if len(def.Checks) != 3 {
		t.Fatalf("stored checks = %q", def.Checks)
	}
	mustExec(t, db, "insert into posts (id, views) values (1, 0)")
	mustExec(t, db, "insert into posts (id, views, state, rating) values (2, 5, 'live', 4.5)")
	mustExec(t, db, "insert into posts (id) values (3)") // NULL views: the check is unknown and passes
	for _, q := range []string{
		"insert into posts (id, views) values (4, -1)",
		"insert into posts (id, state) values (4, 'gone')",
		"insert into posts (id, rating) values (4, 9)",
		"update posts set views = -5 where id = 1",
		"update posts set state = 'x' where id >= 1",
	} {
		if err := execErr(t, db, q); !errors.Is(err, ErrCheck) {
			t.Fatalf("%s: %v", q, err)
		}
	}
	wantIDs(t, db, "select id from posts where views = 0", 1) // failed UPDATE changed nothing

	mustExec(t, db, "alter table posts rename column views to hits")
	if err := execErr(t, db, "update posts set hits = -1 where id = 2"); !errors.Is(err, ErrCheck) {
		t.Fatalf("check after RENAME COLUMN: %v", err)
	}
	if err := execErr(t, db, "alter table posts drop column hits"); !errors.Is(err, ErrExec) {
		t.Fatalf("DROP COLUMN used by a check: %v", err)
	}
	for _, q := range []string{
		"create table bad (id int64, n int64 check (n), primary key (id))",
		"create table bad (id int64, n int64 check (missing > 0), primary key (id))",
		"create table bad (id int64, n int64 check (n = 'x'), primary key (id))",
	} {
		if err := execErr(t, db, q); err == nil {
			t.Fatalf("%s accepted", q)
		}
	}
}
