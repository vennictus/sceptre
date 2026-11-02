package sql

import (
	"errors"
	"testing"

	"github.com/vennictus/sceptre/internal/table"
)

func TestOnConflict(t *testing.T) {
	t.Parallel()
	db := mustOpenSQLTable(t)
	defer db.Close()
	mustExec(t, db, "create table views (slug text primary key, hits int64 not null default 1, title text, email text unique)")
	mustExec(t, db, "insert into views (slug, title) values ('a', 'A')")

	// DO NOTHING without columns skips any key or unique conflict.
	r := mustExec(t, db, "insert into views (slug, title) values ('a', 'dup'), ('b', 'B') on conflict do nothing")
	if r.RowsAffected != 1 {
		t.Fatalf("DO NOTHING affected %d", r.RowsAffected)
	}
	if r := mustExec(t, db, "select title from views where slug = 'a'"); string(r.Rows[0][0].Bytes) != "A" {
		t.Fatalf("DO NOTHING changed the row: %+v", r.Rows)
	}

	// DO UPDATE with excluded values and literals.
	mustExec(t, db, "insert into views (slug, hits, title) values ('a', 5, 'New A') on conflict (slug) do update set hits = excluded.hits, title = 'updated'")
	r = mustExec(t, db, "select hits, title from views where slug = 'a'")
	if r.Rows[0][0].I64 != 5 || string(r.Rows[0][1].Bytes) != "updated" {
		t.Fatalf("DO UPDATE = %+v", r.Rows)
	}

	// A unique index as the target; rows in one statement see each other.
	mustExec(t, db, "update views set email = 'x@y' where slug = 'b'")
	r = mustExec(t, db, "insert into views (slug, email, title) values ('c', 'x@y', 'by email'), ('d', 'd@y', 'D'), ('e', 'd@y', 'again') on conflict (email) do update set title = excluded.title")
	if r.RowsAffected != 3 {
		t.Fatalf("DO UPDATE on email affected %d", r.RowsAffected)
	}
	wantSlugs := func(q string, want ...string) {
		t.Helper()
		r := mustExec(t, db, q)
		if len(r.Rows) != len(want) {
			t.Fatalf("%s = %+v, want %v", q, r.Rows, want)
		}
		for i, w := range want {
			if string(r.Rows[i][0].Bytes) != w {
				t.Fatalf("%s = %+v, want %v", q, r.Rows, want)
			}
		}
	}
	wantSlugs("select slug from views where title = 'by email'", "b")
	wantSlugs("select slug from views where title = 'again'", "d")

	// A conflict on a constraint other than the target still fails.
	if err := execErr(t, db, "insert into views (slug, email) values ('a', 'new@y') on conflict (email) do nothing"); !errors.Is(err, table.ErrRowExists) {
		t.Fatalf("key conflict with an email target: %v", err)
	}
	for _, q := range []string{
		"insert into views (slug) values ('z') on conflict (title) do nothing",
		"insert into views (slug) values ('a') on conflict (slug) do update set slug = 'b'",
		"insert into views (slug) values ('a') on conflict (slug) do update set hits = excluded.title",
	} {
		if err := execErr(t, db, q); !errors.Is(err, ErrExec) {
			t.Fatalf("%s: %v", q, err)
		}
	}
	if _, err := Parse("insert into views (slug) values ('a') on conflict do update set hits = 1"); err == nil {
		t.Fatal("DO UPDATE without conflict columns parsed")
	}
}

func TestReturning(t *testing.T) {
	t.Parallel()
	db := mustOpenSQLTable(t)
	defer db.Close()
	mustExec(t, db, "create table posts (id int64 primary key autoincrement, title text, state text default 'draft')")
	r := mustExec(t, db, "insert into posts (title) values ('a'), ('b') returning id, state")
	if len(r.Rows) != 2 || r.Rows[1][0].I64 != 2 || string(r.Rows[0][1].Bytes) != "draft" || r.Columns[0] != "id" {
		t.Fatalf("INSERT RETURNING = %+v", r)
	}
	r = mustExec(t, db, "update posts set state = 'live' where id = 2 returning *")
	if len(r.Rows) != 1 || len(r.Columns) != 3 || string(r.Rows[0][2].Bytes) != "live" {
		t.Fatalf("UPDATE RETURNING = %+v", r)
	}
	r = mustExec(t, db, "delete from posts where id = 1 returning title")
	if len(r.Rows) != 1 || string(r.Rows[0][0].Bytes) != "a" || r.RowsAffected != 1 {
		t.Fatalf("DELETE RETURNING = %+v", r)
	}
	r = mustExec(t, db, "insert into posts (id, title) values (2, 'x') on conflict do nothing returning id")
	if len(r.Rows) != 0 {
		t.Fatalf("RETURNING for a skipped row = %+v", r)
	}
	if err := execErr(t, db, "delete from posts returning missing"); !errors.Is(err, ErrExec) {
		t.Fatalf("RETURNING an unknown column: %v", err)
	}
	wantIDs(t, db, "select id from posts", 2) // the failed DELETE removed nothing
}
