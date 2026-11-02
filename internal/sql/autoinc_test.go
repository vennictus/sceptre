package sql

import "testing"

func TestAutoIncrementPrimaryKey(t *testing.T) {
	t.Parallel()
	db := mustOpenSQLTable(t)
	defer db.Close()
	mustExec(t, db, "create table posts (id int64 primary key autoincrement, title text not null)")
	mustExec(t, db, "insert into posts (title) values ('a')")
	mustExec(t, db, "insert into posts (title) values ('b')")
	mustExec(t, db, "insert into posts (id, title) values (10, 'explicit')") // advances the sequence
	mustExec(t, db, "insert into posts (title) values ('c')")
	wantIDs(t, db, "select id from posts", 1, 2, 10, 11)
	// Deleting the newest row does not reuse its number.
	mustExec(t, db, "delete from posts where id = 11")
	mustExec(t, db, "insert into posts (title) values ('d')")
	wantIDs(t, db, "select id from posts where title = 'd'", 12)
	// Renaming the table keeps the sequence.
	mustExec(t, db, "alter table posts rename to articles")
	mustExec(t, db, "insert into articles (title) values ('e')")
	wantIDs(t, db, "select id from articles where title = 'e'", 13)
	// Dropping and recreating starts over.
	mustExec(t, db, "drop table articles")
	mustExec(t, db, "create table articles (id int64 primary key autoincrement, title text)")
	mustExec(t, db, "insert into articles (title) values ('f')")
	wantIDs(t, db, "select id from articles", 1)
	if rep, err := db.Check(); err != nil || !rep.OK() {
		t.Fatalf("check: %+v %v", rep, err)
	}
}

func TestAutoIncrementRules(t *testing.T) {
	t.Parallel()
	db := mustOpenSQLTable(t)
	defer db.Close()
	for _, q := range []string{
		"create table bad (id text primary key autoincrement)",
		"create table bad (id int64, n int64 autoincrement, primary key (id))",
		"create table bad (a int64 autoincrement, b int64, primary key (a, b))",
		"create table bad (a int64 primary key, b int64 primary key)",
	} {
		if err := execErr(t, db, q); err == nil {
			t.Fatalf("%s accepted", q)
		}
	}
	mustExec(t, db, "create table plain (id int64 primary key, n int64)")
	if err := execErr(t, db, "insert into plain (n) values (1)"); err == nil {
		t.Fatal("insert without a key accepted on a table without AUTOINCREMENT")
	}
	def, _, _ := db.Table("plain")
	if len(def.PrimaryKey) != 1 || def.PrimaryKey[0] != "id" || def.Columns[0].Nullable {
		t.Fatalf("inline PRIMARY KEY = %+v", def)
	}
	if _, err := db.NextIDs("plain", 1); err == nil {
		t.Fatal("NextIDs on a table without AUTOINCREMENT")
	}
}
