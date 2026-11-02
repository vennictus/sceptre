package sql

import (
	"errors"
	"testing"

	"github.com/vennictus/sceptre/internal/table"
)

func TestUniqueConstraints(t *testing.T) {
	t.Parallel()
	db := mustOpenSQLTable(t)
	defer db.Close()
	mustExec(t, db, "create table users (id int64, email text unique, handle text not null, primary key (id))")
	mustExec(t, db, "create unique index users_handle on users (handle)")
	def, _, _ := db.Table("users")
	if len(def.Indexes) != 2 || def.Indexes[0].Name != "users_email_key" || !def.Indexes[0].Unique || !def.Indexes[1].Unique {
		t.Fatalf("indexes = %+v", def.Indexes)
	}
	mustExec(t, db, "insert into users (id, email, handle) values (1, 'a@x', 'a')")
	mustExec(t, db, "insert into users (id, handle) values (2, 'b')")
	mustExec(t, db, "insert into users (id, handle) values (3, 'c')") // NULL emails do not conflict
	for _, q := range []string{
		"insert into users (id, email, handle) values (4, 'a@x', 'd')",
		"insert into users (id, handle) values (4, 'a')",
		"update users set email = 'a@x' where id = 2",
		"update users set handle = 'z' where id >= 2", // two rows would share 'z'
	} {
		if _, err := ExecuteStatement(db, mustParse(t, q)); !errors.Is(err, table.ErrUniqueViolation) {
			t.Fatalf("%s: %v", q, err)
		}
	}
	mustExec(t, db, "update users set email = 'b@x' where id = 2")
	if r := mustExec(t, db, "select id from users where email = 'b@x'"); len(r.Rows) != 1 {
		t.Fatalf("lookup through unique index = %+v", r.Rows)
	}
	if _, err := Parse("create unique table t (id int64, primary key (id))"); err == nil {
		t.Fatal("CREATE UNIQUE TABLE accepted")
	}
}
