package sceptre

import (
	"errors"
	"testing"
)

func TestDurabilityNormalCommitsSurviveReopen(t *testing.T) {
	t.Parallel()
	db, path := openTestDB(t, Options{Durability: DurabilityNormal})
	execOK(t, db, createPosts)
	execOK(t, db, "insert into posts (id, body, state) values (1, 'first', 0)")
	if err := db.Sync(background); err != nil {
		t.Fatalf("Sync: %v", err)
	}
	execOK(t, db, "insert into posts (id, body, state) values (2, 'second', 0)")
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	db, err := Open(path, Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	res, err := db.Execute(background, "select id from posts")
	if err != nil || len(res.Rows) != 2 {
		t.Fatalf("rows after reopen = %v, %v", res.Rows, err)
	}
}

func TestDurabilityOptionsAreValidated(t *testing.T) {
	t.Parallel()
	if _, err := Open(t.TempDir()+"/db", Options{Durability: 7}); codeOf(err) != CodeInvalid {
		t.Fatalf("Open with unknown durability: %v", err)
	}
	if d, err := ParseDurability("normal"); err != nil || d != DurabilityNormal {
		t.Fatalf("ParseDurability(normal) = %v, %v", d, err)
	}
	if _, err := ParseDurability("off"); !errors.Is(err, ErrInvalidOptions) {
		t.Fatalf("ParseDurability(off) error = %v", err)
	}
}

func TestSyncFailsAfterClose(t *testing.T) {
	t.Parallel()
	db, _ := openTestDB(t, Options{})
	db.Close()
	if err := db.Sync(background); codeOf(err) != CodeClosed {
		t.Fatalf("Sync after Close: %v", err)
	}
}

func codeOf(err error) Code {
	var e *Error
	if errors.As(err, &e) {
		return e.Code
	}
	return ""
}
