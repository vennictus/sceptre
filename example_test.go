package sceptre_test

import (
	"context"
	"fmt"
	"os"
	"path/filepath"

	"github.com/vennictus/sceptre"
)

func ExampleDB_Begin() {
	dir, err := os.MkdirTemp("", "sceptre-example-")
	if err != nil {
		panic(err)
	}
	defer os.RemoveAll(dir)
	db, err := sceptre.Open(filepath.Join(dir, "blog.db"), sceptre.Options{})
	if err != nil {
		panic(err)
	}
	defer db.Close()
	ctx := context.Background()
	tx, err := db.Begin(ctx)
	if err != nil {
		panic(err)
	}
	defer tx.Rollback()
	if _, err := tx.Execute(ctx, "create table posts (id int64, body bytes, primary key (id))"); err != nil {
		panic(err)
	}
	if _, err := tx.Execute(ctx, "insert into posts (id, body) values (?, ?)", sceptre.Int64(1), sceptre.Bytes([]byte("Hello"))); err != nil {
		panic(err)
	}
	if err := tx.Commit(ctx); err != nil {
		panic(err)
	}
	result, err := db.Execute(ctx, "select body from posts where id = ?", sceptre.Int64(1))
	if err != nil {
		panic(err)
	}
	fmt.Println(string(result.Rows[0][0].Bytes))
	// Output: Hello
}
