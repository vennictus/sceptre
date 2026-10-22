package sceptre

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/vennictus/sceptre/internal/kv"
	"github.com/vennictus/sceptre/internal/pager"
)

func TestPublicCommitOutcomesPreserveWholeRelationalTransaction(t *testing.T) {
	t.Parallel()
	for _, fault := range []string{"data_short", "data_sync", "meta_short", "meta_sync", "committed_error"} {
		t.Run(fault, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "db")
			db, err := Open(path, Options{})
			if err != nil {
				t.Fatal(err)
			}
			execOK(t, db, createPosts)
			if err := db.Close(); err != nil {
				t.Fatal(err)
			}
			armed, fired, syncs := false, false, 0
			opts := kv.Options{IO: &pager.IOHooks{
				WriteAt: func(f *os.File, data []byte, offset int64) (int, error) {
					meta := offset < 8192
					if armed && !fired && ((fault == "data_short" && !meta) || (fault == "meta_short" && meta)) {
						fired = true
						return f.WriteAt(data[:32], offset)
					}
					return f.WriteAt(data, offset)
				},
				Sync: func(f *os.File) error {
					if armed {
						syncs++
					}
					if armed && !fired && ((fault == "data_sync" && syncs == 1) || (fault == "meta_sync" && syncs == 2)) {
						fired = true
						return errors.New("injected sync error")
					}
					return f.Sync()
				},
			}}
			if fault == "committed_error" {
				opts.FailAfterCommitStage = "meta-published"
			}
			store, err := kv.Open(path, opts)
			if err != nil {
				t.Fatal(err)
			}
			db = &DB{store: store, gate: make(chan struct{}, 1), done: make(chan struct{}), maxRows: 1000}
			db.gate <- struct{}{}
			defer db.Close()
			tx, err := db.Begin(background)
			if err != nil {
				t.Fatal(err)
			}
			txOK(t, tx, "insert into posts (id, body, state) values (1, 'one', 1)")
			txOK(t, tx, "insert into posts (id, body, state) values (2, 'two', 1)")
			txOK(t, tx, "create index state_idx on posts (state)")
			armed = true
			err = tx.Commit(background)
			want := CodeStorage
			if fault == "meta_short" || fault == "meta_sync" {
				want = CodeUncertain
			}
			if fault == "committed_error" {
				want = CodeCommitted
			}
			assertCode(t, err, want)
			assertCode(t, tx.Commit(background), CodeTransactionClosed)
			if fault != "committed_error" && !fired {
				t.Fatal("fault not exercised")
			}
			if want == CodeUncertain {
				if !errors.Is(err, ErrRecoveryRequired) {
					t.Fatal(err)
				}
				_, err := db.Begin(background)
				assertCode(t, err, CodeUncertain)
			} else {
				r := execOK(t, db, "select * from posts")
				n := 0
				if want == CodeCommitted {
					n = 2
				}
				if len(r.Rows) != n {
					t.Fatal(r)
				}
			}
			if err := db.Close(); err != nil {
				t.Fatal(err)
			}
			reopened, err := Open(path, Options{})
			if err != nil {
				t.Fatal(err)
			}
			defer reopened.Close()
			committed := fault == "meta_sync" || fault == "committed_error"
			r := execOK(t, reopened, "select * from posts where state=1")
			n := 0
			if committed {
				n = 2
			}
			if len(r.Rows) != n {
				t.Fatal(r)
			}
			defs, err := reopened.Tables(background)
			if err != nil || len(defs) != 1 {
				t.Fatal(defs, err)
			}
			if (len(defs[0].Indexes) == 1) != committed {
				t.Fatal(defs)
			}
			check, err := reopened.Check(background)
			if err != nil || !check.OK() {
				t.Fatal(check, err)
			}
			execOK(t, reopened, "insert into posts (id, body, state) values (3, 'after recovery', 1)")
		})
	}
}
