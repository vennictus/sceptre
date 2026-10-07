package sceptre

import (
	"math/rand"
	"strconv"
	"testing"
)

func TestTransactionalIndexedWorkloadAgainstModel(t *testing.T) {
	t.Parallel()
	for _, seed := range []int64{7, 42, 2026} {
		t.Run(strconv.FormatInt(seed, 10), func(t *testing.T) {
			db, path := openTestDB(t, Options{PageSize: 512})
			execOK(t, db, createPosts)
			execOK(t, db, "create index state_idx on posts (state)")
			model := map[int64]int64{}
			rng := rand.New(rand.NewSource(seed))
			for group := 0; group < 24; group++ {
				pending := map[int64]int64{}
				for k, v := range model {
					pending[k] = v
				}
				tx, err := db.Begin(background)
				if err != nil {
					t.Fatal(err)
				}
				for step := 0; step < 5; step++ {
					id := int64(rng.Intn(20))
					state := int64(rng.Intn(3))
					if rng.Intn(4) == 0 {
						txOK(t, tx, "delete from posts where id=?", Int64(id))
						delete(pending, id)
					} else if _, exists := pending[id]; exists {
						txOK(t, tx, "update posts set state=? where id=?", Int64(state), Int64(id))
						pending[id] = state
					} else {
						txOK(t, tx, "insert into posts (id, body, state) values (?, 'body', ?)", Int64(id), Int64(state))
						pending[id] = state
					}
				}
				verify := func(q string, args []Value, want map[int64]int64) {
					r := txOK(t, tx, q, args...)
					got := map[int64]int64{}
					for _, row := range r.Rows {
						if _, duplicate := got[row[0].I64]; duplicate {
							t.Fatal("duplicate row")
						}
						got[row[0].I64] = row[1].I64
					}
					if len(got) != len(want) {
						t.Fatalf("seed=%d group=%d got=%v want=%v", seed, group, got, want)
					}
					for k, v := range want {
						if actual, ok := got[k]; !ok || actual != v {
							t.Fatal(got, want)
						}
					}
				}
				verify("select id,state from posts", nil, pending)
				for state := int64(0); state < 3; state++ {
					matching := map[int64]int64{}
					for k, v := range pending {
						if v == state {
							matching[k] = v
						}
					}
					verify("select id,state from posts where state=?", []Value{Int64(state)}, matching)
				}
				if group%3 == 0 {
					tx.Rollback()
				} else {
					if err := tx.Commit(background); err != nil {
						t.Fatal(err)
					}
					model = pending
				}
				if group%6 == 5 {
					if err := db.Close(); err != nil {
						t.Fatal(err)
					}
					db, err = Open(path, Options{})
					if err != nil {
						t.Fatal(err)
					}
					defer db.Close()
				}
				r := execOK(t, db, "select id,state from posts")
				if len(r.Rows) != len(model) {
					t.Fatal(r, model)
				}
				for _, row := range r.Rows {
					if v, ok := model[row[0].I64]; !ok || v != row[1].I64 {
						t.Fatal(r, model)
					}
				}
				check, err := db.Check(background)
				if err != nil || !check.OK() {
					t.Fatal(check, err)
				}
			}
		})
	}
}
