package table

import (
	"errors"
	"math/rand"
	"testing"

	"github.com/vennictus/sceptre/internal/kv"
	"github.com/vennictus/sceptre/internal/tx"
)

var usersDef = TableDef{
	Name:       "users",
	Columns:    []Column{{Name: "id", Type: TypeInt64}, {Name: "email", Type: TypeText, Nullable: true}},
	PrimaryKey: []string{"id"},
}

func user(id int64, email Value) Record {
	return NewRecord(map[string]Value{"id": Int64Value(id), "email": email})
}

func openUsers(t *testing.T) *DB {
	t.Helper()
	db := openNullDB(t)
	if err := db.CreateTable(usersDef); err != nil {
		t.Fatal(err)
	}
	if err := db.CreateIndex("users", IndexDef{Name: "users_email", Columns: []string{"email"}, Unique: true}); err != nil {
		t.Fatal(err)
	}
	return db
}

// Random inserts, updates and deletes against a model: every write must
// succeed exactly when it keeps emails unique.
func TestUniqueIndexMatchesModel(t *testing.T) {
	t.Parallel()
	db := openUsers(t)
	rng := rand.New(rand.NewSource(5))
	model := map[int64]string{} // id -> email ("" = NULL)
	emailOf := func(i int) (Value, string) {
		if i == 0 {
			return NullValue(), ""
		}
		s := string(rune('a' + i))
		return TextValue(s), s
	}
	taken := func(email string, except int64) bool {
		if email == "" {
			return false
		}
		for id, e := range model {
			if id != except && e == email {
				return true
			}
		}
		return false
	}
	for step := 0; step < 600; step++ {
		id := int64(rng.Intn(15))
		v, email := emailOf(rng.Intn(8))
		_, exists := model[id]
		switch rng.Intn(3) {
		case 0:
			err := db.Insert("users", user(id, v))
			wantOK := !exists && !taken(email, -1)
			if (err == nil) != wantOK {
				t.Fatalf("step %d insert %d %q: err %v, want ok %v", step, id, email, err, wantOK)
			}
			if err == nil {
				model[id] = email
			} else if !exists && !errors.Is(err, ErrUniqueViolation) {
				t.Fatalf("step %d: %v", step, err)
			}
		case 1:
			err := db.Update("users", user(id, v))
			wantOK := exists && !taken(email, id)
			if (err == nil) != wantOK {
				t.Fatalf("step %d update %d %q: err %v, want ok %v", step, id, email, err, wantOK)
			}
			if err == nil {
				model[id] = email
			}
		default:
			if _, err := db.Delete("users", NewRecord(map[string]Value{"id": Int64Value(id)})); err != nil {
				t.Fatal(err)
			}
			delete(model, id)
		}
	}
	mustCheckClean(t, db)
}

func TestUniqueBatches(t *testing.T) {
	t.Parallel()
	db := openUsers(t)
	if err := db.InsertMany("users", []Record{user(1, TextValue("a")), user(2, TextValue("a"))}); !errors.Is(err, ErrUniqueViolation) {
		t.Fatalf("duplicate inside one batch: %v", err)
	}
	if err := db.InsertMany("users", []Record{user(1, TextValue("a")), user(2, TextValue("b")), user(3, NullValue()), user(4, NullValue())}); err != nil {
		t.Fatalf("distinct values and repeated NULLs: %v", err)
	}
	// Swapping two values in one batch is allowed.
	if err := db.UpdateMany("users", []Record{user(1, TextValue("b")), user(2, TextValue("a"))}); err != nil {
		t.Fatalf("swap: %v", err)
	}
	if err := db.UpdateMany("users", []Record{user(3, TextValue("a"))}); !errors.Is(err, ErrUniqueViolation) {
		t.Fatalf("update onto a stored value: %v", err)
	}
	mustCheckClean(t, db)
}

func TestUniqueIndexBuildRejectsDuplicates(t *testing.T) {
	t.Parallel()
	db := openNullDB(t)
	if err := db.CreateTable(usersDef); err != nil {
		t.Fatal(err)
	}
	if err := db.InsertMany("users", []Record{user(1, TextValue("a")), user(2, TextValue("a")), user(3, NullValue())}); err != nil {
		t.Fatal(err)
	}
	if err := db.CreateIndex("users", IndexDef{Name: "u", Columns: []string{"email"}, Unique: true}); !errors.Is(err, ErrUniqueViolation) {
		t.Fatalf("unique index over duplicates: %v", err)
	}
	if def, _, _ := db.Table("users"); len(def.Indexes) != 0 {
		t.Fatalf("failed index build left a definition: %+v", def.Indexes)
	}
}

func TestUniqueSeesTransactionPendingRows(t *testing.T) {
	t.Parallel()
	store, err := kv.Open(t.TempDir()+"/db", kv.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	base := New(store)
	if err := base.CreateTable(usersDef); err != nil {
		t.Fatal(err)
	}
	if err := base.CreateIndex("users", IndexDef{Name: "u", Columns: []string{"email"}, Unique: true}); err != nil {
		t.Fatal(err)
	}
	transaction := tx.Begin(store)
	defer transaction.Abort()
	db := NewTransactional(transaction)
	if err := db.Insert("users", user(1, TextValue("a"))); err != nil {
		t.Fatal(err)
	}
	if err := db.Insert("users", user(2, TextValue("a"))); !errors.Is(err, ErrUniqueViolation) {
		t.Fatalf("duplicate of a pending row: %v", err)
	}
}
