package sql

import (
	"encoding/json"
	"errors"
	"math"
	"math/rand"
	"reflect"
	"testing"
	"time"
	"unicode/utf8"
)

// storeNames include keywords, quotes, spaces and non-ASCII: all must stay
// column references through the stored form.
var storeNames = []string{"a", "true", "NULL", "select", "and", "timestamp", "cast", "x", "is", "has space", `quo"te`, "日本"}

func randomLiteral(r *rand.Rand) Literal {
	switch r.Intn(6) {
	case 0:
		ints := []int64{0, 1, -1, math.MaxInt64, math.MinInt64, r.Int63() - r.Int63()}
		return Literal{Kind: LiteralInt, Int: ints[r.Intn(len(ints))]}
	case 1:
		b := make([]byte, r.Intn(6))
		for i := range b {
			const alphabet = "a'\"\x00\xff\xc3x\\"
			b[i] = alphabet[r.Intn(len(alphabet))]
		}
		return Literal{Kind: LiteralString, String: string(b)}
	case 2:
		floats := []float64{0, 2, -2, 0.1, 1e20, 1e21, -1e-300, math.MaxFloat64, math.SmallestNonzeroFloat64, r.NormFloat64() * 1e6}
		return Literal{Kind: LiteralFloat, Float: floats[r.Intn(len(floats))]}
	case 3:
		return Literal{Kind: LiteralBool, Bool: r.Intn(2) == 0}
	case 4:
		return Literal{Kind: LiteralNull}
	default:
		micros := []int64{0, -1, 1, math.MaxInt64, math.MinInt64, time.Date(2026, 11, 5, 1, 2, 3, 456000, time.UTC).UnixMicro(), time.Date(9999, 12, 31, 23, 59, 59, 999999000, time.UTC).UnixMicro(), r.Int63n(1 << 50)}
		return Literal{Kind: LiteralTimestamp, Int: micros[r.Intn(len(micros))]}
	}
}

// randomExpr returns an expression and the one ParseExpr(StoreExpr(e))
// must return: the same, except timestamps outside four-digit years, which
// are stored as an exact CAST of their microseconds.
func randomExpr(r *rand.Rand, depth int) (Expr, Expr) {
	if depth == 0 || r.Intn(3) == 0 {
		if r.Intn(2) == 0 {
			name := storeNames[r.Intn(len(storeNames))]
			return &IdentExpr{Name: name}, &IdentExpr{Name: name}
		}
		l := randomLiteral(r)
		if l.Kind == LiteralTimestamp {
			if y := time.UnixMicro(l.Int).UTC().Year(); y < 0 || y > 9999 {
				return &l, &CastExpr{Expr: &Literal{Kind: LiteralInt, Int: l.Int}, Type: "timestamp"}
			}
		}
		c := l
		return &l, &c
	}
	switch r.Intn(4) {
	case 0:
		inner, want := randomExpr(r, depth-1)
		not := r.Intn(2) == 0
		return &IsNullExpr{Expr: inner, Not: not}, &IsNullExpr{Expr: want, Not: not}
	case 1:
		inner, want := randomExpr(r, depth-1)
		typ := []string{"int64", "float64", "bool", "text", "bytes", "timestamp"}[r.Intn(6)]
		return &CastExpr{Expr: inner, Type: typ}, &CastExpr{Expr: want, Type: typ}
	default:
		op := []string{"=", "!=", "<", "<=", ">", ">=", "and", "or"}[r.Intn(8)]
		l, lw := randomExpr(r, depth-1)
		rt, rw := randomExpr(r, depth-1)
		return &BinaryExpr{Left: l, Op: op, Right: rt}, &BinaryExpr{Left: lw, Op: op, Right: rw}
	}
}

// The stored form of an expression parses back to the same expression,
// is valid UTF-8 (so the JSON catalog keeps it byte for byte), and survives
// JSON exactly.
func TestStoreExprRoundTrips(t *testing.T) {
	t.Parallel()
	r := rand.New(rand.NewSource(1))
	for i := 0; i < 20000; i++ {
		expr, want := randomExpr(r, 4)
		text := StoreExpr(expr)
		if !utf8.ValidString(text) {
			t.Fatalf("stored form is not UTF-8: %q", text)
		}
		var back string
		raw, _ := json.Marshal(text)
		if err := json.Unmarshal(raw, &back); err != nil || back != text {
			t.Fatalf("JSON changed %q to %q (%v)", text, back, err)
		}
		got, err := ParseExpr(text)
		if err != nil {
			t.Fatalf("ParseExpr(%q): %v", text, err)
		}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("round trip of %q:\n got %#v\nwant %#v", text, got, want)
		}
	}
}

// Checks stored before the lossless form (display text) still run.
func TestDisplayFormChecksStillRun(t *testing.T) {
	t.Parallel()
	db := mustOpenSQLTable(t)
	defer db.Close()
	mustExec(t, db, "create table t (id int64 primary key, views int64, state text)")
	if err := db.SetChecks("t", []string{"views >= 0", "(state = 'draft' OR state = 'live')"}); err != nil {
		t.Fatal(err)
	}
	mustExec(t, db, "insert into t values (1, 0, 'draft')")
	for _, q := range []string{"insert into t values (2, -1, 'draft')", "insert into t values (2, 1, 'gone')"} {
		if err := execErr(t, db, q); !errors.Is(err, ErrCheck) {
			t.Errorf("%s: %v", q, err)
		}
	}
}
