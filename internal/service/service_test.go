package service

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/vennictus/sceptre"
)

const (
	testHost   = "127.0.0.1:7463"
	testToken  = "0123456789abcdef0123456789abcdef"
	testClient = "studio-client-0001"
)

type fixture struct {
	t  *testing.T
	db *sceptre.DB
	h  *Handler
}

func newFixture(t *testing.T, opts Options) *fixture {
	t.Helper()
	db, err := sceptre.Open(filepath.Join(t.TempDir(), "db"), sceptre.Options{})
	if err != nil {
		t.Fatal(err)
	}
	opts.Host, opts.Token = testHost, testToken
	h, err := New(db, opts)
	if err != nil {
		db.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() { h.Close(); db.Close() })
	return &fixture{t: t, db: db, h: h}
}

type reply struct {
	Status int
	Body   struct {
		Result *struct {
			Columns      []string   `json:"columns"`
			Rows         [][]Scalar `json:"rows"`
			RowsAffected string     `json:"rows_affected"`
		} `json:"result"`
		Schema      []schema     `json:"schema"`
		Plan        *plan        `json:"plan"`
		Transaction *transaction `json:"transaction"`
		Error       *failure     `json:"error"`
	}
}

// call sends an authorized API request; mutate adjusts it before sending.
func (f *fixture) call(path, body string, mutate func(*http.Request)) reply {
	f.t.Helper()
	req := httptest.NewRequest(http.MethodPost, "http://"+testHost+path, strings.NewReader(body))
	req.Header.Set("Authorization", "Bearer "+testToken)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Sceptre-Client", testClient)
	if mutate != nil {
		mutate(req)
	}
	rec := httptest.NewRecorder()
	f.h.ServeHTTP(rec, req)
	var out reply
	out.Status = rec.Code
	if rec.Header().Get("Content-Type") == "application/json" {
		if err := json.Unmarshal(rec.Body.Bytes(), &out.Body); err != nil {
			f.t.Fatalf("%s: invalid JSON %q: %v", path, rec.Body.String(), err)
		}
	}
	return out
}

func (f *fixture) sql(path, sql string, mutate func(*http.Request)) reply {
	f.t.Helper()
	body, _ := json.Marshal(query{SQL: sql})
	return f.call(path, string(body), mutate)
}

func (f *fixture) mustSQL(sql string) reply {
	f.t.Helper()
	r := f.sql("/api/execute", sql, nil)
	if r.Status != 200 || r.Body.Error != nil {
		f.t.Fatalf("%s: status %d error %+v", sql, r.Status, r.Body.Error)
	}
	return r
}

func withTx(id string) func(*http.Request) {
	return func(r *http.Request) { r.Header.Set("X-Sceptre-Transaction", id) }
}

func (f *fixture) begin() string {
	f.t.Helper()
	r := f.call("/api/tx/begin", "{}", nil)
	if r.Status != 200 || r.Body.Transaction == nil {
		f.t.Fatalf("begin: status %d error %+v", r.Status, r.Body.Error)
	}
	return r.Body.Transaction.ID
}

func (f *fixture) count(table string) int {
	f.t.Helper()
	return len(f.mustSQL("SELECT * FROM " + table).Body.Result.Rows)
}

func TestNewValidatesOptions(t *testing.T) {
	db, err := sceptre.Open(filepath.Join(t.TempDir(), "db"), sceptre.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	for name, opts := range map[string]Options{
		"short token":    {Host: testHost, Token: "short"},
		"missing host":   {Token: testToken},
		"negative ttl":   {Host: testHost, Token: testToken, TransactionTTL: -time.Second},
		"tiny responses": {Host: testHost, Token: testToken, MaxResponseBytes: 10},
	} {
		if h, err := New(db, opts); err == nil {
			h.Close()
			t.Errorf("%s: accepted", name)
		}
	}
	if _, err := New(nil, Options{Host: testHost, Token: testToken}); err == nil {
		t.Error("nil database accepted")
	}
}

func TestRequestBoundaryRejections(t *testing.T) {
	f := newFixture(t, Options{})
	cases := []struct {
		name   string
		mutate func(*http.Request)
		body   string
		path   string
		status int
		code   string
	}{
		{"wrong host", func(r *http.Request) { r.Host = "evil.example:7463" }, "{}", "/api/schema", 403, "forbidden"},
		{"cross origin", func(r *http.Request) { r.Header.Set("Origin", "http://evil.example") }, "{}", "/api/schema", 403, "forbidden"},
		{"cross site fetch", func(r *http.Request) { r.Header.Set("Sec-Fetch-Site", "cross-site") }, "{}", "/api/schema", 403, "forbidden"},
		{"missing token", func(r *http.Request) { r.Header.Del("Authorization") }, "{}", "/api/schema", 401, "unauthorized"},
		{"wrong token", func(r *http.Request) { r.Header.Set("Authorization", "Bearer "+strings.Repeat("x", 32)) }, "{}", "/api/schema", 401, "unauthorized"},
		{"wrong method", func(r *http.Request) { r.Method = http.MethodGet }, "{}", "/api/schema", 405, "method"},
		{"wrong content type", func(r *http.Request) { r.Header.Set("Content-Type", "text/plain") }, "{}", "/api/schema", 415, "invalid"},
		{"unknown field", nil, `{"sql":"x","extra":1}`, "/api/execute", 400, "invalid"},
		{"trailing data", nil, `{} {}`, "/api/schema", 400, "invalid"},
		{"null body", nil, `null`, "/api/schema", 400, "invalid"},
		{"oversized body", nil, `{"sql":"` + strings.Repeat("a", 200<<10) + `"}`, "/api/execute", 413, "limit"},
		{"bad int binding", nil, `{"sql":"SELECT ?","params":[{"type":"int64","value":"1.5"}]}`, "/api/execute", 400, "parameter"},
		{"bad bytes binding", nil, `{"sql":"SELECT ?","params":[{"type":"bytes","value":"!!"}]}`, "/api/execute", 400, "parameter"},
		{"unknown binding type", nil, `{"sql":"SELECT ?","params":[{"type":"float","value":"1"}]}`, "/api/execute", 400, "parameter"},
		{"unknown route", nil, "{}", "/api/nope", 404, "not_found"},
		{"tx route without tx", nil, "{}", "/api/tx/execute", 409, "transaction_closed"},
		{"short client identity", func(r *http.Request) { r.Header.Set("X-Sceptre-Client", "short") }, "{}", "/api/tx/begin", 400, "invalid"},
	}
	for _, c := range cases {
		r := f.call(c.path, c.body, c.mutate)
		if r.Status != c.status || r.Body.Error == nil || r.Body.Error.Code != c.code {
			t.Errorf("%s: status %d error %+v, want %d %s", c.name, r.Status, r.Body.Error, c.status, c.code)
		}
	}
}

func TestAutocommitRoundTripsTypedValues(t *testing.T) {
	f := newFixture(t, Options{})
	f.mustSQL("CREATE TABLE items (id int64, name bytes, PRIMARY KEY (id))")
	body := `{"sql":"INSERT INTO items (id, name) VALUES (?, ?)","params":[{"type":"int64","value":"9223372036854775807"},{"type":"bytes","value":"AP9oaQ=="}]}`
	if r := f.call("/api/execute", body, nil); r.Status != 200 || r.Body.Result.RowsAffected != "1" {
		t.Fatalf("insert: %d %+v %+v", r.Status, r.Body.Error, r.Body.Result)
	}
	r := f.mustSQL("SELECT id, name FROM items")
	want := []Scalar{{"int64", "9223372036854775807"}, {"bytes", "AP9oaQ=="}}
	if len(r.Body.Result.Rows) != 1 || r.Body.Result.Rows[0][0] != want[0] || r.Body.Result.Rows[0][1] != want[1] {
		t.Fatalf("rows = %+v, want %+v", r.Body.Result.Rows, want)
	}
	if r.Body.Transaction != nil {
		t.Fatalf("autocommit reported a transaction: %+v", r.Body.Transaction)
	}
}

func TestErrorsMapToStableCodesWithoutResults(t *testing.T) {
	f := newFixture(t, Options{})
	f.mustSQL("CREATE TABLE items (id int64, PRIMARY KEY (id))")
	f.mustSQL("INSERT INTO items (id) VALUES (1)")
	for sql, want := range map[string]struct {
		status int
		code   string
	}{
		"INSERT INTO items (id) VALUES (1)": {409, "constraint"},
		"SELECT * FROM missing":             {404, "not_found"},
		"SELEC nonsense":                    {400, "parse"},
	} {
		r := f.sql("/api/execute", sql, nil)
		if r.Status != want.status || r.Body.Error == nil || r.Body.Error.Code != want.code {
			t.Errorf("%s: status %d error %+v, want %d %s", sql, r.Status, r.Body.Error, want.status, want.code)
		}
		if r.Body.Result != nil {
			t.Errorf("%s: failed statement returned a result %+v", sql, r.Body.Result)
		}
	}
}

func TestSchemaAndExplainWireShapes(t *testing.T) {
	f := newFixture(t, Options{})
	f.mustSQL("CREATE TABLE users (id int64, name bytes, age int64, PRIMARY KEY (id))")
	f.mustSQL("CREATE INDEX users_age ON users (age)")
	r := f.call("/api/schema", "{}", nil)
	if r.Status != 200 || len(r.Body.Schema) != 1 {
		t.Fatalf("schema: %d %+v", r.Status, r.Body)
	}
	s := r.Body.Schema[0]
	if s.Name != "users" || len(s.Columns) != 3 || s.Columns[1] != (column{"name", "bytes"}) || len(s.Indexes) != 1 || s.Indexes[0].Name != "users_age" {
		t.Fatalf("schema shape: %+v", s)
	}
	r = f.sql("/api/explain", "SELECT * FROM users WHERE age = 42 LIMIT 5", nil)
	if r.Status != 200 || r.Body.Plan == nil || r.Body.Plan.Index != "users_age" || r.Body.Plan.Limit == nil || *r.Body.Plan.Limit != "5" {
		t.Fatalf("explain: %d %+v %+v", r.Status, r.Body.Error, r.Body.Plan)
	}
}

func TestTransactionCommitAndRollback(t *testing.T) {
	f := newFixture(t, Options{})
	f.mustSQL("CREATE TABLE items (id int64, PRIMARY KEY (id))")

	id := f.begin()
	if r := f.sql("/api/tx/execute", "INSERT INTO items (id) VALUES (1)", withTx(id)); r.Status != 200 || r.Body.Transaction == nil || r.Body.Transaction.ID != id {
		t.Fatalf("tx insert: %d %+v %+v", r.Status, r.Body.Error, r.Body.Transaction)
	}
	if r := f.sql("/api/tx/execute", "SELECT * FROM items", withTx(id)); len(r.Body.Result.Rows) != 1 {
		t.Fatalf("transaction does not read its own write: %+v", r.Body.Result)
	}
	if r := f.call("/api/tx/rollback", "{}", withTx(id)); r.Status != 200 || r.Body.Transaction != nil {
		t.Fatalf("rollback: %d %+v", r.Status, r.Body)
	}
	if n := f.count("items"); n != 0 {
		t.Fatalf("rolled-back row visible: %d rows", n)
	}

	id = f.begin()
	f.sql("/api/tx/execute", "INSERT INTO items (id) VALUES (2)", withTx(id))
	f.sql("/api/tx/execute", "CREATE TABLE other (id int64, PRIMARY KEY (id))", withTx(id))
	if r := f.call("/api/tx/commit", "{}", withTx(id)); r.Status != 200 || r.Body.Transaction != nil || r.Body.Error != nil {
		t.Fatalf("commit: %d %+v", r.Status, r.Body)
	}
	if n := f.count("items"); n != 1 {
		t.Fatalf("committed row missing: %d rows", n)
	}
	if n := f.count("other"); n != 0 {
		t.Fatalf("committed table unreadable: %d rows", n)
	}
	if r := f.call("/api/tx/commit", "{}", withTx(id)); r.Status != 409 {
		t.Fatalf("second commit of a finished transaction: %d %+v", r.Status, r.Body)
	}
}

func TestTransactionOwnershipAndBusy(t *testing.T) {
	f := newFixture(t, Options{})
	f.mustSQL("CREATE TABLE items (id int64, PRIMARY KEY (id))")
	id := f.begin()
	f.sql("/api/tx/execute", "INSERT INTO items (id) VALUES (7)", withTx(id))

	// Reads outside the transaction run on a snapshot: not busy, and they do
	// not see its uncommitted row. Schema and explain likewise.
	if r := f.sql("/api/execute", "SELECT * FROM items", nil); r.Status != 200 || len(r.Body.Result.Rows) != 0 {
		t.Fatalf("read during another client's transaction: %d %+v %+v", r.Status, r.Body.Error, r.Body.Result)
	}
	if r := f.call("/api/schema", "{}", nil); r.Status != 200 || len(r.Body.Schema) != 1 {
		t.Fatalf("schema during transaction: %d %+v", r.Status, r.Body.Error)
	}
	if r := f.sql("/api/explain", "SELECT * FROM items WHERE id = 7", nil); r.Status != 200 || r.Body.Plan == nil {
		t.Fatalf("explain during transaction: %d %+v", r.Status, r.Body.Error)
	}
	// Writes outside the transaction still wait their turn: busy.
	if r := f.sql("/api/execute", "INSERT INTO items (id) VALUES (8)", nil); r.Status != 409 || r.Body.Error.Code != "busy" {
		t.Fatalf("write during another client's transaction: %d %+v", r.Status, r.Body.Error)
	}
	if r := f.call("/api/tx/begin", "{}", nil); r.Status != 409 || r.Body.Error.Code != "busy" {
		t.Fatalf("second begin: %d %+v", r.Status, r.Body.Error)
	}
	otherClient := func(r *http.Request) { withTx(id)(r); r.Header.Set("X-Sceptre-Client", "another-client-0002") }
	if r := f.sql("/api/tx/execute", "INSERT INTO items (id) VALUES (1)", otherClient); r.Status != 409 || r.Body.Error.Code != "transaction_closed" {
		t.Fatalf("foreign client used transaction: %d %+v", r.Status, r.Body.Error)
	}
	if r := f.sql("/api/tx/execute", "INSERT INTO items (id) VALUES (1)", withTx("guessed-id")); r.Status != 409 {
		t.Fatalf("wrong transaction id accepted: %d %+v", r.Status, r.Body.Error)
	}
	// A failed statement keeps the transaction usable.
	if r := f.sql("/api/tx/execute", "SELECT * FROM missing", withTx(id)); r.Status != 404 || r.Body.Transaction == nil {
		t.Fatalf("failed statement ended transaction: %d %+v", r.Status, r.Body)
	}
	if r := f.call("/api/tx/rollback", "{}", withTx(id)); r.Status != 200 {
		t.Fatalf("rollback: %d %+v", r.Status, r.Body.Error)
	}
}

func TestAbandonedTransactionExpires(t *testing.T) {
	f := newFixture(t, Options{TransactionTTL: 50 * time.Millisecond})
	f.mustSQL("CREATE TABLE items (id int64, PRIMARY KEY (id))")
	id := f.begin()
	f.sql("/api/tx/execute", "INSERT INTO items (id) VALUES (1)", withTx(id))

	// Poll the transaction itself until the service expires it.
	deadline := time.Now().Add(2 * time.Second)
	for {
		r := f.sql("/api/tx/execute", "SELECT * FROM items", withTx(id))
		if r.Status == 409 && r.Body.Error.Code == "transaction_closed" {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("transaction never expired: %d %+v", r.Status, r.Body.Error)
		}
		time.Sleep(10 * time.Millisecond)
	}
	// It was rolled back, and the writer slot is free again.
	if r := f.sql("/api/execute", "SELECT * FROM items", nil); r.Status != 200 || len(r.Body.Result.Rows) != 0 {
		t.Fatalf("expired transaction committed: %d %+v", r.Status, r.Body.Result)
	}
	f.mustSQL("INSERT INTO items (id) VALUES (2)")
}

func TestOversizedResponseKeepsTransaction(t *testing.T) {
	f := newFixture(t, Options{MaxResponseBytes: 1024})
	f.mustSQL("CREATE TABLE items (id int64, data bytes, PRIMARY KEY (id))")
	id := f.begin()
	for i := 0; i < 8; i++ {
		body, _ := json.Marshal(query{
			SQL:    "INSERT INTO items (id, data) VALUES (?, ?)",
			Params: []Scalar{scalar(sceptre.Int64(int64(i))), scalar(sceptre.Bytes([]byte(strings.Repeat("x", 200))))},
		})
		if r := f.call("/api/tx/execute", string(body), withTx(id)); r.Status != 200 {
			t.Fatalf("insert %d: %d %+v", i, r.Status, r.Body.Error)
		}
	}
	r := f.sql("/api/tx/execute", "SELECT * FROM items", withTx(id))
	if r.Status != 413 || r.Body.Error.Code != "limit" || r.Body.Transaction == nil || r.Body.Transaction.ID != id {
		t.Fatalf("oversized result: %d %+v %+v", r.Status, r.Body.Error, r.Body.Transaction)
	}
	if r := f.call("/api/tx/commit", "{}", withTx(id)); r.Status != 200 {
		t.Fatalf("transaction lost after oversized result: %d %+v", r.Status, r.Body.Error)
	}
	if r := f.sql("/api/execute", "SELECT id FROM items", nil); r.Status != 200 || len(r.Body.Result.Rows) != 8 {
		t.Fatalf("committed rows: %d %+v", r.Status, r.Body.Result)
	}
}

func TestCloseRollsBackAndRejectsRequests(t *testing.T) {
	f := newFixture(t, Options{})
	f.mustSQL("CREATE TABLE items (id int64, PRIMARY KEY (id))")
	id := f.begin()
	f.sql("/api/tx/execute", "INSERT INTO items (id) VALUES (1)", withTx(id))
	f.h.Close()
	if r := f.call("/api/schema", "{}", nil); r.Status != 503 || r.Body.Error.Code != "closed" {
		t.Fatalf("request after close: %d %+v", r.Status, r.Body.Error)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	res, err := f.db.Execute(ctx, "SELECT * FROM items")
	if err != nil || len(res.Rows) != 0 {
		t.Fatalf("close did not roll back: %v %+v", err, res.Rows)
	}
}

func TestStaticAssets(t *testing.T) {
	f := newFixture(t, Options{})
	for path, typ := range map[string]string{"/": "text/html", "/studio.js": "text/javascript", "/studio.css": "text/css"} {
		rec := httptest.NewRecorder()
		f.h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "http://"+testHost+path, nil))
		if rec.Code != 200 || !strings.HasPrefix(rec.Header().Get("Content-Type"), typ) || rec.Body.Len() == 0 {
			t.Errorf("%s: %d %q", path, rec.Code, rec.Header().Get("Content-Type"))
		}
		if !strings.Contains(rec.Header().Get("Content-Security-Policy"), "default-src 'none'") {
			t.Errorf("%s: missing CSP", path)
		}
	}
	rec := httptest.NewRecorder()
	f.h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "http://"+testHost+"/secret", nil))
	if rec.Code != 404 {
		t.Errorf("unknown asset: %d", rec.Code)
	}
	rec = httptest.NewRecorder()
	f.h.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "http://"+testHost+"/", nil))
	if rec.Code != 405 {
		t.Errorf("POST to asset: %d", rec.Code)
	}
}

func TestConcurrentReadsDuringATransaction(t *testing.T) {
	f := newFixture(t, Options{TransactionTTL: time.Minute})
	f.mustSQL("CREATE TABLE items (id int64, PRIMARY KEY (id))")
	f.mustSQL("INSERT INTO items (id) VALUES (1)")
	id := f.begin()
	f.sql("/api/tx/execute", "INSERT INTO items (id) VALUES (2)", withTx(id))

	var wg sync.WaitGroup
	failures := make(chan string, 64)
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 20; i++ {
				body, _ := json.Marshal(query{SQL: "SELECT id FROM items"})
				req := httptest.NewRequest(http.MethodPost, "http://"+testHost+"/api/execute", strings.NewReader(string(body)))
				req.Header.Set("Authorization", "Bearer "+testToken)
				req.Header.Set("Content-Type", "application/json")
				rec := httptest.NewRecorder()
				f.h.ServeHTTP(rec, req)
				if rec.Code != 200 || !strings.Contains(rec.Body.String(), `"rows":[[{"type":"int64","value":"1"}]]`) {
					failures <- fmt.Sprintf("%d %s", rec.Code, rec.Body.String())
					return
				}
			}
		}()
	}
	wg.Wait()
	close(failures)
	for msg := range failures {
		t.Fatalf("concurrent read: %s", msg)
	}
	if r := f.call("/api/tx/commit", "{}", withTx(id)); r.Status != 200 {
		t.Fatalf("commit after concurrent reads: %d %+v", r.Status, r.Body.Error)
	}
}
