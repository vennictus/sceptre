package service

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// BenchmarkLargeResults serves 1,000 rows of 1 KB text through the handler:
// a SELECT, and an UPDATE ... RETURNING in a transaction (sized before it is
// kept). It measures response building on the real path, not encoding alone.
func BenchmarkLargeResults(b *testing.B) {
	f := newFixture(b, Options{})
	f.mustSQL("CREATE TABLE t (id int64 PRIMARY KEY, flag bool, body text)")
	body := strings.Repeat("x", 1024)
	for i := 0; i < 1000; i += 50 {
		var rows []string
		for j := i; j < i+50; j++ {
			rows = append(rows, "("+itoa(j)+", FALSE, '"+body+"')")
		}
		f.mustSQL("INSERT INTO t VALUES " + strings.Join(rows, ", "))
	}
	serve := func(b *testing.B, path, sql string, mutate func(*http.Request)) {
		q, _ := json.Marshal(query{SQL: sql})
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			req := httptest.NewRequest(http.MethodPost, "http://"+testHost+path, strings.NewReader(string(q)))
			req.Header.Set("Authorization", "Bearer "+testToken)
			req.Header.Set("Content-Type", "application/json")
			req.Header.Set("X-Sceptre-Client", testClient)
			if mutate != nil {
				mutate(req)
			}
			rec := httptest.NewRecorder()
			f.h.ServeHTTP(rec, req)
			if rec.Code != 200 {
				b.Fatalf("status %d: %.200s", rec.Code, rec.Body.String())
			}
		}
	}
	b.Run("select", func(b *testing.B) { serve(b, "/api/execute", "SELECT * FROM t", nil) })
	b.Run("update_returning_in_tx", func(b *testing.B) {
		id := f.begin()
		defer f.call("/api/tx/rollback", "{}", withTx(id))
		serve(b, "/api/tx/execute", "UPDATE t SET flag = TRUE RETURNING *", withTx(id))
	})
}

func itoa(n int) string {
	b, _ := json.Marshal(n)
	return string(b)
}
