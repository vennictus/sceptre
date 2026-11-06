package service

import (
	"bytes"
	"encoding/json"
	"testing"
	"time"

	"github.com/vennictus/sceptre"
)

// A spliced write response is byte for byte what encoding the typed
// response gives, and tooLarge measures it exactly (with the widest
// duration), so the size checked before commit is the size sent.
func TestSplicedResponseMatchesTypedEncoding(t *testing.T) {
	res := sceptre.Result{
		Columns: []string{"id", "body", "at", "n"},
		Rows: [][]sceptre.Value{
			{sceptre.Int64(1), sceptre.Text(`<a href="x">&é</a>`), sceptre.Timestamp(time.Unix(0, 0)), sceptre.Null()},
			{sceptre.Int64(-2), sceptre.Bytes([]byte{0xff, 0}), sceptre.Timestamp(time.Unix(1, 5000)), sceptre.Float64(0.1)},
		},
		RowsAffected: 2,
	}
	for _, tx := range []*transaction{nil, {ID: "abc", ExpiresAt: time.Unix(1e9, 0).UTC()}} {
		typed := response{Result: resultWire(res), Transaction: tx, DurationMS: 12.345}
		want, err := json.Marshal(typed)
		if err != nil {
			t.Fatal(err)
		}
		encoded, err := json.Marshal(resultWire(res))
		if err != nil {
			t.Fatal(err)
		}
		got, err := encodeResponse(response{encoded: encoded, Transaction: tx, DurationMS: 12.345})
		if err != nil || !bytes.Equal(got, want) {
			t.Fatalf("spliced:\n%s\ntyped:\n%s\n(%v)", got, want, err)
		}

		widest, _ := encodeResponse(response{encoded: encoded, Transaction: tx, DurationMS: -1.7976931348623157e308})
		h := &Handler{opts: Options{MaxResponseBytes: len(widest)}}
		if h.tooLarge(encoded, tx) {
			t.Fatalf("a response of exactly the limit (%d bytes) was judged too large", len(widest))
		}
		h.opts.MaxResponseBytes--
		if !h.tooLarge(encoded, tx) {
			t.Fatalf("a response one byte over the limit was judged to fit")
		}
	}
}
