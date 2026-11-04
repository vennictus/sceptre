package service

import (
	"strings"
	"testing"
	"time"

	"github.com/vennictus/sceptre"
)

// allScalars holds one value of every type the service can send.
var allScalars = []sceptre.Value{
	sceptre.Int64(-7),
	sceptre.Float64(1.5),
	sceptre.Bool(true),
	sceptre.Text("hello!"),
	sceptre.Bytes([]byte{0xff, 0x00}),
	sceptre.Timestamp(time.Date(2026, 11, 2, 10, 0, 0, 123000, time.UTC)),
	sceptre.Null(),
}

func TestScalarsRoundTrip(t *testing.T) {
	for _, v := range allScalars {
		s := scalar(v)
		got, err := s.decode()
		if err != nil {
			t.Fatalf("%s %q: decode: %v", s.Type, s.Value, err)
		}
		if scalar(got) != s {
			t.Errorf("%s %q round-tripped to %+v", s.Type, s.Value, scalar(got))
		}
	}
}

// Studio must offer every wire type as a binding and render it by type; only
// bytes are base64. A type added to the wire but not to Studio once made
// result rendering throw after a statement had already committed.
func TestStudioKnowsEveryWireType(t *testing.T) {
	js, err := assets.ReadFile("studio/studio.js")
	if err != nil {
		t.Fatal(err)
	}
	src := string(js)
	for _, v := range allScalars {
		typ := scalar(v).Type
		if !strings.Contains(src, "['"+typ+"','") {
			t.Errorf("studio.js parameter picker lacks %s", typ)
		}
	}
	if !strings.Contains(src, "value.type === 'bytes'") || !strings.Contains(src, "value.type === 'null'") {
		t.Error("studio.js no longer renders cells by wire type")
	}
}
