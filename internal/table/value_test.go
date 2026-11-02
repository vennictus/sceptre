package table

import (
	"bytes"
	"math"
	"math/rand"
	"testing"
	"time"
)

// Encoded keys must sort exactly like the values they encode.
func TestEncodingPreservesOrder(t *testing.T) {
	t.Parallel()
	rng := rand.New(rand.NewSource(9))
	floats := []float64{0, math.Copysign(0, -1), 1, -1, 0.5, -0.5, math.MaxFloat64, -math.MaxFloat64, math.SmallestNonzeroFloat64, -math.SmallestNonzeroFloat64, 1e300, -1e-300}
	for i := 0; i < 500; i++ {
		floats = append(floats, (rng.Float64()-0.5)*math.Pow(10, float64(rng.Intn(40)-20)))
	}
	var values []Value
	for _, f := range floats {
		values = append(values, Float64Value(f))
	}
	for i := 0; i < 200; i++ {
		values = append(values, TimestampValue(time.Unix(rng.Int63n(1<<34)-(1<<33), rng.Int63n(1e9))))
	}
	values = append(values, BoolValue(false), BoolValue(true))
	for _, a := range values {
		for _, b := range values {
			if a.Type != b.Type {
				continue
			}
			want := Compare(a, b)
			got := bytes.Compare(appendEncodedValue(nil, a), appendEncodedValue(nil, b))
			if got != want {
				t.Fatalf("order of %s and %s: encoded %d, values %d", FormatValue(a), FormatValue(b), got, want)
			}
			back, n, err := consumeEncodedValue(appendEncodedValue(nil, a), a.Type)
			if err != nil || n != len(appendEncodedValue(nil, a)) || Compare(back, a) != 0 {
				t.Fatalf("round trip of %s = %s, %v", FormatValue(a), FormatValue(back), err)
			}
		}
	}
}

func TestFloatValuesMustBeFinite(t *testing.T) {
	t.Parallel()
	for _, f := range []float64{math.NaN(), math.Inf(1), math.Inf(-1)} {
		if valueMatchesType(Value{Type: TypeFloat64, F64: f}, TypeFloat64) {
			t.Fatalf("%v accepted", f)
		}
	}
	if !bytes.Equal(appendEncodedValue(nil, Value{Type: TypeFloat64, F64: math.Copysign(0, -1)}), appendEncodedValue(nil, Float64Value(0))) {
		t.Fatal("-0 and 0 encode differently")
	}
}
