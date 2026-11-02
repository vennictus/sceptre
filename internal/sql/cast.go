package sql

import (
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/vennictus/sceptre/internal/table"
)

// CastExpr is CAST(expr AS type).
type CastExpr struct {
	Expr Expr
	Type string
}

func (*CastExpr) exprNode() {}

// castValue converts v to type to. NULL stays NULL. Conversions that can lose
// meaning fail instead: a fraction or out-of-range float to int64, text that
// does not parse, bytes that are not UTF-8.
func castValue(v table.Value, to table.Type) (table.Value, error) {
	if v.IsNull() || v.Type == to {
		return v, nil
	}
	fail := func() (table.Value, error) {
		return table.Value{}, fmt.Errorf("%w: cannot cast %s %s to %s", table.ErrInvalidValue, v.Type, table.FormatValue(v), to)
	}
	switch to {
	case table.TypeText:
		if v.Type == table.TypeBytes {
			if !utf8.Valid(v.Bytes) {
				return fail()
			}
			return table.TextValue(string(v.Bytes)), nil
		}
		return table.TextValue(table.FormatValue(v)), nil
	case table.TypeBytes:
		if v.Type == table.TypeText {
			return table.BytesValue(v.Bytes), nil
		}
	case table.TypeInt64:
		switch v.Type {
		case table.TypeFloat64:
			if v.F64 != math.Trunc(v.F64) || v.F64 < math.MinInt64 || v.F64 >= math.MaxInt64 {
				return fail()
			}
			return table.Int64Value(int64(v.F64)), nil
		case table.TypeBool:
			if v.Bool {
				return table.Int64Value(1), nil
			}
			return table.Int64Value(0), nil
		case table.TypeTimestamp:
			return table.Int64Value(v.I64), nil
		case table.TypeText:
			n, err := strconv.ParseInt(strings.TrimSpace(string(v.Bytes)), 10, 64)
			if err != nil {
				return fail()
			}
			return table.Int64Value(n), nil
		}
	case table.TypeFloat64:
		switch v.Type {
		case table.TypeInt64:
			return table.Float64Value(float64(v.I64)), nil
		case table.TypeText:
			f, err := strconv.ParseFloat(strings.TrimSpace(string(v.Bytes)), 64)
			if err != nil || math.IsNaN(f) || math.IsInf(f, 0) {
				return fail()
			}
			return table.Float64Value(f), nil
		}
	case table.TypeBool:
		switch v.Type {
		case table.TypeInt64:
			if v.I64 == 0 || v.I64 == 1 {
				return table.BoolValue(v.I64 == 1), nil
			}
		case table.TypeText:
			switch strings.ToLower(strings.TrimSpace(string(v.Bytes))) {
			case "true":
				return table.BoolValue(true), nil
			case "false":
				return table.BoolValue(false), nil
			}
		}
	case table.TypeTimestamp:
		switch v.Type {
		case table.TypeText:
			t, err := parseTimestamp(strings.TrimSpace(string(v.Bytes)))
			if err != nil {
				return fail()
			}
			return table.TimestampValue(t), nil
		case table.TypeInt64:
			return table.TimestampValue(time.UnixMicro(v.I64)), nil
		}
	}
	return fail()
}
