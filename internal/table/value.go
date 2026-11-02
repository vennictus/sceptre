package table

import (
	"bytes"
	"cmp"
	"strconv"
	"time"
)

// Compare orders two non-NULL values of the same type: -1, 0, or 1. Bytes
// and text compare in byte order, and false sorts before true.
func Compare(a, b Value) int {
	switch a.Type {
	case TypeInt64, TypeTimestamp:
		return cmp.Compare(a.I64, b.I64)
	case TypeFloat64:
		return cmp.Compare(a.F64, b.F64)
	case TypeBool:
		switch {
		case a.Bool == b.Bool:
			return 0
		case b.Bool:
			return -1
		}
		return 1
	}
	return bytes.Compare(a.Bytes, b.Bytes)
}

// FormatValue renders a value for display: decimal numbers, true/false,
// RFC 3339 timestamps in UTC, raw text and bytes, and NULL.
func FormatValue(v Value) string {
	switch v.Type {
	case TypeInt64:
		return strconv.FormatInt(v.I64, 10)
	case TypeFloat64:
		return strconv.FormatFloat(v.F64, 'g', -1, 64)
	case TypeBool:
		return strconv.FormatBool(v.Bool)
	case TypeTimestamp:
		return v.Time().Format(time.RFC3339Nano)
	case TypeBytes, TypeText:
		return string(v.Bytes)
	case TypeNull:
		return "NULL"
	}
	return "<invalid>"
}
