package table

import (
	"encoding/binary"
	"errors"
)

var ErrCorruptRecord = errors.New("table: corrupt record encoding")

func appendEncodedValue(dst []byte, value Value) []byte {
	switch value.Type {
	case TypeInt64:
		var buf [8]byte
		encoded := uint64(value.I64) ^ (1 << 63)
		binary.BigEndian.PutUint64(buf[:], encoded)
		return append(dst, buf[:]...)
	case TypeBytes, TypeText:
		for _, b := range value.Bytes {
			switch b {
			case 0x00:
				dst = append(dst, 0x01, 0x01)
			case 0x01:
				dst = append(dst, 0x01, 0x02)
			default:
				dst = append(dst, b)
			}
		}
		return append(dst, 0x00)
	default:
		return dst
	}
}

func consumeEncodedValue(src []byte, valueType Type) (Value, int, error) {
	switch valueType {
	case TypeInt64:
		if len(src) < 8 {
			return Value{}, 0, ErrCorruptRecord
		}
		encoded := binary.BigEndian.Uint64(src[:8]) ^ (1 << 63)
		return Int64Value(int64(encoded)), 8, nil
	case TypeBytes, TypeText:
		out := make([]byte, 0)
		for i := 0; i < len(src); {
			switch src[i] {
			case 0x00:
				return Value{Type: valueType, Bytes: out}, i + 1, nil
			case 0x01:
				if i+1 >= len(src) {
					return Value{}, 0, ErrCorruptRecord
				}
				switch src[i+1] {
				case 0x01:
					out = append(out, 0x00)
				case 0x02:
					out = append(out, 0x01)
				default:
					return Value{}, 0, ErrCorruptRecord
				}
				i += 2
			default:
				out = append(out, src[i])
				i++
			}
		}
		return Value{}, 0, ErrCorruptRecord
	default:
		return Value{}, 0, ErrInvalidValue
	}
}

// Tags for column values in tagged row formats. NULL sorts before any value.
const (
	tagNull  byte = 0x00
	tagValue byte = 0x01
)

// appendColumnValue appends a non-key column value. Tagged formats mark each
// value as NULL or present; the legacy format stores the value alone.
func appendColumnValue(dst []byte, value Value, tagged bool) []byte {
	if !tagged {
		return appendEncodedValue(dst, value)
	}
	if value.Type == TypeNull {
		return append(dst, tagNull)
	}
	return appendEncodedValue(append(dst, tagValue), value)
}

// consumeColumnValue reads one value written by appendColumnValue.
func consumeColumnValue(src []byte, valueType Type, tagged bool) (Value, int, error) {
	if !tagged {
		return consumeEncodedValue(src, valueType)
	}
	if len(src) == 0 {
		return Value{}, 0, ErrCorruptRecord
	}
	switch src[0] {
	case tagNull:
		return NullValue(), 1, nil
	case tagValue:
		v, n, err := consumeEncodedValue(src[1:], valueType)
		return v, n + 1, err
	}
	return Value{}, 0, ErrCorruptRecord
}
