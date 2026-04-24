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
	case TypeBytes:
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
	case TypeBytes:
		out := make([]byte, 0)
		for i := 0; i < len(src); {
			switch src[i] {
			case 0x00:
				return BytesValue(out), i + 1, nil
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
