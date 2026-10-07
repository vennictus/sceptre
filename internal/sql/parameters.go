package sql

import (
	"errors"
	"fmt"
	"strconv"

	"github.com/vennictus/sceptre/internal/table"
)

var ErrParameter = errors.New("sql: invalid parameters")

// ParseParameters binds typed values as literal tokens, never as SQL text.
// Question marks in quoted strings are ordinary bytes. Bindings may appear
// where the grammar accepts literals, including integer LIMIT and OFFSET.
func ParseParameters(input string, values []table.Value) (Statement, error) {
	tokens, err := lex(input)
	if err != nil {
		return nil, err
	}
	n := 0
	for i := range tokens {
		if tokens[i].kind != tokenParameter {
			continue
		}
		if n >= len(values) {
			return nil, fmt.Errorf("%w: missing binding %d", ErrParameter, n+1)
		}
		v := values[n]
		n++
		switch v.Type {
		case table.TypeInt64:
			tokens[i] = token{kind: tokenNumber, text: strconv.FormatInt(v.I64, 10)}
		case table.TypeBytes:
			tokens[i] = token{kind: tokenString, text: string(v.Bytes)}
		default:
			return nil, fmt.Errorf("%w: binding %d has unsupported type", ErrParameter, n)
		}
	}
	if n != len(values) {
		return nil, fmt.Errorf("%w: expected %d bindings, got %d", ErrParameter, n, len(values))
	}
	return parseTokens(tokens)
}
