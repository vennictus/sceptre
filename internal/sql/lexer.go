package sql

import (
	"fmt"
	"strconv"
	"strings"
	"unicode"
)

type tokenKind int

const (
	tokenEOF tokenKind = iota
	tokenIdent
	tokenNumber
	tokenString
	tokenComma
	tokenLParen
	tokenRParen
	tokenStar
	tokenEqual
	tokenNotEqual
	tokenLess
	tokenLessEqual
	tokenGreater
	tokenGreaterEqual
)

type token struct {
	kind tokenKind
	text string
}

func lex(input string) ([]token, error) {
	var tokens []token
	for i := 0; i < len(input); {
		r := rune(input[i])
		if unicode.IsSpace(r) {
			i++
			continue
		}

		switch input[i] {
		case ',':
			tokens = append(tokens, token{kind: tokenComma, text: ","})
			i++
		case '(':
			tokens = append(tokens, token{kind: tokenLParen, text: "("})
			i++
		case ')':
			tokens = append(tokens, token{kind: tokenRParen, text: ")"})
			i++
		case '*':
			tokens = append(tokens, token{kind: tokenStar, text: "*"})
			i++
		case '=':
			tokens = append(tokens, token{kind: tokenEqual, text: "="})
			i++
		case '!':
			if i+1 >= len(input) || input[i+1] != '=' {
				return nil, fmt.Errorf("%w: unexpected !", ErrParse)
			}
			tokens = append(tokens, token{kind: tokenNotEqual, text: "!="})
			i += 2
		case '<':
			if i+1 < len(input) && input[i+1] == '=' {
				tokens = append(tokens, token{kind: tokenLessEqual, text: "<="})
				i += 2
			} else if i+1 < len(input) && input[i+1] == '>' {
				tokens = append(tokens, token{kind: tokenNotEqual, text: "<>"})
				i += 2
			} else {
				tokens = append(tokens, token{kind: tokenLess, text: "<"})
				i++
			}
		case '>':
			if i+1 < len(input) && input[i+1] == '=' {
				tokens = append(tokens, token{kind: tokenGreaterEqual, text: ">="})
				i += 2
			} else {
				tokens = append(tokens, token{kind: tokenGreater, text: ">"})
				i++
			}
		case '\'':
			value, next, err := lexString(input, i+1)
			if err != nil {
				return nil, err
			}
			tokens = append(tokens, token{kind: tokenString, text: value})
			i = next
		default:
			switch {
			case isIdentStart(input[i]):
				start := i
				i++
				for i < len(input) && isIdentPart(input[i]) {
					i++
				}
				tokens = append(tokens, token{kind: tokenIdent, text: input[start:i]})
			case input[i] == '-' || isDigit(input[i]):
				start := i
				if input[i] == '-' {
					i++
					if i >= len(input) || !isDigit(input[i]) {
						return nil, fmt.Errorf("%w: expected digit after -", ErrParse)
					}
				}
				for i < len(input) && isDigit(input[i]) {
					i++
				}
				if _, err := strconv.ParseInt(input[start:i], 10, 64); err != nil {
					return nil, fmt.Errorf("%w: invalid integer %q", ErrParse, input[start:i])
				}
				tokens = append(tokens, token{kind: tokenNumber, text: input[start:i]})
			default:
				return nil, fmt.Errorf("%w: unexpected character %q", ErrParse, input[i])
			}
		}
	}
	return append(tokens, token{kind: tokenEOF}), nil
}

func lexString(input string, start int) (string, int, error) {
	var out strings.Builder
	for i := start; i < len(input); i++ {
		if input[i] != '\'' {
			out.WriteByte(input[i])
			continue
		}
		if i+1 < len(input) && input[i+1] == '\'' {
			out.WriteByte('\'')
			i++
			continue
		}
		return out.String(), i + 1, nil
	}
	return "", 0, fmt.Errorf("%w: unterminated string", ErrParse)
}

func isIdentStart(b byte) bool {
	return b == '_' || ('a' <= b && b <= 'z') || ('A' <= b && b <= 'Z')
}

func isIdentPart(b byte) bool {
	return isIdentStart(b) || isDigit(b)
}

func isDigit(b byte) bool {
	return '0' <= b && b <= '9'
}
