package service

import (
	"encoding/base64"
	"fmt"
	"strconv"

	"github.com/vennictus/sceptre"
)

// Scalar never serializes int64 as a JSON number or bytes as lossy UTF-8.
type Scalar struct {
	Type  string `json:"type"`
	Value string `json:"value"`
}

func (v Scalar) decode() (sceptre.Value, error) {
	switch v.Type {
	case "int64":
		n, err := strconv.ParseInt(v.Value, 10, 64)
		if err != nil {
			return sceptre.Value{}, fmt.Errorf("invalid int64 binding")
		}
		return sceptre.Int64(n), nil
	case "bytes":
		b, err := base64.StdEncoding.Strict().DecodeString(v.Value)
		if err != nil {
			return sceptre.Value{}, fmt.Errorf("invalid base64 binding")
		}
		return sceptre.Bytes(b), nil
	default:
		return sceptre.Value{}, fmt.Errorf("binding type must be int64 or bytes")
	}
}
func scalar(v sceptre.Value) Scalar {
	if v.Type == sceptre.TypeInt64 {
		return Scalar{Type: "int64", Value: strconv.FormatInt(v.I64, 10)}
	}
	return Scalar{Type: "bytes", Value: base64.StdEncoding.EncodeToString(v.Bytes)}
}

type query struct {
	SQL    string   `json:"sql"`
	Params []Scalar `json:"params"`
}

func (q query) bindings() ([]sceptre.Value, error) {
	if len(q.Params) > 256 {
		return nil, fmt.Errorf("at most 256 bindings")
	}
	values := make([]sceptre.Value, len(q.Params))
	for i, p := range q.Params {
		v, err := p.decode()
		if err != nil {
			return nil, fmt.Errorf("binding %d: %w", i+1, err)
		}
		values[i] = v
	}
	return values, nil
}

type result struct {
	Columns      []string   `json:"columns"`
	Rows         [][]Scalar `json:"rows"`
	RowsAffected string     `json:"rows_affected"`
}

func resultWire(r sceptre.Result) *result {
	out := &result{Columns: r.Columns, Rows: make([][]Scalar, len(r.Rows)), RowsAffected: strconv.Itoa(r.RowsAffected)}
	if out.Columns == nil {
		out.Columns = []string{}
	}
	for i, row := range r.Rows {
		out.Rows[i] = make([]Scalar, len(row))
		for j, v := range row {
			out.Rows[i][j] = scalar(v)
		}
	}
	return out
}

type column struct {
	Name string `json:"name"`
	Type string `json:"type"`
}
type schema struct {
	Name       string             `json:"name"`
	Columns    []column           `json:"columns"`
	PrimaryKey []string           `json:"primary_key"`
	Indexes    []sceptre.IndexDef `json:"indexes"`
}

func schemas(defs []sceptre.TableDef) []schema {
	out := make([]schema, len(defs))
	for i, d := range defs {
		out[i] = schema{Name: d.Name, PrimaryKey: d.PrimaryKey, Indexes: d.Indexes, Columns: make([]column, len(d.Columns))}
		if out[i].Indexes == nil {
			out[i].Indexes = []sceptre.IndexDef{}
		}
		for j, c := range d.Columns {
			typ := "bytes"
			if c.Type == sceptre.TypeInt64 {
				typ = "int64"
			}
			out[i].Columns[j] = column{Name: c.Name, Type: typ}
		}
	}
	return out
}

// Plan deliberately excludes internal AST encoding. Limits remain decimal strings.
type plan struct {
	Statement   string  `json:"statement"`
	Table       string  `json:"table"`
	Access      string  `json:"access"`
	Index       string  `json:"index,omitempty"`
	HasResidual bool    `json:"has_residual"`
	Limit       *string `json:"limit,omitempty"`
	Offset      *string `json:"offset,omitempty"`
}

func planWire(p sceptre.Plan) *plan {
	out := &plan{Statement: p.Statement, Table: p.Table, Access: string(p.Access), Index: p.Index, HasResidual: p.Residual != nil}
	if p.Limit != nil {
		n := strconv.FormatInt(*p.Limit, 10)
		out.Limit = &n
	}
	if p.Offset != nil {
		n := strconv.FormatInt(*p.Offset, 10)
		out.Offset = &n
	}
	return out
}
