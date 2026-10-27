// Package service exposes the public Sceptre API to local clients and Studio.
package service

import (
	"context"
	"crypto/rand"
	"crypto/subtle"
	"embed"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"mime"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/vennictus/sceptre"
)

//go:embed studio/*
var assets embed.FS

// Options configures a Handler. Host is the exact Host header accepted, which
// blocks DNS rebinding. Zero durations and sizes select the defaults.
type Options struct {
	Host             string
	Token            string
	TransactionTTL   time.Duration // default 1m
	RequestTimeout   time.Duration // default 15s
	MaxResponseBytes int           // default 8 MiB
}
type session struct {
	id, owner string
	tx        *sceptre.Tx
	expires   time.Time
}
type transaction struct {
	ID        string    `json:"id"`
	ExpiresAt time.Time `json:"expires_at"`
}
type failure struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}
type response struct {
	Result      *result      `json:"result,omitempty"`
	Schema      []schema     `json:"schema,omitempty"`
	Plan        *plan        `json:"plan,omitempty"`
	Transaction *transaction `json:"transaction"`
	Error       *failure     `json:"error,omitempty"`
	DurationMS  float64      `json:"duration_ms"`
}

// Handler owns service sessions, but not DB lifetime. Close rolls sessions back.
type Handler struct {
	db      *sceptre.DB
	opts    Options
	gate    chan struct{}
	done    chan struct{}
	once    sync.Once
	workers sync.WaitGroup
	active  *session
}

func New(db *sceptre.DB, opts Options) (*Handler, error) {
	if db == nil || opts.Host == "" || len(opts.Token) < 32 {
		return nil, errors.New("service: database, exact host, and token of at least 32 characters required")
	}
	if opts.TransactionTTL == 0 {
		opts.TransactionTTL = time.Minute
	}
	if opts.RequestTimeout == 0 {
		opts.RequestTimeout = 15 * time.Second
	}
	if opts.MaxResponseBytes == 0 {
		opts.MaxResponseBytes = 8 << 20
	}
	if opts.TransactionTTL < time.Millisecond || opts.RequestTimeout < time.Millisecond {
		return nil, errors.New("service: timeouts must be positive")
	}
	if opts.MaxResponseBytes < 1024 {
		return nil, errors.New("service: response limit must be at least 1024 bytes")
	}
	h := &Handler{db: db, opts: opts, gate: make(chan struct{}, 1), done: make(chan struct{})}
	h.gate <- struct{}{}
	h.workers.Add(1)
	go h.reap()
	return h, nil
}
func (h *Handler) Close() {
	h.once.Do(func() { close(h.done); <-h.gate; h.rollback(); h.gate <- struct{}{}; h.workers.Wait() })
}
func (h *Handler) rollback() {
	if h.active != nil {
		h.active.tx.Rollback()
		h.active = nil
	}
}
func (h *Handler) reap() {
	defer h.workers.Done()
	interval := h.opts.TransactionTTL / 4
	if interval > time.Second {
		interval = time.Second
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-h.done:
			return
		case <-ticker.C:
			select {
			case <-h.gate:
				if h.active != nil && !time.Now().Before(h.active.expires) {
					h.rollback()
				}
				h.gate <- struct{}{}
			default:
			}
		}
	}
}
func same(a, b string) bool { return subtle.ConstantTimeCompare([]byte(a), []byte(b)) == 1 }
func (h *Handler) sessionFor(r *http.Request) *transaction {
	if h.active == nil || !same(h.active.owner, r.Header.Get("X-Sceptre-Client")) || !same(h.active.id, r.Header.Get("X-Sceptre-Transaction")) {
		return nil
	}
	return &transaction{ID: h.active.id, ExpiresAt: h.active.expires}
}
func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.Header().Set("Referrer-Policy", "no-referrer")
	w.Header().Set("Content-Security-Policy", "default-src 'none'; script-src 'self'; style-src 'self'; connect-src 'self'; img-src 'self'; base-uri 'none'; frame-ancestors 'none'; form-action 'self'")
	if r.Host != h.opts.Host || (r.Header.Get("Origin") != "" && r.Header.Get("Origin") != "http://"+h.opts.Host) || r.Header.Get("Sec-Fetch-Site") == "cross-site" {
		h.write(w, http.StatusForbidden, response{Error: &failure{"forbidden", "host or origin rejected"}})
		return
	}
	if !strings.HasPrefix(r.URL.Path, "/api/") {
		h.static(w, r)
		return
	}
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", "POST")
		h.write(w, 405, response{Error: &failure{"method", "POST required"}})
		return
	}
	if !same(r.Header.Get("Authorization"), "Bearer "+h.opts.Token) {
		h.write(w, 401, response{Error: &failure{"unauthorized", "valid bearer token required"}})
		return
	}
	media, _, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
	if err != nil || media != "application/json" {
		h.write(w, 415, response{Error: &failure{"invalid", "application/json required"}})
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), h.opts.RequestTimeout)
	defer cancel()
	// Schema, explain, and query requests outside a transaction read a
	// snapshot: they skip the request gate and are never refused as busy.
	var pre *query
	var preValues []sceptre.Value
	if path := r.URL.Path; path == "/api/schema" || path == "/api/explain" || path == "/api/execute" {
		q, values, status, fail := h.decodeQuery(w, r)
		if fail != nil {
			h.write(w, status, response{Error: fail})
			return
		}
		if path != "/api/execute" || sceptre.IsQuery(q.SQL, values...) {
			h.serveRead(ctx, w, path, q, values)
			return
		}
		pre, preValues = q, values // a write: continue under the gate
	}
	select {
	case <-h.done:
		h.write(w, 503, response{Error: &failure{"closed", "service closed"}})
		return
	case <-ctx.Done():
		h.write(w, 408, response{Error: &failure{"canceled", ctx.Err().Error()}})
		return
	case <-h.gate:
	}
	defer func() { h.gate <- struct{}{} }()
	defer func() {
		if ctx.Err() != nil && h.sessionFor(r) != nil {
			h.rollback()
		}
	}()
	select {
	case <-h.done:
		h.write(w, 503, response{Error: &failure{"closed", "service closed"}})
		return
	default:
	}
	if err := ctx.Err(); err != nil {
		h.write(w, 408, response{Error: &failure{"canceled", err.Error()}})
		return
	}
	start := time.Now()
	if h.active != nil && !start.Before(h.active.expires) {
		h.rollback()
	}
	q, values := pre, preValues
	if q == nil {
		var status int
		var fail *failure
		if q, values, status, fail = h.decodeQuery(w, r); fail != nil {
			h.write(w, status, response{Error: fail, Transaction: h.sessionFor(r)})
			return
		}
	}
	path := r.URL.Path
	withTx := strings.HasPrefix(path, "/api/tx/") && path != "/api/tx/begin"
	if withTx {
		if h.sessionFor(r) == nil {
			h.write(w, 409, response{Error: &failure{"transaction_closed", "transaction missing, expired, or owned by another client"}})
			return
		}
	} else if h.active != nil {
		h.write(w, 409, response{Error: &failure{"busy", "another transaction owns execution; finish it or wait for expiry"}})
		return
	}
	if (path == "/api/tx/begin" || withTx) && (len(r.Header.Get("X-Sceptre-Client")) < 16 || len(r.Header.Get("X-Sceptre-Client")) > 128) {
		h.write(w, 400, response{Error: &failure{"invalid", "client identity must have 16 through 128 characters"}})
		return
	}
	out := response{}
	switch path {
	case "/api/execute", "/api/tx/execute":
		var res sceptre.Result
		if withTx {
			res, err = h.active.tx.Execute(ctx, q.SQL, values...)
		} else {
			res, err = h.db.Execute(ctx, q.SQL, values...)
		}
		if err == nil {
			out.Result = resultWire(res)
		}
	case "/api/explain", "/api/tx/explain":
		var p sceptre.Plan
		if withTx {
			p, err = h.active.tx.Explain(ctx, q.SQL, values...)
		} else {
			p, err = h.db.Explain(ctx, q.SQL, values...)
		}
		if err == nil {
			out.Plan = planWire(p)
		}
	case "/api/schema", "/api/tx/schema":
		var defs []sceptre.TableDef
		if withTx {
			defs, err = h.active.tx.Tables(ctx)
		} else {
			defs, err = h.db.Tables(ctx)
		}
		if err == nil {
			out.Schema = schemas(defs)
		}
	case "/api/tx/begin":
		// Generate the capability before taking database ownership.
		var bytes [32]byte
		if _, err = rand.Read(bytes[:]); err == nil {
			var t *sceptre.Tx
			t, err = h.db.Begin(ctx)
			if err == nil {
				h.active = &session{id: base64.RawURLEncoding.EncodeToString(bytes[:]), owner: r.Header.Get("X-Sceptre-Client"), tx: t, expires: time.Now().Add(h.opts.TransactionTTL)}
				r.Header.Set("X-Sceptre-Transaction", h.active.id)
			}
		}
	case "/api/tx/commit":
		err = h.active.tx.Commit(ctx)
		h.active = nil
	case "/api/tx/rollback":
		h.rollback()
	default:
		h.write(w, 404, response{Error: &failure{"not_found", "unknown API route"}, Transaction: h.sessionFor(r)})
		return
	}
	if ctx.Err() != nil && h.active != nil && h.sessionFor(r) != nil {
		h.rollback()
		if err == nil {
			err = ctx.Err()
		}
	}
	out.Transaction = h.sessionFor(r)
	out.DurationMS = float64(time.Since(start).Microseconds()) / 1000
	status := 200
	if err != nil {
		out.Error, status = errorWire(err)
	}
	if !h.write(w, status, out) && h.sessionFor(r) != nil {
		h.rollback()
	}
}

// decodeQuery reads exactly one bounded JSON query and its bindings. On
// failure it returns the HTTP status and error to report. It touches no
// transaction state, so it is safe without the request gate.
func (h *Handler) decodeQuery(w http.ResponseWriter, r *http.Request) (*query, []sceptre.Value, int, *failure) {
	var q *query
	r.Body = http.MaxBytesReader(w, r.Body, 128<<10)
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	err := decoder.Decode(&q)
	if err == nil && q == nil {
		err = errors.New("null request")
	}
	if err == nil && decoder.Decode(new(any)) != io.EOF {
		err = errors.New("exactly one JSON object required")
	}
	if err != nil {
		var size *http.MaxBytesError
		if errors.As(err, &size) {
			return nil, nil, 413, &failure{"limit", "invalid or oversized JSON request"}
		}
		return nil, nil, 400, &failure{"invalid", "invalid or oversized JSON request"}
	}
	values, err := q.bindings()
	if err != nil {
		return nil, nil, 400, &failure{"parameter", err.Error()}
	}
	return q, values, 0, nil
}

// serveRead answers a schema, explain, or query request from a snapshot. It
// takes neither the request gate nor transaction state, so a read never waits
// for, or is refused because of, another client's open transaction.
func (h *Handler) serveRead(ctx context.Context, w http.ResponseWriter, path string, q *query, values []sceptre.Value) {
	select {
	case <-h.done:
		h.write(w, 503, response{Error: &failure{"closed", "service closed"}})
		return
	default:
	}
	start := time.Now()
	out := response{}
	var err error
	switch path {
	case "/api/execute":
		var res sceptre.Result
		if res, err = h.db.Execute(ctx, q.SQL, values...); err == nil {
			out.Result = resultWire(res)
		}
	case "/api/explain":
		var p sceptre.Plan
		if p, err = h.db.Explain(ctx, q.SQL, values...); err == nil {
			out.Plan = planWire(p)
		}
	case "/api/schema":
		var defs []sceptre.TableDef
		if defs, err = h.db.Tables(ctx); err == nil {
			out.Schema = schemas(defs)
		}
	}
	out.DurationMS = float64(time.Since(start).Microseconds()) / 1000
	status := 200
	if err != nil {
		out.Error, status = errorWire(err)
	}
	h.write(w, status, out)
}
func errorWire(err error) (*failure, int) {
	code := sceptre.CodeStorage
	var e *sceptre.Error
	if errors.As(err, &e) {
		code = e.Code
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		code = sceptre.CodeCanceled
	}
	status := 400
	switch code {
	case sceptre.CodeStorage, sceptre.CodeCommitted:
		status = 500
	case sceptre.CodeUncertain, sceptre.CodeClosed:
		status = 503
	case sceptre.CodeConstraint, sceptre.CodeTransactionClosed:
		status = 409
	case sceptre.CodeNotFound:
		status = 404
	case sceptre.CodeCanceled:
		status = 408
	case sceptre.CodeLimit:
		status = 413
	}
	return &failure{string(code), err.Error()}, status
}

// write reports whether the client received the response. An oversized
// result is replaced by a limit error but keeps the transaction open: only a
// client that cannot be reached loses its transaction.
func (h *Handler) write(w http.ResponseWriter, status int, out response) bool {
	data, err := json.Marshal(out)
	if err != nil || len(data) > h.opts.MaxResponseBytes {
		status = http.StatusRequestEntityTooLarge
		data, _ = json.Marshal(response{
			Error:       &failure{"limit", "response exceeds the service size limit; use a smaller LIMIT or projection"},
			Transaction: out.Transaction,
			DurationMS:  out.DurationMS,
		})
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_, err = w.Write(data)
	return err == nil
}
func (h *Handler) static(w http.ResponseWriter, r *http.Request) {
	if r.Method != "GET" && r.Method != "HEAD" {
		w.Header().Set("Allow", "GET, HEAD")
		http.Error(w, "method not allowed", 405)
		return
	}
	name := ""
	typ := ""
	switch r.URL.Path {
	case "/":
		name = "index.html"
		typ = "text/html; charset=utf-8"
	case "/studio.js":
		name = "studio.js"
		typ = "text/javascript; charset=utf-8"
	case "/studio.css":
		name = "studio.css"
		typ = "text/css; charset=utf-8"
	default:
		http.NotFound(w, r)
		return
	}
	data, err := assets.ReadFile("studio/" + name)
	if err != nil {
		http.Error(w, "asset unavailable", 500)
		return
	}
	w.Header().Set("Content-Type", typ)
	if r.Method == "GET" {
		w.Write(data)
	}
}
