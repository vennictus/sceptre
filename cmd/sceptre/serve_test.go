package main

import (
	"bytes"
	"strings"
	"testing"
	"time"
)

func TestParseServeDefaults(t *testing.T) {
	opts, err := parseServe([]string{"app.db"})
	if err != nil {
		t.Fatal(err)
	}
	if opts.path != "app.db" || opts.addr != "127.0.0.1:7463" || opts.rows != 1000 || opts.ttl != time.Minute {
		t.Fatalf("defaults = %+v", opts)
	}
	opts, err = parseServe([]string{"app.db", "--addr", "[::1]:9000", "--max-rows", "50", "--transaction-ttl", "30s"})
	if err != nil || opts.addr != "[::1]:9000" || opts.rows != 50 || opts.ttl != 30*time.Second {
		t.Fatalf("flags = %+v, %v", opts, err)
	}
}

func TestParseServeRejectsUnsafeOrInvalidInput(t *testing.T) {
	for name, args := range map[string][]string{
		"no path":        {},
		"empty path":     {""},
		"public address": {"app.db", "--addr", "0.0.0.0:7463"},
		"hostname":       {"app.db", "--addr", "localhost:7463"},
		"lan address":    {"app.db", "--addr", "192.168.1.5:7463"},
		"missing port":   {"app.db", "--addr", "127.0.0.1"},
		"zero rows":      {"app.db", "--max-rows", "0"},
		"too many rows":  {"app.db", "--max-rows", "100001"},
		"short ttl":      {"app.db", "--transaction-ttl", "10ms"},
		"long ttl":       {"app.db", "--transaction-ttl", "1h"},
		"extra argument": {"app.db", "other.db"},
		"unknown flag":   {"app.db", "--public"},
	} {
		if _, err := parseServe(args); err == nil {
			t.Errorf("%s: accepted %v", name, args)
		}
	}
}

func TestRunServeRejectsShortToken(t *testing.T) {
	t.Setenv("SCEPTRE_TOKEN", "too-short")
	var stdout, stderr bytes.Buffer
	if code := run([]string{"serve", t.TempDir() + "/app.db", "--addr", "127.0.0.1:0"}, &stdout, &stderr); code != 2 {
		t.Fatalf("exit = %d, stderr %q", code, stderr.String())
	}
	if !strings.Contains(stderr.String(), "SCEPTRE_TOKEN") {
		t.Fatalf("stderr = %q", stderr.String())
	}
}
