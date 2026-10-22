package main

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/vennictus/sceptre"
	"github.com/vennictus/sceptre/internal/service"
)

type serveOptions struct {
	path, addr string
	rows       int
	ttl        time.Duration
}

func parseServe(args []string) (serveOptions, error) {
	var opts serveOptions
	if len(args) == 0 || args[0] == "" {
		return opts, errors.New("database path required")
	}
	opts.path = args[0]
	flags := flag.NewFlagSet("serve", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	flags.StringVar(&opts.addr, "addr", "127.0.0.1:7463", "loopback listener")
	flags.IntVar(&opts.rows, "max-rows", 1000, "maximum SELECT output rows")
	flags.DurationVar(&opts.ttl, "transaction-ttl", time.Minute, "transaction lifetime")
	if err := flags.Parse(args[1:]); err != nil {
		return opts, err
	}
	if flags.NArg() != 0 || opts.rows < 1 || opts.rows > 100000 || opts.ttl < time.Second || opts.ttl > 10*time.Minute {
		return opts, errors.New("max-rows must be 1..100000 and transaction-ttl 1s..10m; unexpected arguments rejected")
	}
	host, _, err := net.SplitHostPort(opts.addr)
	if err != nil {
		return opts, err
	}
	ip := net.ParseIP(host)
	if ip == nil || !ip.IsLoopback() {
		return opts, errors.New("serve requires a literal loopback address (127.0.0.1 or ::1)")
	}
	return opts, nil
}
func runServe(args []string, stdout, stderr io.Writer) int {
	opts, err := parseServe(args)
	if err != nil {
		fmt.Fprintln(stderr, "sceptre serve:", err)
		return 2
	}
	token := os.Getenv("SCEPTRE_TOKEN")
	generated := false
	if token == "" {
		var bytes [32]byte
		if _, err = rand.Read(bytes[:]); err != nil {
			fmt.Fprintln(stderr, err)
			return 1
		}
		token = base64.RawURLEncoding.EncodeToString(bytes[:])
		generated = true
	}
	if len(token) < 32 {
		fmt.Fprintln(stderr, "SCEPTRE_TOKEN must have at least 32 characters")
		return 2
	}
	listener, err := net.Listen("tcp", opts.addr)
	if err != nil {
		fmt.Fprintln(stderr, err)
		return 1
	}
	defer listener.Close()
	db, err := sceptre.Open(opts.path, sceptre.Options{MaxRows: opts.rows})
	if err != nil {
		fmt.Fprintln(stderr, err)
		return 1
	}
	defer db.Close()
	handler, err := service.New(db, service.Options{Host: listener.Addr().String(), Token: token, TransactionTTL: opts.ttl})
	if err != nil {
		fmt.Fprintln(stderr, err)
		return 1
	}
	defer handler.Close()
	server := &http.Server{Handler: handler, ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 15 * time.Second, WriteTimeout: 30 * time.Second, IdleTimeout: 30 * time.Second, MaxHeaderBytes: 16 << 10}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	completed := make(chan struct{})
	defer close(completed)
	go func() {
		select {
		case <-ctx.Done():
			shutdown, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			server.Shutdown(shutdown)
		case <-completed:
		}
	}()
	fmt.Fprintf(stdout, "Sceptre Studio: http://%s\n", listener.Addr())
	if generated {
		fmt.Fprintf(stdout, "Access token (this process only): %s\n", token)
	} else {
		fmt.Fprintln(stdout, "Access token: supplied through SCEPTRE_TOKEN")
	}
	fmt.Fprintf(stdout, "SELECT output cap: %d rows; transaction lifetime: %s\n", opts.rows, opts.ttl)
	if err := server.Serve(listener); err != nil && !errors.Is(err, http.ErrServerClosed) {
		fmt.Fprintln(stderr, err)
		return 1
	}
	return 0
}
