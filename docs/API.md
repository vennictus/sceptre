# Go application API

Import `github.com/vennictus/sceptre` from a checkout containing this API. The
module requires Go 1.22+. For local HTTP access and Studio, see [the local service](SERVICE.md).

```go
db, err := sceptre.Open("app.db", sceptre.Options{})
if err != nil { return err }
defer db.Close()

ctx := context.Background()
result, err := db.Execute(ctx,
    "select body from posts where id = ?",
    sceptre.Int64(1))
```

`Execute` runs one statement in an automatic transaction. `Tables` returns
schemas and indexes; `Explain` uses the same typed parameter binding and planning
path as execution. `Check` validates the committed database. Returned data is
materialized and does not retain an execution lease. The compiled example in
[`example_test.go`](../example_test.go) creates a schema and performs real writes.

## Multi-statement transactions

```go
tx, err := db.Begin(ctx)
if err != nil { return err }
defer tx.Rollback()

if _, err = tx.Execute(ctx,
    "insert into posts (id, body) values (?, ?)",
    sceptre.Int64(1), sceptre.Bytes([]byte("Hello"))); err != nil {
    return err
}
if _, err = tx.Execute(ctx,
    "insert into revisions (id, post_id, body) values (?, ?, ?)",
    sceptre.Int64(1), sceptre.Int64(1), sceptre.Bytes([]byte("Hello"))); err != nil {
    return err
}
return tx.Commit(ctx)
```

This example assumes both tables already exist. Schema changes, index backfill,
rows, and index changes also participate in the same transaction. Reads, scans,
schema browsing, and planning see its pending changes. Commit publishes all
changes through one KV batch; rollback discards all of them. Empty transactions
do not publish a generation.

A failed statement preserves earlier successful statements and leaves the
transaction usable. A failed statement does not partially stage its own changes.
**Commit ends the transaction on every outcome**, including canceled commit and
definitely aborted storage errors. Start a new transaction if retry is appropriate.
Repeated rollback is safe; execution and repeated commit after completion return
`transaction_closed`.

One transaction at a time owns a DB's execution path, including read-only
transactions. Public methods coordinate concurrent callers. This provides
serializable execution through this API by serialization, rather than concurrent
snapshot isolation. Call `tx.Execute`, `tx.Tables`, and `tx.Explain` while holding
that transaction: calling `db` operations waits for ownership, including ownership
held by the same caller. Always defer rollback after a successful Begin.

`Close` waits for an executing operation, aborts an open transaction, wakes waiting
requests, and releases file ownership. An abandoned transaction otherwise retains
ownership until explicitly finished or the DB closes. There is no automatic expiry.
The raw internal packages still require caller serialization.

## Typed bindings and bounds

Use positional `?` bindings with `sceptre.Int64` or `sceptre.Bytes`. Bindings become
typed literal tokens; their contents are never parsed as SQL text. Quotes, question
marks, NUL, and arbitrary bytes survive unchanged. Question marks inside SQL string
literals do not consume bindings. Bindings are supported in literal positions,
including integer LIMIT/OFFSET, and cannot replace identifiers or SQL syntax.

Missing, extra, or unsupported-type bindings return `parameter`. Schema type
mismatches return `invalid`; a byte binding where the grammar requires an integer,
such as LIMIT, returns `parse`. There is no implicit coercion or NULL support.

`Options.MaxRows` defaults to 1000 and accepts 1 through 100000. SELECT without a
limit receives that limit; a larger explicit limit is capped to it. Smaller limits,
OFFSET, and LIMIT 0 retain their meaning. SQL text is limited to 65536 bytes and a
statement accepts at most 256 bindings. Encoded rows and index cells must still fit
the configured pages. These bounds do not cap candidate materialization, scanning
work, transaction size, or total engine memory.

Context cancellation interrupts waiting for execution ownership. Execute and
Commit check their context before engine work starts. In-flight engine work is
not yet interruptible; a later cancellation cannot establish that a commit aborted.
A canceled Execute leaves an explicit transaction open; canceled Commit aborts
buffered work and releases ownership.

## Stable errors and commit reconciliation

Use `errors.As(err, &executionError)` with `var executionError *sceptre.Error`.
`Error.Code` is a stable category; message details can change. `Unwrap` preserves
`errors.Is`, including context cancellation and the public commit sentinels.

| Code | Meaning |
| --- | --- |
| `parse`, `parameter`, `invalid` | Invalid syntax, bindings, schema/type use, or options; a format v1 file that needs `sceptre upgrade` |
| `constraint`, `not_found` | Duplicate primary key/schema/index, or missing object |
| `limit` | Request bound exceeded, a stored key above the page-size limit (1000 bytes at 4096-byte pages), or a value above 1 GiB |
| `canceled` | Context canceled or deadline expired before work starts |
| `closed`, `transaction_closed` | Invalid database or transaction lifecycle |
| `storage` | Storage/open error, including a page that fails verification; a commit returning this category was definitely aborted |
| `committed` | Complete commit occurred despite a post-publication error |
| `uncertain` | Metadata write/sync outcome is uncertain; close and reopen required |

`ErrCommitCommitted` and `ErrRecoveryRequired` can be checked with `errors.Is`.
Reconcile an uncertain outcome against application keys after reopening before
retrying. Retrying a committed operation as though it aborted can duplicate work.

The automatic `DB.Execute` returns its statement Result alongside a commit error.
RowsAffected describes the executed statement and does not prove durability.
Explicit transaction results likewise describe pending work until Commit succeeds.
See the [engine contract](ENGINE_CONTRACT.md) for sync and recovery boundaries.
