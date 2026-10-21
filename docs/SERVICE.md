# Local Service and Studio

`sceptre serve` exposes one database to local clients over HTTP and serves
Sceptre Studio, a browser workbench for SQL, parameters, schemas, transactions,
and plans. It uses the [public Go API](API.md), so statements, limits, and error
codes match it exactly.

```bash
sceptre serve app.db
sceptre serve app.db --addr 127.0.0.1:7463 --max-rows 1000 --transaction-ttl 1m
```

The service is **local only** in this version. It is not a network database
server: there are no user accounts, TLS, or remote access.

## Security model

- The listener must be a literal loopback address (`127.0.0.1` or `::1`).
- Every API request needs `Authorization: Bearer <token>`. The token comes from
  `SCEPTRE_TOKEN` (at least 32 characters) or is generated and printed at startup.
- The `Host` header must exactly match the listener address, which blocks DNS
  rebinding. Requests with a foreign `Origin` or `Sec-Fetch-Site: cross-site` are
  rejected.
- Studio assets carry a strict Content-Security-Policy, `no-store`, `nosniff`,
  and `no-referrer`. Studio keeps the token in page memory only.

## API

All API routes are `POST` with an `application/json` body of at most 128 KiB:

```json
{"sql": "SELECT * FROM users WHERE id = ?", "params": [{"type": "int64", "value": "42"}]}
```

| Route | Purpose |
|:--|:--|
| `/api/execute` | Run one statement in its own transaction |
| `/api/explain` | Return the planned access path |
| `/api/schema` | List tables, columns, primary keys, and indexes |
| `/api/tx/begin` | Start a transaction owned by this client |
| `/api/tx/execute`, `/api/tx/explain`, `/api/tx/schema` | Same, inside the transaction |
| `/api/tx/commit`, `/api/tx/rollback` | Finish the transaction |

Values never lose precision in JSON. Integers travel as decimal strings
(`{"type":"int64","value":"9223372036854775807"}`) and byte strings as standard
base64 (`{"type":"bytes","value":"SGk="}`). `rows_affected` is also a string.

Every response has the same shape:

```json
{"result": {"columns": [], "rows": [], "rows_affected": "1"},
 "transaction": null, "error": null, "duration_ms": 0.42}
```

`schema` or `plan` replaces `result` on those routes. Errors carry a stable
`code` (the Go API codes plus `forbidden`, `unauthorized`, `method`, `busy`) and
a human-readable `message` that may change.

## Transactions

The service runs one execution owner at a time, like the Go API.

- `/api/tx/begin` needs an `X-Sceptre-Client` header (16 to 128 characters). Later
  transaction requests send the same header plus `X-Sceptre-Transaction` with the
  returned ID. Other clients cannot use the transaction.
- While a transaction is open, other requests receive `409 busy`.
- A transaction expires after `--transaction-ttl` (default 1 minute) and is rolled
  back, so an abandoned client cannot hold the database.
- A failed statement leaves the transaction usable.
- A request that times out (15 seconds) or whose client disconnects rolls back
  its transaction.
- An oversized response (over 8 MiB) returns `413 limit` and keeps the
  transaction open.

As with the Go API, a commit error with code `committed` means the transaction
was applied, and `uncertain` means the outcome is unknown until the database is
reopened.

## Limits

SELECT output is capped by `--max-rows` (default 1000). The cap bounds returned
rows, not engine memory: execution still collects candidates before limiting.
Requests to the engine are serialized; long statements delay other clients.
