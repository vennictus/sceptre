# Performance

Sceptre is optimized for clarity and inspectability, not for beating SQLite.
The performance goal is to make tradeoffs visible: table scans should look like
table scans, secondary indexes should reduce row work, and copy-on-write commits
should make durability costs explicit.

## Demo Result

Example run:

```bash
go run ./cmd/sceptre demo final-demo.db --rows 100000
```

Observed shape on a WSL run:

```text
without_index  table_scan              100000  248.090315ms
with_index     secondary_index_lookup  1429    14.759564ms
```

Interpretation:

- The table scan checks every row.
- The secondary index narrows candidates to matching `age` entries.
- In this dataset, scanned rows dropped from `100000` to `1429`, roughly a
  70x reduction.
- Query time improved because the executor evaluates far fewer candidate rows.

## Why Writes Are Slower Than Reads

Sceptre uses copy-on-write commits:

1. Mutations update an in-memory B+ tree.
2. A new durable snapshot is assigned pages.
3. New tree/freelist pages are written.
4. The file is synced.
5. A new meta page is published.

This makes recovery behavior easy to reason about, but every durable commit has
real I/O cost. The guided demo uses `InsertMany` and `DeleteMany` so loading
100k rows performs batched durable commits instead of one fsync per row.

## Benchmarks

Run:

```bash
go test -bench=. ./internal/table
```

Typical benchmark categories:

```text
BenchmarkInsertRows
BenchmarkPointLookup
BenchmarkFullScan
BenchmarkSecondaryIndexLookup
```

How to read them:

- `PointLookup` should be much faster than a full scan because it uses the
  primary-key path.
- `SecondaryIndexLookup` should scale with matching index entries, not total
  table rows.
- `FullScan` should scale with total row count.
- Insert benchmarks include durable copy-on-write costs and should not be
  interpreted as append-only throughput.

## Tradeoffs

| Area | Current Choice | Impact |
|:--|:--|:--|
| Commit model | Copy-on-write root publish | Simple recovery, more write amplification |
| Page reuse | Freelist-backed reuse | Deletes/updates can recycle old pages |
| Query execution | Candidate materialization before filtering | Easy stage reporting, less streaming efficiency |
| SQL scope | Small supported subset | Easier correctness, less compatibility |
| Ownership | Single process with sidecar lock | Avoids concurrent writer corruption |

## Future Performance Work

- Stream executor rows instead of materializing all candidates.
- Add deeper page-cache metrics.
- Track logical vs physical I/O in `explain-analyze`.
- Add benchmark comparison for indexed vs non-indexed predicates at multiple
  dataset sizes.
- Add compaction or page defragmentation tooling.
