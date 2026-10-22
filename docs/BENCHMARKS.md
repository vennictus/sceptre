# Durable workload measurements

`cmd/sceptre-bench` runs nine isolated workloads on fresh disposable databases,
verifies integrity and row counts, reopens each database, and verifies again.
Output is JSON on stdout; progress goes to stderr. It never opens an existing
user database. Its own temporary directories are removed after the run.

These workloads use direct internal table/SQL execution. They do not measure the
public Go API's ownership waits or concurrent callers.

```bash
mkdir -p benchmark-results
go run ./cmd/sceptre-bench \
  -rows 100,1000,10000 -payloads 128,2048 \
  -operations 100 -batch 32 -seed 42 \
  -storage "describe filesystem, device, and virtualization" \
  -revision "$(git rev-parse HEAD)" \
  > benchmark-results/baseline.json
```

Use `-dir` to select an existing parent directory on the filesystem being
measured. This parent is preserved; only fresh benchmark subdirectories are
removed. No relaxed durability setting exists. Setup, catalog/index creation,
seeding, verification, and reopening are excluded from operation latency and
physical-write counters. Latency samples shorter than the recorded
`clock_resolution_ns` read as 0; Windows monotonic clocks can step in 0.5 ms or
larger increments, so compare fast operations on a fine-grained clock. Seeding uses batches of up to 1,000 rows. Each workload
starts with `id int64`, `bucket int64`, `payload bytes`, and a bucket index. Initial
bucket values are `id % 64`; operation IDs use the recorded deterministic seed.

## Workloads

| Name | One measured operation |
| --- | --- |
| point_read | Existing primary-key lookup |
| range_scan | Decode up to batch-size rows in a bounded primary-key range |
| secondary_lookup | Exact bucket lookup and row decoding; result size scales with dataset |
| sql_limited_scan | SELECT id WHERE bucket >= 0 LIMIT batch-size; the JSON records the chosen SQL access path |
| single_insert | One new row and index entry, one commit |
| batch_insert | Batch-size new rows and index entries, one commit |
| indexed_update | Replace an existing row and its bucket index entry, one commit |
| indexed_delete_insert | Delete then reinsert one row, two independent commits |
| mixed_80_20 | Every fifth operation updates a row/index; other operations read a primary key |

Read workloads, updates, and delete/reinsert retain the initial row count.
Single inserts grow by operations; batch inserts grow by operations × batch size.
The report includes final row count and initial/final physical file sizes. Batch
and delete/reinsert latency are per operation above, not per individual row.
Mixed latency percentiles describe the mixture; they are not separate read/write
percentiles. Use operation counts divisible by five for an exact 80/20 mixture.

Payloads range from zero through 2,048 bytes. With format v2 and 4096-byte
pages, payloads above about 1,000 bytes are stored in overflow pages, so the
2,048-byte cases exercise them. Inputs are capped at 100,000 initial rows,
100,000 measured operations, and 1,000 batch rows to bound the benchmark tool's
input range. These caps do not imply bounded engine memory or quick large runs.

## Metrics

- p50/p95/p99 use nearest rank over individually timed operations. The mean is
  also recorded. A run with 100 samples has a coarse tail estimate; repeat runs
  before making comparative claims.
- Physical bytes count actual successful WriteAt bytes, including tree, freelist,
  and full metadata pages. Sync calls invoke the real file sync. Each successful
  commit performs a data sync and metadata sync.
- Application mutation bytes count inserted/updated id + bucket + payload
  (16 + payload bytes), and an 8-byte id for deletion. Write amplification divides
  physical bytes by these application bytes. It excludes index and encoding
  overhead in the denominator; it is not device-level amplification.
- Physical bytes per row mutation uses the same run's mutation count.
- Allocation counts and total allocated bytes use Go MemStats deltas. A 1 ms
  sampler records observed peak Go heap; it can miss peaks and adds overhead.
  The ending heap reading is also considered. Heap includes the whole process,
  seed database and measurement instrumentation, not only a query's allocations.
- On Linux, process high-water RSS comes from VmHWM and is cumulative across the
  process, not a resettable per-workload peak. Unsupported platforms omit it.
- CPU, exposed logical cores, GOMAXPROCS, Go version, OS/architecture, page size,
  storage description, revision, seed, payload and operation counts are recorded.
  Timer, correctness checks within an operation, I/O hooks and memory sampling
  affect latency. Results are instrumented baselines, not maximum throughput.

## Recorded baseline: 2026-10-07

Source revision: `e02144d0b478e85a722ddfbe870f0521125108ee`.
Go 1.22.2, linux/amd64, Ubuntu under WSL2, Intel Core Ultra 5 125H,
18 exposed logical CPUs and GOMAXPROCS 18. Databases were under `/tmp` on the
WSL ext4 `/dev/sdd` virtual disk; the underlying host storage device was not
identified. Page size 4096, two real syncs per commit, one caller, warm resident
tree after setup, seed 42, 100 measured operations, batch size 32.

Two passes covered 100/1,000/10,000 rows with 128-byte and 2,048-byte payloads:
54 workload cases, each with integrity and reopen verification. This was a shared
development machine, without CPU/I/O isolation or repeated statistical trials;
other validation work overlapped part of the larger-payload pass. Treat latency
as an initial observation under that environment. Physical write counts expose
the engine's current full-tree rewrite cost independently of those timing limits.

At 10,000 initial rows:

| Workload | Payload bytes | p50 ms | p95 ms | Written MiB over 100 operations | Sampled peak Go heap MiB |
| --- | ---: | ---: | ---: | ---: | ---: |
| point_read | 128 | 0.006 | 0.008 | 0.00 | 3.85 |
| range_scan | 128 | 0.050 | 0.183 | 0.00 | 7.16 |
| secondary_lookup | 128 | 1.130 | 2.142 | 0.00 | 7.78 |
| sql_limited_scan | 128 | 26.710 | 33.630 | 0.00 | 19.71 |
| single_insert | 128 | 22.716 | 27.697 | 329.44 | 28.92 |
| batch_insert | 128 | 28.950 | 34.506 | 379.73 | 34.44 |
| indexed_update | 128 | 20.981 | 25.486 | 328.12 | 29.20 |
| indexed_delete_insert | 128 | 42.852 | 50.222 | 656.25 | 29.01 |
| mixed_80_20 | 128 | 0.011 | 24.469 | 65.62 | 28.96 |
| point_read | 2048 | 0.010 | 0.012 | 0.00 | 42.20 |
| range_scan | 2048 | 0.159 | 0.307 | 0.00 | 79.36 |
| secondary_lookup | 2048 | 1.798 | 2.988 | 0.00 | 83.44 |
| sql_limited_scan | 2048 | 82.448 | 93.597 | 0.00 | 133.74 |
| single_insert | 2048 | 235.170 | 286.321 | 4034.67 | 324.34 |
| batch_insert | 2048 | 250.768 | 391.194 | 4664.56 | 424.39 |
| indexed_update | 2048 | 212.340 | 332.517 | 4015.01 | 334.39 |
| indexed_delete_insert | 2048 | 443.757 | 670.855 | 8029.29 | 334.29 |
| mixed_80_20 | 2048 | 0.021 | 267.266 | 802.96 | 323.21 |

Dataset growth, 128-byte payload:

| Initial rows | Limited SQL scan p50 ms | Indexed update p50 ms | Physical bytes per updated row |
| ---: | ---: | ---: | ---: |
| 100 | 0.135 | 4.213 | 40,960 |
| 1,000 | 1.861 | 8.047 | 331,776 |
| 10,000 | 26.710 | 20.981 | 3,440,640 |

The LIMIT scan returned at most 32 rows but materialized all candidates. An
indexed update at 10,000 rows wrote about 3.28 MiB for a 144-byte application
mutation. Batches amortize publication cost across rows while growing the dataset.
These observations agree with full-tree rewriting and materialized execution;
they do not establish superiority over any other database or production capacity.

The older `go test -bench=. ./internal/table` microbenchmarks remain available.
Their individual-insert setup and growing datasets differ from this fixed-input
runner; do not combine their numbers into a comparison without matching methods.

## Format v2 compared with v1: 2026-10-22

The same runner and parameters (rows 100/1,000/10,000, payloads 128 and 2,048
bytes, 100 operations, batch 32, seed 42) ran twice back to back on the same
machine as the baseline above: first the v2 engine at revision `491b682`, then
the v1 engine at revision `33826e4`. Go 1.22.2 on WSL2, databases under `/tmp`
on ext4, page size 4096, two real syncs per commit, measured clock resolution
39 ns. Each workload case verified integrity and reopened its database. The
runs were not isolated from other activity and are single trials, so treat small
timing differences as noise; physical bytes written do not depend on timing.

At 10,000 initial rows:

| Workload | Payload | v1 p50 ms | v2 p50 ms | v1 MiB written | v2 MiB written | v1 peak heap MiB | v2 peak heap MiB |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| point_read | 128 | 0.007 | 0.008 | 0 | 0 | 3.9 | 5.3 |
| range_scan | 128 | 0.036 | 0.067 | 0 | 0 | 7.1 | 9.6 |
| secondary_lookup | 128 | 1.220 | 1.274 | 0 | 0 | 7.7 | 10.4 |
| sql_limited_scan | 128 | 21.153 | 25.168 | 0 | 0 | 20.1 | 22.9 |
| single_insert | 128 | 23.483 | 4.003 | 329.44 | 3.15 | 30.7 | 9.5 |
| batch_insert | 128 | 31.378 | 7.057 | 379.73 | 16.78 | 35.8 | 12.2 |
| indexed_update | 128 | 22.103 | 4.177 | 328.12 | 3.71 | 27.6 | 8.7 |
| indexed_delete_insert | 128 | 50.049 | 8.422 | 656.25 | 6.24 | 31.9 | 9.1 |
| mixed_80_20 (p95) | 128 | 25.801 | 8.279 | 65.62 | 0.75 | 28.8 | 7.2 |
| point_read | 2048 | 0.009 | 0.018 | 0 | 0 | 42.2 | 43.2 |
| range_scan | 2048 | 0.291 | 0.215 | 0 | 0 | 80.5 | 81.4 |
| secondary_lookup | 2048 | 1.986 | 2.367 | 0 | 0 | 84.9 | 83.4 |
| sql_limited_scan | 2048 | 74.633 | 71.260 | 0 | 0 | 131.9 | 133.6 |
| single_insert | 2048 | 224.541 | 4.566 | 4034.67 | 3.51 | 335.4 | 52.8 |
| batch_insert | 2048 | 281.121 | 9.385 | 4664.56 | 28.44 | 418.8 | 102.8 |
| indexed_update | 2048 | 192.548 | 6.785 | 4015.01 | 4.08 | 323.5 | 54.9 |
| indexed_delete_insert | 2048 | 391.612 | 8.151 | 8029.29 | 6.60 | 332.0 | 60.1 |
| mixed_80_20 (p95) | 2048 | 196.942 | 4.562 | 802.96 | 0.82 | 327.5 | 45.5 |

Bytes written per indexed update, 128-byte payload:

| Initial rows | v1 bytes per updated row | v2 bytes per updated row |
| ---: | ---: | ---: |
| 100 | 40,960 | 24,945 |
| 1,000 | 331,776 | 28,385 |
| 10,000 | 3,440,640 | 38,912 |

What the measurements show:

- **Writes no longer scale with the dataset.** v1 rewrote the whole tree on every
  commit, so bytes per update grew about 84× from 100 to 10,000 rows. In v2 they
  grow about 1.6×, with tree height and freelist size. An update writes two
  root-to-leaf paths (row and index entry share the one data tree), the directory
  leaf, and the freelist, which v2 rewrites in full on each commit.
- **Write latency at 10,000 rows fell 4–6× for small rows and 28–49× for 2 KB
  rows,** where v1 also copied a large in-memory tree.
- **Files stopped growing under steady updates.** At 10,000 rows and 128 bytes,
  v1 files grew from 9.1 MB to 10.3 MB across each write workload; v2 files stayed
  at 4.3 MB, less than half v1's size.
- **Reads are slower in places.** v1 read pages straight from a resident map; v2
  reads through a locked page cache and decodes entries as a cursor moves.
  Primary-key range scans at 128 bytes and point reads of 2 KB values (which now
  span overflow pages) roughly doubled in latency; most other reads are within
  noise. LIMIT scans remain dominated by candidate materialization in the SQL
  executor.

These numbers compare Sceptre with its own earlier engine. They do not establish
performance relative to any other database.
