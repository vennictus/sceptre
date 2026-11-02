# SQL support matrix

What Sceptre's SQL accepts today, and what it does not. Every row here has
tests. Keywords are case insensitive; identifiers match by spelling. One
statement per call.

## Types

| Type | Also spelled | Values | Literals | Order |
| --- | --- | --- | --- | --- |
| `int64` | `int`, `integer`, `bigint` | signed 64-bit | `42`, `-7` | numeric |
| `float64` | `float`, `double`, `real` | finite IEEE 754 doubles; NaN and ±Inf rejected; −0 stored as 0 | `1.5`, `-2.5e3`; an integer literal also fits | numeric |
| `bool` | `boolean` | true, false | `TRUE`, `FALSE` | false < true |
| `text` | `string`, `varchar` | UTF-8 (validated) | `'it''s'` | byte order |
| `bytes` | `blob` | any bytes | `'...'` | byte order |
| `timestamp` | | UTC, microsecond precision | `'2026-11-02T10:00:00Z'`, `'2026-11-02 10:00:00'`, `'2026-11-02'` (no zone means UTC) | time |
| NULL | | the absence of a value | `NULL` | |

Go bindings: `sceptre.Int64`, `Float64`, `Bool`, `Text`, `Bytes`, `Timestamp`,
`Null`. A literal takes the type of the column it is stored in or compared
with; there is no other implicit conversion. A `bytes` binding fits a `text`
column when it is valid UTF-8. Tables created before `text` existed keep their
`text` columns as `bytes`.

### CAST

`CAST(expr AS type)` converts explicitly, in WHERE and CHECK expressions.
Conversions that would lose meaning fail instead of guessing:

| From \ to | int64 | float64 | bool | text | bytes | timestamp |
| --- | --- | --- | --- | --- | --- | --- |
| int64 | | exact | 0 or 1 only | decimal | | microseconds since epoch |
| float64 | whole and in range only | | | shortest form | | |
| bool | 1 / 0 | | | `true` / `false` | | |
| text | parses | parses (finite) | `true` / `false` | | bytes | parses |
| bytes | | | | valid UTF-8 only | | |
| timestamp | microseconds | | | RFC 3339, UTC | | |

NULL casts to NULL. A string literal casts as text.

## NULL

Columns accept NULL unless declared `NOT NULL` or in the primary key. A
comparison with NULL is unknown; WHERE keeps only rows whose predicate is
true; AND and OR use three-valued logic. Use `IS NULL` / `IS NOT NULL`. A
CHECK that is unknown passes. Unique indexes never treat two NULLs as equal.

## Statements

| Statement | Supported | Not supported |
| --- | --- | --- |
| `CREATE TABLE [IF NOT EXISTS]` | columns with `NOT NULL`, `NULL`, `UNIQUE`, `DEFAULT literal`, `DEFAULT CURRENT_TIMESTAMP`, `CHECK (expr)`, `PRIMARY KEY`, `AUTOINCREMENT`; table-level `PRIMARY KEY (cols)` and `CHECK (expr)` | foreign keys, generated columns, `CREATE TABLE AS` |
| `CREATE [UNIQUE] INDEX [IF NOT EXISTS] name ON table (cols)` | multi-column; built in the creating commit | partial and expression indexes, `DESC` |
| `DROP TABLE [IF EXISTS]` | removes rows, indexes and its AUTOINCREMENT sequence | `CASCADE` |
| `DROP INDEX [IF EXISTS] name [ON table]` | `ON` needed when several tables have the name | |
| `ALTER TABLE ... ADD [COLUMN]` | nullable, or `NOT NULL DEFAULT ...`; existing rows read the default (for `CURRENT_TIMESTAMP`, the time of the ALTER); `UNIQUE` | adding constraints to existing columns |
| `ALTER TABLE ... DROP [COLUMN]` | rewrites every row | key columns, indexed columns, columns a CHECK uses |
| `ALTER TABLE ... RENAME [COLUMN] a TO b` / `RENAME TO name` | catalog only; CHECKs follow the rename | |
| `INSERT INTO t [(cols)] VALUES (...), (...)` | many rows; omitted columns get DEFAULT, AUTOINCREMENT, or NULL | expressions as values (literals and bindings only) |
| `INSERT INTO t [(cols)] SELECT ...` | columns matched by position; int64 widens to float64 | |
| `... ON CONFLICT [(cols)] DO NOTHING` | without columns, any key or unique conflict | |
| `... ON CONFLICT (cols) DO UPDATE SET c = literal \| excluded.c` | target is the primary key or a unique index; rows of one statement see each other | `WHERE` on the update |
| `... RETURNING * \| cols` | on INSERT (with filled defaults and ids), UPDATE (new rows), DELETE (removed rows) | expressions |
| `SELECT cols \| * FROM t [WHERE] [LIMIT n] [OFFSET n]` | | joins, aggregates, `ORDER BY`, `DISTINCT`, subqueries |
| `UPDATE t SET c = literal, ... [WHERE]` | | expressions as values, key columns |
| `DELETE FROM t [WHERE]` | | |
| `BEGIN`, `COMMIT`, `ROLLBACK` | through a `Session` | |
| `SAVEPOINT`, `RELEASE`, `ROLLBACK TO` | in a transaction | |

WHERE supports `=`, `!=`, `<>`, `<`, `<=`, `>`, `>=`, `AND`, `OR`,
parentheses, `IS [NOT] NULL` and `CAST`. There is no `NOT`, arithmetic,
`LIKE`, `IN` or functions yet (Phase 4).

## Constraints

| Constraint | Enforced on |
| --- | --- |
| Primary key | INSERT (duplicates fail with `constraint`) |
| `NOT NULL` | INSERT, UPDATE |
| `UNIQUE` | INSERT, UPDATE (against the statement's final state), index builds over existing rows, `sceptre check` |
| `CHECK` | INSERT, UPDATE and ON CONFLICT DO UPDATE through SQL; violations fail with `constraint` |
| `DEFAULT` | INSERT omitting the column; rows written before an ADD COLUMN |
| `AUTOINCREMENT` | INSERT omitting the column: one past the largest value the table has seen, never reused |

## Atomicity

Through the Go API, a Session and the service, each statement is atomic: a
failed statement leaves none of its writes behind, and in a transaction the
transaction stays usable. Each schema change is one commit.

## Index builds

`CREATE INDEX` scans the table and writes every entry in its transaction.
Readers keep running on snapshots during the build; writers wait for it, as
for any write. Builds that let writers continue need concurrent writers
(roadmap Phase 8).
