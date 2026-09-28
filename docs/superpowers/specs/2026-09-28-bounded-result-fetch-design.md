# Bounded result fetch — design

Issue: #116. Target release: 0.55.0.

## Problem

`run_query` hands the agent every row a query produces. `DuckDBAdapter.execute`
calls `fetchall()`, and `run_query` serialises all of it to JSON. Nothing bounds
the rows fetched and nothing caps engine memory. A model-written many-to-many
join can produce hundreds of millions of rows; building them as Python objects
exhausts memory, and because DuckDB's conversion holds the GIL it stalls every
other session in the process. `max_query_time_seconds` (v0.54.0) stops such a
query at its deadline, but memory is unbounded until then, and a caller that
sets no limit has no protection.

The DABStep harness shows the same defect twice: `execute_sql` drains the
cursor with `fetchall()` only to count rows for its truncation marker, and
`_truncate_run_query` cuts `run_query`'s JSON to 50 rows after the library has
materialised and serialised everything.

## Goals

- A query tool never materialises more rows than it needs, on any adapter that
  can fetch incrementally.
- Every caller is protected by default; opting out is explicit.
- Result checks give the same verdict they give today.
- An embedded engine can be given a memory ceiling that turns a runaway query
  into an ordinary query error.
- The DABStep harness bounds rows, time and memory identically for all four
  arms.

## Non-goals

- Counting the true total of a truncated result. A `count(*)` re-run costs as
  much as the query on exactly the queries this protects against.
- Bounding `_scalar_value`, verified-example and sensitivity execution. They run
  author-designated SQL that returns a scalar, not agent SQL.
- A contract field for the row cap (see Decisions).

## Design

### 1. Row cap on the query tools

`create_tools(..., max_result_rows: int | None = 1000)`.

- An int ≥ 1 caps the rows `run_query` and `preview_table` return. `None`
  disables the cap. A value < 1 raises `ValueError` at wiring, as
  `validate_row_format` does for `row_format`.
- `run_query`: when the result is truncated, the JSON payload gains
  `"truncated": true` and `row_count` is the number of rows returned. When it
  is not, the payload is byte-identical to today's. The key order stays
  `columns, rows, row_count, [truncated,] session`.
- `preview_table`: its SQL limit becomes `min(requested, 100, max_result_rows)`.
  It asks for at most that many rows, so it is never "truncated".
- The recorder's `row_count` is the number of rows returned.

Every entry point that forwards `row_format` forwards `max_result_rows` with
the same default: `tools/sdk.py` (`create_sdk_mcp_server` path),
`tools/langchain.py`, and both builders in `tools/pydantic_ai.py`.

### 2. Optional adapter capability: `RowLimitAdapter`

In `adapters/base.py`, alongside `TimeoutAdapter`:

```python
@runtime_checkable
class RowLimitAdapter(Protocol):
    def execute_limited(
        self, sql: str, max_rows: int, timeout_seconds: float | None = None
    ) -> QueryResult: ...
```

Contract for implementers:

- Fetch at most `max_rows + 1` rows from the database and return at most
  `max_rows`, with `truncated=True` when the extra row existed.
- `timeout_seconds` is passed only when the adapter also implements
  `TimeoutAdapter`; it must then be honoured exactly as `execute_with_timeout`
  honours it (raise `QueryTimeoutError`, cancel in the database). Otherwise
  the tools pass `None` and apply their caller-side deadline around the call.
- `DatabaseAdapter` is unchanged; the capability is detected once, at wiring.

`QueryResult` gains `truncated: bool = False`, appended last so existing
positional construction keeps binding as before.

Dispatch in `_execute_bounded`, given a fetch limit `n` (see section 3) or `None` when uncapped:

| adapter | `n` is None | `n` is set |
|---|---|---|
| `RowLimitAdapter` | today's path | `execute_limited(sql, n, timeout)`, timeout routed as above |
| otherwise | today's path | today's path, then slice to `n` and set `truncated` |

The second row of the right column bounds what the agent sees but not memory.
The first time a given adapter class takes it, log one warning naming the
class and saying that implementing `execute_limited` bounds memory — the same
once-per-key pattern as `_warn_uncancellable`.

### 3. Result checks stay exact

The tool fetches `n = max(max_result_rows, T) + 1` rows, where `T` is the
largest `min_rows` or `max_rows` across the contract's `result_check` rules
(0 when there are none), then trims to `max_result_rows` only when building the
response. Result checks run on all fetched rows.

- Truncation means more than `T` rows exist and the fetch saw `T + 1` of them,
  so `min_rows` passes and `max_rows` fails exactly as they would on the full
  result.
- `min_value`, `max_value` and `not_null` run on a superset of the rows the
  agent sees. A violation among the fetched rows blocks, as today. A violation
  only in rows never fetched is also never shown to the agent.
- A contract that declares a very large `max_rows` threshold raises the fetch
  accordingly. That is the author's declared intent, and the docs say so.

`T` is computed once at wiring from the contract, not per query.

### 4. DuckDB implementation

- `DuckDBAdapter.execute_limited` runs the statement under the lock and calls
  `fetchmany(max_rows + 1)`. It does not call `fetchall()` on the rest; the
  result is closed when the connection is reused.
- The interrupt watchdog is factored out of `execute_with_timeout` into one
  private helper that both methods use, so the lock, the re-interrupt loop and
  the `_caused_by_interrupt` mapping to `QueryTimeoutError` stay in one place.
- `execute_with_timeout` still routes through `self.execute`, so subclass
  overrides of `execute` keep applying there. `execute_limited` does not call
  `execute`; its docstring says a subclass that rewrites SQL in `execute` must
  override `execute_limited` too.

### 5. DuckDB memory ceiling

`DuckDBAdapter(database=":memory:", *, memory_limit: str | None = None)`.

- When set, the constructor runs `SET memory_limit = ?`. `None` leaves DuckDB's
  default (80% of RAM).
- It uses `SET` after `connect`, not `duckdb.connect(config=...)`: two
  in-process connections to the same file with different `config` raise, which
  is the crash the harness's module docstring describes.
- A query that exceeds it raises DuckDB's out-of-memory error, which reaches
  the agent as a query error (v0.54.0 already passes engine errors through
  unchanged). The connection stays usable.
- `memory_limit` bounds the engine, not Python objects built from its results;
  the row cap bounds those. The docstring says both are needed.

## DABStep harness

The panel under way runs from a pinned commit, so the harness changes here
apply only to future runs.

- `HARNESS_QUERY_SECONDS = 120` and `HARNESS_MEMORY_LIMIT = "512MB"` in
  `arms.py`, next to `MAX_ROWS`, each with a comment giving its reason: 512 MB
  per worker's database leaves headroom for four workers under the 4 GiB
  container.
- Ungoverned arms (`schema_only`, `manual_prompt`): `execute_sql` builds a
  `DuckDBAdapter(db_path, memory_limit=HARNESS_MEMORY_LIMIT)` per call and
  calls `execute_limited(sql, MAX_ROWS, HARNESS_QUERY_SECONDS)`. No cursor
  drain.
- Governed arms (`contract`, `contract_hollow`): `create_tools(...,
  max_result_rows=MAX_ROWS)` over a `DuckDBAdapter` subclass, with the same
  `memory_limit`, whose `execute_limited` substitutes `HARNESS_QUERY_SECONDS`
  when given `None`. The frozen contracts declare no `resources` block, and
  adding one would change their published digests.
- `_truncate_run_query` stops truncating: it appends the marker when the
  payload says `"truncated": true`, and wraps only `run_query`. `preview_table`
  is bounded by `max_result_rows`.
- Every arm's marker becomes `-- truncated at 50 rows (more rows not shown)`.
- `experiments/dabstep-contract-eval/tests/test_arms.py` updates its marker
  assertions; completed-run documentation (FINDINGS, paper) describes runs
  that used the old marker and is not edited.

## Decisions

- **Row cap on `create_tools`, not in the contract.** It governs how results
  are delivered, like `row_format`, not what the agent may do. It also lets the
  harness bound the governed arms without editing the frozen contracts.
- **Default 1000.** Safe by default is the point of the issue. 1000 rows is
  already past what a model can use (the harness measured 10,000 rows at about
  429k tokens), truncation is labelled, and `None` opts out. It changes
  behaviour, so the CHANGELOG entry says so.
- **No true total.** Reported as `truncated`, not as a count.
- **`memory_limit` defaults to `None`.** The right value depends on the
  deployment, and DuckDB already caps itself at 80% of RAM.

## Testing (written first)

Library (`tests/test_adapters/`, `tests/test_tools/`):

- `execute_limited` on a 40M-row cross join returns `max_rows` rows,
  `truncated=True`, within a small time budget and a small RSS delta.
- `execute_limited` with `timeout_seconds` raises `QueryTimeoutError` on a
  slow statement, and the adapter serves the next query.
- `memory_limit`: a sort over a large generated table raises an out-of-memory
  error, and the next query on the same adapter succeeds.
- `run_query` default: a result over 1000 rows returns 1000 with
  `"truncated": true`; a small result's payload is byte-identical to today's.
- `max_result_rows=None` returns every row.
- `max_result_rows=0` raises at wiring.
- Result checks: with `max_result_rows=5`, a `min_rows: 20` rule passes on a
  100-row result and a `max_rows: 20` rule blocks it.
- Fallback: an adapter without `execute_limited` still gets a sliced result,
  and the warning is logged once per adapter class.
- `preview_table` asks for at most `max_result_rows` rows.

Harness (`experiments/dabstep-contract-eval/tests/`):

- All arms emit the same marker for the same over-cap query.
- `execute_sql` returns within the timeout on a runaway query and reports it
  as an error.

## Docs

README, `docs/architecture.md` and CHANGELOG (0.55.0): `max_result_rows`,
`RowLimitAdapter`, `QueryResult.truncated`, `DuckDBAdapter(memory_limit=)`,
and the behaviour change. Close #116 from the PR.
