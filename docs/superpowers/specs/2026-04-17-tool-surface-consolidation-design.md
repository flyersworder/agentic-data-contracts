# Tool Surface Consolidation

**Date:** 2026-04-17
**Status:** Approved (pending user review of this written spec)
**Target version:** 0.11.0

## Problem

The agent tool factory (`src/agentic_data_contracts/tools/factory.py`) exposes 13 tools. Two concerns motivate reducing this surface:

1. **Agent tool-selection confusion.** Several tools cluster around the same input — most sharply, `validate_query`, `query_cost_estimate`, and `run_query` all take a `sql` string and an agent has to pick which. The validate/cost pair is especially ambiguous because both are read-only SQL checks.
2. **Prompt bloat.** Every tool's description and input schema is rendered into the agent's system prompt. Tools that are fully redundant with the data the `ClaudePromptRenderer` already injects pay token cost for no capability gain.

The `ClaudePromptRenderer` (`src/agentic_data_contracts/core/prompt.py`) already injects: the full allowed-tables list, domains with name/summary/metric-count, metrics with name/description (when ≤ 20), relationships (when ≤ 30), resource limits, and rules. Tools covering that same ground are pure overhead.

## Non-goals

- No changes to `Validator`, `ContractSession`, `DatabaseAdapter`, `SemanticSource`, or `PromptRenderer` protocols.
- No backward-compatibility shims. Pre-1.0 library, clean break.
- No new capabilities. Every dropped tool's capability is either redundant with the prompt or absorbed into a kept tool.

## Final tool set (9 tools)

| Category | Tool | Purpose |
|---|---|---|
| Discovery | `describe_table` | Column details from DB |
| Discovery | `preview_table` | Sample rows |
| Semantic | `list_metrics` | Browse metrics, filtered by domain/tier/indicator_kind |
| Semantic | `lookup_metric` | Full metric definition (SQL, tier, 1-hop impacts) |
| Semantic | `lookup_domain` | Full domain description with metrics and tables |
| Semantic | `lookup_relationships` | Direct joins + multi-hop path finding |
| Semantic | `trace_metric_impacts` | BFS walk of metric-impact graph |
| Query lifecycle | `inspect_query` | Read-only SQL inspection (validation + cost) |
| Query lifecycle | `run_query` | Validate and execute |

## Dropped tools (5)

| Tool | Reason |
|---|---|
| `list_schemas` | Fully redundant — schemas are implicit in the allowed-tables list the prompt already injects. |
| `list_tables` | Fully redundant — the prompt injects all allowed tables. Column enrichment was the only extra feature and `describe_table` covers it per-table. |
| `get_contract_info` | Fully redundant except for `session_remaining`, which is absorbed into `run_query` responses. |
| `validate_query` | Merged into `inspect_query`. |
| `query_cost_estimate` | Merged into `inspect_query`. |

## New / modified tools

### `inspect_query` (new, replaces `validate_query` + `query_cost_estimate`)

**Rationale:** both merged tools share a single underlying call — `validator.validate()` already runs Layer 1 (sqlglot static checks) plus Layer 2 (EXPLAIN), which produces the cost estimate. The two old tools were thin wrappers that surfaced different slices of the same result. Agents currently face a "which one?" decision that the merge removes.

**Input schema:**
```json
{
  "type": "object",
  "properties": {
    "sql": {"type": "string", "description": "SQL query to inspect"}
  },
  "required": ["sql"]
}
```

**Response shape** (JSON, inside the standard `{"content": [{"type": "text", "text": ...}]}` wrapper):
```json
{
  "valid": true,
  "violations": [],
  "warnings": [],
  "estimated_cost_usd": 0.0012,
  "estimated_rows": 15000,
  "schema_valid": true,
  "explain_errors": [],
  "pending_result_checks": ["row_limit_check"]
}
```

Field semantics:
- `valid`: inverse of `validation_result.blocked`.
- `violations`, `warnings`: from `validation_result.reasons` and `validation_result.warnings`.
- `estimated_cost_usd`, `estimated_rows`, `schema_valid`, `explain_errors`: from the EXPLAIN layer (may be `null` / empty if no adapter is configured).
- `pending_result_checks`: from `validator.pending_result_check_names()`.

No-adapter case: the validator still runs Layer 1; cost/row/schema fields are null, `explain_errors` is empty. Response is still well-formed.

**Description (tool-facing):**
> Inspect a SQL query without executing it. Returns validation result (violations and warnings from contract rules), estimated cost and row count from EXPLAIN, schema validity, and a list of result checks that would run after execution. Use this to iterate on SQL before spending retry budget on `run_query`.

### `run_query` (modified)

**Logic:** unchanged. Still: session limit check → `validator.validate()` (blocks or records cost) → `adapter.execute()` → `validator.validate_results()` → return rows.

**Response addition:** on successful execution, include a `session` block alongside `columns`, `rows`, `row_count`:
```json
{
  "columns": [...],
  "rows": [...],
  "row_count": 42,
  "session": {
    "remaining": {
      "cost_usd": 4.88,
      "retries": 3,
      "rows_scanned": null,
      "queries": null
    }
  }
}
```

Fields mirror what `ContractSession.remaining()` returns. Keys with no limit configured are `null`. This removes the agent's need to call a dedicated status tool when it wants to gauge remaining budget.

**Blocked-response addition:** when `run_query` returns a BLOCKED text response (validation failure, limit exceeded, execution error), append a short `Remaining budget: ...` line derived from `session.remaining()` so the agent can reason about whether to retry.

## Unchanged tools

The following tools keep current behavior, signatures, and descriptions:
- `describe_table`
- `preview_table`
- `list_metrics`
- `lookup_metric`
- `lookup_domain`
- `lookup_relationships`
- `trace_metric_impacts`

## Prompt renderer interaction

`ClaudePromptRenderer` references tool names in its hint strings at:
- `prompt.py:100-101` — `lookup_domain`, `lookup_metric`
- `prompt.py:124-125` — `list_metrics`, `lookup_metric`
- `prompt.py:131-132` — `lookup_metric`
- `prompt.py:146-147` — generic semantic-source hint
- `prompt.py:173-175` — `lookup_relationships`

All referenced tools are kept. **No prompt renderer changes required.**

## File changes

| File | Change |
|---|---|
| `src/agentic_data_contracts/tools/factory.py` | Remove `list_schemas`, `list_tables`, `get_contract_info`, `validate_query`, `query_cost_estimate`. Add `inspect_query`. Modify `run_query` to include `session.remaining` in responses. |
| `src/agentic_data_contracts/tools/sdk.py` | Update module docstring ("13 contract tools" → "9 contract tools"). |
| `src/agentic_data_contracts/tools/__init__.py` | Update any re-exports if present. |
| `tests/test_tools/` | Delete tests for 5 dropped tools. Add `test_inspect_query.py`. Extend `test_run_query.py` to assert `session.remaining` shape. |
| `CHANGELOG.md` | 0.11.0 entry: breaking change, list dropped tools and rename of validate/cost into `inspect_query`. |
| `README.md` | Update any tool lists or examples that name dropped tools. |
| `docs/architecture.md` | Update tool count and list if mentioned. |

## Testing approach

1. **Remove tests** for dropped tools. Each is currently self-contained under `tests/test_tools/` — deletion is safe.
2. **Add `test_inspect_query.py`** covering:
   - Valid SQL with adapter → all fields populated, `valid: true`.
   - Blocked SQL (e.g., query touches a forbidden table) → `valid: false`, violations surfaced.
   - Warning-only SQL → `valid: true`, warnings surfaced.
   - SQL that fails EXPLAIN (bad column) → `schema_valid: false`, `explain_errors` populated, `valid` still reflects contract-rule outcome.
   - No-adapter case → Layer 1 runs, cost/row/schema fields null, no crash.
   - Pending result checks → surfaced in response.
3. **Extend `test_run_query.py`** to assert `session.remaining` structure on success and that blocked responses include remaining-budget text.
4. Full suite: `uv run pytest -v`. Nothing outside `tests/test_tools/` should need changes; if it does, that's a signal the change is leaking beyond scope.
5. Lint and type-check: `uv run ruff check src/ tests/`, `uv run ruff format src/ tests/`, `ty check`.

## Risks and mitigations

| Risk | Mitigation |
|---|---|
| External consumers depend on dropped tool names. | Pre-1.0 library (v0.10.0). Breaking change is called out in CHANGELOG and version bump. |
| `inspect_query` response shape diverges from what downstream tooling expects. | Response is newly defined; no prior contract to break. Document shape in tool description and in an example test. |
| Agents trained on the old tool surface underperform. | Out of scope for this spec — would be addressed by example updates. The new surface is strictly smaller and more orthogonal, which should net-help agent performance. |
| `session.remaining` in `run_query` responses bloats successful-query payloads. | The block is small (~4 fields, all scalar). Net effect dwarfed by row data. |

## Success criteria

- Tool count in `create_tools()` return value is exactly 9.
- `inspect_query` passes all new tests with and without a database adapter.
- `run_query` responses (both success and blocked) include remaining budget information.
- Full test suite passes.
- `ruff check`, `ruff format --check`, and `ty check` are clean.
- CHANGELOG and README reflect the new tool surface.
