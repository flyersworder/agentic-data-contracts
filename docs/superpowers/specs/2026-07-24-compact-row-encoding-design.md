# Compact row encoding for `run_query` and `preview_table`

**Date:** 2026-07-24
**Issue:** [#44](https://github.com/flyersworder/agentic-data-contracts/issues/44)
**Target version:** 0.31.0

## Problem

`run_query` and `preview_table` render result rows as one JSON object per row,
repeating every column name on every row:

```python
# factory.py:788 — run_query
rows = [dict(zip(qresult.columns, row)) for row in qresult.rows]
data = {"columns": qresult.columns, "rows": rows, ...}

# factory.py:400 — preview_table
rows = [dict(zip(result.columns, row)) for row in result.rows]
body = json.dumps({"schema": schema, "table": table, "rows": rows}, default=str)
```

`run_query` already carries a `columns` key, so its header is transmitted twice:
once in `columns`, then again as the key of every row. `preview_table` has no
`columns` key at all, so there the names repeat without ever being stated
canonically.

Tool results persist in the message history and are re-sent on every subsequent
model request, so an oversized result is paid for repeatedly rather than once.

Measured on synthetic results with realistic short column names, comparing the
serialized character count of the current rendering against the alternatives:

| shape | dict-per-row | arrays-of-arrays | TSV |
|---|---|---|---|
| 20 cols x 500 rows | 198,003 (~49.5k tok) | 84,503 (43%) | 66,747 (34%) |
| 10 cols x 200 rows | 36,783 | 16,583 (45%) | 12,833 (35%) |
| 5 cols x 50 rows | 3,920 | 2,120 (54%) | 1,577 (40%) |

The saving costs no information: the column names are already available, or can
be made available, in a single `columns` key.

## Decisions

### D1 — A better default, not a model-facing lever

Anthropic's `concise` / `detailed` tool-response pattern exists because concise
*drops information*, and the agent is the party who knows whether it needs the
dropped fields. This change drops nothing — arrays-of-arrays carries information
identical to dict-per-row. The model therefore has no basis on which to choose,
and a field in `run_query`'s `input_schema` would cost input tokens on every
request while inviting the agent to select the expensive branch for no reason.

The party with a legitimate opinion is the operator wiring up `create_tools()`.
The knob lives there, and the compact rendering becomes the default so that
users get the saving without opting in.

### D2 — JSON arrays-of-arrays, not TSV

TSV is roughly 9 percentage points smaller but requires hand-written escaping
for tab, newline, and backslash, and collapses `NULL` into the empty string
unless a sentinel value is invented. In a library whose purpose is that an
agent's reported numbers are trustworthy, a rendering that cannot distinguish
"no value" from "empty value" is disqualifying. Arrays-of-arrays stays valid
JSON: no escaping code, no delimiter-injection class of bug, and `null` remains
unambiguous.

### D3 — `columns` in both modes on `preview_table`

Both tools emit the same `{columns, rows}` envelope regardless of `row_format`,
so a consumer writes one parser. Adding `columns` to `records` mode is purely
additive. It also closes a real gap: a zero-row preview today returns
`{"rows": []}` and tells the agent nothing about the table's shape.

### D4 — Describe the encoding to the model

A positional misread fails silently. One clause in the tool description costs
~15 tokens once per request against ~57% saved per result, so the insurance is
cheap relative to the risk.

### D5 — Forward `row_format` through the ecosystem wrappers

An escape hatch reachable only by hand-building a `ToolDef` list is not much of
an escape hatch. All four wrapper call sites accept and forward the parameter.

## Design

### Public API

```python
RowFormat = Literal["compact", "records"]

def create_tools(
    contract: DataContract,
    *,
    adapter: DatabaseAdapter | None = None,
    semantic_source: SemanticSource | None = None,
    session: ContractSession | None = None,
    caller_principal: Principal = None,
    staleness_threshold_days: int = 90,
    row_format: RowFormat = "compact",
) -> list[ToolDef]: ...
```

Keyword-only and last, so the addition is source-compatible with every existing
call. The value is validated eagerly inside `create_tools`; an unrecognised
string raises `ValueError` naming both valid options.

Eager validation is deliberate. Tool callables are async and their failures
surface as agent-visible text, so a deferred check would turn a wiring typo into
a confusing mid-session tool error rather than an immediate startup failure.

### Response envelopes

`run_query` — `columns` is already present; only the `rows` rendering changes:

```json
{"columns": ["region", "units", "note"],
 "rows": <rendered>,
 "row_count": 3,
 "session": {"remaining": {"elapsed_seconds": 4.2}}}
```

`preview_table` — gains `columns`, placed before `rows`:

```json
{"schema": "sales", "table": "orders",
 "columns": ["region", "units", "note"],
 "rows": <rendered>}
```

`rows` renders as one of:

```json
"compact": [["EMEA", 412, null], ["APAC", 87, "re-run\tpending"]]
"records": [{"region": "EMEA", "units": 412, "note": null},
            {"region": "APAC", "units": 87, "note": "re-run\tpending"}]
```

`records` reproduces today's rendering exactly. `json.dumps(..., default=str)`
is unchanged, so `Decimal` and `date` coerce identically in both modes — the
values are byte-identical and only the container differs.

Key order is load-bearing: `json.dumps` preserves dict insertion order, so
placing `columns` before `rows` guarantees the model reads the header before the
values it must align to. The header also travels with the data in a single tool
result, so rows from one call can never be aligned against columns from another.

The `WARNINGS:` / `LOG:` preambles both tools prepend are untouched, as is every
`BLOCKED —` early return.

### Shared helper

```python
def _render_rows(
    columns: Sequence[str],
    rows: Iterable[Sequence[Any]],
    row_format: RowFormat,
) -> list[Any]:
    """Render result rows as positional arrays or as one dict per row."""
```

Placed in `factory.py` beside `_text_response`. Pure, no I/O, called by both
tools. Keeping it separate makes the branch unit-testable without a database
adapter, rather than reachable only through two 40-line async functions.

### Tool descriptions

In `compact` mode, both descriptions gain one clause:

- `run_query`: "Validate and execute a SQL query, returning the results. Rows
  are arrays of values positionally aligned to `columns`."
- `preview_table`: "Preview sample rows from an allowed table. Rows are arrays
  of values positionally aligned to `columns`."

In `records` mode both keep today's wording verbatim.

### Wrapper forwarding

Four call sites accept and forward `row_format: RowFormat = "compact"`:

| function | file |
|---|---|
| `create_langchain_tools` | `tools/langchain.py:79` |
| `create_sdk_mcp_server` | `tools/sdk.py:76` |
| `create_pydantic_ai_tools` | `tools/pydantic_ai.py:81` |
| `create_pydantic_ai_toolset` | `tools/pydantic_ai.py:194` |

For `create_pydantic_ai_toolset` the parameter is captured by the closure and
applied inside `_factory`, alongside the existing per-run session and principal
threading. Each wrapper's existing `tools=` escape hatch keeps precedence: when
a caller supplies a pre-built list, `row_format` is not consulted.

## Error handling

No new failure modes. The only new raise is the eager `ValueError` on an
unrecognised `row_format`. Rendering cannot fail for inputs that render today:
`_render_rows` performs no type inspection, and serialization keeps the existing
`default=str` fallback. Empty result sets produce `"rows": []` in both modes,
now accompanied by a populated `columns` list.

## Testing

Test-driven, red first, under `tests/test_tools/`.

**`_render_rows` unit tests** — both modes on the same fixture; empty row list;
`None` values preserved as `null` and distinct from `""`; a value containing a
tab and a newline, documenting that the delimiter bug class does not exist here;
`Decimal` and `date` coerced identically in both modes via `default=str`.

**`run_query`** — compact yields lists and records yields dicts; both agree
column-for-column on one fixture; the `WARNINGS:` / `LOG:` preamble still
precedes the JSON body in compact mode.

**`preview_table`** — `columns` present in both modes; a zero-row preview still
carries `columns`.

**Configuration** — `create_tools(row_format="bogus")` raises `ValueError`; each
of the four wrappers forwards the parameter; a wrapper given an explicit `tools=`
list ignores `row_format`.

**Existing test to update** — `tests/test_tools/test_factory_principals.py:128`
asserts `body["rows"][0]["salary"]`. Rewritten as
`body["rows"][0][body["columns"].index("salary")]` so it continues to test
principal-based masking under the new default. Line 176's
`isinstance(body["rows"], list)` passes unchanged.

## Documentation and release

- `README.md` — tools table entries for `run_query` / `preview_table`, plus a
  short "Result encoding" note covering the envelope and `row_format`.
- `docs/architecture.md` — the tool inventory section (~L357, ~L364).
- `CHANGELOG.md` — a `0.31.0` entry with `Added`, `Changed`, and `Compatibility`
  sections. The `Compatibility` paragraph must state plainly that the default
  output shape changed and that `row_format="records"` restores it.
- `pyproject.toml` — version `0.30.0` -> `0.31.0`.

## Non-goals

- **No row cap or truncation.** `run_query` continues to return every row the
  query produced. The `require_limit` checker and `result_check.max_rows` rule
  already exist as opt-in contract rules, and blocking is the correct governance
  behaviour where truncating would let an agent report confidently on partial
  data.
- **No third format.** TSV and markdown table are rejected per D2.
- **No model-facing tool argument.** Rejected per D1.
- **No wrapper refactor.** `create_langchain_tools` and `create_sdk_mcp_server`
  still do not accept `caller_principal`; that pre-existing asymmetry is out of
  scope.
