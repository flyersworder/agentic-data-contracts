# Expected-value assertions for the verified-examples corpus

**Date:** 2026-08-22
**Status:** approved, pending implementation plan
**Target version:** 0.44.0

## Motivation

The verified-examples corpus validates `question → sql`: it re-checks each
example's SQL against the contract with the same `Validator` that gates live
agent queries, and reports whether the SQL *complies*. It cannot report whether
the SQL is *right*. A query that satisfies every contract rule — allowed tables,
tenant filter present, explicit columns, within cost budget — and still returns
the wrong number passes today with `status: "valid"`.

That is the gap this spec closes. An example may now carry the certified answer
alongside the question and the SQL, and a second pass executes the compliant
examples and compares.

The framing comes from MotherDuck's "AI writes the semantic layer": when the
semantic layer itself becomes cheap to regenerate, the durable asset is the set
of question/answer pairs that say what the system must produce. This library
already holds the governance half of that contract; expected values make the
corpus a behavioural one as well, so a regenerated semantic layer can be checked
against the answers it is required to still produce.

## Scope

**In scope.** Scalar assertions: an example declares one expected number, its
SQL is scalar-shaped, and the checker compares within tolerance.

**Out of scope.** Multi-column single-row assertions and full result-set
comparison. Both were considered and rejected for this version: the row-set case
drags in row ordering, column naming, type coercion, float-vs-decimal, and NULL
equality — a large semantics surface for a case the scalar form already covers
("what was revenue in Q1 2026?"). The design does not foreclose them; `expected`
is typed to a scalar today and a future version may widen it.

**Also out of scope.** Storing, loading, retrieving, or serving examples. The
existing boundary holds unchanged: the framework takes already-parsed records
and returns verdicts. Harvesting certified answers from a live agent session is
a separate, later question.

## Design

### Data model

Three additive optional fields on `VerifiedExample`, so every existing corpus
keeps parsing unchanged:

| Field | Type | Meaning |
|---|---|---|
| `expected` | `float \| None` | The certified answer. `None` means the row is not an assertion. |
| `tolerance` | `Tolerance \| None` | Per-example `rel_tol` / `abs_tol` override. |
| `time_scoped` | `bool` | The author asserts the query's time window is pinned by other means. |

`Tolerance` is a small frozen dataclass with `rel_tol: float` and
`abs_tol: float`, both optional in YAML; an absent field falls back to the
call-level default. `_KNOWN_KEYS` gains all three, so they stop being swept into
`metadata`.

`from_dict` validates the new fields the way it already validates `metadata`:
a non-numeric `expected`, a non-mapping `tolerance`, or a non-boolean
`time_scoped` raises `ValueError` with an actionable message. External YAML is
untrusted; a malformed assertion must fail loudly at parse time rather than
silently degrade to "not an assertion".

### Result types

```python
@dataclass
class ExampleAnswerResult:
    example: VerifiedExample
    status: str                     # "match" | "mismatch" | "unassertable" | "error"
    expected: float | None = None
    actual: float | None = None
    abs_diff: float | None = None
    rel_diff: float | None = None
    rel_tol: float = 1e-9
    abs_tol: float = 0.0
    reason: str | None = None
```

Field order and naming deliberately mirror `ReconciliationResult` so the two
contract-integrity checks read alike.

Statuses, each mutually exclusive:

- `match` — executed and equal within tolerance.
- `mismatch` — executed and outside tolerance. Both numbers and both diffs are
  populated.
- `unassertable` — the SQL uses a relative time window, so the expected value
  decays. **Not executed.**
- `error` — no verdict was possible: the query was not scalar-shaped, returned
  no rows, returned NULL, returned a non-finite value, or the adapter raised.

`ExampleAnswerReport` holds `results: list[ExampleAnswerResult]` with
`matches` / `mismatches` / `unassertable` / `errors` properties, an `ok`
property, and a `summary()` markdown renderer.

### Entry point

```python
def check_example_answers(
    report: ExampleValidationReport,
    *,
    adapter: DatabaseAdapter,
    dialect: str | None = None,
    sql_normalizer: SqlNormalizer | None = None,
    rel_tol: float = 1e-9,
    abs_tol: float = 0.0,
) -> ExampleAnswerReport
```

`dialect` and `sql_normalizer` serve the time-scope scan, which re-parses the
SQL. They must be passed the same values given to `validate_examples`: a corpus
whose SQL only parses after normalisation (Denodo/VDP) reached `valid` through
the normalizer, and re-parsing the raw string without it would fail. Passing
them keeps the two passes looking at the same statement.

It consumes an `ExampleValidationReport`, not raw examples. This is the load-
bearing choice in the design: **an example that failed contract validation must
never be executed.** A row that violates the tenant-filter rule is precisely the
query that must not be run against a warehouse to see what it returns. Taking
the report as input makes that ordering a property of the type signature rather
than a rule in a docstring — there is no way to hand the checker unvalidated
SQL.

Two consequences follow for free:

- `validate_examples` keeps its existing property of never executing a query
  (it plans, via `ExplainAdapter`, and nothing more). The execute-capable
  `DatabaseAdapter` enters only here.
- `status == "valid"` implies `contract_checked=True`, which implies a
  successful sqlglot parse. The second pass therefore has a statement it can
  expect to re-parse for the time-scope scan, provided it is given the same
  `dialect` and `sql_normalizer` (see above). The residual case where it cannot
  is handled as an `error`, never as a silent pass.

### Per-example flow

For each row of `report.results`, in input order:

1. **Filter.** Skip unless `status == "valid"` *and* `example.expected is not
   None`. Skipped rows produce no `ExampleAnswerResult` — they are already
   accounted for by `ExampleValidationReport`.
2. **Time-scope scan.** Parse the SQL and walk the AST for non-deterministic
   time functions: sqlglot's `CurrentDate`, `CurrentTimestamp`, `CurrentTime`,
   and `CurrentDatetime` nodes, which is where the dialect spellings normalise
   to. Verified against sqlglot: `NOW()` (postgres), `GETDATE()` (tsql),
   `SYSDATE` (oracle), and `CURRENT_TIMESTAMP()` (bigquery) all parse to
   `CurrentTimestamp`; `CURRENT_DATE` (duckdb) and `CURRENT_DATE()` (snowflake)
   to `CurrentDate`. `CurrentTime` and `CurrentDatetime` are covered for
   completeness. A hit, with `time_scoped` False, yields `unassertable` and
   **no execution**.

   Should the re-parse fail despite the row being `valid` — a normalizer or
   dialect mismatch between the two passes — the row degrades to `error`. It is
   never executed on an unscanned statement: an unparseable statement cannot be
   cleared of a relative time window, so it does not get to run.
3. **Execute.** `_scalar(adapter, example.sql, label)` measures the result.
4. **Compare.** `math.isclose(actual, expected, rel_tol=..., abs_tol=...)`
   decides `match` or `mismatch`; both diffs are recorded either way.

Effective tolerance is per-example if given, else the call-level argument.

### Tolerance default

`rel_tol=1e-9`, `abs_tol=0.0` — deliberately tighter than
`reconcile_decomposition`'s `1e-4`. The two defaults encode different
assumptions and should not be unified:

- A decomposition identity is *approximate by construction*. Operands carried at
  limited precision leave a real residual, so its default leaves room for one.
- A certified answer is meant to be **the** number. The default tolerates
  floating-point representation and nothing else.

An answer certified from a dashboard that rounds to cents will not match a
full-precision `SUM` at this default. That is intended: the author sets an
explicit per-example tolerance matching the precision the answer actually has,
exactly as `reconcile_decomposition` requires for operand precision. The
mismatch message names the effective tolerance so the fix is visible from the
failure.

### The `ok` gate

`ExampleAnswerReport.ok` is true only when there is at least one asserted
example and every one is `match`. `unassertable` and `error` both fail it.

An **empty** report is not ok, mirroring `ExampleValidationReport.ok`. Calling
the checker on a corpus where nothing declared an `expected` means a filter, a
schema change, or an emptied file silently dropped every assertion; that must
surface as a failure rather than pass a no-op gate. A consumer wanting a laxer
view tests `report.mismatches` directly.

CI composes the two gates rather than merging them, keeping a policy violation
and a wrong number distinguishable:

```python
report = validate_examples(rows, contract, explain_adapter=db)
answers = check_example_answers(report, adapter=db)

if not (report.ok and answers.ok):
    sys.exit(1)
```

### Error handling

The per-example body is wrapped in the same `except Exception` guard
`validate_examples` already uses. `_scalar` raises `ValueError` when the result
is not scalar-shaped; here that degrades to `status: "error"` rather than
aborting, because one malformed row must never kill validation of the rest of
the corpus. `_scalar`'s existing non-raising cases (no rows, NULL, non-finite)
already return a reason string, which becomes the result's `reason` verbatim.

An adapter that raises — a driver error, a permissions failure, a timeout —
degrades that single row to `error` with the exception text.

### Shared `_scalar`

`_scalar` currently lives in `validation/reconciliation.py` and is exercised by
its tests. It moves to `validation/_scalar.py` and is imported by both callers,
so the empty / NULL / non-finite / non-scalar semantics have one implementation.
This is a pure move: no behaviour change, no signature change, and
`reconciliation.py` keeps working through the re-import. The module is private
(leading underscore) and is not exported.

## Files touched

| File | Change |
|---|---|
| `src/agentic_data_contracts/validation/_scalar.py` | New. `_scalar` moved here verbatim. |
| `src/agentic_data_contracts/validation/reconciliation.py` | Import `_scalar` from its new home; delete the local copy. |
| `src/agentic_data_contracts/validation/examples.py` | `Tolerance`; three fields on `VerifiedExample` plus `from_dict` validation; `ExampleAnswerResult`; `ExampleAnswerReport`; `check_example_answers`. |
| `src/agentic_data_contracts/validation/__init__.py` | Export the new public names. |
| `src/agentic_data_contracts/__init__.py` | Export the new public names. |
| `tests/test_validation/test_examples.py` | New cases (below). |
| `tests/test_validation/test_reconciliation.py` | Unchanged behaviour; confirm the import move breaks nothing. |
| `tests/test_public_api.py` | New exports. |
| `README.md` | "Validating a verified-examples corpus" section gains the assertion subsection. |
| `examples/revenue_agent/verified_examples.yml` | An asserted row, a deliberate mismatch, and a relative-window row. |
| `examples/revenue_agent/verify_examples.py` | Run the second pass and print its summary. |
| `CHANGELOG.md` | 0.44.0 entry. |
| `pyproject.toml` | Version 0.44.0. |

## Testing

Tests are written first, from this spec.

**Comparison**
- An assertion matching its expected value → `match`.
- An assertion outside tolerance → `mismatch`, with `expected`, `actual`,
  `abs_diff`, and `rel_diff` all populated.
- A per-example `tolerance` widening the default rescues an answer certified at
  lower precision.
- A per-example `tolerance` is used in preference to the call-level default.

**Time scoping**
- SQL containing `CURRENT_DATE` → `unassertable`, **and the adapter is never
  called** (asserted against a spy adapter, not merely inferred from the
  status — the no-execution guarantee is the security-relevant property).
- The same SQL with `time_scoped: true` executes and compares normally.
- `NOW()` and `CURRENT_TIMESTAMP` are detected alongside `CURRENT_DATE`.
- A row whose SQL fails to re-parse degrades to `error` and is never executed
  (spy adapter).

**Filtering**
- A `violation` row carrying an `expected` is never executed (spy adapter) and
  produces no result.
- An `unverified` row carrying an `expected` is never executed.
- A `valid` row with no `expected` produces no result.

**Errors**
- Non-scalar SQL (two columns) with an `expected` → `error`, batch continues.
- Empty result, SQL NULL, and non-finite value each → `error` carrying
  `_scalar`'s reason.
- An adapter that raises degrades that one row to `error`; the remaining rows
  still get verdicts.

**Report**
- `ok` is true only when every asserted row matched.
- An empty report is not ok.
- `unassertable` and `error` each make `ok` false.
- `summary()` renders `expected vs actual (Δ rel)` per mismatch and labels rows
  `id → question → #index`, matching the existing renderer.

**Parsing**
- Round-trip of the three new YAML fields through `from_dict`.
- A non-numeric `expected`, a non-mapping `tolerance`, and a non-boolean
  `time_scoped` each raise `ValueError`.
- The new keys do not land in `metadata`.
- An existing corpus with none of the new fields parses and behaves unchanged.
