# Verified-Examples Contract Validation — Design

**Date:** 2026-07-19
**Status:** Design approved, pending implementation plan

## Goal

Let an **external** verified-examples database (a corpus of human-reviewed
`question → SQL` pairs that grounds an analytics agent) be validated against a
`DataContract` using the framework's existing validation engine. The framework
contributes exactly one verb — *validate* — and never stores, loads, retrieves,
or serves the examples.

## Boundary — why the store lives outside

The framework's semantic layer (metrics, relationships, decompositions) is
**curated and governed**: every entry has an owner, a `last_reviewed` date, a
review cadence, and static validation. A lessons-learned / verified-examples
corpus has the opposite lifecycle:

| | Framework semantic layer | Verified-examples corpus |
|---|---|---|
| Origin | authored by a steward | accumulated from agent sessions |
| Growth | deliberate, reviewed | continuous, emergent |
| Trust comes from | ownership + review cadence | human MR/PR review of merged lessons |
| Home | the contract | the agent's repo + memory |

Putting a continuously-accumulating, session-derived store inside a governance
library would fork the source of truth — especially since the consuming project
(the analytics agent, built on this framework) **already** harvests lessons from
real sessions and consolidates them through a human-gated MR/PR. That MR flow is
a *better* trust model than anything the framework would invent, so it stays.

What the framework uniquely owns is **the contract and the validator**. A human
reviewer eyeballing SQL cannot reliably catch that a lesson quietly violates the
current contract (a dropped table, a newly-required filter, a forbidden op that
crept in). Re-checking SQL against a live contract is precisely the framework's
existing capability (sqlglot static analysis + optional EXPLAIN dry-run). That —
and only that — is what this feature exposes for examples.

### Scope — what enters this path

The consuming project's lessons are a **mix**: some are concrete `question → SQL`
pairs (the SQL / data-model type), others are freeform prose guidance. Only a
lesson that **resolves to one complete, parseable query** enters this path:

- A lesson carrying an exemplar query maps that query to `VerifiedExample.sql`
  and validates like any other example.
- A guidance-only lesson (*"always join orders to customers via `customer_id`"*,
  *"filter `is_deleted = false` on revenue"*) references SQL but is not an
  executable statement — there is nothing to parse. That is a *rule*, not an
  *example*, and belongs elsewhere (potentially the contract's `SemanticRule`
  layer), not here.
- **Witness-query pattern (caller's choice):** a guidance-style SQL/data-model
  lesson can be made validatable by attaching a canonical query that embodies it
  (the `is_deleted` lesson → `SELECT … FROM revenue WHERE is_deleted = false
  LIMIT 1`). Validating that witness is then a proxy for "does this lesson still
  hold under the contract?" This is a formatting decision on the caller's side;
  it just means every validatable lesson ultimately reduces to an example.

The framework never inspects or interprets a lesson taxonomy — the caller filters
to SQL-bearing lessons and maps each to a `VerifiedExample` before calling.

## What validation asserts — and what it does not

The strongest verdict this feature produces is:

> **parses (or the engine plans it) + complies with the contract + the live
> engine can still plan it against the current schema.**

It never **executes** the query — no rows are read, nothing is materialized, no
side effects. `EXPLAIN` makes the engine *plan* the SQL, not run it. Therefore
this feature **cannot confirm the query returns the right answer**. Result
correctness still rests entirely on the human MR gate; the framework re-checks
that an example is still *allowed and well-formed*, not that it is still *right*.
Result-level verification (execute, snapshot, diff against a golden value) is a
categorically heavier capability (cost, side effects, stored expected results),
out of scope here, and by the same boundary logic belongs in the agent's system.

## Architecture

`validate_examples` builds one `Validator` (the same class used for live agent
queries) and runs `Validator.validate(example.sql)` per example. Results are
identical to what the agent would see at run time, because examples ride the same
enforcement code — with one deliberate, examples-only extension (decision B
below) for engines sqlglot cannot parse.

Two-layer validation, inherited from live validation:

- **Layer 1 (static):** sqlglot parse → checkers (allowed tables, forbidden ops,
  required filters, joins/relationships), after the caller's optional
  `SqlNormalizer` runs. No database connection — a CI job with no warehouse
  credentials still gets a real gate.
- **Layer 2 (dry run):** `EXPLAIN` against the real engine via the caller's
  `ExplainAdapter`. The engine *plans* the SQL against the **current schema** and
  reports whether it is schema-valid, plus a cost/row estimate.

### Why the dry run matters more here than for live queries

Examples **age**. Layer 1 catches *contract-policy* drift (a table pulled from
the allowlist, a filter now required). It has no view of the live schema, so it
cannot catch *schema* drift — a column renamed, a table dropped, a type changed
in the warehouse while the contract did not move. **The dry run is the only layer
that checks the SQL against the real current schema.** For a corpus meant to stay
"verified" as the world changes underneath it, the dry run is arguably the
*primary* check, not a nice-to-have.

### Order of gates in `Validator.validate` (as of validator.py)

1. sqlglot parse — on failure, `validate()` returns early
   (`ValidationResult(blocked=True, parse_error=True, …)`); Layer 2 never runs.
2. static checkers.
3. Layer 2 runs only when `not reasons and explain_adapter is not None` — i.e.
   the query parsed **and** has no static violation. A statically-failing query
   is not dry-run (no point planning a query that is already blocked).

### Decision B — engine fallback on sqlglot parse failure (examples-only)

sqlglot does not model every engine (the consumer runs a custom Denodo/VDP
adapter). Under the gates above, a VDP-specific query sqlglot cannot parse never
reaches `EXPLAIN`, even though the engine — the authoritative parser — could plan
it. For examples, that gives up too early.

**`validate_examples` therefore extends the path:** on a parse failure
(`ValidationResult.parse_error`), if an `explain_adapter` is present, it calls
`explain_adapter.explain(sql)` itself and lets the engine render the verdict.
This is additive — it never re-implements the checkers; it just declines to give
up before asking the authoritative engine. The fallback lives entirely in
`examples.py`; core `validate()` is unchanged apart from the `parse_error` flag.

**Honest consequence:** the static checkers require the sqlglot AST. When sqlglot
cannot parse, there is no AST, so *contract-policy* checks cannot run — the engine
fallback confirms **plannability against the live schema**, not **contract
compliance**. A fallback pass is therefore a weaker guarantee than a full-path
pass, and the report records that difference (see `contract_checked` below) rather
than reporting a flat `valid`. In practice this bucket is small: sqlglot parses
ordinary SELECT/JOIN/GROUP BY without a dedicated dialect; VDP-specific parse
failures are the tail. B rescues the tail without weakening verification for the
common case.

### Engine agnosticism

The feature adds **zero** new engine surface. It exposes the same three engine
knobs the live `Validator` already has, threaded straight through:

- `dialect` → `sqlglot.parse_one(sql, dialect=...)`
- `sql_normalizer` → runs before the parse (how VDP/Denodo-style syntax is
  reconciled to something sqlglot can parse)
- `explain_adapter` → Layer 2 (and the decision-B fallback), entirely the
  caller's engine

**Guarantee:** whatever `(dialect, normalizer, adapter)` combination validates a
project's live queries also validates its stored examples identically. For the
current consumer, the existing custom Denodo (VDP) adapter and normalizer apply
to examples with no additional work. "Does it work with engine X?" reduces to
"does your live validation work with engine X?" — already answered yes in
production.

## Components

### 1. Interchange shape — `VerifiedExample`

A plain dataclass. **No file IO and no format assumption** — the caller parses
their own YAML (or any store) and maps each row to this shape. Only `sql` is
load-bearing.

```python
@dataclass
class VerifiedExample:
    sql: str                                   # the only validated field
    question: str = ""                         # NL question — for the report, not checked
    id: str | None = None                      # stable key from the caller's DB
    principal: str | None = None               # identity this example was recorded for (see §3)
    metadata: dict[str, Any] = field(default_factory=dict)  # round-tripped, never interpreted

    @classmethod
    def from_dict(cls, raw: dict[str, Any]) -> VerifiedExample: ...
```

- `from_dict` is a **shape adapter**, not a loader: it maps an already-parsed
  dict and silently ignores keys it does not recognize. The caller's YAML may
  carry arbitrary fields (`verified_by`, `last_verified`, `metrics_used`, source
  MR, a `type` taxonomy); the framework preserves them in `metadata` and never
  acts on them.
- Report identity falls back `id → question → positional index` when `id` is
  absent.

Example of a caller-owned file (the framework never reads this — shown only to
fix the mapping contract):

```yaml
# examples-repo/examples/wau.yaml  — owned by the consuming project
- id: wau-by-region
  question: "weekly active users by region"
  sql: "SELECT region, COUNT(DISTINCT user_id) FROM ... "
  type: sql              # caller taxonomy; non-sql types filtered out by the caller
  verified_by: jsmith
  last_verified: 2026-07-10
```

### 2. The verb — `validate_examples`

```python
def validate_examples(
    examples: Iterable[VerifiedExample],
    contract: DataContract,
    *,
    dialect: str | None = None,
    sql_normalizer: SqlNormalizer | None = None,
    explain_adapter: ExplainAdapter | None = None,
    semantic_source: SemanticSource | None = None,
) -> ExampleValidationReport: ...
```

Builds a `Validator` and validates each example's SQL through it. Layer 1 always
runs; Layer 2 runs when `explain_adapter` is provided; the decision-B fallback
runs when a parse fails and `explain_adapter` is present. `semantic_source`
enables relationship checks, mirroring live validation.

### 3. Principal handling

Some contract rules are per-caller (`required_filter_values`, principal-scoped
allow/block lists). An example recorded for analyst *A* must be validated **as
A**, or a per-principal rule would be checked against the wrong identity.

`validate_examples` groups the batch by `example.principal` and constructs one
`Validator` per distinct principal via the public `caller_principal` constructor
argument — correct and cheap (few principals per corpus). `principal=None`
validates under default resolution, exactly like an anonymous live query. A
corpus that uses no principal-scoped rules can ignore the field entirely.

### 4. The report

```python
@dataclass
class ExampleResult:
    example: VerifiedExample
    status: str              # "valid" | "violation" | "unchecked"
    reasons: list[str]       # contract violations and/or engine EXPLAIN errors
    warnings: list[str]      # warn-level rule hits + "compliance not statically verified" caveat
    contract_checked: bool   # static Layer-1 checkers ran (needs a successful sqlglot parse)
    engine_checked: bool     # Layer-2 EXPLAIN ran (direct or via decision-B fallback)

@dataclass
class ExampleValidationReport:
    results: list[ExampleResult]

    @property
    def valid(self) -> list[ExampleResult]: ...
    @property
    def violations(self) -> list[ExampleResult]: ...
    @property
    def unchecked(self) -> list[ExampleResult]: ...
    @property
    def unverified_compliance(self) -> list[ExampleResult]: ...  # status valid, contract_checked False
    @property
    def ok(self) -> bool: ...           # no violations (unchecked handled by caller policy)
    def summary(self) -> str: ...       # markdown, ready to post as an MR comment
```

Status truth table:

| Situation | status | contract_checked | engine_checked |
|---|---|---|---|
| parses, static clean, engine plans (or no adapter) | `valid` | ✓ | ✓ / — |
| parses, static violation | `violation` | ✓ | — |
| parses, static clean, engine **rejects** (schema drift) | `violation` | ✓ | ✓ |
| sqlglot fails, engine plans (decision B) | `valid` | ✗ | ✓ |
| sqlglot fails, engine **rejects** (decision B) | `violation` | ✗ | ✓ |
| sqlglot fails, no adapter | `unchecked` | ✗ | ✗ |

- **valid** — passed every layer that ran. When `contract_checked is False` (the
  decision-B pass), a caveat warning is added — *"contract policy not statically
  verified; engine confirmed plannability only"* — and the row appears in
  `unverified_compliance` so a sweep can route it to a human.
- **violation** — a layer rejected it: a static contract check, or an engine
  `EXPLAIN` rejection (schema drift / type error). `reasons` name the cause.
- **unchecked** — no layer could render a verdict (sqlglot failed to parse and no
  `explain_adapter` was available). Caller decides whether this fails the gate.
- `warnings` carry warn-enforcement rule hits regardless of status and never flip
  `ok`, matching the framework's block/warn split.

### 5. Required framework change — `ValidationResult.parse_error`

Add one field to `ValidationResult`:

```python
@dataclass
class ValidationResult:
    ...
    parse_error: bool = False   # NEW — set True at the parse-error early return
```

Set `parse_error=True` at the parse-error return in `Validator.validate`
(validator.py:266). Backward-compatible (defaults `False`); it lets live callers
distinguish a parse failure from a policy block, and it is the signal
`validate_examples` reads to trigger the decision-B engine fallback and to assign
`status`. This is the *only* change to core validation.

## Usage modes — two tiers, two triggers

**Tier 1 — fast static gate (every MR, no DB credentials):** Layer 1 only. Catches
parse errors and contract-policy violations. Cheap, no warehouse access.

```python
report = validate_examples(load_my_yaml(), contract)   # caller writes load_my_yaml
if not report.ok:
    print(report.summary())     # post as an MR comment
    sys.exit(1)                 # fail before the human reviews
```

**Tier 2 — dry-run sweep (on contract change / nightly, with DB access):** pass
the `explain_adapter` so Layer 2 (and the decision-B fallback) run. Additionally
catches schema drift and, for VDP-specific SQL, gets an authoritative verdict.

```python
report = validate_examples(
    load_my_yaml(), new_contract, explain_adapter=denodo_adapter, sql_normalizer=vdp_normalizer
)
for r in report.violations:              # examples the change (or schema drift) just broke
    ...
for r in report.unverified_compliance:   # plannable but policy unverified — route to a human
    ...
```

The dry-run sweep is the analogue of `find_stale_reviews` (metric staleness)
applied to stored SQL. Callers may cheaply pre-filter by stamping
`metadata["contract_digest"]` in their store and re-validating only examples
whose stored digest differs from the contract's current `contract_digest`; the
authoritative answer is always the re-run.

Multi-contract corpora: an example belongs to one contract. The caller groups by
contract and calls once per contract.

## Placement

- New module: `src/agentic_data_contracts/validation/examples.py`
- Exports added to `src/agentic_data_contracts/validation/__init__.py`:
  `VerifiedExample`, `ExampleResult`, `ExampleValidationReport`,
  `validate_examples`.
- One-line edit to `validation/validator.py` for `ValidationResult.parse_error`.
- No changes to the tools layer, prompt renderer, or semantic layer.

This mirrors the precedent set by `validation/reconciliation.py` (PR #39): a
focused validation helper module with its own exports and test suite.

## Explicitly out of scope (keeps the boundary sharp)

- **No YAML loader / file IO** — the caller's format and parse step.
- **No `find_examples` retrieval tool, no prompt injection** — the agent's job.
- **No storage, write-back, embeddings, or similarity search.**
- **No query execution or result-correctness check** — dry run only; result
  verification stays with the human gate / the agent's system.
- **No handling of prose lessons** — nothing to contract-validate; they never
  enter this path.
- **No taxonomy interpretation** — the caller filters to SQL-bearing examples.

## Testing (TDD)

`tests/test_validation/test_examples.py`:

- valid example → `status == "valid"`, `contract_checked is True`
- forbidden-op / disallowed-table / missing-required-filter → `status ==
  "violation"` with the specific reason
- warn-level rule → recorded in `warnings`, does not flip `ok`
- principal-scoped example validates under its `principal`
- **contract-policy drift:** valid under contract A becomes a `violation` under
  contract B
- **schema drift (Layer 2):** static-clean SQL whose table/column no longer
  exists → `status == "violation"`, `engine_checked is True` (fake
  `ExplainAdapter` returning `schema_valid=False`)
- **decision B — engine plans:** sqlglot-unparseable SQL + adapter that plans it
  → `status == "valid"`, `contract_checked is False`, caveat warning present, row
  in `unverified_compliance`
- **decision B — engine rejects:** sqlglot-unparseable SQL + adapter that rejects
  it → `status == "violation"`, `contract_checked is False`
- **unchecked:** sqlglot-unparseable SQL + no adapter → `status == "unchecked"`
- `VerifiedExample.from_dict` tolerates and preserves unknown keys in `metadata`
- empty input → empty report, `ok is True`

## Naming note

`VerifiedExample` matches the consumer's vocabulary (the corpus is "verified"
by their MR gate). Since the framework's role is to *re-verify*, `SqlExample` or
`ExampleQuery` are alternatives if the type name should not assert "verified."
Decide at implementation.
