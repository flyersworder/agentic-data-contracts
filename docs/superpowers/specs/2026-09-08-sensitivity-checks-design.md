# Sensitivity checks: verifying that a query derives what the contract says it depends on

**Status:** design, approved 2026-09-08. Process scaffolding — delete once the
feature ships (see the DABStep precedent, `8c0faf7`).

## Problem

The two-layer validator sees **policy** (allowed tables, forbidden operations,
required filters, `SELECT *`) and **plannability** (a live `EXPLAIN`). Neither
sees a query that is authorized, parseable, plannable — and computes the wrong
thing.

The DABStep eval measured how large that blind spot is and established that it
cannot be closed by reading the query text. Two independent detector families
were built over the stored transcripts of four models:

- **absence of a clause** (`analysis/clauses.py --within`) — do the contract's
  load-bearing clauses appear? Correct and incorrect attempts write the same
  ones, every Fisher p >= 0.14.
- **presence of a wrong construction** — a detector for band literals, hand-rolled
  month windows and unmapped `capture_delay`, whose four patterns were read off
  the contract arm's own wrong answers. Still null: on Sonnet 5 it fires on 33
  of 97 queries, 29 of them **correct** (precision 0.12, p = 1).

Presence and absence both fail, so the property is not in the text. A static
validator, a lint rule and a post-tool hook all read the text, so none of them
can see this class of defect.

`experiments/dabstep-contract-eval/analysis/sensitivity.py` showed what does
work. Mutate the data in a way the contract itself dictates, re-run the agent's
own query, and ask whether the answer **moved**. Restricted to contract-arm
queries the benchmark scored *correct*, every query that failed to respond was
wrong in the mutated world — 0 of 31, against 103 of 112 for the responsive
ones, Fisher exact **p = 1.2e-23**. As a detector of the defect (rather than of
a wrong benchmark answer) precision is **1.00**, recall 0.78.

The mechanism has a name: **premature materialization**. An agent computes an
intermediate value in one turn, decides a band or a version or a rate itself,
and pastes it into the next query as a literal. The answer is right on today's
data and wrong one boundary away. Nothing downstream disagrees.

This feature moves that instrument out of the experiment and into the library,
as a third member of the family that already contains `reconcile_decomposition`
(0.29.0) and `validate_examples` (0.30.0): the contract declares a property,
the caller supplies execution, the library contributes one verb.

### The mechanism was validated before the feature was written

The p = 1.2e-23 result above was produced by copying the database and running
`UPDATE` against the copy. This feature does no writes: it rewrites the
caller's text and injects the mutation as a CTE. Those are different
mechanisms, so the result does not transfer for free.

A prototype of sections 2.1-2.2 was therefore run against the stored corpus,
expressing `volume_x10` as a declared shadow instead of an `UPDATE`:

```yaml
shadow:
  table: main.payments
  sql: SELECT * REPLACE (eur_amount * 10 AS eur_amount) FROM main.payments
```

which returns exactly the 138,236 rows the `UPDATE` touched. Both arms of the
comparison read the same **read-only** base database.

| over 1,411 stored agent queries, four models, all four arms | |
|---|---:|
| comparable (both methods returned a verdict) | 1,007 |
| **agreement** | **1,006 (99.9%)** |
| moved under `UPDATE`-on-copy | 652 |
| moved under shadow CTE | 653 |
| `not_applicable` (references `fees`, not `payments`) | 388 |
| `unchecked` (non-deterministic) | 13 |

The two move counts matter as much as the agreement: 65% of queries move, so
this is not a degenerate "both always said no". The rewrite held on real agent
SQL -- nested CTEs, `WITH RECURSIVE`, correlated subqueries, bare and
schema-qualified spellings, and `'payments'` as a string literal, which is
correctly left alone.

The single residual disagreement was a `LIMIT` without `ORDER BY`; see step 3.
The exercise also produced the two guards marked in this spec as measured
rather than anticipated: the table-position restriction in step 2 and the
honesty note in step 3.

**`expect: unchanged` has no DABStep case** -- all three mutations are
`changes` -- so it was confirmed only on the worked example below. That
asymmetry is real, and it is the harder direction: "did not move" has more
innocent explanations than "moved".

## Non-goals

Named so the scope cannot drift:

- **No agent tool.** This is a validation function, like both siblings. A
  runtime `verify_query` tool is a separate decision that the DABStep advisory-
  hook arm should inform, not this spec.
- **No auto-derived properties.** Inferring mutations from `sql_expression` is
  the obvious next idea and the wrong first one: it would generate properties
  no human reviewed.
- **No materialization.** No DDL, no scratch schema, no shadow tables. A
  governance library must not require write access.
- **No sampling or cost control.** The caller narrows an expensive query.
- **No claim about correctness.** A passing property says the query *responds*
  to an input the contract says it depends on. It never says the answer is right.

## Design

### 1. Contract schema

`sensitivity` becomes an optional field on a metric, alongside `decompositions`
and `drill_by`. The metric is the unit the agent looks up and the unit
`lookup_metric` already surfaces.

```yaml
metrics:
  - name: mql_count
    description: Count of leads flagged is_mql, attributed to the FIRST touch.
    sensitivity:
      - name: attribution_is_first_touch
        description: >
          A touchpoint after qualification cannot change a first-touch
          attribution.
        shadow:
          table: mkt.touchpoints
          sql: >
            SELECT * FROM mkt.touchpoints
            UNION ALL
            SELECT lead_id, 'display', qualified_date + 1 FROM mkt.lead_scores
        expect: unchanged
```

A property is **one shadow SELECT that replaces one table**, plus the response
the contract requires. `expect` is `unchanged` or `changes`; there is no third
value.

A structured `{set:, where:}` mutation DSL was considered and rejected: it is a
second dialect to maintain, and it cannot express the insert-shaped mutation the
worked example needs. `shadow.sql` is engine-native SQL, which is what
`sql_expression` already is throughout a contract — so a Denodo author writes VQL
here exactly as they do there.

New key sets, exported for the reason `SEMANTIC_KEYS` and its siblings are
(a guard cannot be written against key names that exist only as string literals
in a constructor):

```python
SENSITIVITY_KEYS = frozenset({"name", "description", "shadow", "expect"})
SHADOW_KEYS = frozenset({"table", "sql"})
```

New dataclasses in `semantic/base.py`:

```python
@dataclass(frozen=True)
class Shadow:
    table: str  # "schema.table"
    sql: str  # a SELECT that replaces it


@dataclass(frozen=True)
class SensitivityProperty:
    name: str
    shadow: Shadow
    expect: Literal["unchanged", "changes"]
    description: str = ""
```

`MetricDefinition` gains `sensitivity: tuple[SensitivityProperty, ...] = ()`.

**Load-time validation, loud** (on file load *and* on frozen-contract `from_raw`
rehydration, matching how `decompositions` is validated):

- unknown keys rejected at both levels (`SENSITIVITY_KEYS`, `SHADOW_KEYS`);
- `name` unique within a metric;
- `expect` in the enum;
- `shadow.table` a well-formed `schema.table` identifier;
- **`shadow.sql`'s table references rejected unless every one is in the
  contract's `allowed_tables`.** A shadow that reads a table the contract does
  not govern is a governance hole, not a convenience. This is a hard check, not
  the soft one `drill_by` columns get: `allowed_tables` is always declared,
  where table *schemas* are optional.

**A `shadow.sql` sqlglot cannot parse is not a load error.** It degrades that
property to `unchecked` at run time — the same decision-B fallback
`validate_examples` made for Denodo/VDP, and for the same reason: a dialect the
engine understands and sqlglot does not must not make a contract unloadable.
The allowed-tables check above is only reachable when the shadow parses, so an
unparseable shadow is never *silently* trusted: it is refused a verdict instead,
and `report.ok` is false while it stands.

**Digest stability.** `dump_semantic_source` must omit `sensitivity` when empty,
exactly as 0.28.1 fixed for `decompositions` / `drill_by`. A contract that
declares no properties must freeze byte-identically to its pre-feature form, or
every published `contract_digest` changes on upgrade.

### 2. The public function

```python
def check_sensitivity(
    contract: DataContract,
    sql: str,
    *,
    metric: str,
    adapter: DatabaseAdapter,
    properties: Sequence[str] | None = None,
) -> SensitivityReport
```

`properties` selects a subset by name; `None` runs every property the metric
declares. Unknown names raise `ValueError` (malformed input raises, data
conditions are findings — the `reconcile_decomposition` split).

**Step 0, once per call: the caller's query must pass Layer 1.** `check_sensitivity`
runs `sql` through the contract's `Validator` static analysis before executing
anything, and raises if it is blocked. Without this the function is a policy
bypass — an entry point that executes arbitrary SQL against the adapter without
the checks every other path applies. It is not a re-run of the caller's own
validation so much as a refusal to be the weak door.

Then, per property, five steps:

1. **Locate.** Parse `sql` with sqlglot. Enumerate real table references as
   `find_all(exp.Table)` minus the aliases from `find_all(exp.CTE)` — verified:
   expression nodes carry no source positions, but this correctly separates a
   base table from a reference to a CTE. If the target table is not referenced,
   the result is `not_applicable`: an outcome, not a failure.
2. **Rewrite.** Locate character spans with `sqlglot.tokenize` (tokens *do*
   carry `start`/`end`), matching qualified names as alternating identifier/DOT
   token runs. **A candidate must sit in table position** — its preceding token
   is `FROM`, `JOIN`, `,` or `(`. Without that restriction a *column alias*
   sharing the table's name is counted as a reference: four stored DABStep
   queries write `COUNT(DISTINCT psp_reference) AS payments`, and each one
   inflated the span count past the table-node count and was refused although it
   was perfectly checkable. Since `unchecked` fails `report.ok`, that is a
   spurious CI failure, so the restriction is load-bearing rather than tidiness.
   **Assert the span count equals the table-node count from step 1**;
   a mismatch refuses to edit and returns `unchecked`. Apply edits
   right-to-left so earlier offsets stay valid. Inject `__sens_0 AS (<shadow.sql>)`
   as a CTE — prepending `WITH ...` when the query has none, and splicing after
   the leading `WITH` token when it does.
3. **Probe determinism** — once per call, not per property. Run the base query
   **twice** and cache the result. If it disagrees with itself, every property
   returns `unchecked: query is not deterministic`. Without this a query with
   `LIMIT` and no `ORDER BY`, `random()` or `current_date` reads as sensitive
   and the check silently passes garbage.

   **This probe is a filter, not a proof, and the docstring must say so.**
   A query that is merely *usually* stable passes it and then produces a
   verdict it did not earn. Measured on the DABStep corpus, two executions
   caught 13 of ~14 flaky queries; the survivor differed on every repetition of
   the experiment and was always a `LIMIT` with no `ORDER BY` — one such query
   returned 2 distinct results across 12 identical runs. No finite number of
   probes closes this. `repeats: int = 2` is therefore a parameter, not a
   constant, and the limitation is stated rather than engineered away.
4. **Execute** base and mutated through the `DatabaseAdapter`. Any engine error
   on either side → `unchecked` carrying the engine's message.
5. **Judge.** Compare normalised results — rows sorted by their repr, floats
   rounded, so "the answer moved" means the values moved and not that the engine
   returned them in a different order. `expect: changes` against an empty base
   result is `unchecked: vacuous`, because an empty answer cannot move.

The library reads and never writes. It executes exactly two kinds of statement:
the caller's own query, and that query with one table shadowed by a
caller-authored SELECT. No DDL, no DML, no scratch schema.

**No SQL is regenerated.** sqlglot is used to decide *what* to rewrite and to
guard the edit; the emitted string is the caller's original text with spans
replaced. That is what makes the feature work on a dialect sqlglot can parse but
not emit, and it is the same template-assembly discipline the Denodo constraint
forces everywhere else.

### 3. Result types

Mirrors `ExampleValidationReport` field for field, so a caller who has used one
already knows this one.

```python
@dataclass(frozen=True)
class SensitivityResult:
    name: str
    metric: str
    status: Literal["pass", "violation", "not_applicable", "unchecked"]
    expected: Literal["unchanged", "changes"]
    moved: bool | None      # None when no comparison was made
    reason: str = ""        # mechanical only; never infers a cause

@dataclass(frozen=True)
class SensitivityReport:
    results: tuple[SensitivityResult, ...]

    @property
    def ok(self) -> bool          # no violation and no unchecked
    @property
    def violations(self) -> tuple[SensitivityResult, ...]
    def summary(self) -> str      # markdown
```

`report.ok` is a **safe CI gate**: `if not report.ok: sys.exit(1)` fails on
violations *and* on unchecked. `not_applicable` does not block it — a query that
never touches the table has nothing to answer for. `unchecked` does, for the
reason `validate_examples` gives: "no verdict was possible" must not read as
"passed". Test `report.violations` directly for a laxer gate.

A metric that declares no properties yields an empty report: no results, `ok`
True. Silence is the honest answer — nothing was claimed and nothing was checked.

`reason` reports the mechanical condition only and never infers a cause, the
same governance/agent boundary `ReconciliationResult.reason` draws: the check
says "the answer did not move", not "you hardcoded the threshold".

### 4. Placement and exports

- `src/agentic_data_contracts/validation/sensitivity.py` — new module.
- `validation` exports `check_sensitivity`, `SensitivityResult`, `SensitivityReport`.
- `semantic` exports `SensitivityProperty`, `Shadow`, `SENSITIVITY_KEYS`, `SHADOW_KEYS`.
- `lookup_metric` surfaces a metric's `sensitivity` properties (name +
  description + expect), omitted when empty, matching how it surfaces
  `decompositions` / `drill_by`. The agent seeing what will be checked is a
  pre-commitment device and costs nothing.

### 5. Guards

| risk | guard |
|---|---|
| text surgery on someone else's SQL | span count must equal sqlglot's table-node count, else refuse |
| target name inside a string literal or comment | tokenizer classifies both; only identifier tokens are candidates |
| column alias sharing the table's name | candidate must sit in table position (`FROM`/`JOIN`/`,`/`(`) |
| non-deterministic query reads as sensitive | base run `repeats` times; disagreement -> `unchecked`. Probabilistic — see step 3 |
| vacuous test (empty base, `expect: changes`) | -> `unchecked`, not `pass` |
| CTE alias collision with `__sens_0` | assert absent from the SQL; pick the next free `__sens_N` |
| dialect sqlglot cannot parse | -> `unchecked`; never silently skipped |
| shadow reads an ungoverned table | rejected at load against `allowed_tables` |
| `check_sensitivity` used to run SQL the contract forbids | step 0 raises if Layer 1 blocks the caller's query |
| a lazily-parsing `SemanticSource` raising on bulk read | never crash a check; degrade to `unchecked` |

Cost is **two base executions per call plus one per property** — the base result
is computed once and cached, so a metric with four properties costs six
executions, not twelve. Stated in the module docstring, because it is the reason
this belongs at a promotion gate rather than in a hot path.

## Worked example (the acceptance test)

Fixture: `mkt.lead_scores` (lead_id, region, is_mql, qualified_date) and
`mkt.touchpoints` (lead_id, channel, touch_date). Property as declared in
section 1.

A fan-out query — `JOIN mkt.touchpoints` with `COUNT(DISTINCT lead_id)`, which
counts each lead once per channel it ever touched — **moves** under the shadow
and must produce `violation`.

A first-touch query — the same join restricted to `MIN(touch_date)` per lead —
is **unchanged** and must produce `pass`.

Both were confirmed end to end on DuckDB during design, including the rewrite
splicing into an existing `WITH` clause and rewriting two references (one of
them inside a correlated subquery) under the count guard.

## Testing

TDD, red first, one task per behaviour, per `CLAUDE.md`.

`tests/test_validation/test_sensitivity.py`:

- the two acceptance cases above (`violation` / `pass`);
- `not_applicable` when the query never references the target;
- rewrite: no `WITH`; existing `WITH`; existing `WITH RECURSIVE`; two references
  including a correlated subquery; target inside a string literal (must not be
  rewritten); alias collision with `__sens_0`; **a column alias sharing the
  target's name (`COUNT(*) AS payments ... FROM payments`) rewrites the table
  and not the alias**;
- `unchecked` paths: unparseable SQL, count-guard mismatch, engine error,
  non-deterministic base, vacuous (empty base with `expect: changes`);
- `report.ok` false on violation *and* on unchecked, true when every result is
  `pass` or `not_applicable`, and true for a metric with no properties;
- step 0 raises when the caller's own query is blocked by Layer 1;
- the base result is executed `repeats` times per call and reused across
  properties, not re-executed per property; `repeats` is configurable;
- `properties=` selection, and `ValueError` on an unknown name.

`tests/test_semantic/`:

- parse, round-trip through `dump_semantic_source` / `from_raw`;
- unknown keys rejected at both levels;
- duplicate property name, bad `expect`, malformed `shadow.table` all raise;
- shadow referencing a table outside `allowed_tables` raises;
- **a contract declaring no `sensitivity` dumps byte-identically to its
  pre-feature form** (the 0.28.1 digest-stability regression, as a test).

`tests/test_tools/`: `lookup_metric` surfaces the properties, and omits the key
when there are none.

## Compatibility

Fully backward compatible. `sensitivity` is a new optional field defaulting to
empty; existing contracts and dbt/Cube-sourced metrics (which do not populate
it) behave identically. Net-new module plus additive exports. No new
dependencies — sqlglot and the `DatabaseAdapter` protocol are already required.

Digest stability is the one real compatibility risk and is covered by an
explicit test.

## Docs

- New README section "Checking that a query derives what it depends on",
  cross-referencing `reconcile_decomposition` and `validate_examples` as the
  other two members of the family.
- A runnable demo under `examples/` built on the marketing worked example — the
  fan-out query flagged, the first-touch query cleared, against a live DuckDB.
- `docs/architecture.md`: the `MetricDefinition` field enumeration and the
  validation-module list.

## Open question, deliberately deferred

**Scope.** A property such as "a NULL region means all regions" belongs to a
*table*, not a metric, and declaring it on every metric that touches that table
duplicates it. Metric scope is chosen for v1 because it matches `decompositions`,
keeps `lookup_metric` as the single discovery surface, and is the smaller change.
If duplication bites, a top-level `sensitivity_mutations:` block referenced by
name from metrics is an additive follow-up — it does not invalidate anything
here.
