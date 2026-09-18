# Sensitivity checks: verifying that a query derives what the contract says it depends on

**Status:** design approved 2026-09-08; implemented on `feat/sensitivity-checks`
(0.52.0). Implementation changed several decisions, and a second design —
dialects that parse only after normalization — was approved 2026-09-18. Every
section above **What shipped differently** keeps its original text, so the reason
for each change stays legible; where one disagrees with **What shipped
differently** or **Dialects that parse only after normalization**, those two
sections win. Process scaffolding — delete once
the feature ships (see the DABStep precedent, `8c0faf7`).

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
    description: str  # required -- the claim, in the owner's words. See Ownership.
    shadow: Shadow
    expect: Literal["unchanged", "changes"]
```

`MetricDefinition` gains `sensitivity: tuple[SensitivityProperty, ...] = ()`.

**Load-time validation, loud** (on file load *and* on frozen-contract `from_raw`
rehydration, matching how `decompositions` is validated):

- unknown keys rejected at both levels (`SENSITIVITY_KEYS`, `SHADOW_KEYS`);
- `name` unique within a metric;
- **`description` present and non-empty** -- see Ownership; a property nobody
  stated in words is one nobody can review;
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

> **Superseded:** a shadow is now normalized before it is parsed, so a VQL shadow is checked rather than refused. See **Dialects that parse only after normalization**.
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

> **Superseded:** `metric` is a `MetricDefinition` passed positionally and
> `contract` is keyword-only; `repeats` and `dialect` were added. A
> `sql_normalizer` keyword is designed in **Dialects that parse only after
> normalization**. See **What shipped differently**.

`properties` selects a subset by name; `None` runs every property the metric
declares. Unknown names raise `ValueError` (malformed input raises, data
conditions are findings — the `reconcile_decomposition` split).

**Step 0, once per call: the caller's query must pass Layer 1.** `check_sensitivity`
runs `sql` through the contract's `Validator` static analysis before executing
anything, and raises if it is blocked. Without this the function is a policy
bypass — an entry point that executes arbitrary SQL against the adapter without
the checks every other path applies. It is not a re-run of the caller's own
validation so much as a refusal to be the weak door.

> **Superseded:** only a *policy* block raises. A query Layer 1 cannot parse returns every property `unchecked` and executes nothing. See **What shipped differently**.

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

> **Superseded:** `ok` is also False when every result is `not_applicable`. See **What shipped differently**.

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

> **Superseded:** the alias stem is `sens_shadow_`, letter-leading for portability.
| dialect sqlglot cannot parse | -> `unchecked`; never silently skipped |
| shadow reads an ungoverned table | rejected at load against `allowed_tables` |
| `check_sensitivity` used to run SQL the contract forbids | step 0 raises if Layer 1 blocks the caller's query |
| a lazily-parsing `SemanticSource` raising on bulk read | never crash a check; degrade to `unchecked` |

Cost is **two base executions per call plus one per property** — the base result
is computed once and cached, so a metric with four properties costs six
executions, not twelve. Stated in the module docstring, because it is the reason
this belongs at a promotion gate rather than in a hot path.

## Ownership

`sensitivity` adds a field to the contract, so it adds a question about who
keeps it true. The answer is not one role, and the split is not arbitrary --
it follows the one `MetricDefinition` already draws between `business_owner`
(the definition and its review cadence) and `operational_owner` (data health).

| part of the field | who | why |
|---|---|---|
| `description` + `expect` -- the **claim** | business owner states it, analytics engineer records it | "A touchpoint after qualification cannot change a first-touch attribution" is a business sentence, not a SQL one. It is the same kind of sentence that already justifies `sql_expression`. |
| `shadow.sql` -- the **encoding** | analytics engineer | Engine-native SQL against governed tables, in the same dialect and the same review as `sql_expression`. |
| `shadow.table` in `allowed_tables`, adapter, where the gate runs | data engineer / platform | Load-time validation already refuses a shadow reading an ungoverned table, so this role owns the surface the property may be written against -- not the property. |

A property therefore belongs to the metric's **`business_owner`**, on the
definition side of that line, and **not** to `operational_owner`: a violation
means the query disagrees with the definition, never that the data is unhealthy.

**Why the business owner cannot simply be left out.** A property invented by
the engineer who wrote `sql_expression` encodes the same understanding that
produced it, so it can only catch queries that disagree with *their* reading.
That is exactly what this spec claims -- it is a derivation check, not a
definition check, and "no claim about correctness" is already a non-goal -- so
engineer-authored properties are sufficient for the stated purpose. But the
properties worth having are the ones where an owner holds a standing opinion
they would defend: first-touch attribution, what a NULL region means, which
month boundary counts. An engineer working alone does not think to write those,
because to them the query obviously does the right thing. This is the same
structure as deriving a test from the implementation rather than the spec: it
catches transcription, not translation. **This is why `description` is
required** -- it is the one field that carries the owner's contribution, and a
property with no description is an assertion nobody can review.

**Deletion needs the sign-off that addition does.** The change control here is
asymmetric in a way that is easy to miss. Adding a property tightens the gate
*loudly*: CI fails and someone investigates. Removing one loosens it *silently*
and nothing ever complains again. Contract review must treat a deleted
property as a withdrawn commitment, not as cleanup.

**Staleness rides `last_reviewed`, and does not get its own field.** A property
that stops holding because the business changed -- first-touch became
last-touch -- does not go quiet; it fires `violation` on correct queries and
burns trust quickly. That makes it exactly as perishable as the definition it
belongs to, and the metric's existing `last_reviewed` is the right clock. A
second, per-property date would be a second thing to forget.

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
- duplicate property name, bad `expect`, malformed `shadow.table`, and a
  missing or empty `description` all raise;
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

## What shipped differently

Each change below was decided during implementation, recorded with its reason,
and reviewed. Where one contradicts an earlier section, this section wins.

| Topic | Earlier sections said | Shipped |
|---|---|---|
| Signature | `check_sensitivity(contract, sql, *, metric: str, …)` | `check_sensitivity(metric: MetricDefinition, sql, *, contract, adapter, properties=None, repeats=2, dialect=None)`, matching `reconcile_decomposition` and removing a lookup-by-name path |
| Container types | `tuple`, `Literal` | `list` with a default factory; `expect` is a `str` checked against `VALID_EXPECT`, matching `decompositions` |
| Ungoverned shadow | rejected at load | refused at run time by `check_sensitivity` and at CI time by `validate_sensitivity_tables`: `YamlSource` holds no `DataContract` to check against |
| Step 0 | raise on any Layer 1 block | raise on a *policy* block; a query Layer 1 cannot parse returns every property `unchecked` and executes nothing. Sections 2 and 5 contradicted each other here |
| Unparseable shadow | degrades to `unchecked` | the same, and now also never executed and reported by `validate_sensitivity_tables`; the first implementation silently ran it |
| `report.ok` | False on `violation` or `unchecked` | also False when every result is `not_applicable`: a query reading none of the shadowed tables — a hardcoded answer included — checked nothing. `properties=[]` is the explicit opt-out |
| Multiple statements | not considered | Layer 1 blocks a string holding more than one statement. Pre-existing in every release from 0.1.0; found because `check_sensitivity` leans on Layer 1 |
| Determinism | two runs | `repeats` runs, a parameter; the probe is a filter, not a proof, and `repeats < 1` raises |

## Dialects that parse only after normalization

Approved 2026-09-18.

### Problem

`check_sensitivity` parses the caller's raw text. A dialect sqlglot parses only
after a `SqlNormalizer` rewrites it — Denodo VQL, in this library's known
deployment — returns `unchecked` for every query. That fails closed, so it opens
no governance hole, but it leaves the feature unusable where it was meant to run.

### Constraint

`SqlNormalizer` is an opaque `normalize_sql(sql) -> str`. The library ships no
implementation; the Denodo one lives outside it and reports no offsets. The
protocol also requires that the **original** text execute, never the normalized
one. So the library must edit text it cannot parse, guided by text it can.

### Design: parse the normalized text, edit the original

1. **Step 0.** Normalize the query once and hand Layer 1 the normalized text,
   so it is policy-checked in normalized form, as `run_query` does it. This is
   equivalent to building the `Validator` with the normalizer — its only other
   use of the raw text is an EXPLAIN, and this Validator has none — and it
   stops a normalizer that raises something other than a parse error from
   escaping `validate()`, which catches only `ParseError` and `TokenError`.
2. **Locate.** Count the real references to the target table in the AST of
   `normalize(sql)`.
3. **Rewrite.** Tokenize the **original** text, find the target's spans, and
   require the span count to equal the reference count. Zero references after
   normalization is `not_applicable` only when the original and normalized
   texts hold the *same* number of name-matching spans — the identity case,
   and any normalizer that leaves look-alikes untouched. That comparison is
   what a bare-name column (`SELECT lead_id, touchpoints FROM
   mkt.lead_scores`) needs: `_spans` cannot tell a column that shares the
   target's bare name from an actual table reference, so it produces a span
   with no normalizer involved at all, and with no normalizer a renamed
   target is impossible — the counts are always equal, so this reads as
   `not_applicable` exactly as it did before normalization support existed.
   A *different* span count — a normalizer that renamed or re-qualified the
   target, which erases the look-alike spans along with the real one — is the
   count guard instead. Edit the original.
4. **Prove, in two parts.** First, normalize the *edited body* — before the CTE
   is injected — and require that it parses and holds **zero** real references
   to the target. The proof cannot run on the final text: the shadow itself
   legitimately reads the target (`SELECT * FROM mkt.touchpoints UNION ALL …`).
   Second, inject the CTE, normalize the final text, and require that it parses
   as exactly **one** statement. "Parses" alone is not enough: at the sqlglot
   28.6 floor `parse_one` keeps only the first statement.
5. **Shadows.** Normalize `shadow.sql` before the governance check parses its
   tables, so a VQL shadow reading an ungoverned view is still refused. A
   shadow holding more than one statement counts as unparseable. That is not
   exploitable — the CTE's parentheses make the semicolon a syntax error, and
   both the parser and DuckDB reject the string — but governance used to see
   only governed tables and wave it through to an opaque engine error.

Without a normalizer, `normalize` is the identity and the same path runs. The
proof then costs one extra parse on plain SQL, and it turns the existing count
guard from "the counts agree" into "the edit is shown to have worked".

**Why tokenizing the original works.** Tokenizing is far more permissive than
parsing. VQL's `CONTEXT ('k' = 'v')` clause fails `sqlglot.parse_one` but
tokenizes cleanly (probed 2026-09-18).

**Why the proof is needed.** Count equality alone can be fooled. If the
normalizer dropped one reference and the original holds an extra look-alike
token in table position, the counts match and the wrong span is edited. The
proof catches any edit that leaves a real reference to the target behind.

**Assumption: the normalizer changes syntax, not names.** Infineon's Denodo
normalizer is syntax-only. A normalizer that changes how the target is named —
renaming it, or qualifying it differently from the original — makes the span
and reference counts disagree, so every query fails closed as `unchecked`.

### Where it lives: inside the library

The normalizer lives outside because it holds dialect knowledge the library
cannot have. The span mapping holds none: it treats `normalize` as a black box
and verifies the result, so it serves any syntax-only normalizer with no work
from its author. It also decides what SQL runs, and the post-edit proof is the
library's guarantee. Dialect knowledge stays outside; verification stays inside.

**Named follow-up, not built:** an optional `SqlNormalizer` method returning
exact offsets, for a normalizer that also rewrites names. The library would
prefer it when present, fall back to the generic mapping when absent, and still
run the proof on whatever it returns. It is additive and breaks no existing
normalizer.

### Denodo's `WITH` clause

The Virtual DataPort VQL Guide documents `WITH` in every release from 8.0 to
9.3-beta — "Virtual DataPort supports Common Table Expressions in SELECT and
CREATE VIEW statements" — and its formal `SELECT` grammar (9.2) settles the
shape the rewrite emits:

```
<query>                    ::= [ WITH <common table expressions> ] { <select> | <complex select> } … [ CONTEXT ( … ) ] [ TRACE ]
<common table expressions> ::= <common table expression> [ , <common table expression> ]*
<common table expression>  ::= <query name> [ ( <field> [ , <field> ]* ) ] AS ( { <select> | <complex select> } [ <order by> ] )
<view>                     ::= <simple view> | <join view> | ( <select> )
```

| Detail | The rewrite emits | Settled by |
|---|---|---|
| No column list | `sens_shadow_0 AS (<shadow>)` | grammar: the list is optional |
| Several CTEs in one `WITH` | `WITH sens_shadow_0 AS (…), <caller's CTE> …` | grammar: `<cte> [, <cte>]*` |
| A `UNION ALL` shadow | `sens_shadow_0 AS (SELECT … UNION ALL SELECT …)` | grammar: a CTE body may be a `<complex select>` |
| Letter-leading alias | `sens_shadow_0` | **unconfirmed** — the grammar names it `<query name>` without spelling out identifier rules |

`WITH RECURSIVE` does not appear in the grammar, so a VQL caller never writes it.

**The multi-CTE splice is the path that matters.** 68% of the 1,430 answer-
computing agent queries in the DABStep corpus open with their own `WITH`, so the
choice of how to inject a shadow into a query that already has one governs two
thirds of real traffic.

**Hardening, regardless of Denodo:** the alias stem changes from `__sens_` to
`sens_shadow_`. Oracle requires unquoted identifiers to start with a letter, so
`__sens_0` would fail there too; a letter-leading stem also removes the one
detail the grammar leaves open. `_free_alias` already steps past collisions.

**Rejected alternatives, with the grammar's reasons:**

- *Inline derived table at each reference* (`JOIN (<shadow>) t`). In VQL a
  derived table in a join is only `( <select> )` — no `UNION`, no alias — so the
  worked example's `UNION ALL` shadow cannot be expressed. It would also need
  alias-aware surgery at every reference and copy the shadow per reference.
- *Wrap the caller's query* (`WITH s AS (…) SELECT * FROM (<caller>)`). `WITH`
  may open only a top-level `<query>`, and a subquery is `( <select> )`, so the
  nested `WITH` is invalid — exactly when the caller wrote one. SQL Server rejects
  it too.
- *Refuse any query that already has a `WITH`.* Valid everywhere, but gives up
  68% of real queries.
- *A temporary view or table.* Breaks the no-writes rule.
- *Emitting a column list pre-emptively.* The grammar makes it optional; a
  `describe_table` round trip on every call buys nothing.

**Verification on a live Denodo** (not blocking; it settles the one open row):

```sql
WITH sens_shadow_0 AS (SELECT * FROM <some_view>) SELECT COUNT(*) FROM sens_shadow_0;
```

### Guards

Malformed input raises; data conditions are findings. Every `unchecked` below
executes nothing for that property.

| Condition | Result |
|---|---|
| the normalizer raises on the query | every property `unchecked` (`normalizer failed: …`); nothing executes |
| the normalized query does not parse | every property `unchecked`; nothing executes |
| the normalized query is blocked by policy | `ValueError` |
| a normalized shadow reads an ungoverned table | `ValueError` |
| the normalizer raises on a shadow, or its normalized form does not parse or holds more than one statement | that property `unchecked` (`unparseable shadow`); `validate_sensitivity_tables` reports it |
| zero references in the normalized AST, and the original and normalized texts hold the same number of name-matching spans (including both zero) | `not_applicable` |
| the original text does not tokenize | `unchecked` (`original text could not be tokenized`) |
| spans in the original ≠ references in the normalized AST, or (when references are zero) the normalized text's span count differs from the original's — including a normalizer that renamed the target | `unchecked` (count guard across the normalizer boundary) |
| the edited body fails to normalize or parse, or still references the target | `unchecked` (`rewrite could not be proved`) |
| the final text fails to normalize, to parse, or to be one statement | `unchecked` (`rewrite could not be proved`) |
| the engine rejects the base or mutated query | `unchecked`, including the one unconfirmed `WITH` detail above |

Normalizing is in-process string work — the query once for Layer 1 and once
per property to locate, each property's edited body and final text once, each
shadow once — and adds no database round trips.

### Public API

```python
check_sensitivity(metric, sql, *, contract, adapter, properties=None,
                  repeats=2, dialect=None, sql_normalizer=None)
validate_sensitivity_tables(contract, metrics, *, dialect=None, sql_normalizer=None)
```

Both keywords are additive. When `sql_normalizer` is omitted, `check_sensitivity`
uses the adapter if it implements `SqlNormalizer`, the convention `create_tools`
follows; a Denodo adapter needs no wiring. `validate_sensitivity_tables` takes no
adapter, so it has no fallback. Internally the rewrite takes a `normalize`
callable that defaults to the identity.

### Testing without Denodo

A test adapter wraps DuckDB and accepts a VQL `CONTEXT (…)` clause: its
`normalize_sql` strips the clause for sqlglot, and its `execute` strips it for
DuckDB, as Denodo accepts its own syntax.

| Test | Proves |
|---|---|
| first-touch and fan-out queries with `CONTEXT` appended come back `pass` and `violation` | the feature works on a dialect that parses only after normalization |
| an execute-spy sees `CONTEXT` and the CTE in what runs | the original text executes, never the normalized one |
| the same queries through a plain adapter come back `unchecked` | before and after — the capability added |
| a normalizer that renames the target gives `unchecked`, nothing run for that property | the count guard fails closed across the boundary |
| a normalizer that raises gives every property `unchecked`, nothing run | the normalizer cannot crash the call or leak execution |
| the proof helper refuses an edited body that still references the target | the proof has teeth; tested directly, since the case is hard to reach |
| a `CONTEXT` shadow: governed passes, ungoverned raises | shadow governance is judged after normalization |
| a `SqlNormalizer` adapter with the keyword omitted | the fallback is used |
| `sens_shadow_0` appears, and a collision steps to `sens_shadow_1` | the alias hardening |

Every test runs at the sqlglot floor (28.6) and at the lockfile's version.
Existing tests that name `__sens_` move to the new stem.

### Non-goals

- No change to the `SqlNormalizer` protocol.
- No offsets method yet (the named follow-up above).
- No pre-emptive column list and no derived-table rewrite: the VQL grammar makes
  the first unnecessary and the second unable to express a `UNION ALL` shadow.

### Docs

The README, CHANGELOG 0.52.0 entry and module docstring drop "Denodo/VQL is not
yet supported" and state instead: queries are normalized before they are parsed
and edited on their original text; the normalizer must change syntax only; the
what the VQL grammar settles about the `WITH` form, the one unconfirmed detail
and its fail-closed behaviour; the verification query;
and the named follow-up.

