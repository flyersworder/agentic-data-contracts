# Metric Decomposition Reconciliation Check — Design

**Date:** 2026-07-19
**Status:** Design — awaiting review
**Author:** Qing Ye + Claude

## Goal

Verify that a metric's declared arithmetic `decompositions` actually hold **in
the data** — execute the parent and its operands, apply the declared operator,
and assert they agree within tolerance — so definition drift and ETL breakage
that leave every per-query check green are caught by a contract-integrity gate.

## Motivation

`v0.28.0` added `decompositions` to `MetricDefinition` as **exact, exhaustive
arithmetic identities** (`total_revenue = product(active_customers,
revenue_per_customer)`; `conversion_rate = ratio(first_purchase_users,
cohort_signups)`). The distinction from `MetricImpact` (causal/influence) edges
is that identity edges are *arithmetically exact by claim*.

Nothing verifies that claim against real data. The existing validation layers —
sqlglot static analysis and EXPLAIN dry-run — catch **unauthorized** SQL (bad
tables, forbidden ops, missing filters, `SELECT *`). They do not catch an
identity that has quietly become **false**: an ETL change that skews a child's
population, a metric SQL that drifted from the parent's definition, a
join that silently filters rows. Every per-query checker stays green while the
metric is misreported — the "confident wrong answer" failure, at the identity
level.

This is the **reconciliation half of Spec B**. The other half — a
variance-*diagnosis* tool that explains *why* a metric moved — is deliberately
out of scope and agent-owned (see `docs/architecture.md`, Future Extensions).
Reconciliation reports *that* an identity disagrees and by how much; it never
infers the cause. Keeping that boundary is a design requirement, not an
omission.

## Scope

**In scope:**

- A validation-layer function that reconciles **one metric's one
  decomposition** against a live `DatabaseAdapter`.
- Framework owns: reading the declared decomposition (operator, operand names,
  arity), executing caller-supplied scalar SQL, applying the operator,
  comparing within tolerance, returning a structured result.
- Caller owns: the scalar SQL for the parent and each declared operand, over
  whatever slice (period / filter / grain) the caller chooses.

**Out of scope (with rationale):**

- **Query assembly / metric execution.** Turning a `MetricDefinition` into an
  executable scalar query is the deferred `compose_metric_query` kernel (needs
  relationship-driven joins; see the executor analysis below). Reconciliation
  is deliberately the cheap 80% that a later executor upgrades — it takes SQL
  as input rather than generating it.
- **Cause diagnosis.** Inferring *why* two numbers disagree (population
  mismatch, join filtering, tautological identity) needs symbolic SQL analysis
  and is agent-owned. The result reports numbers; interpretation is the
  caller's / agent's job.
- **Per-metric tolerance in YAML.** Caller passes tolerance; revisit only on
  demand.
- **Time-windowing beyond the caller's SQL.** Whatever window the caller bakes
  into their queries is the window.
- **dbt/Cube extraction of decompositions.** Still `YamlSource`-only.

## Why the caller supplies SQL: the executor constraint

The framework has no metric executor. `DatabaseAdapter` exposes only
`execute(sql) -> QueryResult`; a `MetricDefinition` is not executable on its
own (`sql_expression` is an aggregate *fragment*, `source_model` is one table,
`filters` is a list). Assembling `SELECT <sql_expression> FROM <source_model>
[+joins] WHERE <filters AND slice>` is **not uniform** across the fixtures:

| Metric | `sql_expression` shape | Executable standalone? |
|---|---|---|
| `total_revenue` | `SUM(amount) FILTER(...)` | ✅ single-table |
| `first_purchase_users` | `COUNT(DISTINCT e.user_id) FILTER(...)` | ✅ single-table |
| `cohort_signups` | `COUNT(DISTINCT u.id)` | ✅ single-table |
| `conversion_rate` (parent) | `... / COUNT(DISTINCT u.id)` | ❌ `u.` alias needs `events⋈users` join |
| `revenue_by_region` | `... GROUP BY c.region` | ❌ join + non-scalar |

Two consequences drove the "caller supplies SQL" decision:

1. **The executor is real but out of scope.** Assembling the parent's joins
   (`conversion_rate`) requires the relationship graph — that is
   `compose_metric_query`. Building it first would reorder the roadmap behind
   the big deferred item.
2. **The executable-today subset is the tautological subset.** `total_revenue
   = active_customers × revenue_per_customer` executes today *and* is
   algebraically guaranteed to reconcile (`revenue_per_customer` is defined as
   revenue ÷ count). The *substantive* check — whether `conversion_rate`'s
   joined-parent population matches the standalone `cohort_signups` denominator
   — is exactly the one the current schema can't execute. A framework that
   auto-assembled only the easy cases would pass the worthless checks and skip
   the valuable ones: false confidence, the precise anti-goal.

Taking SQL as input sidesteps both. The caller can express the substantive
cross-table cases today; the day `compose_metric_query` lands, it becomes the
SQL provider and this API is unchanged.

## Design

### The seam

The check is **keyed off the declared decomposition**, not a free-floating
operator. This is what makes it a contract-integrity check rather than a
calculator:

- The framework reads the metric's `decompositions[i]` for the operator,
  operand **names**, and arity — the contract remains the source of truth for
  *what the identity is*.
- The caller provides scalar SQL per **declared operand name**. If the caller's
  keys do not exactly match the declared operand set, the framework raises —
  the caller cannot reconcile against an identity the contract doesn't declare.

### API

```python
def reconcile_decomposition(
    metric: MetricDefinition,
    *,
    parent_sql: str,                    # scalar query for the parent metric
    operand_sql: Mapping[str, str],     # declared-operand-name -> scalar query
    adapter: DatabaseAdapter,
    rel_tol: float = 1e-4,
    abs_tol: float = 0.0,
    decomposition: int = 0,             # which entry, if the metric declares several
) -> ReconciliationResult
```

- Synchronous: `adapter.execute` is sync and the primary home is tests/CI.
  Optional agent exposure later wraps this in `asyncio.to_thread`, exactly as
  `run_query` does today.
- Takes a `MetricDefinition` directly (obtained via
  `semantic_source.get_metric(name)`), decoupling the function from any
  particular source accessor.

### Operator semantics

Operands are applied in **declared order**:

- `sum` → `Σ operand_values`
- `product` → `Π operand_values`
- `ratio` → `operand_values[0] / operand_values[1]`
- `difference` → `operand_values[0] - operand_values[1]`

Arity is trusted from `validate_decompositions()` (already enforced at load
time: `ratio`/`difference` are binary; `sum`/`product` take ≥ 2). The function
re-checks that `operand_sql` covers exactly the declared operands.

### Tolerance

```
reconciles = abs_diff <= max(abs_tol, rel_tol * abs(actual_parent))
```

- **Default `rel_tol=1e-4` is deliberately tight.** Decompositions are defined
  as *exact* identities, so a gap beyond float/`FILTER`/`DISTINCT`/division
  rounding noise is a real finding. A loose tolerance would defeat the check.
- `abs_tol` defaults to `0.0`; callers set it when the parent can legitimately
  be near zero (where relative tolerance is unstable).

### Result shape — reports the discrepancy, does not explain it

```python
@dataclass(frozen=True)
class ReconciliationResult:
    metric: str
    operator: str
    operands: dict[str, float]   # declared-operand-name -> measured value
    implied_parent: float        # operator applied to operand values
    actual_parent: float         # parent_sql result
    abs_diff: float
    rel_diff: float              # abs_diff / abs(actual_parent), or inf if actual == 0
    reconciles: bool
    rel_tol: float
    abs_tol: float
    reason: str | None           # populated when it cannot / does not reconcile
```

The result carries the numbers and the verdict. It does **not** carry a
cause ("population differs", "tautological") — that inference is agent-owned
and would cross the governance/agent boundary the doc draws.

### Errors and edge cases

- **Operand-key mismatch** (`operand_sql` keys ≠ declared operand names) →
  raise `ValueError`. The caller is reconciling against an identity the
  contract doesn't declare.
- **No decomposition / index out of range** → raise `ValueError`.
- **Non-scalar query** (query returns multiple rows or columns) → raise
  `ValueError`; caller SQL must produce a single scalar.
- **`NULL` / empty result** from any query → `reconciles=False`,
  `reason="operand '<name>' returned NULL"` (or parent). A missing measurement
  is a finding, not a crash.
- **`ratio` denominator == 0** → `reconciles=False`,
  `reason="ratio denominator (operand '<name>') is zero"`; `implied_parent` is
  reported as `inf`.

## Usage model

Reconciliation only validates *meaningfully against real data*, so the design
assumes a warehouse-backed schedule is the real target and a hermetic test is
the regression guard. Three usage modes:

### 1. CI — two distinct modes

**Hermetic mode (DuckDB + seeded fixtures, runs on every PR).** Tests the
reconcile *logic* and pins "this identity holds on this fixture." Fast, no
credentials. Catches only breakage present in the fixture — a regression guard,
not a drift detector.

**Live mode (scheduled job against staging/prod, e.g. nightly).** The real
value: catches drift an ETL change introduced, a child metric SQL that
diverged, a join that skews a population. Needs warehouse credentials and a
schedule; alerts rather than blocks.

**Recommended blend:** hermetic on PRs (cheap regression guard) + live nightly
against real data (the actual drift detector).

Worked example (`growth_agent`, the substantive cross-table case):

```python
def test_conversion_rate_identity(warehouse_adapter, source):
    metric = source.get_metric("conversion_rate")  # ratio(first_purchase_users, cohort_signups)
    result = reconcile_decomposition(
        metric,
        parent_sql="""
            SELECT COUNT(DISTINCT e.user_id) FILTER (WHERE e.event_name='first_purchase')
                   / COUNT(DISTINCT u.id)::FLOAT
            FROM analytics.events e JOIN analytics.users u ON e.user_id = u.id
            WHERE u.cohort_month = '2026-05'""",
        operand_sql={
            "first_purchase_users":
                "SELECT COUNT(DISTINCT e.user_id) FILTER (WHERE e.event_name='first_purchase') "
                "FROM analytics.events e JOIN analytics.users u ON e.user_id=u.id "
                "WHERE u.cohort_month='2026-05'",
            "cohort_signups":
                "SELECT COUNT(DISTINCT id) FROM analytics.users WHERE cohort_month='2026-05'",
        },
        adapter=warehouse_adapter,
    )
    assert result.reconciles, result   # fails loudly with the numbers attached
```

What this catches: the parent's denominator counts users *who appear in events*
(after the join); `cohort_signups` counts *all* cohort users. If those
populations differ, `reconciles` is `False` and a real definitional
inconsistency is surfaced — invisible to every other layer.

### 2. Contract-authoring gate (highest-value trigger)

Run live mode on **contract-change PRs that touch `decompositions`**, proving a
newly declared identity is actually true before it merges. This is the moment a
wrong identity gets introduced, so it is the most valuable trigger — a
specialization of live mode scoped to the diff.

### 3. Agent runtime self-check (optional, secondary)

An optional 10th factory tool wrapping `reconcile_decomposition` (via
`asyncio.to_thread`) lets an agent sanity-check a metric it just computed before
reporting it — a guardrail against its own SQL mistakes. Marginal value (the
agent's queries are already validated) but cheap. Deferred to a follow-up pass;
the library function ships first.

### The honest cost

Because the caller supplies SQL (no executor), that SQL is hand-written per
metric and can itself drift from the metric's `sql_expression`. It is written
once and version-controlled alongside the contract, so the cost is tolerable —
and it disappears when `compose_metric_query` lands and becomes the SQL
provider. This feature is the cheap-to-build layer that the executor later
upgrades for free.

## Home and packaging

- Module: `src/agentic_data_contracts/validation/reconciliation.py` (natural
  neighbor of the other checkers).
- Exports: `reconcile_decomposition`, `ReconciliationResult`.
- Sync, no new dependencies (uses the existing `DatabaseAdapter` protocol and
  `MetricDefinition` model).

## Testing approach

DuckDB-backed, mirroring `tests/test_validation/` layout, in
`tests/test_validation/test_reconciliation.py`:

- **Holds on seeded data** — a `product`/`sum` identity that reconciles exactly.
- **Breaks on population mismatch** — the `conversion_rate` join-vs-standalone
  denominator case, asserting `reconciles is False` with the numbers.
- **`ratio` division by zero** — denominator 0 → `reconciles False`, reason set.
- **`NULL` / empty operand** → `reconciles False`, reason set.
- **Operand-key mismatch** → raises `ValueError`.
- **Non-scalar query** → raises `ValueError`.
- **Tolerance boundary** — a diff just inside and just outside `rel_tol`.
- **`decomposition` index selection** — a metric with two decompositions.

## Future upgrade path

When `compose_metric_query` lands, its relationship-driven assembly becomes the
SQL provider and reconciliation collapses to `reconcile(metric, cuts)` — the
caller supplies structured cuts instead of raw SQL, the executor produces the
queries, and this function's arithmetic/tolerance/result core is reused
unchanged. The API designed here (`MetricDefinition` + per-operand queries) is
the seam that upgrade plugs into.

## Decisions taken (flag at review)

- **`rel_tol=1e-4` default** — tight enough to catch real drift, loose enough
  for float/`FILTER`/`DISTINCT` noise. Caller-overridable.
- **Library function first, agent tool later** — ship the CI/test-facing
  function in the first pass; the optional 10th factory tool is a follow-up.
