# Metric Identity Decomposition — Design (Spec A)

**Date:** 2026-07-19
**Status:** Design, pending implementation plan
**Scope:** Representation only. No SQL execution.

## Motivation

Root-cause analysis ("why did Revenue drop?") is a high-frequency agent task
(weekly reviews, operational standups). Doing it *deterministically* requires
knowing what a metric decomposes into. Today the repo models only the
*influence* graph (`MetricImpact`: causal, evidential, non-exhaustive). It has no
notion of a metric's *arithmetic identity* — the exhaustive, always-true
breakdown (`Revenue = Paying Users × ARPU`) — nor of the dimensions a metric can
be exhaustively sliced by (`Revenue GROUP BY region`).

The article that prompted this ("Grounding Agentic Analytics") reports that a
metric tree with identity edges both lifted diagnostic accuracy and cut a
root-cause task from ~12 tool calls to 3. The determinism is the point:
identity decomposition and dimensional slicing are *exhaustive* (the parts fully
reconstruct the whole), so an agent can localize a change without guessing —
unlike influence edges, which are hypotheses.

This spec adds the **representation** that makes deterministic diagnosis
possible. It deliberately stops short of the diagnosis *tool* itself, which
executes queries and attributes variance — that is Spec B.

### The three kinds of "driver" (why this design splits them)

| Kind | Nature | Where it lives |
| --- | --- | --- |
| Arithmetic decomposition | Exhaustive, exact (`Revenue = Users × ARPU`) | **New:** `MetricDefinition.decompositions` |
| Dimensional slice | Exhaustive, exact (`Revenue GROUP BY region`) | **New:** `MetricDefinition.drill_by` |
| Causal / influence | Speculative, needs evidence | Existing `MetricImpact` |

The first two are deterministic and belong on the metric *definition*. The third
is relational/evidential and stays as a separately-reviewed graph edge.

## Non-goals (explicitly out of scope)

- **Reconciliation check** — executing parent vs. children SQL to assert the
  arithmetic holds within a tolerance. Needs a database adapter; a strong Spec B
  follow-up.
- **Diagnosis tool** — walking the tree over a time window, attributing variance,
  ranking contributors. Spec B.
- **dbt/Cube extraction** — `DbtSource`/`CubeSource` will return empty
  `decompositions`/`drill_by`. Their mappings (dbt `ratio`/`derived` metrics,
  Cube calculated measures and cube dimensions) are real but each is its own
  parsing exercise; deferred.
- **Importance weights** beyond `drill_by` list ordering. Priority is encoded as
  order; an explicit numeric weight is YAGNI for now.
- **Operands that are raw columns or constants.** Decomposition operands are
  other declared metrics only. Keeps the tree well-defined.

## Data model (`semantic/base.py`)

Two new dataclasses, two new optional fields on `MetricDefinition`. Both fields
default empty — most metrics are leaves (atomic counts, pre-computed columns, or
simply not-yet-decomposed), and an empty decomposition is a valid, first-class
state.

```python
@dataclass
class Decomposition:
    """An arithmetic identity: how a metric is reconstructed from other metrics."""
    operator: str          # "sum" | "product" | "ratio" | "difference"
    operands: list[str]    # names of other MetricDefinitions

@dataclass
class DrillDimension:
    """A dimension a metric can be exhaustively sliced by. List order = priority."""
    dimension: str         # business name, e.g. "region"
    column: str            # "schema.table.column" — same convention as Relationship

@dataclass
class MetricDefinition:
    # ... existing fields ...
    decompositions: list[Decomposition] = field(default_factory=list)
    drill_by: list[DrillDimension] = field(default_factory=list)
```

### Operator semantics

The operator is the key that lets a future diagnosis tool attribute a change
*analytically* (per-operator formula) rather than parsing an arbitrary
expression. All four are defined in the data model; only `sum`/`product`
attribution matters until Spec B.

| operator | identity | arity |
| --- | --- | --- |
| `product` | parent = a × b × … | ≥ 2 |
| `sum` | parent = a + b + … | ≥ 2 |
| `ratio` | parent = a ÷ b | exactly 2 |
| `difference` | parent = a − b | exactly 2 |

A metric may declare **multiple** decompositions (e.g. `Revenue` as both
`product(paying_users, arpu)` and `sum(new, expansion, reactivation)`). Each is
validated independently; both contribute edges to the graph.

### Example YAML

```yaml
metrics:
  - name: revenue
    sql_expression: "SUM(amount)"
    decompositions:
      - operator: product
        operands: [paying_users, arpu]
      - operator: sum
        operands: [new_revenue, expansion_revenue, reactivation_revenue]
    drill_by:
      - {dimension: region,    column: analytics.dim_customer.region}
      - {dimension: plan_tier, column: analytics.dim_plan.tier}
  - name: activation_rate
    sql_expression: "..."
    decompositions:
      - operator: ratio
        operands: [activated_users, signups]
  - name: signups            # leaf: no decomposition, and that's fine
    sql_expression: "COUNT(*)"
```

## Loading & validation (`semantic/yaml_source.py` + `semantic/base.py`)

`YamlSource._load_from_raw` parses the two new lists into the dataclasses (empty
when absent). After all metrics are parsed, a **post-load validation pass**
runs — placed in `base.py` as a pure helper `validate_decompositions(metrics)`
so it is source-agnostic and unit-testable, and called by `YamlSource`.

Validation is **optional to declare, validated only when present.** A metric with
no `decompositions` is untouched. When a decomposition *is* declared, the
following raise `ValueError` (loud, at load — matching the existing
`_parse_date` behaviour; a broken decomposition must never reach an agent):

1. **Unknown operator** — not one of the four.
2. **Wrong arity** — `ratio`/`difference` ≠ 2 operands; `sum`/`product` < 2.
3. **Unresolved operand** — an operand name not matching any declared metric.
4. **Cycle / self-reference** — the combined identity edges must form a DAG. A
   metric cannot (transitively) decompose into itself. (Influence edges keep
   their existing cycle-*tolerant* behaviour; only identity edges get the DAG
   constraint, because "walk the tree" and any future reconciliation depend on
   it.)

`drill_by` columns are validated **softly**: a column reference is checked
against the table schema only when that table is declared in the source; when the
table schema is absent (schemas are optional in these contracts), the check is
skipped silently — mirroring how `_freshness_fields` stays quiet about metadata
the org never adopted.

## Frozen-contract roundtrip (`semantic/base.py`)

`dump_semantic_source` must emit `decompositions` and `drill_by` per metric so an
inline-frozen contract survives `dump → from_raw`. `from_raw` reuses
`_load_from_raw`, so parsing (and re-validation) is already symmetric. The
`SemanticSource` protocol is unaffected (the new data rides on
`MetricDefinition`, which `get_metrics()`/`get_metric()` already return).

## Traversal integration (`semantic/base.py` + `tools/factory.py`)

Identity decomposition is a **directed labelled edge**, the same *graph* shape as
`MetricImpact` despite a different *authoring* shape. We unify them at the index
boundary so the BFS stays single-purpose.

- New `IdentityEdge(from_metric, to_metric, operator)` exposing `kind ==
  "identity"`. `MetricImpact` exposes `kind == "influence"`. On both, `kind` is a
  read-only property (or class constant) — no stored-field churn.
- New helper `identity_edges_from_metrics(metrics) -> list[IdentityEdge]`: for
  each metric's each decomposition's each operand, emit `IdentityEdge(parent →
  operand, operator)`.
- The impact-index builder is generalized to accept any edge exposing
  `.from_metric`/`.to_metric` and merges influence + identity edges into one
  index. `walk_metric_impacts` already reads only those two attributes, so it
  works over the union unchanged; its return type broadens to the edge union.

**Direction mapping for identity edges:** edge is `from_metric = parent`,
`to_metric = child`. Thus `direction="downstream"` from a parent yields its
components (drill *into* the metric); `direction="upstream"` from a component
yields the metrics it is *part of*. This is consistent with the influence graph's
downstream=affected / upstream=drivers semantics.

### `trace_metric_impacts` tool changes

- Build the merged index (influence + identity) in `create_tools`.
- Each emitted edge is tagged with `kind`. Identity edges emit `operator`
  (instead of `direction`/`confidence`/`evidence`); influence edges are
  unchanged.
- New optional arg `kinds`: `"all"` (default) | `"identity"` | `"influence"`.
  Lets the agent request the identity skeleton first (the article's "walk
  identity edges before influence edges" recipe), then the causal drivers.
- Tool description updated to explain the two edge kinds and the recommended
  identity-first ordering.

### `lookup_metric` tool changes (`_metric_details`)

Add `decompositions` (list of `{operator, operands}`) and `drill_by` (list of
`{dimension, column}`) to the serialized metric, following the existing
"omit when empty" convention. Gives the agent a metric's immediate breakdown in
one call; `trace_metric_impacts` gives the multi-level tree.

`list_metrics` is unchanged.

## Error handling

- Load-time structural errors raise `ValueError` with a message naming the
  offending metric and rule (e.g. `metric 'revenue' decomposition 'ratio'
  requires exactly 2 operands, got 3`).
- Cycle errors name the cycle path.
- Runtime tools never see invalid decompositions (load already failed), so tool
  code needs no decomposition-specific error branches beyond "metric not found".

## Testing

Follows the existing layout: `tests/test_semantic/`, `tests/test_tools/`,
fixtures in `tests/fixtures/`. TDD — tests first.

**Unit (`test_semantic`):**
- Dataclass construction; defaults are empty lists.
- `identity_edges_from_metrics` fan-out (multi-operand, multi-decomposition).
- `validate_decompositions`: each failure mode raises with a clear message;
  valid trees pass; leaf metrics pass; multi-decomposition passes.
- Cycle detection: direct self-reference, 2-cycle, transitive cycle all raise;
  a diamond (shared child, no cycle) passes.
- `drill_by` soft column validation: bad column with declared table raises;
  bad column with undeclared table is silently accepted.
- Roundtrip: `dump_semantic_source → YamlSource.from_raw` preserves both fields.

**Integration (`test_tools`):**
- Fixture with a multi-level tree (`Revenue = Users × ARPU`; `Users = New +
  Reactivated`; `ARPU` ratio; `Net = Gross − Refunds`) plus influence edges.
- `lookup_metric` surfaces `decompositions`/`drill_by`; omits them for a leaf.
- `trace_metric_impacts` walks identity edges (downstream = components, upstream
  = parents), returns mixed graph with `kind` tags, and honours the `kinds`
  filter.

## Files touched

- `src/agentic_data_contracts/semantic/base.py` — dataclasses, edge unification,
  `identity_edges_from_metrics`, `validate_decompositions`, `dump_semantic_source`.
- `src/agentic_data_contracts/semantic/yaml_source.py` — parse + call validation.
- `src/agentic_data_contracts/tools/factory.py` — merged index, `_metric_details`,
  `trace_metric_impacts`.
- Tests + a YAML fixture.

`DbtSource`/`CubeSource` are untouched (they inherit the empty defaults).
