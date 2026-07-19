# Metric Identity Decomposition Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add optional arithmetic decomposition (`decompositions`) and dimensional drill hints (`drill_by`) to metric definitions, and let the metric-graph traversal walk identity edges alongside influence edges.

**Architecture:** Two new dataclasses (`Decomposition`, `DrillDimension`) become optional, default-empty fields on `MetricDefinition`. A decomposition's operands are converted into typed `IdentityEdge`s that share the metric-graph index with the existing `MetricImpact` (influence) edges, so one BFS walks both. YAML loading validates decompositions loudly (operator, arity, operand resolution, DAG) and drill columns softly. Tools surface the new fields on `lookup_metric` and let `trace_metric_impacts` filter by edge kind.

**Tech Stack:** Python 3.12+, dataclasses, PyYAML, pytest. Spec: `docs/superpowers/specs/2026-07-19-metric-identity-decomposition-design.md`.

## Global Constraints

- Python 3.12+ syntax (`X | Y` unions, `list[...]` generics).
- Run linters/type-checks through prek: `prek run ruff-check --all-files`, `prek run ruff-format --all-files`, `prek run ty --all-files`. Never invoke bare `ruff`/`ty`.
- Run tests with `uv run pytest`.
- TDD: failing test first, minimal implementation, green, commit.
- `decompositions` and `drill_by` are OPTIONAL — default to empty lists; a leaf metric (no decomposition) is a valid, first-class state.
- Operands reference other declared metrics only (never raw columns/constants).
- `DbtSource` / `CubeSource` are NOT modified — they inherit the empty defaults.
- Work happens on branch `feat/metric-identity-decomposition` (already created).

---

### Task 1: Data model — `Decomposition`, `DrillDimension`, `MetricDefinition` fields, `MetricImpact.kind`

**Files:**
- Modify: `src/agentic_data_contracts/semantic/base.py` (dataclasses near lines 16–54)
- Test: `tests/test_semantic/test_decomposition.py` (create)

**Interfaces:**
- Produces: `Decomposition(operator: str, operands: list[str])`, `DrillDimension(dimension: str, column: str)`, `MetricDefinition.decompositions: list[Decomposition]`, `MetricDefinition.drill_by: list[DrillDimension]`, `MetricImpact.kind` property returning `"influence"`.

- [ ] **Step 1: Write the failing test**

Create `tests/test_semantic/test_decomposition.py`:

```python
"""Tests for metric identity decomposition + drill dimensions."""

from __future__ import annotations

from agentic_data_contracts.semantic.base import (
    Decomposition,
    DrillDimension,
    MetricDefinition,
    MetricImpact,
)


class TestDataModel:
    def test_metric_defaults_to_no_decomposition(self) -> None:
        m = MetricDefinition(name="signups", description="", sql_expression="COUNT(*)")
        assert m.decompositions == []
        assert m.drill_by == []

    def test_decomposition_holds_operator_and_operands(self) -> None:
        d = Decomposition(operator="product", operands=["paying_users", "arpu"])
        assert d.operator == "product"
        assert d.operands == ["paying_users", "arpu"]

    def test_drill_dimension_holds_dimension_and_column(self) -> None:
        dd = DrillDimension(dimension="region", column="analytics.dim_customer.region")
        assert dd.dimension == "region"
        assert dd.column == "analytics.dim_customer.region"

    def test_metric_impact_kind_is_influence(self) -> None:
        assert MetricImpact(from_metric="a", to_metric="b").kind == "influence"
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/test_semantic/test_decomposition.py -v`
Expected: FAIL with `ImportError: cannot import name 'Decomposition'`.

- [ ] **Step 3: Write minimal implementation**

In `src/agentic_data_contracts/semantic/base.py`, add the two dataclasses immediately before `MetricDefinition`:

```python
@dataclass
class Decomposition:
    """An arithmetic identity: how a metric is reconstructed from other metrics."""

    operator: str  # "sum" | "product" | "ratio" | "difference"
    operands: list[str] = field(default_factory=list)


@dataclass
class DrillDimension:
    """A dimension a metric can be exhaustively sliced by. List order = priority."""

    dimension: str
    column: str  # "schema.table.column" — same convention as Relationship endpoints
```

Add the two fields to `MetricDefinition` (after `last_reviewed`):

```python
    decompositions: list[Decomposition] = field(default_factory=list)
    drill_by: list[DrillDimension] = field(default_factory=list)
```

Add a `kind` property to `MetricImpact` (after its fields):

```python
    @property
    def kind(self) -> str:
        return "influence"
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/test_semantic/test_decomposition.py -v`
Expected: PASS (4 tests).

- [ ] **Step 5: Commit**

```bash
git add src/agentic_data_contracts/semantic/base.py tests/test_semantic/test_decomposition.py
git commit -m "feat: add Decomposition/DrillDimension data model to metrics"
```

---

### Task 2: `IdentityEdge` + `identity_edges_from_metrics`

**Files:**
- Modify: `src/agentic_data_contracts/semantic/base.py`
- Test: `tests/test_semantic/test_decomposition.py`

**Interfaces:**
- Consumes: `Decomposition`, `MetricDefinition` (Task 1).
- Produces: `IdentityEdge(from_metric: str, to_metric: str, operator: str)` with `.kind == "identity"`; `identity_edges_from_metrics(metrics: list[MetricDefinition]) -> list[IdentityEdge]`.

- [ ] **Step 1: Write the failing test**

Append to `tests/test_semantic/test_decomposition.py`:

```python
from agentic_data_contracts.semantic.base import (  # noqa: E402
    IdentityEdge,
    identity_edges_from_metrics,
)


def _metric(name: str, decompositions: list[Decomposition] | None = None) -> MetricDefinition:
    return MetricDefinition(
        name=name,
        description="",
        sql_expression="x",
        decompositions=decompositions or [],
    )


class TestIdentityEdges:
    def test_edge_kind_is_identity(self) -> None:
        assert IdentityEdge(from_metric="revenue", to_metric="arpu", operator="product").kind == "identity"

    def test_fans_out_one_edge_per_operand(self) -> None:
        metrics = [
            _metric("revenue", [Decomposition(operator="product", operands=["paying_users", "arpu"])]),
            _metric("paying_users"),
            _metric("arpu"),
        ]
        edges = identity_edges_from_metrics(metrics)
        assert {(e.from_metric, e.to_metric, e.operator) for e in edges} == {
            ("revenue", "paying_users", "product"),
            ("revenue", "arpu", "product"),
        }

    def test_multiple_decompositions_all_contribute(self) -> None:
        metrics = [
            _metric(
                "revenue",
                [
                    Decomposition(operator="product", operands=["paying_users", "arpu"]),
                    Decomposition(operator="sum", operands=["new_revenue", "expansion_revenue"]),
                ],
            ),
        ]
        edges = identity_edges_from_metrics(metrics)
        assert len(edges) == 4

    def test_leaf_metric_produces_no_edges(self) -> None:
        assert identity_edges_from_metrics([_metric("signups")]) == []
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/test_semantic/test_decomposition.py::TestIdentityEdges -v`
Expected: FAIL with `ImportError: cannot import name 'IdentityEdge'`.

- [ ] **Step 3: Write minimal implementation**

In `base.py`, add the `IdentityEdge` dataclass after `MetricImpact`:

```python
@dataclass
class IdentityEdge:
    """A directed identity edge parent -> operand in the metric graph."""

    from_metric: str  # parent metric
    to_metric: str  # operand metric
    operator: str  # the decomposition operator that produced this edge

    @property
    def kind(self) -> str:
        return "identity"


MetricEdge = MetricImpact | IdentityEdge
```

Add the builder after `MetricImpact`/`IdentityEdge` (near the other graph helpers is also fine, but keep it close to the dataclasses):

```python
def identity_edges_from_metrics(
    metrics: list[MetricDefinition],
) -> list[IdentityEdge]:
    """Flatten each metric's decompositions into directed identity edges.

    For every operand of every decomposition, emit one edge
    ``parent -> operand`` carrying the decomposition's operator. Leaf metrics
    (no decompositions) contribute nothing.
    """
    edges: list[IdentityEdge] = []
    for metric in metrics:
        for decomp in metric.decompositions:
            for operand in decomp.operands:
                edges.append(
                    IdentityEdge(
                        from_metric=metric.name,
                        to_metric=operand,
                        operator=decomp.operator,
                    )
                )
    return edges
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/test_semantic/test_decomposition.py -v`
Expected: PASS (all tests).

- [ ] **Step 5: Commit**

```bash
git add src/agentic_data_contracts/semantic/base.py tests/test_semantic/test_decomposition.py
git commit -m "feat: add IdentityEdge and identity_edges_from_metrics"
```

---

### Task 3: `validate_decompositions` (operator, arity, operand, DAG)

**Files:**
- Modify: `src/agentic_data_contracts/semantic/base.py`
- Test: `tests/test_semantic/test_decomposition.py`

**Interfaces:**
- Consumes: `MetricDefinition`, `Decomposition` (Task 1).
- Produces: `validate_decompositions(metrics: list[MetricDefinition]) -> None` (raises `ValueError` on any structural error); module constant `VALID_OPERATORS: frozenset[str]`.

- [ ] **Step 1: Write the failing test**

Append to `tests/test_semantic/test_decomposition.py`:

```python
import pytest  # noqa: E402

from agentic_data_contracts.semantic.base import validate_decompositions  # noqa: E402


def _m(name: str, decompositions: list[Decomposition] | None = None) -> MetricDefinition:
    return MetricDefinition(
        name=name, description="", sql_expression="x", decompositions=decompositions or []
    )


class TestValidateDecompositions:
    def test_valid_tree_passes(self) -> None:
        metrics = [
            _m("revenue", [Decomposition("product", ["paying_users", "arpu"])]),
            _m("paying_users"),
            _m("arpu"),
        ]
        validate_decompositions(metrics)  # no raise

    def test_leaf_only_passes(self) -> None:
        validate_decompositions([_m("signups")])

    def test_unknown_operator_raises(self) -> None:
        metrics = [_m("revenue", [Decomposition("divide", ["a", "b"])]), _m("a"), _m("b")]
        with pytest.raises(ValueError, match="unknown operator"):
            validate_decompositions(metrics)

    def test_ratio_requires_exactly_two_operands(self) -> None:
        metrics = [_m("rate", [Decomposition("ratio", ["a", "b", "c"])]), _m("a"), _m("b"), _m("c")]
        with pytest.raises(ValueError, match="exactly 2 operands"):
            validate_decompositions(metrics)

    def test_sum_requires_at_least_two_operands(self) -> None:
        metrics = [_m("revenue", [Decomposition("sum", ["a"])]), _m("a")]
        with pytest.raises(ValueError, match="at least 2 operands"):
            validate_decompositions(metrics)

    def test_unresolved_operand_raises(self) -> None:
        metrics = [_m("revenue", [Decomposition("product", ["paying_users", "ghost"])]), _m("paying_users")]
        with pytest.raises(ValueError, match="unknown metric 'ghost'"):
            validate_decompositions(metrics)

    def test_self_reference_raises(self) -> None:
        metrics = [_m("revenue", [Decomposition("sum", ["revenue", "arpu"])]), _m("arpu")]
        with pytest.raises(ValueError, match="itself"):
            validate_decompositions(metrics)

    def test_two_cycle_raises(self) -> None:
        metrics = [
            _m("a", [Decomposition("sum", ["b", "c"])]),
            _m("b", [Decomposition("sum", ["a", "c"])]),
            _m("c"),
        ]
        with pytest.raises(ValueError, match="cycle"):
            validate_decompositions(metrics)

    def test_diamond_is_not_a_cycle(self) -> None:
        # a -> b, a -> c, b -> d, c -> d : shared child, no cycle
        metrics = [
            _m("a", [Decomposition("sum", ["b", "c"])]),
            _m("b", [Decomposition("sum", ["d", "e"])]),
            _m("c", [Decomposition("sum", ["d", "e"])]),
            _m("d"),
            _m("e"),
        ]
        validate_decompositions(metrics)  # no raise
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/test_semantic/test_decomposition.py::TestValidateDecompositions -v`
Expected: FAIL with `ImportError: cannot import name 'validate_decompositions'`.

- [ ] **Step 3: Write minimal implementation**

In `base.py`, add near the top (after imports) the operator constants:

```python
VALID_OPERATORS = frozenset({"sum", "product", "ratio", "difference"})
_BINARY_OPERATORS = frozenset({"ratio", "difference"})
```

Add the validator (place it after `identity_edges_from_metrics`):

```python
def validate_decompositions(metrics: list[MetricDefinition]) -> None:
    """Validate every declared decomposition; raise ``ValueError`` on any fault.

    Optional to declare, validated only when present. Checks operator, operand
    arity, operand resolution, and that the identity edges form a DAG (a metric
    cannot transitively decompose into itself). Leaf metrics pass untouched.
    """
    names = {m.name for m in metrics}
    adjacency: dict[str, list[str]] = {}
    for metric in metrics:
        for decomp in metric.decompositions:
            if decomp.operator not in VALID_OPERATORS:
                raise ValueError(
                    f"metric {metric.name!r} decomposition has unknown operator "
                    f"{decomp.operator!r}; expected one of {sorted(VALID_OPERATORS)}"
                )
            count = len(decomp.operands)
            if decomp.operator in _BINARY_OPERATORS and count != 2:
                raise ValueError(
                    f"metric {metric.name!r} decomposition {decomp.operator!r} "
                    f"requires exactly 2 operands, got {count}"
                )
            if decomp.operator not in _BINARY_OPERATORS and count < 2:
                raise ValueError(
                    f"metric {metric.name!r} decomposition {decomp.operator!r} "
                    f"requires at least 2 operands, got {count}"
                )
            for operand in decomp.operands:
                if operand == metric.name:
                    raise ValueError(
                        f"metric {metric.name!r} decomposition cannot reference itself"
                    )
                if operand not in names:
                    raise ValueError(
                        f"metric {metric.name!r} decomposition references "
                        f"unknown metric {operand!r}"
                    )
            adjacency.setdefault(metric.name, []).extend(decomp.operands)
    _assert_identity_acyclic(adjacency)


def _assert_identity_acyclic(adjacency: dict[str, list[str]]) -> None:
    """DFS the identity graph; raise ``ValueError`` naming the first cycle found."""
    visiting: set[str] = set()
    visited: set[str] = set()

    def visit(node: str, stack: list[str]) -> None:
        if node in visiting:
            cycle = " -> ".join([*stack, node])
            raise ValueError(f"metric decomposition cycle detected: {cycle}")
        if node in visited:
            return
        visiting.add(node)
        for neighbor in adjacency.get(node, []):
            visit(neighbor, [*stack, node])
        visiting.discard(node)
        visited.add(node)

    for node in list(adjacency):
        visit(node, [])
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/test_semantic/test_decomposition.py::TestValidateDecompositions -v`
Expected: PASS (9 tests).

- [ ] **Step 5: Commit**

```bash
git add src/agentic_data_contracts/semantic/base.py tests/test_semantic/test_decomposition.py
git commit -m "feat: validate metric decompositions (operator/arity/operand/DAG)"
```

---

### Task 4: `validate_drill_by` (soft column check)

**Files:**
- Modify: `src/agentic_data_contracts/semantic/base.py`
- Test: `tests/test_semantic/test_decomposition.py`

**Interfaces:**
- Consumes: `MetricDefinition`, `DrillDimension` (Task 1); `TableSchema`, `Column` from `agentic_data_contracts.adapters.base` (already imported in `base.py` as `TableSchema`).
- Produces: `validate_drill_by(metrics: list[MetricDefinition], table_schemas: dict[str, TableSchema]) -> None`.

- [ ] **Step 1: Write the failing test**

Append to `tests/test_semantic/test_decomposition.py`:

```python
from agentic_data_contracts.adapters.base import Column, TableSchema  # noqa: E402
from agentic_data_contracts.semantic.base import validate_drill_by  # noqa: E402


def _md(name: str, drill_by: list[DrillDimension]) -> MetricDefinition:
    return MetricDefinition(
        name=name, description="", sql_expression="x", drill_by=drill_by
    )


class TestValidateDrillBy:
    def test_declared_column_passes(self) -> None:
        schemas = {"analytics.dim_customer": TableSchema(columns=[Column(name="region", type="VARCHAR")])}
        metrics = [_md("revenue", [DrillDimension("region", "analytics.dim_customer.region")])]
        validate_drill_by(metrics, schemas)  # no raise

    def test_unknown_column_in_declared_table_raises(self) -> None:
        schemas = {"analytics.dim_customer": TableSchema(columns=[Column(name="region", type="VARCHAR")])}
        metrics = [_md("revenue", [DrillDimension("segment", "analytics.dim_customer.segment")])]
        with pytest.raises(ValueError, match="unknown column"):
            validate_drill_by(metrics, schemas)

    def test_undeclared_table_is_skipped_silently(self) -> None:
        metrics = [_md("revenue", [DrillDimension("plan", "analytics.dim_plan.tier")])]
        validate_drill_by(metrics, {})  # table not declared -> soft skip, no raise

    def test_malformed_column_raises(self) -> None:
        metrics = [_md("revenue", [DrillDimension("region", "region")])]
        with pytest.raises(ValueError, match="schema.table.column"):
            validate_drill_by(metrics, {})
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/test_semantic/test_decomposition.py::TestValidateDrillBy -v`
Expected: FAIL with `ImportError: cannot import name 'validate_drill_by'`.

- [ ] **Step 3: Write minimal implementation**

In `base.py`, add after `validate_decompositions`:

```python
def validate_drill_by(
    metrics: list[MetricDefinition],
    table_schemas: dict[str, TableSchema],
) -> None:
    """Validate drill-by column references.

    A column is ``"schema.table.column"``. The ``schema.table`` portion keys
    into *table_schemas*. Column existence is checked only when that table is
    declared; when the table is absent (schemas are optional in these
    contracts), the check is skipped silently. A malformed reference (no
    ``schema.table.column`` shape) always raises.
    """
    for metric in metrics:
        for drill in metric.drill_by:
            table_key, _, column = drill.column.rpartition(".")
            if not table_key or not column or "." not in table_key:
                raise ValueError(
                    f"metric {metric.name!r} drill_by column {drill.column!r} "
                    f"must be 'schema.table.column'"
                )
            schema = table_schemas.get(table_key)
            if schema is None:
                continue  # table not declared — soft skip
            if not any(col.name == column for col in schema.columns):
                raise ValueError(
                    f"metric {metric.name!r} drill_by references unknown column "
                    f"{drill.column!r}"
                )
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/test_semantic/test_decomposition.py::TestValidateDrillBy -v`
Expected: PASS (4 tests).

- [ ] **Step 5: Commit**

```bash
git add src/agentic_data_contracts/semantic/base.py tests/test_semantic/test_decomposition.py
git commit -m "feat: soft-validate drill_by column references"
```

---

### Task 5: YAML loading — parse `decompositions` / `drill_by`, run validation

**Files:**
- Modify: `src/agentic_data_contracts/semantic/yaml_source.py`
- Test: `tests/test_semantic/test_decomposition.py`

**Interfaces:**
- Consumes: `Decomposition`, `DrillDimension`, `validate_decompositions`, `validate_drill_by` (Tasks 1/3/4).
- Produces: `YamlSource` metrics carry parsed `decompositions`/`drill_by`; `YamlSource(path)` and `YamlSource.from_raw(raw)` raise `ValueError` on invalid decompositions.

- [ ] **Step 1: Write the failing test**

Append to `tests/test_semantic/test_decomposition.py`:

```python
from agentic_data_contracts.semantic.yaml_source import YamlSource  # noqa: E402


class TestYamlSourceLoading:
    def test_parses_decompositions_and_drill_by(self) -> None:
        raw = {
            "metrics": [
                {
                    "name": "revenue",
                    "sql_expression": "SUM(amount)",
                    "decompositions": [
                        {"operator": "product", "operands": ["paying_users", "arpu"]}
                    ],
                    "drill_by": [
                        {"dimension": "region", "column": "analytics.dim_customer.region"}
                    ],
                },
                {"name": "paying_users", "sql_expression": "x"},
                {"name": "arpu", "sql_expression": "x"},
            ]
        }
        source = YamlSource.from_raw(raw)
        revenue = source.get_metric("revenue")
        assert revenue is not None
        assert revenue.decompositions[0].operator == "product"
        assert revenue.decompositions[0].operands == ["paying_users", "arpu"]
        assert revenue.drill_by[0].dimension == "region"

    def test_leaf_metric_has_empty_lists(self) -> None:
        source = YamlSource.from_raw({"metrics": [{"name": "signups", "sql_expression": "COUNT(*)"}]})
        signups = source.get_metric("signups")
        assert signups is not None
        assert signups.decompositions == []
        assert signups.drill_by == []

    def test_invalid_decomposition_raises_at_load(self) -> None:
        raw = {
            "metrics": [
                {"name": "revenue", "sql_expression": "x",
                 "decompositions": [{"operator": "product", "operands": ["ghost", "arpu"]}]},
                {"name": "arpu", "sql_expression": "x"},
            ]
        }
        with pytest.raises(ValueError, match="unknown metric 'ghost'"):
            YamlSource.from_raw(raw)
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/test_semantic/test_decomposition.py::TestYamlSourceLoading -v`
Expected: FAIL — `revenue.decompositions` is `[]` (fields not parsed yet) / no raise.

- [ ] **Step 3: Write minimal implementation**

In `yaml_source.py`, extend the imports from `base`:

```python
from agentic_data_contracts.semantic.base import (
    Decomposition,
    DrillDimension,
    MetricDefinition,
    MetricImpact,
    Relationship,
    build_relationship_index,
    fuzzy_search_metrics,
    validate_decompositions,
    validate_drill_by,
)
```

Inside `_load_from_raw`, in the metric-building loop, add the two parsed lists to the `MetricDefinition(...)` construction (after `last_reviewed=...`):

```python
                    decompositions=[
                        Decomposition(
                            operator=d["operator"],
                            operands=list(d.get("operands", [])),
                        )
                        for d in m.get("decompositions", [])
                    ],
                    drill_by=[
                        DrillDimension(dimension=dd["dimension"], column=dd["column"])
                        for dd in m.get("drill_by", [])
                    ],
```

At the END of `_load_from_raw` (after `self._metric_impacts = [...]`), add the validation calls — they must run after both metrics and tables are built:

```python
        validate_decompositions(self._metrics)
        validate_drill_by(self._metrics, self._tables)
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/test_semantic/test_decomposition.py::TestYamlSourceLoading -v`
Expected: PASS (3 tests). Also run the existing YAML suite to confirm no regression: `uv run pytest tests/test_semantic/test_yaml_source.py -v` → PASS.

- [ ] **Step 5: Commit**

```bash
git add src/agentic_data_contracts/semantic/yaml_source.py tests/test_semantic/test_decomposition.py
git commit -m "feat: parse and validate decompositions/drill_by in YamlSource"
```

---

### Task 6: Frozen-contract roundtrip — `dump_semantic_source`

**Files:**
- Modify: `src/agentic_data_contracts/semantic/base.py` (`dump_semantic_source`, the metrics list comprehension near lines 99–114)
- Test: `tests/test_semantic/test_decomposition.py`

**Interfaces:**
- Consumes: `dump_semantic_source` (existing), `YamlSource.from_raw` (Task 5).
- Produces: `dump_semantic_source` output includes per-metric `decompositions` and `drill_by`, roundtripping through `from_raw`.

- [ ] **Step 1: Write the failing test**

Append to `tests/test_semantic/test_decomposition.py`:

```python
from agentic_data_contracts.semantic.base import dump_semantic_source  # noqa: E402


class TestRoundtrip:
    def test_dump_then_from_raw_preserves_fields(self) -> None:
        raw = {
            "metrics": [
                {"name": "revenue", "sql_expression": "SUM(amount)",
                 "decompositions": [{"operator": "product", "operands": ["paying_users", "arpu"]}],
                 "drill_by": [{"dimension": "region", "column": "analytics.dim_customer.region"}]},
                {"name": "paying_users", "sql_expression": "x"},
                {"name": "arpu", "sql_expression": "x"},
            ]
        }
        source = YamlSource.from_raw(raw)
        dumped = dump_semantic_source(source)
        rebuilt = YamlSource.from_raw(dumped)
        revenue = rebuilt.get_metric("revenue")
        assert revenue is not None
        assert revenue.decompositions[0].operands == ["paying_users", "arpu"]
        assert revenue.drill_by[0].column == "analytics.dim_customer.region"
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/test_semantic/test_decomposition.py::TestRoundtrip -v`
Expected: FAIL — rebuilt `revenue.decompositions` is `[]` (dump dropped the fields).

- [ ] **Step 3: Write minimal implementation**

In `dump_semantic_source`, inside the `"metrics": [...]` comprehension, add two keys to each metric dict (after `"last_reviewed": _iso(m.last_reviewed),`):

```python
                "decompositions": [
                    {"operator": d.operator, "operands": list(d.operands)}
                    for d in m.decompositions
                ],
                "drill_by": [
                    {"dimension": dd.dimension, "column": dd.column}
                    for dd in m.drill_by
                ],
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/test_semantic/test_decomposition.py::TestRoundtrip -v`
Expected: PASS. Also run `uv run pytest tests/test_semantic -v` → PASS.

- [ ] **Step 5: Commit**

```bash
git add src/agentic_data_contracts/semantic/base.py tests/test_semantic/test_decomposition.py
git commit -m "feat: roundtrip decompositions/drill_by through dump_semantic_source"
```

---

### Task 7: Unify edge types for traversal (`build_metric_impact_index` / `walk_metric_impacts`)

**Files:**
- Modify: `src/agentic_data_contracts/semantic/base.py` (signatures of `build_metric_impact_index`, `walk_metric_impacts`)
- Modify: `src/agentic_data_contracts/tools/factory.py` (`_metric_details` loop — add an isinstance guard so ty stays green)
- Test: `tests/test_semantic/test_decomposition.py`

**Interfaces:**
- Consumes: `MetricEdge` alias, `IdentityEdge`, `MetricImpact` (Tasks 1/2).
- Produces: `build_metric_impact_index(impacts: Sequence[MetricEdge]) -> dict[str, list[MetricEdge]]` and `walk_metric_impacts(index: dict[str, list[MetricEdge]], start, *, direction, max_depth=2) -> list[tuple[int, MetricEdge]]` now accept mixed influence + identity edges. Logic unchanged (both only read `.from_metric`/`.to_metric`).

- [ ] **Step 1: Write the failing test**

Append to `tests/test_semantic/test_decomposition.py`:

```python
from agentic_data_contracts.semantic.base import (  # noqa: E402
    build_metric_impact_index,
    walk_metric_impacts,
)


class TestMixedGraphTraversal:
    def test_walk_identity_downstream_returns_components(self) -> None:
        edges = identity_edges_from_metrics(
            [_metric("revenue", [Decomposition("product", ["paying_users", "arpu"])])]
        )
        index = build_metric_impact_index(edges)
        walk = walk_metric_impacts(index, "revenue", direction="downstream", max_depth=2)
        assert {e.to_metric for _, e in walk} == {"paying_users", "arpu"}
        assert all(e.kind == "identity" for _, e in walk)

    def test_walk_identity_upstream_returns_parents(self) -> None:
        edges = identity_edges_from_metrics(
            [_metric("revenue", [Decomposition("product", ["paying_users", "arpu"])])]
        )
        index = build_metric_impact_index(edges)
        walk = walk_metric_impacts(index, "arpu", direction="upstream", max_depth=2)
        assert {e.from_metric for _, e in walk} == {"revenue"}

    def test_index_mixes_influence_and_identity(self) -> None:
        influence = [MetricImpact(from_metric="conv", to_metric="revenue")]
        identity = identity_edges_from_metrics(
            [_metric("revenue", [Decomposition("product", ["paying_users", "arpu"])])]
        )
        index = build_metric_impact_index([*influence, *identity])
        kinds = {e.kind for edges in index.values() for e in edges}
        assert kinds == {"influence", "identity"}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/test_semantic/test_decomposition.py::TestMixedGraphTraversal -v`
Expected: FAIL — `prek run ty --all-files` reports `build_metric_impact_index` rejects `IdentityEdge` (typed `list[MetricImpact]`). (The runtime logic already works; this task is about broadening the types and keeping ty green.)

- [ ] **Step 3: Write minimal implementation**

In `base.py`, add `Sequence` to the typing import at the top:

```python
from collections.abc import Callable, Sequence
```

Broaden `build_metric_impact_index`:

```python
def build_metric_impact_index(
    impacts: Sequence[MetricEdge],
) -> dict[str, list[MetricEdge]]:
```

(and its internal `index: dict[str, list[MetricEdge]] = {}` annotation).

Broaden `walk_metric_impacts`:

```python
def walk_metric_impacts(
    index: dict[str, list[MetricEdge]],
    start: str,
    *,
    direction: str,
    max_depth: int = 2,
) -> list[tuple[int, MetricEdge]]:
```

(and its internal `result: list[tuple[int, MetricEdge]] = []` annotation). No logic changes.

In `factory.py`, in `_metric_details`, guard the impact loop so ty knows only `MetricImpact` reaches `_format_impact_edge` (the index passed here only ever holds influence edges, so this skips nothing at runtime). Change:

```python
    for edge in impact_index.get(metric.name, []):
        if edge.from_metric == metric.name:
```

to:

```python
    for edge in impact_index.get(metric.name, []):
        if not isinstance(edge, MetricImpact):
            continue
        if edge.from_metric == metric.name:
```

Ensure `MetricImpact` is imported in `factory.py` (it already is — confirm the import block near the top includes it).

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/test_semantic/test_decomposition.py::TestMixedGraphTraversal -v`
Expected: PASS.
Run: `prek run ty --all-files`
Expected: Passed.
Run: `uv run pytest tests/test_semantic/test_metric_impacts.py tests/test_tools/test_metric_impacts_tools.py -v`
Expected: PASS (no regression to existing graph tests).

- [ ] **Step 5: Commit**

```bash
git add src/agentic_data_contracts/semantic/base.py src/agentic_data_contracts/tools/factory.py tests/test_semantic/test_decomposition.py
git commit -m "feat: unify influence+identity edges in metric graph traversal"
```

---

### Task 8: Surface `decompositions` / `drill_by` on `lookup_metric`

**Files:**
- Modify: `src/agentic_data_contracts/tools/factory.py` (`_metric_details`, near lines 104–118)
- Create: `tests/fixtures/decomposition_source.yml`
- Test: `tests/test_tools/test_decomposition_tools.py` (create)

**Interfaces:**
- Consumes: `_metric_details` (existing), `create_tools` (existing), `MetricDefinition.decompositions`/`drill_by` (Task 1).
- Produces: `lookup_metric` JSON includes `decompositions` (list of `{operator, operands}`) and `drill_by` (list of `{dimension, column}`) when non-empty; omitted for leaf metrics.

- [ ] **Step 1: Write the failing test**

Create `tests/fixtures/decomposition_source.yml`:

```yaml
metrics:
  - name: revenue
    description: "Total revenue"
    sql_expression: "SUM(amount)"
    decompositions:
      - operator: product
        operands: [paying_users, arpu]
      - operator: sum
        operands: [new_revenue, expansion_revenue]
    drill_by:
      - {dimension: region, column: analytics.dim_customer.region}
  - name: paying_users
    description: "Distinct paying customers"
    sql_expression: "COUNT(DISTINCT customer_id)"
  - name: arpu
    description: "Average revenue per user"
    sql_expression: "SUM(amount) / NULLIF(COUNT(DISTINCT customer_id), 0)"
    decompositions:
      - operator: ratio
        operands: [revenue, paying_users]
  - name: new_revenue
    description: "Revenue from new customers"
    sql_expression: "SUM(amount) FILTER (WHERE is_new)"
  - name: expansion_revenue
    description: "Revenue expansion from existing customers"
    sql_expression: "SUM(amount) FILTER (WHERE is_expansion)"

tables:
  - schema: analytics
    table: dim_customer
    columns:
      - {name: region, type: VARCHAR, description: "Geographic region"}

metric_impacts:
  - from: paying_users
    to: revenue
    direction: positive
    confidence: verified
    evidence: "trivially true"
```

Note: `arpu = ratio(revenue, paying_users)` while `revenue = product(paying_users, arpu)` — these are two *different* decompositions of two *different* metrics and do NOT form a cycle in the identity graph (`revenue -> arpu -> revenue` would; but here it is `revenue -> {paying_users, arpu}` and `arpu -> {revenue, paying_users}`, which IS a cycle). **Fix:** give `arpu` no decomposition to keep the fixture acyclic. Use this corrected `arpu` entry instead:

```yaml
  - name: arpu
    description: "Average revenue per user"
    sql_expression: "SUM(amount) / NULLIF(COUNT(DISTINCT customer_id), 0)"
```

Create `tests/test_tools/test_decomposition_tools.py`:

```python
"""Integration tests for decomposition surfacing on the tool layer."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from agentic_data_contracts.core.contract import DataContract
from agentic_data_contracts.semantic.yaml_source import YamlSource
from agentic_data_contracts.tools.factory import create_tools

FIXTURE = Path(__file__).parent.parent / "fixtures" / "decomposition_source.yml"


def _tools() -> dict:
    contract = DataContract(
        name="test",
        tables=["analytics.dim_customer"],
        forbidden_operations=["DELETE"],
    )
    source = YamlSource(FIXTURE)
    tools = create_tools(contract, semantic_source=source)
    return {t.name: t.callable for t in tools}


async def _call(fn, **args) -> dict:
    result = await fn(args)
    return json.loads(result["content"][0]["text"])


class TestLookupMetricSurfacesDecomposition:
    @pytest.mark.asyncio
    async def test_includes_decompositions_and_drill_by(self) -> None:
        data = await _call(_tools()["lookup_metric"], metric_name="revenue")
        assert {"operator": "product", "operands": ["paying_users", "arpu"]} in data["decompositions"]
        assert {"dimension": "region", "column": "analytics.dim_customer.region"} in data["drill_by"]

    @pytest.mark.asyncio
    async def test_leaf_metric_omits_fields(self) -> None:
        data = await _call(_tools()["lookup_metric"], metric_name="paying_users")
        assert "decompositions" not in data
        assert "drill_by" not in data
```

Confirm the tool response shape (`result["content"][0]["text"]`) matches how other tests in `tests/test_tools/test_semantic_tools.py` read tool output; mirror that exact accessor if it differs.

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/test_tools/test_decomposition_tools.py -v`
Expected: FAIL — `KeyError: 'decompositions'` (not yet surfaced).

- [ ] **Step 3: Write minimal implementation**

In `factory.py` `_metric_details`, after the `indicator_kind` block and before `data.update(owner_context(...))`, add:

```python
    if metric.decompositions:
        data["decompositions"] = [
            {"operator": d.operator, "operands": list(d.operands)}
            for d in metric.decompositions
        ]
    if metric.drill_by:
        data["drill_by"] = [
            {"dimension": dd.dimension, "column": dd.column}
            for dd in metric.drill_by
        ]
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/test_tools/test_decomposition_tools.py -v`
Expected: PASS (2 tests).

- [ ] **Step 5: Commit**

```bash
git add src/agentic_data_contracts/tools/factory.py tests/test_tools/test_decomposition_tools.py tests/fixtures/decomposition_source.yml
git commit -m "feat: surface decompositions/drill_by on lookup_metric"
```

---

### Task 9: `trace_metric_impacts` walks identity edges + `kinds` filter

**Files:**
- Modify: `src/agentic_data_contracts/tools/factory.py` (`create_tools` index setup near lines 184–190; `trace_metric_impacts` body near lines 624–665; tool schema/description near lines 948–981)
- Test: `tests/test_tools/test_decomposition_tools.py`

**Interfaces:**
- Consumes: `identity_edges_from_metrics`, `build_metric_impact_index`, `walk_metric_impacts`, `IdentityEdge`, `MetricEdge` (Tasks 2/7).
- Produces: `trace_metric_impacts` accepts `kinds` in `{"all","identity","influence"}` (default `"all"`); each returned edge carries `kind`; identity edges emit `operator` (not `direction`/`confidence`/`evidence`).

- [ ] **Step 1: Write the failing test**

Append to `tests/test_tools/test_decomposition_tools.py`:

```python
class TestTraceWalksIdentity:
    @pytest.mark.asyncio
    async def test_downstream_all_includes_identity_and_influence(self) -> None:
        data = await _call(_tools()["trace_metric_impacts"], metric_name="revenue", direction="downstream")
        kinds = {e["kind"] for e in data["edges"]}
        assert "identity" in kinds
        identity_edges = [e for e in data["edges"] if e["kind"] == "identity"]
        assert any(e["to"] == "arpu" and e["operator"] == "product" for e in identity_edges)

    @pytest.mark.asyncio
    async def test_kinds_identity_excludes_influence(self) -> None:
        data = await _call(
            _tools()["trace_metric_impacts"], metric_name="revenue", direction="upstream", kinds="identity"
        )
        assert all(e["kind"] == "identity" for e in data["edges"])

    @pytest.mark.asyncio
    async def test_kinds_influence_excludes_identity(self) -> None:
        data = await _call(
            _tools()["trace_metric_impacts"], metric_name="revenue", direction="upstream", kinds="influence"
        )
        assert all(e["kind"] == "influence" for e in data["edges"])

    @pytest.mark.asyncio
    async def test_invalid_kinds_returns_message(self) -> None:
        result = await _tools()["trace_metric_impacts"]({"metric_name": "revenue", "kinds": "bogus"})
        assert "kinds must be" in result["content"][0]["text"]
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/test_tools/test_decomposition_tools.py::TestTraceWalksIdentity -v`
Expected: FAIL — no `identity` kinds appear (tool only walks influence edges; no `kind` key).

- [ ] **Step 3: Write minimal implementation**

In `factory.py`, extend the `base` import to include `identity_edges_from_metrics`, `IdentityEdge`, and `MetricEdge`:

```python
    IdentityEdge,
    MetricEdge,
    MetricImpact,
    build_metric_impact_index,
    identity_edges_from_metrics,
    walk_metric_impacts,
```

In `create_tools`, right after `_impact_index = build_metric_impact_index(_metric_impacts)` (which stays influence-only, used by `_metric_details`), add the identity edge list:

```python
    _identity_edges: list[IdentityEdge] = (
        identity_edges_from_metrics(semantic_source.get_metrics())
        if semantic_source is not None
        else []
    )
```

Replace the body of `trace_metric_impacts` from the `kinds`/index construction through the `edges = [...]` build. After the existing `direction` and `metric not found` guards, insert the `kinds` handling and build a per-call merged index, then render edges kind-aware:

```python
        kinds = args.get("kinds", "all")
        if kinds not in ("all", "identity", "influence"):
            return _text_response(
                f"kinds must be 'all', 'identity', or 'influence', got {kinds!r}."
            )
        graph_edges: list[MetricEdge] = []
        if kinds in ("all", "influence"):
            graph_edges.extend(_metric_impacts)
        if kinds in ("all", "identity"):
            graph_edges.extend(_identity_edges)
        graph_index = build_metric_impact_index(graph_edges)

        walk = walk_metric_impacts(
            graph_index, metric_name, direction=direction, max_depth=max_depth
        )
        edges: list[dict[str, Any]] = []
        for depth, edge in walk:
            entry: dict[str, Any] = {
                "depth": depth,
                "from": edge.from_metric,
                "to": edge.to_metric,
                "kind": edge.kind,
            }
            if isinstance(edge, IdentityEdge):
                entry["operator"] = edge.operator
            else:
                entry["direction"] = edge.direction
                entry["confidence"] = edge.confidence
                if edge.evidence:
                    entry["evidence"] = edge.evidence
                if edge.description:
                    entry["description"] = edge.description
            edges.append(entry)
```

(Leave the existing `return _text_response(json.dumps({...}))` block below; it already serializes `metric_name`, `direction`, `max_depth`, and `edges`.) Delete the OLD index-less `walk = walk_metric_impacts(_impact_index, ...)` line and the OLD `edges = [ ... ]` comprehension that this replaces.

Update the tool schema/description (near line 948). Add a `kinds` property to `input_schema["properties"]`:

```python
                    "kinds": {
                        "type": "string",
                        "enum": ["all", "identity", "influence"],
                        "description": (
                            "Which edge kinds to walk. 'identity' = arithmetic"
                            " decomposition (exhaustive, deterministic);"
                            " 'influence' = causal driver edges (with evidence);"
                            " 'all' (default) = both. For root-cause, walk"
                            " 'identity' first to localize the change, then"
                            " 'influence' for candidate explanations."
                        ),
                    },
```

Extend the tool `description` string to mention the two edge kinds, e.g. append:
`" Edges are tagged with 'kind': identity edges carry an 'operator' (sum/product/ratio/difference) and are exact; influence edges carry direction/confidence/evidence and are hypotheses."`

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/test_tools/test_decomposition_tools.py -v`
Expected: PASS (all decomposition tool tests).
Run: `uv run pytest tests/test_tools/test_metric_impacts_tools.py -v`
Expected: PASS (existing influence-only behavior unchanged — default `kinds="all"` still includes influence).

- [ ] **Step 5: Commit**

```bash
git add src/agentic_data_contracts/tools/factory.py tests/test_tools/test_decomposition_tools.py
git commit -m "feat: trace_metric_impacts walks identity edges with kinds filter"
```

---

### Task 10: Full suite + linters green, docs note

**Files:**
- Modify: `docs/architecture.md` (Semantic Layer section — one paragraph)
- No test file changes.

**Interfaces:** none (finalization).

- [ ] **Step 1: Run the whole suite**

Run: `uv run pytest -q`
Expected: all PASS.

- [ ] **Step 2: Run all linters/type-checks through prek**

Run: `prek run --all-files`
Expected: ruff-check, ruff-format, ty all Passed. Fix any findings inline (e.g. import ordering) and re-run until green.

- [ ] **Step 3: Add a short docs note**

In `docs/architecture.md`, under the "Semantic Layer" section, add a paragraph documenting `decompositions` (arithmetic identity edges: sum/product/ratio/difference, validated as a DAG) and `drill_by` (priority-ordered dimensional slice hints), and that `trace_metric_impacts` walks both edge kinds via its `kinds` filter. Note dbt/Cube extraction and the reconciliation check / diagnosis tool are deferred (Spec B).

- [ ] **Step 4: Commit**

```bash
git add docs/architecture.md
git commit -m "docs: document metric decompositions and drill_by in architecture"
```

- [ ] **Step 5: Update the Future Extensions note**

In `docs/architecture.md`, the "Future Extensions" bullet added for deterministic execution stays. Optionally add a one-line bullet: "Metric decomposition *reconciliation check* and *diagnosis tool* (Spec B) — execute parent vs. children to verify the arithmetic and attribute variance across a time window." Commit if changed:

```bash
git add docs/architecture.md
git commit -m "docs: note reconciliation check + diagnosis tool as Spec B follow-ups"
```

---

## Self-Review

**Spec coverage:**
- Data model (`Decomposition`, `DrillDimension`, fields) → Task 1. ✓
- Operator semantics / all four operators → Tasks 1 (model) + 3 (validation). ✓
- Multiple decompositions per metric → Task 2 (`identity_edges_from_metrics`) + Task 3 (validated independently). ✓
- Optional-to-declare, validated-when-present → Tasks 3/4/5. ✓
- Loud validation (operator/arity/operand/cycle) → Task 3. ✓
- Soft drill_by column check → Task 4. ✓
- YAML parse + validation wiring → Task 5. ✓
- Frozen-contract roundtrip → Task 6. ✓
- Edge unification + traversal → Task 7. ✓
- `lookup_metric` surfacing → Task 8. ✓
- `trace_metric_impacts` + `kinds` filter → Task 9. ✓
- dbt/Cube untouched (inherit empty defaults) → no task needed; confirmed by running full suite in Task 10. ✓
- Testing (unit + integration over multi-level fixture) → Tasks 1–9 tests + Task 8 fixture. ✓

**Placeholder scan:** No TBD/TODO. Every code step shows complete code. The fixture note in Task 8 explicitly corrects the `arpu` entry to keep the identity graph acyclic (a real trap: `revenue -> arpu` + `arpu -> revenue` is a cycle Task 3 would reject).

**Type consistency:** `Decomposition(operator, operands)`, `DrillDimension(dimension, column)`, `IdentityEdge(from_metric, to_metric, operator)`, `MetricEdge = MetricImpact | IdentityEdge`, `identity_edges_from_metrics(metrics)`, `validate_decompositions(metrics)`, `validate_drill_by(metrics, table_schemas)` — names used consistently across Tasks 1–9. Tool output keys (`decompositions`, `drill_by`, `kind`, `operator`) consistent between Tasks 8/9 and their tests.

**One risk flagged for the implementer:** the async tool-response accessor (`result["content"][0]["text"]`) in Task 8/9 tests is written to match the existing pattern in `tests/test_tools/`; if the repo's helper differs (e.g. a `_text` helper), mirror that instead — Step 1 of Task 8 says to confirm against `test_semantic_tools.py`.
