# Contract-Declared Sensitivity Checks Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Let a contract declare what a metric's query must respond to, and give
callers one verb that checks it by running the caller's own SQL against a
shadowed table.

**Architecture:** A `sensitivity:` list on a metric carries, per property, one
shadow SELECT that replaces one table plus the response the contract requires
(`unchanged` / `changes`). `check_sensitivity` parses the caller's SQL with
sqlglot, rewrites the *original text* at token spans to point the table at an
injected CTE, executes base and mutated through the `DatabaseAdapter`, and
compares. It reads and never writes: no DDL, no DML, no scratch schema. SQL is
never regenerated — only spans of the caller's own string are replaced — which
is what keeps the feature working on a dialect sqlglot can parse but not emit.

**Tech Stack:** Python ≥3.12, sqlglot (already a dependency, floor `>=28.6`),
`DatabaseAdapter` protocol, DuckDB for tests, pytest, ruff + ty via prek.

**Spec:** `docs/superpowers/specs/2026-09-08-sensitivity-checks-design.md` — read
it alongside this plan. This plan argues from it and deviates from it in three
places, each flagged inline as **DEVIATION** with the reason.

## Global Constraints

- Python `>=3.12`. Library version is `0.51.0`; this ships as **0.52.0**.
- **No new dependencies.** sqlglot and `DatabaseAdapter` are already required.
- sqlglot floor is **`>=28.6`** (`pyproject.toml:53`) while the lockfile
  resolves much newer. Every sqlglot symbol this feature uses must exist at the
  floor — Task 4 has an explicit step to verify that, per the house rule that a
  new symbol from a floored dep gets a lowest-floor run.
- Run linters **through prek**, never bare: `prek run --all-files`. Never name a
  tool version outside `.pre-commit-config.yaml`.
- Run everything Python through `uv run`. There is no bare `python` on PATH.
- **TDD, red first.** Write the failing test, watch it fail, then implement.
- **Digest stability is a hard requirement.** `dump_semantic_source` must omit
  `sensitivity` when empty so a contract that declares none freezes
  byte-identically to its pre-feature form. Every published `contract_digest`
  depends on this.
- **The library never writes.** It executes exactly two shapes: the caller's own
  query, and that query with one table shadowed by a contract-authored SELECT.
- Line length 88 (ruff default in this repo). Do not write `file.py:NNN`
  references in committed prose — a prek hook rejects them.
- **Ruff `select = ["E", "F", "I", "UP"]`, so E402 is on.** When a task says
  "append to" a test file that already exists, its imports go into that file's
  **top-of-file import block**, merged and kept sorted — never in a new block
  beside the appended class. An import lower down fails `prek run`.

---

## File Structure

| file | responsibility |
|---|---|
| `src/agentic_data_contracts/semantic/base.py` | `Shadow`, `SensitivityProperty`, `VALID_EXPECT`, `MetricDefinition.sensitivity`, `validate_sensitivity`, dump support |
| `src/agentic_data_contracts/semantic/yaml_source.py` | `SENSITIVITY_KEYS`, `SHADOW_KEYS`, `METRIC_KEYS` entry, key guard, `from_raw` construction |
| `src/agentic_data_contracts/validation/sensitivity.py` | **new** — rewrite engine, result types, `check_sensitivity`, `validate_sensitivity_tables` |
| `src/agentic_data_contracts/validation/__init__.py` | exports |
| `src/agentic_data_contracts/tools/factory.py` | `_metric_details` surfaces the properties |
| `tests/test_semantic/test_sensitivity.py` | **new** — parsing, validation, round-trip, digest stability |
| `tests/test_validation/test_sensitivity.py` | **new** — rewrite, result types, `check_sensitivity` |
| `tests/test_tools/test_lookup_metric_sensitivity.py` | **new** — tool surface |
| `examples/revenue_agent/check_sensitivity.py` | **new** — runnable demo |
| `examples/revenue_agent/semantic.yml` | gains one property on `total_revenue` |
| `README.md`, `docs/architecture.md` | docs |

---

### Task 1: Schema types and YAML parsing

**Files:**
- Modify: `src/agentic_data_contracts/semantic/base.py`
- Modify: `src/agentic_data_contracts/semantic/yaml_source.py`
- Test: `tests/test_semantic/test_sensitivity.py`

**Interfaces:**
- Consumes: nothing.
- Produces: `Shadow(table: str, sql: str)`, `SensitivityProperty(name: str,
  description: str, shadow: Shadow, expect: str)`, `VALID_EXPECT: frozenset[str]`,
  `MetricDefinition.sensitivity: list[SensitivityProperty]`,
  `SENSITIVITY_KEYS`, `SHADOW_KEYS`. All later tasks use these names.

**DEVIATION 1 — `list`, not `tuple`.** The spec writes
`sensitivity: tuple[SensitivityProperty, ...] = ()`. Use
`list[SensitivityProperty] = field(default_factory=list)` instead, matching
`decompositions` and `drill_by` on the same dataclass. They share the dump
logic, the key-guard loop and the tests; one tuple among two lists is a
difference a reader has to explain.

**DEVIATION 2 — `expect: str`, not `Literal[...]`.** `Decomposition.operator`
is a plain `str` validated against `VALID_OPERATORS`. Mirror that with
`VALID_EXPECT` so the vocabulary lives in one greppable frozenset rather than
in a type annotation the YAML loader cannot consult.

- [ ] **Step 1: Write the failing test**

Create `tests/test_semantic/test_sensitivity.py`:

```python
"""Contract-declared sensitivity properties: schema, parsing, validation."""

from __future__ import annotations

import pytest

from agentic_data_contracts.semantic.base import (
    MetricDefinition,
    SensitivityProperty,
    Shadow,
)
from agentic_data_contracts.semantic.yaml_source import YamlSource

SHADOW_SQL = (
    "SELECT * FROM mkt.touchpoints UNION ALL "
    "SELECT lead_id, 'display', qualified_date + 1 FROM mkt.lead_scores"
)


def _raw(**overrides: object) -> dict:
    """A one-metric semantic document carrying one sensitivity property."""
    prop: dict = {
        "name": "attribution_is_first_touch",
        "description": (
            "A touchpoint after qualification cannot change a first-touch attribution."
        ),
        "shadow": {"table": "mkt.touchpoints", "sql": SHADOW_SQL},
        "expect": "unchanged",
    }
    prop.update(overrides)
    return {
        "metrics": [
            {
                "name": "mql_count",
                "description": "Count of leads flagged is_mql, first touch.",
                "sql_expression": "COUNT(DISTINCT lead_id)",
                "sensitivity": [prop],
            }
        ]
    }


class TestDataModel:
    def test_metric_defaults_to_no_sensitivity(self) -> None:
        m = MetricDefinition(name="signups", description="", sql_expression="COUNT(*)")
        assert m.sensitivity == []

    def test_property_holds_claim_and_shadow(self) -> None:
        p = SensitivityProperty(
            name="p",
            description="the claim",
            shadow=Shadow(table="mkt.touchpoints", sql=SHADOW_SQL),
            expect="unchanged",
        )
        assert p.shadow.table == "mkt.touchpoints"
        assert p.expect == "unchanged"


class TestParsing:
    def test_parses_a_declared_property(self) -> None:
        metric = YamlSource.from_raw(_raw()).get_metric("mql_count")
        assert metric is not None
        (prop,) = metric.sensitivity
        assert prop.name == "attribution_is_first_touch"
        assert prop.expect == "unchanged"
        assert prop.shadow.table == "mkt.touchpoints"
        assert "UNION ALL" in prop.shadow.sql
        assert prop.description.startswith("A touchpoint after qualification")

    def test_metric_without_the_key_parses_to_empty(self) -> None:
        raw = {"metrics": [{"name": "m", "description": "", "sql_expression": "1"}]}
        metric = YamlSource.from_raw(raw).get_metric("m")
        assert metric is not None
        assert metric.sensitivity == []

    def test_non_mapping_shadow_raises(self) -> None:
        with pytest.raises(ValueError, match="shadow"):
            YamlSource.from_raw(_raw(shadow="SELECT 1"))
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `uv run pytest tests/test_semantic/test_sensitivity.py -v`
Expected: FAIL — `ImportError: cannot import name 'SensitivityProperty'`.

- [ ] **Step 3: Add the dataclasses to `semantic/base.py`**

Place `VALID_EXPECT` next to the existing `VALID_OPERATORS` / `VALID_CONVENTIONS`
frozensets, and the two dataclasses immediately before `MetricDefinition`:

```python
#: The only two responses a property may require. There is no third value: a
#: property that cannot say which way the answer must go is not a property.
VALID_EXPECT = frozenset({"unchanged", "changes"})


@dataclass(frozen=True)
class Shadow:
    """One SELECT that stands in for ``table`` while a property is checked.

    ``sql`` is engine-native, exactly as ``sql_expression`` is throughout a
    contract, so a Denodo author writes VQL here as they do there. It is
    contract-authored and therefore trusted: it is NOT put through the
    Validator, which would reject the ``SELECT *`` most shadows want.
    """

    table: str  # "schema.table"
    sql: str


@dataclass(frozen=True)
class SensitivityProperty:
    """A claim about what a metric's query must respond to.

    ``description`` is required and carries the claim in the owner's words --
    see the Ownership section of the design. It is the one field a business
    owner contributes, and a property nobody stated in words is one nobody can
    review.
    """

    name: str
    description: str
    shadow: Shadow
    expect: str  # one of VALID_EXPECT
```

Then add the field to `MetricDefinition`, immediately after `drill_by`:

```text
    sensitivity: list[SensitivityProperty] = field(default_factory=list)
```

- [ ] **Step 4: Add the key sets and parsing to `semantic/yaml_source.py`**

Add `"sensitivity"` to `METRIC_KEYS`, and define the two new key sets next to
`DRILL_BY_KEYS`:

```python
#: Exported for the same reason as ``SEMANTIC_KEYS``: a guard cannot be written
#: against key names that exist only as string literals in a constructor.
SENSITIVITY_KEYS = frozenset({"name", "description", "shadow", "expect"})
SHADOW_KEYS = frozenset({"table", "sql"})
```

Import `SensitivityProperty` and `Shadow` from `semantic.base` alongside the
existing `Decomposition` / `DrillDimension` imports. Add this module-level
helper next to the other parsing helpers:

```python
def _shadow_from(raw: Any, *, where: str) -> Shadow:
    """Parse one ``shadow:`` block, refusing a non-mapping loudly.

    Without the isinstance check a scalar ``shadow: SELECT 1`` reaches
    ``.get`` and raises ``AttributeError`` naming neither the metric nor the
    property.
    """
    if not isinstance(raw, dict):
        raise ValueError(
            f"{where} 'shadow' must be a mapping with 'table' and 'sql', got "
            f"{type(raw).__name__}"
        )
    return Shadow(
        table=require_text(raw.get("table"), where=f"{where} shadow.table"),
        sql=require_text(raw.get("sql"), where=f"{where} shadow.sql"),
    )
```

In `_load_from_raw`'s `MetricDefinition(...)` construction, add the field
immediately after `drill_by=[...]`:

```text
                    sensitivity=[
                        SensitivityProperty(
                            name=require_text(
                                sp.get("name"),
                                where="metrics[] sensitivity[] name",
                            ),
                            description=require_text(
                                sp.get("description"),
                                where="metrics[] sensitivity[] description",
                            ),
                            shadow=_shadow_from(
                                sp.get("shadow"), where="metrics[] sensitivity[]"
                            ),
                            expect=as_text(sp.get("expect")),
                        )
                        for sp in m.get("sensitivity") or []
                    ],
```

- [ ] **Step 5: Run the tests to verify they pass**

Run: `uv run pytest tests/test_semantic/test_sensitivity.py -v`
Expected: PASS, 5 tests.

- [ ] **Step 6: Run the full semantic suite for regressions**

Run: `uv run pytest tests/test_semantic -q`
Expected: PASS, no new failures.

- [ ] **Step 7: Commit**

```bash
git add src/agentic_data_contracts/semantic/base.py \
        src/agentic_data_contracts/semantic/yaml_source.py \
        tests/test_semantic/test_sensitivity.py
git commit -m "feat(semantic): parse contract-declared sensitivity properties"
```

---

### Task 2: Load-time validation

**Files:**
- Modify: `src/agentic_data_contracts/semantic/base.py`
- Modify: `src/agentic_data_contracts/semantic/yaml_source.py`
- Test: `tests/test_semantic/test_sensitivity.py`

**Interfaces:**
- Consumes: `SensitivityProperty`, `Shadow`, `VALID_EXPECT`, `SENSITIVITY_KEYS`,
  `SHADOW_KEYS` from Task 1.
- Produces: `validate_sensitivity(metrics: list[MetricDefinition]) -> None`,
  raising `ValueError`. Called from `_load_from_raw` beside
  `validate_decompositions` and `validate_drill_by`.

**DEVIATION 3 — the `allowed_tables` check moves out of load time.** The spec
says a shadow reading a table outside `allowed_tables` is rejected at load. It
cannot be: `YamlSource` is built with no knowledge of a `DataContract`, and
`allowed_tables` lives on `contract.schema.semantic`. `validate_drill_by` checks
against the semantic source's *own* `tables:` section, which is a different
thing. The governance hole is therefore closed in two places instead, in Task 6:
`check_sensitivity` refuses to execute a shadow reading an ungoverned table, and
`validate_sensitivity_tables(contract, source)` is exposed as the CI-time gate
that finds it before anyone runs a query. Everything decidable from the source
alone stays here, at load, loud.

- [ ] **Step 1: Write the failing tests**

Append to `tests/test_semantic/test_sensitivity.py`:

```python
class TestLoadTimeValidation:
    # `_check_entry_keys` raises only in strict mode, and `strict` is
    # `expected_extras is not None` -- declaring it at all is this loader's
    # documented way to say "fail my build on a key you do not read". Without
    # it an unknown nested key logs a warning and is dropped, which is the
    # behaviour every sibling key set already has.
    def test_unknown_property_key_raises(self) -> None:
        with pytest.raises(ValueError, match="sensitivity"):
            YamlSource.from_raw(_raw(expects="unchanged"), expected_extras=[])

    def test_unknown_shadow_key_raises(self) -> None:
        raw = _raw(
            shadow={"table": "mkt.touchpoints", "sql": SHADOW_SQL, "where": "1=1"}
        )
        with pytest.raises(ValueError, match="shadow"):
            YamlSource.from_raw(raw, expected_extras=[])

    def test_empty_description_raises(self) -> None:
        with pytest.raises(ValueError, match="description"):
            YamlSource.from_raw(_raw(description="   "))

    def test_bad_expect_raises(self) -> None:
        with pytest.raises(ValueError, match="expect"):
            YamlSource.from_raw(_raw(expect="maybe"))

    def test_missing_expect_raises(self) -> None:
        raw = _raw()
        del raw["metrics"][0]["sensitivity"][0]["expect"]
        with pytest.raises(ValueError, match="expect"):
            YamlSource.from_raw(raw)

    def test_malformed_shadow_table_raises(self) -> None:
        with pytest.raises(ValueError, match="schema.table"):
            YamlSource.from_raw(
                _raw(shadow={"table": "touchpoints", "sql": "SELECT 1"})
            )

    def test_duplicate_property_name_raises(self) -> None:
        raw = _raw()
        raw["metrics"][0]["sensitivity"].append(
            dict(raw["metrics"][0]["sensitivity"][0])
        )
        with pytest.raises(ValueError, match="duplicate"):
            YamlSource.from_raw(raw)

    def test_two_distinct_names_are_fine(self) -> None:
        raw = _raw()
        second = dict(raw["metrics"][0]["sensitivity"][0])
        second["name"] = "another_property"
        raw["metrics"][0]["sensitivity"].append(second)
        metric = YamlSource.from_raw(raw).get_metric("mql_count")
        assert metric is not None
        assert [p.name for p in metric.sensitivity] == [
            "attribution_is_first_touch",
            "another_property",
        ]
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `uv run pytest tests/test_semantic/test_sensitivity.py::TestLoadTimeValidation -v`
Expected: FAIL — most raise nothing; `test_malformed_shadow_table_raises` and
`test_duplicate_property_name_raises` fail with "DID NOT RAISE".

- [ ] **Step 3: Add `validate_sensitivity` to `semantic/base.py`**

Place it next to `validate_drill_by`:

```python
def validate_sensitivity(metrics: list[MetricDefinition]) -> None:
    """Validate declared sensitivity properties, loudly.

    Everything checkable from the semantic source alone is checked here. The
    one rule that is NOT -- that ``shadow.sql`` may only read tables the
    contract governs -- needs a ``DataContract``, which a source does not have;
    see ``validation.sensitivity.validate_sensitivity_tables``.
    """
    for metric in metrics:
        seen: set[str] = set()
        for prop in metric.sensitivity:
            if prop.name in seen:
                raise ValueError(
                    f"metric {metric.name!r} declares duplicate sensitivity "
                    f"property name {prop.name!r}"
                )
            seen.add(prop.name)
            if not prop.description.strip():
                raise ValueError(
                    f"metric {metric.name!r} sensitivity property "
                    f"{prop.name!r} needs a non-empty description: it is the "
                    "claim the property asserts, and a property nobody stated "
                    "in words is one nobody can review"
                )
            if prop.expect not in VALID_EXPECT:
                raise ValueError(
                    f"metric {metric.name!r} sensitivity property "
                    f"{prop.name!r} has expect {prop.expect!r}; "
                    f"must be one of {sorted(VALID_EXPECT)}"
                )
            schema, _, table = prop.shadow.table.partition(".")
            if not schema or not table or "." in table:
                raise ValueError(
                    f"metric {metric.name!r} sensitivity property "
                    f"{prop.name!r} shadow.table {prop.shadow.table!r} must be "
                    "'schema.table'"
                )
```

- [ ] **Step 4: Wire the key guard and the call site in `yaml_source.py`**

In the key-guard loop, after the `drill_by` block, add:

```text
        for j, sp in enumerate(_entries(m.get("sensitivity"), f"{label} sensitivity")):
            _check_entry_keys(
                sp,
                SENSITIVITY_KEYS,
                where=f"{label} sensitivity[{j}]",
                strict=strict,
            )
            shadow = sp.get("shadow")
            if isinstance(shadow, dict):
                _check_entry_keys(
                    shadow,
                    SHADOW_KEYS,
                    where=f"{label} sensitivity[{j}] shadow",
                    strict=strict,
                )
```

Import `validate_sensitivity` from `semantic.base` and call it beside the two
existing validators at the end of `_load_from_raw`:

```text
        validate_decompositions(self._metrics)
        validate_drill_by(self._metrics, self._tables)
        validate_sensitivity(self._metrics)
```

- [ ] **Step 5: Run the tests to verify they pass**

Run: `uv run pytest tests/test_semantic/test_sensitivity.py -v`
Expected: PASS, 13 tests.

- [ ] **Step 6: Commit**

```bash
git add src/agentic_data_contracts/semantic/base.py \
        src/agentic_data_contracts/semantic/yaml_source.py \
        tests/test_semantic/test_sensitivity.py
git commit -m "feat(semantic): validate sensitivity properties at load"
```

---

### Task 3: Dump, round-trip, and digest stability

**Files:**
- Modify: `src/agentic_data_contracts/semantic/base.py`
- Test: `tests/test_semantic/test_sensitivity.py`

**Interfaces:**
- Consumes: everything from Tasks 1–2.
- Produces: `dump_semantic_source` emits `sensitivity` when non-empty and omits
  it when empty. No new names.

This is its own task because it is the feature's one real compatibility risk. A
`sensitivity` key that is always present moves every published
`contract_digest` on upgrade — the exact regression 0.28.1 fixed for
`decompositions` and `drill_by`.

- [ ] **Step 1: Write the failing tests**

Append to `tests/test_semantic/test_sensitivity.py`, merging
`dump_semantic_source` into the existing `semantic.base` import at the top of
the file (E402 is on — see Global Constraints):

```python
class TestDumpAndRoundTrip:
    def test_dump_omits_the_key_when_no_property_is_declared(self) -> None:
        raw = {"metrics": [{"name": "m", "description": "", "sql_expression": "1"}]}
        dumped = dump_semantic_source(YamlSource.from_raw(raw))
        assert "sensitivity" not in dumped["metrics"][0], (
            "an always-present key moves every published contract_digest"
        )

    def test_dump_emits_the_property_when_declared(self) -> None:
        dumped = dump_semantic_source(YamlSource.from_raw(_raw()))
        (prop,) = dumped["metrics"][0]["sensitivity"]
        assert prop == {
            "name": "attribution_is_first_touch",
            "description": (
                "A touchpoint after qualification cannot change a first-touch "
                "attribution."
            ),
            "shadow": {"table": "mkt.touchpoints", "sql": SHADOW_SQL},
            "expect": "unchanged",
        }

    def test_round_trips_through_from_raw(self) -> None:
        once = dump_semantic_source(YamlSource.from_raw(_raw()))
        twice = dump_semantic_source(YamlSource.from_raw(once))
        assert once == twice

    def test_pre_feature_contract_dumps_byte_identically(self) -> None:
        """The 0.28.1 digest-stability regression, as a test."""
        raw = {
            "metrics": [
                {
                    "name": "total_revenue",
                    "description": "Total revenue",
                    "sql_expression": "SUM(amount)",
                }
            ]
        }
        dumped = dump_semantic_source(YamlSource.from_raw(raw))
        assert set(dumped["metrics"][0]) == {
            "name",
            "description",
            "sql_expression",
            "source_model",
            "filters",
            "domains",
            "tier",
            "indicator_kind",
            "business_owner",
            "operational_owner",
            "last_reviewed",
        }
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `uv run pytest tests/test_semantic/test_sensitivity.py::TestDumpAndRoundTrip -v`
Expected: FAIL on `test_dump_emits_the_property_when_declared` with `KeyError:
'sensitivity'`. The omit tests pass already (nothing emits the key yet) —
that is correct; they are regression guards for Step 3.

- [ ] **Step 3: Emit the key in `_dump_metric`**

In `dump_semantic_source`'s inner `_dump_metric`, after the `drill_by` block:

```text
        # Omitted when empty for the reason spelled out on `decompositions`
        # above: `contract_canonical_bytes` dumps with no `exclude_none`, so an
        # always-present key moves every published digest.
        if m.sensitivity:
            data["sensitivity"] = [
                {
                    "name": p.name,
                    "description": p.description,
                    "shadow": {"table": p.shadow.table, "sql": p.shadow.sql},
                    "expect": p.expect,
                }
                for p in m.sensitivity
            ]
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `uv run pytest tests/test_semantic/test_sensitivity.py -v`
Expected: PASS, 17 tests.

- [ ] **Step 5: Run the portability and semantic suites**

Run: `uv run pytest tests/test_semantic tests/test_portability -q`
Expected: PASS. `tests/test_portability` is where digest stability is exercised
end to end; a failure here means the omit-when-empty rule is broken.

- [ ] **Step 6: Commit**

```bash
git add src/agentic_data_contracts/semantic/base.py \
        tests/test_semantic/test_sensitivity.py
git commit -m "feat(semantic): dump sensitivity properties, omitted when empty"
```

---

### Task 4: The rewrite engine

**Files:**
- Create: `src/agentic_data_contracts/validation/sensitivity.py`
- Test: `tests/test_validation/test_sensitivity.py`

**Interfaces:**
- Consumes: `Shadow` from Task 1.
- Produces, all module-private but directly tested:
  `_Refused(Exception)` with `.args[0]` the mechanical reason;
  `_table_refs(sql: str, target: str, *, dialect: str | None) -> int`;
  `_spans(sql: str, target: str, *, dialect: str | None) -> list[tuple[int, int]]`;
  `_free_alias(sql: str) -> str`;
  `_rewrite(sql: str, shadow: Shadow, *, dialect: str | None) -> str`;
  `_norm(rows: list[tuple]) -> list[tuple]`.

This task is pure string and AST work — no database, no contract. It is the
part most likely to be subtly wrong, so it gets its own gate.

- [ ] **Step 1: Write the failing tests**

Create `tests/test_validation/test_sensitivity.py`:

```python
"""Sensitivity checks: rewrite mechanics, result types, check_sensitivity."""

from __future__ import annotations

import pytest

from agentic_data_contracts.semantic.base import Shadow
from agentic_data_contracts.validation.sensitivity import (
    _Refused,
    _free_alias,
    _norm,
    _rewrite,
    _spans,
    _table_refs,
)

SHADOW = Shadow(
    table="main.payments",
    sql="SELECT * REPLACE (eur_amount * 10 AS eur_amount) FROM main.payments",
)


def _rw(sql: str) -> str:
    return _rewrite(sql, SHADOW, dialect="duckdb")


class TestLocate:
    def test_counts_a_qualified_reference(self) -> None:
        assert (
            _table_refs(
                "SELECT 1 FROM main.payments", "main.payments", dialect="duckdb"
            )
            == 1
        )

    def test_counts_a_bare_reference(self) -> None:
        assert (
            _table_refs("SELECT 1 FROM payments", "main.payments", dialect="duckdb")
            == 1
        )

    def test_ignores_another_schema(self) -> None:
        assert (
            _table_refs(
                "SELECT 1 FROM other.payments", "main.payments", dialect="duckdb"
            )
            == 0
        )

    def test_ignores_a_cte_of_the_same_name(self) -> None:
        sql = "WITH payments AS (SELECT 1 x) SELECT * FROM payments"
        with pytest.raises(_Refused, match="shadowed by a CTE"):
            _table_refs(sql, "main.payments", dialect="duckdb")

    def test_unparseable_sql_is_refused(self) -> None:
        with pytest.raises(_Refused, match="unparseable"):
            _table_refs("SELECT FROM WHERE )(", "main.payments", dialect="duckdb")


class TestRewrite:
    def test_prepends_a_with_clause(self) -> None:
        out = _rw("SELECT count(*) FROM main.payments")
        assert out.startswith("WITH __sens_0 AS (")
        assert "FROM __sens_0" in out
        assert "FROM main.payments)" in out  # inside the shadow, untouched

    def test_rewrites_a_bare_reference_and_keeps_its_alias(self) -> None:
        out = _rw("SELECT count(*) FROM payments p WHERE p.eur_amount > 10")
        assert "FROM __sens_0 p" in out

    def test_splices_into_an_existing_with(self) -> None:
        sql = "WITH v AS (SELECT 1 FROM payments) SELECT * FROM v"
        out = _rw(sql)
        assert out.startswith("WITH __sens_0 AS (")
        assert "v AS (SELECT 1 FROM __sens_0)" in out

    def test_splices_after_with_recursive(self) -> None:
        sql = "WITH RECURSIVE r(n) AS (SELECT 1) SELECT (SELECT count(*) FROM payments) FROM r"
        out = _rw(sql)
        assert out.startswith("WITH RECURSIVE __sens_0 AS (")
        assert "FROM __sens_0) FROM r" in out

    def test_rewrites_a_reference_inside_a_correlated_subquery(self) -> None:
        sql = (
            "SELECT (SELECT count(*) FROM payments x WHERE x.merchant=f.merchant) "
            "FROM main.fees f"
        )
        out = _rw(sql)
        assert "FROM __sens_0 x" in out

    def test_leaves_a_string_literal_alone(self) -> None:
        out = _rw("SELECT 'payments' AS label, count(*) FROM main.payments")
        assert "'payments' AS label" in out
        assert "FROM __sens_0" in out

    def test_leaves_a_column_alias_alone(self) -> None:
        # The count guard's false-positive class, measured on the DABStep
        # corpus: four stored queries write this shape and were refused.
        sql = "SELECT count(DISTINCT psp_reference) AS payments FROM main.payments"
        out = _rw(sql)
        assert "AS payments" in out
        assert "FROM __sens_0" in out

    def test_not_applicable_when_the_target_is_absent(self) -> None:
        with pytest.raises(_Refused, match="not_applicable"):
            _rw("SELECT * FROM main.fees")

    def test_alias_collision_picks_the_next_free_name(self) -> None:
        assert _free_alias("SELECT 1") == "__sens_0"
        assert _free_alias("SELECT __sens_0 FROM t") == "__sens_1"
        assert _free_alias("SELECT __sens_0, __sens_1 FROM t") == "__sens_2"


class TestNormalise:
    def test_orders_and_rounds(self) -> None:
        assert _norm([(2, 1.000000001), (1, 2.0)]) == _norm([(1, 2.0), (2, 1.0)])

    def test_distinguishes_different_values(self) -> None:
        assert _norm([(1,)]) != _norm([(2,)])
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `uv run pytest tests/test_validation/test_sensitivity.py -v`
Expected: FAIL — `ModuleNotFoundError: agentic_data_contracts.validation.sensitivity`.

- [ ] **Step 3: Create `validation/sensitivity.py` with the rewrite engine**

```python
"""Does the caller's query DERIVE what the contract says it depends on?

The two-layer validator sees policy (allowed tables, forbidden operations,
required filters) and plannability (a live EXPLAIN). Neither sees a query that
is authorized, parseable, plannable -- and computes the wrong thing. The
DABStep eval established that this class of defect cannot be found by reading
the query text: detectors for the absence of a clause and for the presence of a
wrong construction are both null across four models.

This module reads BEHAVIOUR instead. It replaces one table with a
contract-authored SELECT, re-runs the caller's own query, and asks whether the
answer MOVED. It never computes an answer, so it cannot say a query is right --
only that it responds, or fails to respond, to an input the contract says it
depends on.

COST. Two base executions per call plus one per property: the base result is
computed once and cached, so a metric with four properties costs six
executions, not twelve. That is why this belongs at a promotion gate rather
than in an agent's hot path.

NO SQL IS REGENERATED. sqlglot decides *what* to rewrite and guards the edit;
the string executed is the caller's own text with spans replaced. That is what
makes this work on a dialect sqlglot can parse but not emit (Denodo/VQL), and
it is the same template-assembly discipline the rest of the library follows.
"""

from __future__ import annotations

from dataclasses import dataclass

import sqlglot
from sqlglot import exp

from agentic_data_contracts.semantic.base import Shadow

#: Only an identifier in one of these positions names a table. Without this, a
#: COLUMN ALIAS sharing the table's name (``COUNT(DISTINCT psp_reference) AS
#: payments``, which four stored DABStep queries write) is counted as a
#: reference, the span count exceeds the table-node count, and the guard
#: refuses a query it could have checked. ``unchecked`` fails ``report.ok``, so
#: that is a spurious CI failure rather than a harmless conservatism.
_TABLE_POS = frozenset(
    {
        sqlglot.TokenType.FROM,
        sqlglot.TokenType.JOIN,
        sqlglot.TokenType.COMMA,
        sqlglot.TokenType.L_PAREN,
    }
)
_IDENT = frozenset({sqlglot.TokenType.VAR, sqlglot.TokenType.IDENTIFIER})
_ALIAS_STEM = "__sens_"


class _Refused(Exception):
    """The mechanical reason no verdict could be rendered for one property."""


def _cte_aliases(tree: exp.Expression) -> set[str]:
    return {c.alias_or_name.lower() for c in tree.find_all(exp.CTE)}


def _table_refs(sql: str, target: str, *, dialect: str | None) -> int:
    """How many real references to *target* the query holds.

    ``find_all(exp.Table)`` minus the CTE aliases: expression nodes carry no
    source positions, but this correctly separates a base table from a
    reference to a CTE of the same name.
    """
    try:
        tree = sqlglot.parse_one(sql, dialect=dialect)
    except Exception as e:  # noqa: BLE001 - any parse failure is one outcome
        raise _Refused(f"unparseable: {e}") from e
    if tree is None:
        raise _Refused("unparseable: empty statement")
    ctes = _cte_aliases(tree)
    db, _, name = target.rpartition(".")
    if name.lower() in ctes:
        raise _Refused(f"target {target!r} is shadowed by a CTE of the same name")
    count = 0
    for node in tree.find_all(exp.Table):
        if node.name.lower() != name.lower():
            continue
        if node.db and node.db.lower() != db.lower():
            continue
        count += 1
    return count


def _in_table_position(toks: list, i: int) -> bool:
    return i > 0 and toks[i - 1].token_type in _TABLE_POS


def _spans(sql: str, target: str, *, dialect: str | None) -> list[tuple[int, int]]:
    """Character spans of *target*, via tokens -- which do carry positions.

    Matches both the qualified spelling and the bare one, as alternating
    identifier/DOT runs. A name inside a string literal or a comment is a
    different token type and is never a candidate.
    """
    toks = list(sqlglot.tokenize(sql, dialect=dialect))
    db, _, name = target.rpartition(".")
    out: list[tuple[int, int]] = []
    for i, tok in enumerate(toks):
        if tok.token_type not in _IDENT:
            continue
        if tok.text.strip('"').lower() != name.lower():
            continue
        prev = toks[i - 1] if i else None
        if prev is not None and prev.token_type == sqlglot.TokenType.DOT:
            prev2 = toks[i - 2] if i > 1 else None
            if prev2 is None or prev2.text.strip('"').lower() != db.lower():
                continue  # qualified by some other schema
            if not _in_table_position(toks, i - 2):
                continue
            out.append((prev2.start, tok.end + 1))
        else:
            if not _in_table_position(toks, i):
                continue
            out.append((tok.start, tok.end + 1))
    return out


def _free_alias(sql: str) -> str:
    """The first ``__sens_N`` the query does not already contain."""
    n = 0
    while f"{_ALIAS_STEM}{n}" in sql:
        n += 1
    return f"{_ALIAS_STEM}{n}"


def _rewrite(sql: str, shadow: Shadow, *, dialect: str | None) -> str:
    """The caller's own text, with *shadow.table* pointed at an injected CTE."""
    refs = _table_refs(sql, shadow.table, dialect=dialect)
    if refs == 0:
        raise _Refused("not_applicable: query does not reference the target")
    spans = _spans(sql, shadow.table, dialect=dialect)
    if len(spans) != refs:
        raise _Refused(
            f"count guard: {len(spans)} spans vs {refs} table nodes; refusing to edit"
        )
    alias = _free_alias(sql)
    out = sql
    for start, end in sorted(spans, reverse=True):  # right-to-left
        out = out[:start] + alias + out[end:]

    toks = list(sqlglot.tokenize(out, dialect=dialect))
    cte = f"{alias} AS ({shadow.sql})"
    if toks and toks[0].token_type == sqlglot.TokenType.WITH:
        head = toks[1] if len(toks) > 1 else None
        cut = (
            head.end + 1
            if head is not None and head.token_type == sqlglot.TokenType.RECURSIVE
            else toks[0].end + 1
        )
        return f"{out[:cut]} {cte}, {out[cut:]}"
    return f"WITH {cte} {out}"


def _norm(rows: list[tuple]) -> list[tuple]:
    """Order- and float-noise-insensitive form.

    So "the answer moved" means the values moved, not that the engine returned
    them in a different order.
    """
    rounded = [
        tuple(round(v, 6) if isinstance(v, float) else v for v in row) for row in rows
    ]
    return sorted(rounded, key=repr)
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `uv run pytest tests/test_validation/test_sensitivity.py -v`
Expected: PASS, 17 tests.

- [ ] **Step 5: Verify every sqlglot symbol exists at the dependency floor**

This feature uses `sqlglot.tokenize`, `sqlglot.TokenType.{FROM,JOIN,COMMA,
L_PAREN,VAR,IDENTIFIER,DOT,WITH,RECURSIVE}`, `Token.start` / `Token.end`,
`exp.Table.db`, and `exp.CTE.alias_or_name`. `uv.lock` resolves a much newer
sqlglot than the `>=28.6` floor, which makes a floor break invisible to every
local run and review.

```bash
uv run --isolated --with 'sqlglot==28.6' --no-project \
   python -c "
import sqlglot
from sqlglot import exp
for n in ('FROM','JOIN','COMMA','L_PAREN','VAR','IDENTIFIER','DOT','WITH','RECURSIVE'):
    assert hasattr(sqlglot.TokenType, n), n
t = list(sqlglot.tokenize('SELECT 1 FROM main.t', dialect='duckdb'))[-1]
assert isinstance(t.start, int) and isinstance(t.end, int)
tree = sqlglot.parse_one('WITH c AS (SELECT 1) SELECT * FROM main.t', dialect='duckdb')
assert next(iter(tree.find_all(exp.Table))).db == 'main'
assert next(iter(tree.find_all(exp.CTE))).alias_or_name == 'c'
print('floor OK')
"
```

Expected: `floor OK`. If any assertion fails, raise the floor in
`pyproject.toml` to the first version that has the symbol and say so in the
commit message — do not silently rely on the lockfile.

- [ ] **Step 6: Commit**

```bash
git add src/agentic_data_contracts/validation/sensitivity.py \
        tests/test_validation/test_sensitivity.py
git commit -m "feat(validation): CTE-shadow rewrite for sensitivity checks"
```

---

### Task 5: Result types

**Files:**
- Modify: `src/agentic_data_contracts/validation/sensitivity.py`
- Test: `tests/test_validation/test_sensitivity.py`

**Interfaces:**
- Consumes: nothing from Task 4 (independent types in the same module).
- Produces: `SensitivityResult(name, metric, status, expected, moved=None,
  reason="")` and `SensitivityReport(results: tuple[SensitivityResult, ...])`
  with properties `ok`, `violations`, `unchecked`, `not_applicable` and a
  `summary() -> str`. Task 6 returns these.

- [ ] **Step 1: Write the failing tests**

Append to `tests/test_validation/test_sensitivity.py`, merging
`SensitivityReport` and `SensitivityResult` into the existing
`validation.sensitivity` import at the top of the file (E402 is on — see
Global Constraints):

```python
def _res(status: str, name: str = "p") -> SensitivityResult:
    return SensitivityResult(name=name, metric="m", status=status, expected="unchanged")


class TestReport:
    def test_empty_report_is_ok(self) -> None:
        # Silence is the honest answer: nothing was claimed, nothing checked.
        assert SensitivityReport(results=()).ok is True

    def test_all_pass_is_ok(self) -> None:
        assert SensitivityReport(results=(_res("pass"),)).ok is True

    def test_not_applicable_does_not_block(self) -> None:
        report = SensitivityReport(results=(_res("pass"), _res("not_applicable", "q")))
        assert report.ok is True
        assert len(report.not_applicable) == 1

    def test_violation_blocks(self) -> None:
        report = SensitivityReport(results=(_res("pass"), _res("violation", "q")))
        assert report.ok is False
        assert [r.name for r in report.violations] == ["q"]

    def test_unchecked_blocks(self) -> None:
        # "no verdict was possible" must not read as "passed".
        report = SensitivityReport(results=(_res("unchecked"),))
        assert report.ok is False
        assert len(report.unchecked) == 1

    def test_summary_names_every_status_present(self) -> None:
        report = SensitivityReport(
            results=(_res("pass"), _res("violation", "q"), _res("unchecked", "r"))
        )
        text = report.summary()
        assert "violation" in text and "unchecked" in text and "q" in text
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `uv run pytest tests/test_validation/test_sensitivity.py::TestReport -v`
Expected: FAIL — `ImportError: cannot import name 'SensitivityReport'`.

- [ ] **Step 3: Add the result types**

Append to `validation/sensitivity.py`, after `_norm`:

```python
@dataclass(frozen=True)
class SensitivityResult:
    """The verdict for one property against one query.

    ``status`` (each result has exactly one):
      - ``"pass"``           -- the answer responded the way the contract requires.
      - ``"violation"``      -- it did not.
      - ``"not_applicable"`` -- the query never references the shadowed table.
                                An outcome, not a failure.
      - ``"unchecked"``      -- no verdict was possible: unparseable SQL, the
                                count guard refused the edit, the engine raised,
                                the base query is not deterministic, or the test
                                was vacuous.

    ``moved`` is None when no comparison was made. ``reason`` reports the
    mechanical condition only and never infers a cause -- the same boundary
    ``ReconciliationResult.reason`` draws. The check says "the answer did not
    move", not "you hardcoded the threshold".
    """

    name: str
    metric: str
    status: str
    expected: str
    moved: bool | None = None
    reason: str = ""


@dataclass(frozen=True)
class SensitivityReport:
    results: tuple[SensitivityResult, ...]

    @property
    def violations(self) -> tuple[SensitivityResult, ...]:
        return tuple(r for r in self.results if r.status == "violation")

    @property
    def unchecked(self) -> tuple[SensitivityResult, ...]:
        return tuple(r for r in self.results if r.status == "unchecked")

    @property
    def not_applicable(self) -> tuple[SensitivityResult, ...]:
        return tuple(r for r in self.results if r.status == "not_applicable")

    @property
    def ok(self) -> bool:
        """True when nothing is a violation and nothing is unchecked.

        Safe as a CI gate -- ``if not report.ok: sys.exit(1)``. It is False on
        a ``violation`` and on an ``unchecked``, because "no verdict was
        possible" must not read as "passed". ``not_applicable`` does NOT block:
        a query that never touches the table has nothing to answer for.

        An EMPTY report is ok, which differs from ``ExampleValidationReport``.
        There, zero examples means a corpus failed to load. Here it means the
        metric declares no properties -- nothing was claimed, so nothing failed.
        Test ``report.violations`` directly for a laxer gate.
        """
        return not (self.violations or self.unchecked)

    def summary(self) -> str:
        """A compact markdown report, suitable for an MR comment."""
        if not self.results:
            return "No sensitivity properties declared."
        counts: dict[str, int] = {}
        for r in self.results:
            counts[r.status] = counts.get(r.status, 0) + 1
        head = ", ".join(f"{n} {status}" for status, n in sorted(counts.items()))
        lines = [f"**Sensitivity:** {head}", ""]
        for r in self.results:
            detail = f" — {r.reason}" if r.reason else ""
            lines.append(
                f"- `{r.metric}` / `{r.name}`: **{r.status}** "
                f"(expected {r.expected}, moved={r.moved}){detail}"
            )
        return "\n".join(lines)
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `uv run pytest tests/test_validation/test_sensitivity.py -v`
Expected: PASS, 23 tests.

- [ ] **Step 5: Commit**

```bash
git add src/agentic_data_contracts/validation/sensitivity.py \
        tests/test_validation/test_sensitivity.py
git commit -m "feat(validation): sensitivity result and report types"
```

---

### Task 6: `check_sensitivity`

**Files:**
- Modify: `src/agentic_data_contracts/validation/sensitivity.py`
- Test: `tests/test_validation/test_sensitivity.py`

**Interfaces:**
- Consumes: everything from Tasks 1–5.
- Produces:

```python
DEFAULT_REPEATS: int = 2


def validate_sensitivity_tables(
    contract: DataContract, metrics: list[MetricDefinition]
) -> list[str]: ...


def check_sensitivity(
    metric: MetricDefinition,
    sql: str,
    *,
    contract: DataContract,
    adapter: DatabaseAdapter,
    properties: Sequence[str] | None = None,
    repeats: int = DEFAULT_REPEATS,
    dialect: str | None = None,
) -> SensitivityReport: ...
```

**DEVIATION 4 — `metric` is a `MetricDefinition`, not a `str`.** The spec writes
`check_sensitivity(contract, sql, *, metric: str, ...)`. A `DataContract` does
not hold `MetricDefinition`s — they come from a `SemanticSource` — so a string
would force this function to take a source as well and do its own lookup.
`reconcile_decomposition(metric: MetricDefinition, *, ...)` already takes the
object positionally; matching it keeps the family consistent and deletes the
"unknown metric name" `ValueError` path entirely. `contract` becomes keyword-only,
because step 0 needs it for the Validator.

- [ ] **Step 1: Write the failing tests**

Append to `tests/test_validation/test_sensitivity.py`. These imports merge into
the top-of-file block (E402 is on — see Global Constraints); the existing
`semantic.base` and `validation.sensitivity` imports gain the new names:

```python
from agentic_data_contracts.adapters.duckdb import DuckDBAdapter
from agentic_data_contracts.core.contract import DataContract
from agentic_data_contracts.semantic.base import MetricDefinition, SensitivityProperty
from agentic_data_contracts.validation.sensitivity import (
    check_sensitivity,
    validate_sensitivity_tables,
)

MKT_SHADOW = Shadow(
    table="mkt.touchpoints",
    sql=(
        "SELECT * FROM mkt.touchpoints UNION ALL "
        "SELECT lead_id, 'display', qualified_date + 1 FROM mkt.lead_scores"
    ),
)

FAN_OUT = """
SELECT t.channel, COUNT(DISTINCT l.lead_id) AS mqls
FROM mkt.lead_scores l JOIN mkt.touchpoints t USING (lead_id)
WHERE l.is_mql GROUP BY 1 ORDER BY 1
"""

FIRST_TOUCH = """
WITH first_touch AS (
  SELECT lead_id, channel FROM (
    SELECT lead_id, channel,
           row_number() OVER (PARTITION BY lead_id ORDER BY touch_date) rn
    FROM mkt.touchpoints) WHERE rn = 1)
SELECT f.channel, COUNT(DISTINCT l.lead_id) AS mqls
FROM mkt.lead_scores l JOIN first_touch f USING (lead_id)
WHERE l.is_mql GROUP BY 1 ORDER BY 1
"""


@pytest.fixture
def mkt_adapter() -> DuckDBAdapter:
    adapter = DuckDBAdapter(":memory:")
    adapter.execute("CREATE SCHEMA mkt")
    adapter.execute(
        "CREATE TABLE mkt.lead_scores"
        "(lead_id INT, region TEXT, is_mql BOOL, qualified_date INT)"
    )
    adapter.execute(
        "CREATE TABLE mkt.touchpoints(lead_id INT, channel TEXT, touch_date INT)"
    )
    adapter.execute(
        "INSERT INTO mkt.lead_scores VALUES "
        "(1,'DACH',true,100),(2,'DACH',true,120),(3,'APAC',true,130),"
        "(4,'DACH',false,140)"
    )
    adapter.execute(
        "INSERT INTO mkt.touchpoints VALUES "
        "(1,'search',10),(1,'email',50),(2,'social',20),(2,'search',60),"
        "(3,'search',30),(4,'email',40)"
    )
    return adapter


def _contract(*tables: str) -> DataContract:
    """A minimal contract governing exactly *tables* in schema `mkt`."""
    listed = ", ".join(tables)
    return DataContract.from_yaml_string(
        f"""
version: "1.0"
name: sensitivity-test
semantic:
  allowed_tables:
    - schema: mkt
      tables: [{listed}]
  forbidden_operations: [DELETE, DROP]
  rules: []
"""
    )


@pytest.fixture
def mkt_contract() -> DataContract:
    return _contract("lead_scores", "touchpoints")


def _metric(*props: SensitivityProperty) -> MetricDefinition:
    return MetricDefinition(
        name="mql_count",
        description="MQLs, first-touch attributed",
        sql_expression="COUNT(DISTINCT lead_id)",
        sensitivity=list(props),
    )


FIRST_TOUCH_PROP = SensitivityProperty(
    name="attribution_is_first_touch",
    description="A touchpoint after qualification cannot change a first touch.",
    shadow=MKT_SHADOW,
    expect="unchanged",
)


class TestCheckSensitivity:
    def test_first_touch_query_passes(self, mkt_adapter, mkt_contract) -> None:
        report = check_sensitivity(
            _metric(FIRST_TOUCH_PROP),
            FIRST_TOUCH,
            contract=mkt_contract,
            adapter=mkt_adapter,
        )
        (result,) = report.results
        assert result.status == "pass"
        assert result.moved is False
        assert report.ok is True

    def test_fan_out_query_violates(self, mkt_adapter, mkt_contract) -> None:
        report = check_sensitivity(
            _metric(FIRST_TOUCH_PROP),
            FAN_OUT,
            contract=mkt_contract,
            adapter=mkt_adapter,
        )
        (result,) = report.results
        assert result.status == "violation"
        assert result.moved is True
        assert report.ok is False

    def test_query_not_touching_the_table_is_not_applicable(
        self, mkt_adapter, mkt_contract
    ) -> None:
        report = check_sensitivity(
            _metric(FIRST_TOUCH_PROP),
            "SELECT count(*) FROM mkt.lead_scores",
            contract=mkt_contract,
            adapter=mkt_adapter,
        )
        (result,) = report.results
        assert result.status == "not_applicable"
        assert result.moved is None
        assert report.ok is True

    def test_metric_with_no_properties_yields_an_empty_ok_report(
        self, mkt_adapter, mkt_contract
    ) -> None:
        report = check_sensitivity(
            _metric(),
            FIRST_TOUCH,
            contract=mkt_contract,
            adapter=mkt_adapter,
        )
        assert report.results == ()
        assert report.ok is True

    def test_unparseable_sql_is_unchecked(self, mkt_adapter, mkt_contract) -> None:
        report = check_sensitivity(
            _metric(FIRST_TOUCH_PROP),
            "SELECT FROM mkt.touchpoints )(",
            contract=mkt_contract,
            adapter=mkt_adapter,
        )
        (result,) = report.results
        assert result.status == "unchecked"
        assert report.ok is False

    def test_engine_error_is_unchecked(self, mkt_adapter, mkt_contract) -> None:
        report = check_sensitivity(
            _metric(FIRST_TOUCH_PROP),
            "SELECT no_such_column FROM mkt.touchpoints",
            contract=mkt_contract,
            adapter=mkt_adapter,
        )
        (result,) = report.results
        assert result.status == "unchecked"

    def test_vacuous_changes_test_is_unchecked(self, mkt_adapter, mkt_contract) -> None:
        # An empty answer cannot move, so `expect: changes` asserts nothing.
        prop = SensitivityProperty(
            name="must_move",
            description="the answer must respond",
            shadow=MKT_SHADOW,
            expect="changes",
        )
        report = check_sensitivity(
            _metric(prop),
            "SELECT channel FROM mkt.touchpoints WHERE false",
            contract=mkt_contract,
            adapter=mkt_adapter,
        )
        (result,) = report.results
        assert result.status == "unchecked"
        assert "vacuous" in result.reason

    def test_properties_selects_a_subset(self, mkt_adapter, mkt_contract) -> None:
        second = SensitivityProperty(
            name="other",
            description="another claim",
            shadow=MKT_SHADOW,
            expect="changes",
        )
        report = check_sensitivity(
            _metric(FIRST_TOUCH_PROP, second),
            FIRST_TOUCH,
            contract=mkt_contract,
            adapter=mkt_adapter,
            properties=["other"],
        )
        assert [r.name for r in report.results] == ["other"]

    def test_unknown_property_name_raises(self, mkt_adapter, mkt_contract) -> None:
        with pytest.raises(ValueError, match="nope"):
            check_sensitivity(
                _metric(FIRST_TOUCH_PROP),
                FIRST_TOUCH,
                contract=mkt_contract,
                adapter=mkt_adapter,
                properties=["nope"],
            )

    def test_base_is_executed_once_and_reused(
        self, mkt_adapter, mkt_contract, monkeypatch
    ) -> None:
        # Two base executions per call plus one per property -- not three per
        # property. With two properties that is 2 + 2 = 4, not 6.
        seen: list[str] = []
        original = mkt_adapter.execute

        def spy(sql: str):
            seen.append(sql)
            return original(sql)

        monkeypatch.setattr(mkt_adapter, "execute", spy)
        second = SensitivityProperty(
            name="other",
            description="another claim",
            shadow=MKT_SHADOW,
            expect="unchanged",
        )
        check_sensitivity(
            _metric(FIRST_TOUCH_PROP, second),
            FIRST_TOUCH,
            contract=mkt_contract,
            adapter=mkt_adapter,
        )
        assert len(seen) == 4
        assert sum(1 for s in seen if "__sens_" not in s) == 2


class TestPolicy:
    def test_query_blocked_by_layer_one_raises(self, mkt_adapter) -> None:
        # Without this, check_sensitivity is an entry point that executes
        # arbitrary SQL against the adapter with none of the checks every other
        # path applies.
        contract = _contract("other")
        with pytest.raises(ValueError, match="blocked"):
            check_sensitivity(
                _metric(FIRST_TOUCH_PROP),
                FIRST_TOUCH,
                contract=contract,
                adapter=mkt_adapter,
            )

    def test_shadow_reading_an_ungoverned_table_raises(self, mkt_adapter) -> None:
        # The shadow reads mkt.lead_scores, which this contract does not govern.
        contract = _contract("touchpoints")
        with pytest.raises(ValueError, match="lead_scores"):
            check_sensitivity(
                _metric(FIRST_TOUCH_PROP),
                "SELECT count(*) FROM mkt.touchpoints",
                contract=contract,
                adapter=mkt_adapter,
            )

    def test_validate_sensitivity_tables_reports_the_same_problem(self) -> None:
        contract = _contract("touchpoints")
        problems = validate_sensitivity_tables(contract, [_metric(FIRST_TOUCH_PROP)])
        assert len(problems) == 1
        assert "lead_scores" in problems[0]

    def test_validate_sensitivity_tables_is_silent_when_governed(self) -> None:
        contract = _contract("touchpoints", "lead_scores")
        assert validate_sensitivity_tables(contract, [_metric(FIRST_TOUCH_PROP)]) == []
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `uv run pytest tests/test_validation/test_sensitivity.py -v`
Expected: FAIL — `ImportError: cannot import name 'check_sensitivity'`.

`DataContract` has no `from_dict`; `from_yaml_string` is the inline loader the
existing validation tests use, and the YAML above mirrors
`tests/fixtures/minimal_contract.yml`.

- [ ] **Step 3: Implement `check_sensitivity`**

Append to `validation/sensitivity.py`. Add these imports at the top of the file:

```python
from collections.abc import Sequence
from typing import TYPE_CHECKING

from agentic_data_contracts.core.contract import DataContract
from agentic_data_contracts.semantic.base import MetricDefinition
from agentic_data_contracts.validation.validator import Validator

if TYPE_CHECKING:
    from agentic_data_contracts.adapters.base import DatabaseAdapter
```

Then the body:

```python
#: Base executions per call. Two is a filter, not a proof -- see the note on
#: determinism in ``check_sensitivity``.
DEFAULT_REPEATS = 2


def _shadow_tables(shadow: Shadow, *, dialect: str | None) -> set[str] | None:
    """Qualified names ``shadow.sql`` reads, or None if it does not parse."""
    try:
        tree = sqlglot.parse_one(shadow.sql, dialect=dialect)
    except Exception:  # noqa: BLE001 - an unparseable shadow is one outcome
        return None
    if tree is None:
        return None
    ctes = _cte_aliases(tree)
    names: set[str] = set()
    for node in tree.find_all(exp.Table):
        if node.name.lower() in ctes and not node.db:
            continue
        names.add(f"{node.db}.{node.name}" if node.db else node.name)
    return names


def validate_sensitivity_tables(
    contract: DataContract, metrics: list[MetricDefinition]
) -> list[str]:
    """Problems where a shadow reads a table the contract does not govern.

    A shadow that reads an ungoverned table is a governance hole, not a
    convenience: it would let a contract-authored SELECT reach data no agent
    query may reach. This is the CI-time gate; ``check_sensitivity`` refuses
    the same condition at execution time.

    A shadow sqlglot cannot parse yields no problem here -- it cannot be
    checked, and it degrades to ``unchecked`` at run time rather than being
    silently trusted.
    """
    allowed = {name.lower() for name in contract.allowed_table_names()}
    problems: list[str] = []
    for metric in metrics:
        for prop in metric.sensitivity:
            read = _shadow_tables(prop.shadow, dialect=None)
            if read is None:
                continue
            for name in sorted(read):
                if name.lower() not in allowed:
                    problems.append(
                        f"metric {metric.name!r} sensitivity property "
                        f"{prop.name!r}: shadow reads {name!r}, which the "
                        "contract does not allow"
                    )
    return problems


def check_sensitivity(
    metric: MetricDefinition,
    sql: str,
    *,
    contract: DataContract,
    adapter: DatabaseAdapter,
    properties: Sequence[str] | None = None,
    repeats: int = DEFAULT_REPEATS,
    dialect: str | None = None,
) -> SensitivityReport:
    """Check *sql* against the sensitivity properties *metric* declares.

    ``properties`` selects a subset by name; None runs every declared property.
    An unknown name raises ``ValueError`` -- malformed input raises, data
    conditions are findings, the same split ``reconcile_decomposition`` makes.

    **Step 0: the caller's query must pass Layer 1.** ``sql`` goes through the
    contract's ``Validator`` before anything is executed, and a block raises.
    Without this the function is a policy bypass: an entry point that runs
    arbitrary SQL against the adapter with none of the checks every other path
    applies. It is less a re-run of the caller's own validation than a refusal
    to be the weak door.

    ``shadow.sql`` is NOT put through the Validator. It is contract-authored,
    like ``sql_expression``, and most shadows want the ``SELECT *`` the
    validator would reject. It is instead constrained at both ends: its tables
    must be governed (checked here, and by ``validate_sensitivity_tables``),
    and it can only ever be read.

    **Determinism is filtered, not proved.** The base query is run ``repeats``
    times; disagreement makes every property ``unchecked``. A query that is
    merely *usually* stable passes this and then produces a verdict it did not
    earn. Measured on the DABStep corpus, two executions caught 13 of ~14 flaky
    queries -- the survivor differed on every repetition of the experiment and
    was always a ``LIMIT`` with no ``ORDER BY``. No finite number of probes
    closes this, which is why ``repeats`` is a parameter and the limitation is
    stated rather than engineered away.

    A passing property says the query *responds* to an input the contract says
    it depends on. It never says the answer is right.
    """
    selected = list(metric.sensitivity)
    if properties is not None:
        by_name = {p.name: p for p in selected}
        unknown = [n for n in properties if n not in by_name]
        if unknown:
            raise ValueError(
                f"metric {metric.name!r} declares no sensitivity property named "
                f"{unknown[0]!r}; declared: {sorted(by_name) or 'none'}"
            )
        selected = [by_name[n] for n in properties]

    if not selected:
        return SensitivityReport(results=())

    if dialect is None:
        dialect = adapter.dialect

    # Step 0 -- refuse to be the weak door.
    verdict = Validator(contract, dialect=dialect).validate(sql)
    if verdict.blocked:
        raise ValueError(
            f"query is blocked by the contract and will not be executed: "
            f"{'; '.join(verdict.reasons)}"
        )

    allowed = {name.lower() for name in contract.allowed_table_names()}
    for prop in selected:
        read = _shadow_tables(prop.shadow, dialect=dialect)
        for name in sorted(read or ()):
            if name.lower() not in allowed:
                raise ValueError(
                    f"sensitivity property {prop.name!r} of metric "
                    f"{metric.name!r} has a shadow reading {name!r}, which the "
                    "contract does not allow"
                )

    def _run(statement: str) -> list[tuple]:
        try:
            return _norm(list(adapter.execute(statement).rows))
        except Exception as e:  # noqa: BLE001 - any engine failure is one outcome
            raise _Refused(f"engine error: {e}") from e

    # The base result is computed at most once per call and reused across every
    # property; a metric with four properties costs six executions, not twelve.
    base: list[tuple] | None = None
    base_refusal: str | None = None

    def _base() -> list[tuple]:
        nonlocal base, base_refusal
        if base_refusal is not None:
            raise _Refused(base_refusal)
        if base is None:
            first = _run(sql)
            for _ in range(max(repeats - 1, 0)):
                if _run(sql) != first:
                    base_refusal = "query is not deterministic"
                    raise _Refused(base_refusal)
            base = first
        return base

    results: list[SensitivityResult] = []
    for prop in selected:
        try:
            mutated_sql = _rewrite(sql, prop.shadow, dialect=dialect)
        except _Refused as e:
            reason = str(e)
            status = (
                "not_applicable" if reason.startswith("not_applicable") else "unchecked"
            )
            results.append(
                SensitivityResult(
                    name=prop.name,
                    metric=metric.name,
                    status=status,
                    expected=prop.expect,
                    reason=reason,
                )
            )
            continue

        try:
            before = _base()
            if not before and prop.expect == "changes":
                raise _Refused("vacuous: an empty base result cannot move")
            after = _run(mutated_sql)
        except _Refused as e:
            results.append(
                SensitivityResult(
                    name=prop.name,
                    metric=metric.name,
                    status="unchecked",
                    expected=prop.expect,
                    reason=str(e),
                )
            )
            continue

        moved = after != before
        expected_move = prop.expect == "changes"
        results.append(
            SensitivityResult(
                name=prop.name,
                metric=metric.name,
                status="pass" if moved == expected_move else "violation",
                expected=prop.expect,
                moved=moved,
                reason=""
                if moved == expected_move
                else "the answer did not respond"
                if expected_move
                else "the answer moved",
            )
        )
    return SensitivityReport(results=tuple(results))
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `uv run pytest tests/test_validation/test_sensitivity.py -v`
Expected: PASS, 37 tests.

- [ ] **Step 5: Run the full validation suite**

Run: `uv run pytest tests/test_validation -q`
Expected: PASS, no regressions.

- [ ] **Step 6: Commit**

```bash
git add src/agentic_data_contracts/validation/sensitivity.py \
        tests/test_validation/test_sensitivity.py
git commit -m "feat(validation): check_sensitivity executes declared properties"
```

---

### Task 7: Exports and the `lookup_metric` surface

**Files:**
- Modify: `src/agentic_data_contracts/validation/__init__.py`
- Modify: `src/agentic_data_contracts/tools/factory.py`
- Test: `tests/test_tools/test_lookup_metric_sensitivity.py`
- Test: `tests/test_public_api.py` (if it enumerates exports — read it first)

**Interfaces:**
- Consumes: `check_sensitivity`, `SensitivityResult`, `SensitivityReport`,
  `validate_sensitivity_tables` from Task 6.
- Produces: those four importable from `agentic_data_contracts.validation`, and
  a `sensitivity` key in `_metric_details`' output.

The agent seeing what will be checked is a pre-commitment device and costs
nothing. `shadow.sql` is deliberately NOT surfaced: it is the encoding, not the
claim, and pasting engine SQL into a tool response invites the agent to run it.

- [ ] **Step 1: Write the failing test**

Create `tests/test_tools/test_lookup_metric_sensitivity.py`:

```python
"""lookup_metric surfaces declared sensitivity properties."""

from __future__ import annotations

from datetime import date

from agentic_data_contracts.semantic.base import (
    MetricDefinition,
    SensitivityProperty,
    Shadow,
)
from agentic_data_contracts.tools.factory import _metric_details

PROP = SensitivityProperty(
    name="attribution_is_first_touch",
    description="A touchpoint after qualification cannot change a first touch.",
    shadow=Shadow(table="mkt.touchpoints", sql="SELECT * FROM mkt.touchpoints"),
    expect="unchanged",
)


def _details(metric: MetricDefinition) -> dict:
    # `impact_index` is positional; `today` and `threshold_days` are required
    # keywords feeding the freshness fields, which this test does not exercise.
    return _metric_details(metric, {}, today=date(2026, 9, 18), threshold_days=180)


class TestLookupMetric:
    def test_surfaces_declared_properties(self) -> None:
        metric = MetricDefinition(
            name="mql_count",
            description="",
            sql_expression="COUNT(*)",
            sensitivity=[PROP],
        )
        data = _details(metric)
        assert data["sensitivity"] == [
            {
                "name": "attribution_is_first_touch",
                "description": (
                    "A touchpoint after qualification cannot change a first touch."
                ),
                "expect": "unchanged",
            }
        ]

    def test_does_not_leak_the_shadow_sql(self) -> None:
        metric = MetricDefinition(
            name="mql_count",
            description="",
            sql_expression="COUNT(*)",
            sensitivity=[PROP],
        )
        assert "SELECT" not in repr(_details(metric)["sensitivity"])

    def test_omits_the_key_when_none_declared(self) -> None:
        metric = MetricDefinition(name="m", description="", sql_expression="COUNT(*)")
        assert "sensitivity" not in _details(metric)
```

`_metric_details(metric, impact_index, *, today, threshold_days)` is the real
signature; `impact_index` maps a metric name to its `MetricEdge` list and is
empty here because these tests do not exercise impact rendering.

- [ ] **Step 2: Run the test to verify it fails**

Run: `uv run pytest tests/test_tools/test_lookup_metric_sensitivity.py -v`
Expected: FAIL — `KeyError: 'sensitivity'`.

- [ ] **Step 3: Surface the properties in `_metric_details`**

In `tools/factory.py`, immediately after the `drill_by` block:

```text
    # Name, claim and required response only -- never `shadow.sql`. That is the
    # encoding rather than the claim, and putting engine SQL in a tool response
    # invites the agent to run it.
    if metric.sensitivity:
        data["sensitivity"] = [
            {"name": p.name, "description": p.description, "expect": p.expect}
            for p in metric.sensitivity
        ]
```

- [ ] **Step 4: Add the exports**

In `validation/__init__.py`, add the import block in alphabetical position
(after `reconciliation`):

```python
from agentic_data_contracts.validation.sensitivity import (
    SensitivityReport,
    SensitivityResult,
    check_sensitivity,
    validate_sensitivity_tables,
)
```

and add `"SensitivityReport"`, `"SensitivityResult"`, `"check_sensitivity"`,
`"validate_sensitivity_tables"` to `__all__`, keeping it sorted.

- [ ] **Step 5: Run the tests to verify they pass**

Run: `uv run pytest tests/test_tools/test_lookup_metric_sensitivity.py tests/test_public_api.py -v`
Expected: PASS. If `test_public_api.py` asserts a fixed export list, add the
four names there too.

- [ ] **Step 6: Run the whole suite**

Run: `uv run pytest -q`
Expected: PASS, no regressions.

- [ ] **Step 7: Commit**

```bash
git add src/agentic_data_contracts/validation/__init__.py \
        src/agentic_data_contracts/tools/factory.py \
        tests/test_tools/test_lookup_metric_sensitivity.py \
        tests/test_public_api.py
git commit -m "feat(tools): surface sensitivity properties through lookup_metric"
```

---

### Task 8: Runnable demo and docs

**Files:**
- Create: `examples/revenue_agent/check_sensitivity.py`
- Create: `examples/revenue_agent/expected_sensitivity_output.txt`
- Modify: `examples/revenue_agent/semantic.yml`
- Modify: `README.md`, `docs/architecture.md`, `pyproject.toml`

**Interfaces:**
- Consumes: the public API from Task 7.
- Produces: no new code names.

The demo uses a property readable straight off the existing contract:
`total_revenue` already declares `filters: ["status = 'completed'"]`, so a
pending order cannot change it. That is the same shape as the defect the
DABStep eval found — an agent that reads the metric description and drops the
filter — and it shows the property being *derived from a clause the contract
already states*, not invented.

- [ ] **Step 1: Add the property to the example contract**

In `examples/revenue_agent/semantic.yml`, inside the `total_revenue` metric,
after its `drill_by:` block:

```yaml
    # A claim the contract already makes in `filters:`, restated as something a
    # machine can check. `check_sensitivity` shadows analytics.orders with a
    # copy that carries an extra pending order; a query that filters to
    # completed cannot move, and one that forgot the filter does.
    sensitivity:
      - name: only_completed_orders_count
        description: >
          Revenue recognizes completed orders only. A pending or refunded
          order, however large, cannot change total_revenue.
        shadow:
          table: analytics.orders
          sql: >
            SELECT * FROM analytics.orders
            UNION ALL
            SELECT 900 + c.id, c.id, 999999.00, 'pending', c.tenant_id,
                   DATE '2026-01-01'
            FROM analytics.customers c
        expect: unchanged
```

- [ ] **Step 2: Write the demo script**

Create `examples/revenue_agent/check_sensitivity.py`:

```python
"""Does the query derive what the contract says it depends on?

Run:
    uv run python examples/revenue_agent/setup_db.py       # once
    uv run python examples/revenue_agent/check_sensitivity.py

What this shows
---------------
The validator sees *policy* (allowed tables, forbidden operations, required
filters) and *plannability* (a live EXPLAIN). Neither sees a query that is
authorized, parseable, plannable -- and computes the wrong thing.

Both queries below are allowed and both run. One of them forgot
``status = 'completed'``, which the contract states in the metric's ``filters``
and again in its ``sensitivity`` property. Nothing downstream disagrees with
it: the number is plausible, the query is legal, and the agent moves on.

``check_sensitivity`` shadows ``analytics.orders`` with a copy carrying one
extra *pending* order per customer and re-runs each query. A query that filters
to completed orders cannot move. One that does not, moves -- and that is the
violation.

Nothing is written. The shadow is a SELECT spliced in as a CTE; the database is
never modified, and no DDL runs.

This is the fourth validation verb the library contributes:

  * ``validate_examples``     -- is this SQL still *allowed* and *plannable*?
  * ``check_example_answers`` -- does it still return the *right number*?
  * ``check_schema_drift``    -- do the *declarations* still describe reality?
  * ``check_sensitivity``     -- does the query *derive* what it depends on?

A passing property never says the answer is right. It says the query responded
to an input the contract says it depends on.
"""

from __future__ import annotations

import sys
from pathlib import Path

from agentic_data_contracts.adapters.duckdb import DuckDBAdapter
from agentic_data_contracts.core.contract import DataContract
from agentic_data_contracts.semantic.yaml_source import YamlSource
from agentic_data_contracts.validation import (
    check_sensitivity,
    validate_sensitivity_tables,
)

HERE = Path(__file__).parent

CORRECT = """
SELECT SUM(amount) AS revenue
FROM analytics.orders
WHERE status = 'completed' AND tenant_id = 'acme'
"""

DEFECTIVE = """
SELECT SUM(amount) AS revenue
FROM analytics.orders
WHERE tenant_id = 'acme'
"""


def main() -> int:
    contract = DataContract.from_yaml(HERE / "contract.yml")
    source = YamlSource(HERE / "semantic.yml")
    adapter = DuckDBAdapter(str(HERE / "sample_data.duckdb"))

    problems = validate_sensitivity_tables(contract, source.get_metrics())
    if problems:
        print("Shadow reads an ungoverned table:")
        for p in problems:
            print(f"  - {p}")
        return 1

    metric = source.get_metric("total_revenue")
    assert metric is not None

    failed = False
    for label, sql in (
        ("filters to completed", CORRECT),
        ("no status filter", DEFECTIVE),
    ):
        report = check_sensitivity(metric, sql, contract=contract, adapter=adapter)
        print(f"\n{label}:")
        print(report.summary())
        failed = failed or not report.ok

    print("\nThe second query is legal, plannable and wrong. Only the")
    print("behavioural check disagrees with it.")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
```

- [ ] **Step 3: Run the demo and capture its output**

```bash
uv run python examples/revenue_agent/setup_db.py
uv run python examples/revenue_agent/check_sensitivity.py \
  | tee examples/revenue_agent/expected_sensitivity_output.txt
echo "exit: $?"
```

Expected: the first query reports `1 pass`, the second `1 violation`, and the
script exits 1. If `DataContract.from_yaml` is not the loader this example
already uses, copy the loading lines verbatim from
`examples/revenue_agent/check_drift.py` — that file is known to work.

- [ ] **Step 4: Add the README section**

Add a section titled **"Checking that a query derives what it depends on"**,
placed immediately after the existing verified-examples / schema-drift
sections. It must contain: the `sensitivity:` YAML from Step 1; a
`check_sensitivity` call; the statement that a passing property says the query
*responds*, never that the answer is *right*; the cost (two base executions per
call plus one per property); the no-writes guarantee; and a cross-reference to
`reconcile_decomposition` and `validate_examples` as the other members of the
family. Point at `examples/revenue_agent/check_sensitivity.py` as the runnable
version.

- [ ] **Step 5: Update `docs/architecture.md`**

Add `sensitivity` to the `MetricDefinition` field enumeration and
`validation/sensitivity.py` to the validation-module list. Grep for
`drill_by` and `reconciliation` in that file to find both places.

- [ ] **Step 6: Bump the version**

In `pyproject.toml`, set `version = "0.52.0"`.

- [ ] **Step 7: Run everything**

```bash
uv run pytest -q
prek run --all-files
```

Expected: both clean. `prek` reproduces CI exactly; a bare `ruff` or `ty` does
not.

- [ ] **Step 8: Commit**

```bash
git add examples/revenue_agent/ README.md docs/architecture.md pyproject.toml
git commit -m "docs: sensitivity checks — README, architecture, runnable demo

Release 0.52.0."
```

---

## Self-Review

**Spec coverage.** Section 1 (contract schema, key sets, dataclasses, load-time
validation, digest stability) → Tasks 1–3. Section 2 (the public function,
step 0, the five steps) → Tasks 4 and 6. Section 3 (result types) → Task 5.
Section 4 (placement and exports, `lookup_metric`) → Task 7. Section 5 (guards)
→ Tasks 4 and 6, one test per row. Ownership → Task 2 (`description` required)
and the README in Task 8. Worked example → Task 6's two acceptance tests.
Testing → distributed. Docs → Task 8. Compatibility → Task 3.

**Two spec requirements deliberately not implemented, both flagged inline:**
the `allowed_tables` check moves from load time to Task 6 (DEVIATION 3 — it is
not implementable where the spec puts it), and `metric` becomes a
`MetricDefinition` (DEVIATION 4). DEVIATIONS 1 and 2 are cosmetic consistency
with `decompositions`.

**Deferred with the spec's blessing:** metric-vs-table scope stays the spec's
open question; no agent tool; no auto-derived properties.

**Type consistency.** `SensitivityProperty(name, description, shadow, expect)`
is constructed identically in Tasks 1, 6 and 7. `Shadow(table, sql)` likewise.
`_rewrite(sql, shadow, *, dialect)` is defined in Task 4 and called in Task 6
with the same signature. `SensitivityResult(name, metric, status, expected,
moved, reason)` is defined in Task 5 and constructed in Task 6 with exactly
those keywords. `check_sensitivity` keeps one signature across Tasks 6, 7 and 8.
`validate_sensitivity` (semantic, raises) and `validate_sensitivity_tables`
(validation, returns a list) are distinct names for distinct jobs — that is
deliberate and worth not "fixing".
