# RelationshipChecker Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add advisory validation of SQL JOINs against declared semantic relationships — covering join-key correctness, required-filter presence, and fan-out risk detection.

**Architecture:** A `RelationshipChecker` class in `checkers.py` receives `Relationship` objects, builds a bidirectional lookup map by table pair, and exposes `check_joins(ast) -> list[str]` that returns warning strings. The `Validator` gains an optional `semantic_source` constructor parameter and calls the checker after blocking checks, appending results to `warnings`.

**Tech Stack:** Python 3.12+, sqlglot (AST parsing), Pydantic (existing models), pytest (TDD)

**Spec:** `docs/superpowers/specs/2026-04-11-relationship-checker-design.md`

---

## File Map

| File | Action | Responsibility |
|------|--------|----------------|
| `tests/fixtures/relationships_checker.yml` | Create | YAML fixture with relationships for testing |
| `tests/test_validation/test_relationship_checker.py` | Create | All RelationshipChecker unit tests |
| `src/agentic_data_contracts/validation/checkers.py` | Modify | Add `RelationshipChecker` class |
| `src/agentic_data_contracts/validation/validator.py` | Modify | Add `semantic_source` param, wire up checker |
| `src/agentic_data_contracts/validation/__init__.py` | Modify | Export `RelationshipChecker` |
| `tests/test_validation/test_validator.py` | Modify | Add integration test for Validator + SemanticSource |

---

### Task 1: Create test fixture and write join-key correctness tests

**Files:**
- Create: `tests/fixtures/relationships_checker.yml`
- Create: `tests/test_validation/test_relationship_checker.py`

- [ ] **Step 1: Create the YAML fixture**

```yaml
# tests/fixtures/relationships_checker.yml
relationships:
  - from: analytics.orders.customer_id
    to: analytics.customers.id
    type: many_to_one
    description: Each order belongs to one customer
    required_filter: "status != 'cancelled'"
  - from: analytics.orders.id
    to: analytics.order_items.order_id
    type: one_to_many
    description: Each order has many line items
  - from: analytics.customers.id
    to: analytics.addresses.customer_id
    type: one_to_one
    description: Each customer has one address
```

- [ ] **Step 2: Write the failing tests for join-key correctness**

```python
# tests/test_validation/test_relationship_checker.py
from pathlib import Path
from typing import cast

import sqlglot
from sqlglot import exp

from agentic_data_contracts.semantic.base import Relationship
from agentic_data_contracts.semantic.yaml_source import YamlSource
from agentic_data_contracts.validation.checkers import RelationshipChecker


def _parse(sql: str) -> exp.Expression:
    return cast(exp.Expression, sqlglot.parse_one(sql))


def _load_relationships(fixtures_dir: Path) -> list[Relationship]:
    source = YamlSource(fixtures_dir / "relationships_checker.yml")
    return source.get_relationships()


class TestJoinKeyCorrectness:
    """Tests that the checker warns when join columns don't match declared relationships."""

    def test_correct_join_key_no_warning(self, fixtures_dir: Path) -> None:
        rels = _load_relationships(fixtures_dir)
        checker = RelationshipChecker(rels)
        ast = _parse(
            "SELECT o.id, c.name FROM analytics.orders o"
            " JOIN analytics.customers c ON o.customer_id = c.id"
        )
        warnings = checker.check_joins(ast)
        assert warnings == []

    def test_wrong_join_key_warns(self, fixtures_dir: Path) -> None:
        rels = _load_relationships(fixtures_dir)
        checker = RelationshipChecker(rels)
        ast = _parse(
            "SELECT o.id, c.name FROM analytics.orders o"
            " JOIN analytics.customers c ON o.email = c.email"
        )
        warnings = checker.check_joins(ast)
        assert len(warnings) == 1
        assert "customer_id" in warnings[0]
        assert "email" in warnings[0]

    def test_undeclared_join_silent(self, fixtures_dir: Path) -> None:
        rels = _load_relationships(fixtures_dir)
        checker = RelationshipChecker(rels)
        ast = _parse(
            "SELECT o.id, p.name FROM analytics.orders o"
            " JOIN analytics.products p ON o.product_id = p.id"
        )
        warnings = checker.check_joins(ast)
        assert warnings == []

    def test_bare_table_names_match(self, fixtures_dir: Path) -> None:
        """Agent omits schema prefix — should still match relationship."""
        rels = _load_relationships(fixtures_dir)
        checker = RelationshipChecker(rels)
        ast = _parse(
            "SELECT o.id, c.name FROM orders o"
            " JOIN customers c ON o.customer_id = c.id"
        )
        warnings = checker.check_joins(ast)
        assert warnings == []

    def test_reversed_join_order_matches(self, fixtures_dir: Path) -> None:
        """FROM customers JOIN orders should still match the relationship."""
        rels = _load_relationships(fixtures_dir)
        checker = RelationshipChecker(rels)
        ast = _parse(
            "SELECT c.name, o.id FROM analytics.customers c"
            " JOIN analytics.orders o ON o.customer_id = c.id"
        )
        warnings = checker.check_joins(ast)
        assert warnings == []

    def test_case_insensitive_table_match(self, fixtures_dir: Path) -> None:
        rels = _load_relationships(fixtures_dir)
        checker = RelationshipChecker(rels)
        ast = _parse(
            "SELECT o.id FROM Analytics.Orders o"
            " JOIN Analytics.Customers c ON o.customer_id = c.id"
        )
        warnings = checker.check_joins(ast)
        assert warnings == []
```

- [ ] **Step 3: Run tests to verify they fail**

Run: `uv run pytest tests/test_validation/test_relationship_checker.py -v`
Expected: ImportError — `cannot import name 'RelationshipChecker' from 'agentic_data_contracts.validation.checkers'`

- [ ] **Step 4: Commit test file and fixture**

```bash
git add tests/fixtures/relationships_checker.yml tests/test_validation/test_relationship_checker.py
git commit -m "test: add failing tests for RelationshipChecker join-key correctness"
```

---

### Task 2: Implement RelationshipChecker with join-key correctness

**Files:**
- Modify: `src/agentic_data_contracts/validation/checkers.py`

- [ ] **Step 1: Add the RelationshipChecker class to checkers.py**

Add these imports at the top of `checkers.py`:

```python
from agentic_data_contracts.semantic.base import Relationship
```

Add the class at the end of the file (after `ResultCheckRunner`):

```python
class RelationshipChecker:
    """Validates SQL JOINs against declared semantic relationships.

    Produces warnings only — never blocks. Silent on undeclared joins.
    """

    def __init__(self, relationships: list[Relationship]) -> None:
        self._relationships = relationships
        self._relationship_map = self._build_map(relationships)

    @staticmethod
    def _parse_ref(ref: str) -> tuple[str, str]:
        """Parse 'schema.table.column' into (table, column), case-insensitive."""
        parts = ref.lower().split(".")
        if len(parts) == 3:
            return parts[1], parts[2]  # schema.table.column -> (table, column)
        if len(parts) == 2:
            return parts[0], parts[1]  # table.column -> (table, column)
        return parts[0], ""  # fallback

    @staticmethod
    def _build_map(
        relationships: list[Relationship],
    ) -> dict[tuple[str, str], list[Relationship]]:
        """Build bidirectional lookup: (table_a, table_b) -> [Relationship, ...]."""
        result: dict[tuple[str, str], list[Relationship]] = {}
        for rel in relationships:
            from_table, _ = RelationshipChecker._parse_ref(rel.from_)
            to_table, _ = RelationshipChecker._parse_ref(rel.to)
            key_fwd = (from_table, to_table)
            key_rev = (to_table, from_table)
            result.setdefault(key_fwd, []).append(rel)
            result.setdefault(key_rev, []).append(rel)
        return result

    @staticmethod
    def _build_alias_map(ast: exp.Expression) -> dict[str, str]:
        """Build alias -> table_name map from the AST, case-insensitive."""
        alias_map: dict[str, str] = {}
        for table in ast.find_all(exp.Table):
            name_parts = []
            if table.db:
                name_parts.append(table.db)
            name_parts.append(table.name)
            full_name = ".".join(name_parts)
            # Map the bare table name (lowered) for lookups
            bare_name = table.name.lower()
            alias_map[bare_name] = bare_name
            if table.alias:
                alias_map[table.alias.lower()] = bare_name
        return alias_map

    @staticmethod
    def _extract_join_columns(
        join_expr: exp.Join, alias_map: dict[str, str]
    ) -> list[tuple[str, str, str, str]]:
        """Extract (left_table, left_col, right_table, right_col) from a JOIN's ON clause."""
        results: list[tuple[str, str, str, str]] = []
        on_clause = join_expr.args.get("on")
        if on_clause is None:
            return results
        for eq in on_clause.find_all(exp.EQ):
            left = eq.left
            right = eq.right
            if isinstance(left, exp.Column) and isinstance(right, exp.Column):
                l_table = alias_map.get(
                    left.table.lower(), left.table.lower()
                ) if left.table else ""
                r_table = alias_map.get(
                    right.table.lower(), right.table.lower()
                ) if right.table else ""
                results.append(
                    (l_table, left.name.lower(), r_table, right.name.lower())
                )
        return results

    def check_joins(self, ast: exp.Expression) -> list[str]:
        """Check all JOINs in the AST against declared relationships. Returns warnings."""
        warnings: list[str] = []
        alias_map = self._build_alias_map(ast)

        for join in ast.find_all(exp.Join):
            join_cols = self._extract_join_columns(join, alias_map)
            for l_table, l_col, r_table, r_col in join_cols:
                if not l_table or not r_table:
                    continue
                key = (l_table, r_table)
                rels = self._relationship_map.get(key)
                if rels is None:
                    continue  # undeclared join — silent

                # Check join-key correctness
                for rel in rels:
                    from_table, from_col = self._parse_ref(rel.from_)
                    to_table, to_col = self._parse_ref(rel.to)
                    # Match columns in either direction
                    correct = (
                        ({l_col, r_col} == {from_col, to_col})
                    )
                    if not correct:
                        warnings.append(
                            f"Join `{l_table}` -> `{r_table}` uses columns "
                            f"`{l_col}`, `{r_col}` but declared relationship "
                            f"specifies `{from_col}` -> `{to_col}`"
                        )

        return warnings
```

- [ ] **Step 2: Run the join-key tests to verify they pass**

Run: `uv run pytest tests/test_validation/test_relationship_checker.py::TestJoinKeyCorrectness -v`
Expected: All 6 tests PASS

- [ ] **Step 3: Run the full existing test suite to ensure no regressions**

Run: `uv run pytest tests/ -v`
Expected: All existing tests PASS

- [ ] **Step 4: Commit**

```bash
git add src/agentic_data_contracts/validation/checkers.py
git commit -m "feat: add RelationshipChecker with join-key correctness detection"
```

---

### Task 3: Add required-filter detection tests and implementation

**Files:**
- Modify: `tests/test_validation/test_relationship_checker.py`
- Modify: `src/agentic_data_contracts/validation/checkers.py`

- [ ] **Step 1: Write the failing tests for required-filter detection**

Add to `tests/test_validation/test_relationship_checker.py`:

```python
class TestRequiredFilterEnforcement:
    """Tests that the checker warns when a required_filter is missing."""

    def test_required_filter_present_no_warning(self, fixtures_dir: Path) -> None:
        rels = _load_relationships(fixtures_dir)
        checker = RelationshipChecker(rels)
        ast = _parse(
            "SELECT o.id, c.name FROM analytics.orders o"
            " JOIN analytics.customers c ON o.customer_id = c.id"
            " WHERE o.status != 'cancelled'"
        )
        warnings = checker.check_joins(ast)
        assert warnings == []

    def test_required_filter_absent_warns(self, fixtures_dir: Path) -> None:
        rels = _load_relationships(fixtures_dir)
        checker = RelationshipChecker(rels)
        ast = _parse(
            "SELECT o.id, c.name FROM analytics.orders o"
            " JOIN analytics.customers c ON o.customer_id = c.id"
        )
        warnings = checker.check_joins(ast)
        assert len(warnings) == 1
        assert "status" in warnings[0]
        assert "required_filter" in warnings[0].lower() or "required filter" in warnings[0].lower()

    def test_no_required_filter_on_relationship_no_warning(self, fixtures_dir: Path) -> None:
        """order_items relationship has no required_filter — should be silent."""
        rels = _load_relationships(fixtures_dir)
        checker = RelationshipChecker(rels)
        ast = _parse(
            "SELECT o.id, oi.quantity FROM analytics.orders o"
            " JOIN analytics.order_items oi ON o.id = oi.order_id"
        )
        warnings = checker.check_joins(ast)
        assert warnings == []

    def test_required_filter_with_different_expression_no_warning(self, fixtures_dir: Path) -> None:
        """Agent filters on `status` but with different expression — no warning (column presence only)."""
        rels = _load_relationships(fixtures_dir)
        checker = RelationshipChecker(rels)
        ast = _parse(
            "SELECT o.id, c.name FROM analytics.orders o"
            " JOIN analytics.customers c ON o.customer_id = c.id"
            " WHERE o.status = 'active'"
        )
        warnings = checker.check_joins(ast)
        assert warnings == []
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/test_validation/test_relationship_checker.py::TestRequiredFilterEnforcement -v`
Expected: FAIL — `test_required_filter_absent_warns` expects a warning but gets none

- [ ] **Step 3: Add required-filter detection to check_joins**

In `src/agentic_data_contracts/validation/checkers.py`, update the `check_joins` method in `RelationshipChecker`. Replace the existing `check_joins` method with:

```python
    def check_joins(self, ast: exp.Expression) -> list[str]:
        """Check all JOINs in the AST against declared relationships. Returns warnings."""
        warnings: list[str] = []
        alias_map = self._build_alias_map(ast)
        matched_rels: list[Relationship] = []

        for join in ast.find_all(exp.Join):
            join_cols = self._extract_join_columns(join, alias_map)
            for l_table, l_col, r_table, r_col in join_cols:
                if not l_table or not r_table:
                    continue
                key = (l_table, r_table)
                rels = self._relationship_map.get(key)
                if rels is None:
                    continue  # undeclared join — silent

                for rel in rels:
                    from_table, from_col = self._parse_ref(rel.from_)
                    to_table, to_col = self._parse_ref(rel.to)
                    correct = {l_col, r_col} == {from_col, to_col}
                    if not correct:
                        warnings.append(
                            f"Join `{l_table}` -> `{r_table}` uses columns "
                            f"`{l_col}`, `{r_col}` but declared relationship "
                            f"specifies `{from_col}` -> `{to_col}`"
                        )
                    else:
                        matched_rels.append(rel)

        # Check required_filter for matched relationships
        warnings.extend(self._check_required_filters(ast, matched_rels))

        return warnings

    @staticmethod
    def _extract_where_columns(ast: exp.Expression) -> set[str]:
        """Extract all column names referenced in WHERE clauses."""
        columns: set[str] = set()
        for where in ast.find_all(exp.Where):
            for col in where.find_all(exp.Column):
                columns.add(col.name.lower())
        return columns

    @staticmethod
    def _extract_filter_columns(required_filter: str) -> set[str]:
        """Extract column names from a required_filter expression string."""
        try:
            parsed = sqlglot.parse_one(f"SELECT 1 WHERE {required_filter}")
            columns: set[str] = set()
            for where in parsed.find_all(exp.Where):
                for col in where.find_all(exp.Column):
                    columns.add(col.name.lower())
            return columns
        except sqlglot.errors.ParseError:
            # If we can't parse the filter, extract simple word tokens
            import re
            return {w.lower() for w in re.findall(r"[a-zA-Z_]\w*", required_filter)
                    if w.upper() not in ("AND", "OR", "NOT", "NULL", "IS", "IN", "LIKE", "BETWEEN", "TRUE", "FALSE")}

    @staticmethod
    def _check_required_filters(
        ast: exp.Expression, matched_rels: list[Relationship]
    ) -> list[str]:
        """Warn if matched relationships have required_filter but the column is missing from WHERE."""
        warnings: list[str] = []
        where_columns = RelationshipChecker._extract_where_columns(ast)

        for rel in matched_rels:
            if rel.required_filter is None:
                continue
            filter_columns = RelationshipChecker._extract_filter_columns(rel.required_filter)
            missing = filter_columns - where_columns
            if missing:
                from_table, _ = RelationshipChecker._parse_ref(rel.from_)
                to_table, _ = RelationshipChecker._parse_ref(rel.to)
                warnings.append(
                    f"Join `{from_table}` -> `{to_table}` has required filter "
                    f"`{rel.required_filter}` but query does not filter on: "
                    f"{', '.join(sorted(missing))}"
                )

        return warnings
```

Also add the `sqlglot` import at the top of the `_extract_filter_columns` method — but since `sqlglot` is already imported in the file's scope via `from sqlglot import exp`, you also need to add at the top of `checkers.py`:

```python
import sqlglot
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/test_validation/test_relationship_checker.py -v`
Expected: All tests PASS (both TestJoinKeyCorrectness and TestRequiredFilterEnforcement)

- [ ] **Step 5: Commit**

```bash
git add tests/test_validation/test_relationship_checker.py src/agentic_data_contracts/validation/checkers.py
git commit -m "feat: add required-filter detection to RelationshipChecker"
```

---

### Task 4: Add fan-out risk detection tests and implementation

**Files:**
- Modify: `tests/test_validation/test_relationship_checker.py`
- Modify: `src/agentic_data_contracts/validation/checkers.py`

- [ ] **Step 1: Write the failing tests for fan-out detection**

Add to `tests/test_validation/test_relationship_checker.py`:

```python
class TestFanOutDetection:
    """Tests that the checker warns when aggregating across a one_to_many join."""

    def test_aggregation_with_one_to_many_warns(self, fixtures_dir: Path) -> None:
        rels = _load_relationships(fixtures_dir)
        checker = RelationshipChecker(rels)
        ast = _parse(
            "SELECT SUM(o.amount) FROM analytics.orders o"
            " JOIN analytics.order_items oi ON o.id = oi.order_id"
        )
        warnings = checker.check_joins(ast)
        assert len(warnings) == 1
        assert "one_to_many" in warnings[0]
        assert "order_items" in warnings[0]

    def test_no_aggregation_with_one_to_many_no_warning(self, fixtures_dir: Path) -> None:
        rels = _load_relationships(fixtures_dir)
        checker = RelationshipChecker(rels)
        ast = _parse(
            "SELECT o.id, oi.quantity FROM analytics.orders o"
            " JOIN analytics.order_items oi ON o.id = oi.order_id"
        )
        warnings = checker.check_joins(ast)
        assert warnings == []

    def test_aggregation_with_many_to_one_no_warning(self, fixtures_dir: Path) -> None:
        rels = _load_relationships(fixtures_dir)
        checker = RelationshipChecker(rels)
        ast = _parse(
            "SELECT SUM(o.amount) FROM analytics.orders o"
            " JOIN analytics.customers c ON o.customer_id = c.id"
            " WHERE o.status != 'cancelled'"
        )
        warnings = checker.check_joins(ast)
        assert warnings == []

    def test_aggregation_with_one_to_one_no_warning(self, fixtures_dir: Path) -> None:
        rels = _load_relationships(fixtures_dir)
        checker = RelationshipChecker(rels)
        ast = _parse(
            "SELECT COUNT(c.id) FROM analytics.customers c"
            " JOIN analytics.addresses a ON c.id = a.customer_id"
        )
        warnings = checker.check_joins(ast)
        assert warnings == []

    def test_multiple_aggregation_functions_single_warning(self, fixtures_dir: Path) -> None:
        """Multiple agg functions with same 1:N join should produce one warning, not many."""
        rels = _load_relationships(fixtures_dir)
        checker = RelationshipChecker(rels)
        ast = _parse(
            "SELECT SUM(o.amount), AVG(o.amount), COUNT(*) FROM analytics.orders o"
            " JOIN analytics.order_items oi ON o.id = oi.order_id"
        )
        warnings = checker.check_joins(ast)
        assert len(warnings) == 1
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/test_validation/test_relationship_checker.py::TestFanOutDetection -v`
Expected: FAIL — `test_aggregation_with_one_to_many_warns` expects a warning but gets none

- [ ] **Step 3: Add fan-out detection to check_joins**

In `src/agentic_data_contracts/validation/checkers.py`, update the `check_joins` method to call fan-out detection after required-filter checks. Add before the `return warnings` statement:

```python
        # Check fan-out risk for one_to_many matched relationships
        warnings.extend(self._check_fan_out(ast, matched_rels))

        return warnings
```

Then add the `_check_fan_out` static method to the class:

```python
    _AGG_TYPES: tuple[type[exp.Expression], ...] = (
        exp.Sum,
        exp.Avg,
        exp.Count,
        exp.Min,
        exp.Max,
    )

    @staticmethod
    def _has_aggregation(ast: exp.Expression) -> bool:
        """Check if the AST contains any aggregation functions."""
        return any(ast.find_all(*RelationshipChecker._AGG_TYPES))

    @staticmethod
    def _check_fan_out(
        ast: exp.Expression, matched_rels: list[Relationship]
    ) -> list[str]:
        """Warn if query aggregates across a one_to_many join."""
        if not RelationshipChecker._has_aggregation(ast):
            return []

        warnings: list[str] = []
        seen: set[tuple[str, str]] = set()
        for rel in matched_rels:
            if rel.type != "one_to_many":
                continue
            from_table, _ = RelationshipChecker._parse_ref(rel.from_)
            to_table, _ = RelationshipChecker._parse_ref(rel.to)
            pair = (from_table, to_table)
            if pair in seen:
                continue
            seen.add(pair)
            warnings.append(
                f"Query aggregates across a one_to_many join "
                f"(`{from_table}` -> `{to_table}`). "
                f"Results may be inflated by row multiplication."
            )
        return warnings
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/test_validation/test_relationship_checker.py -v`
Expected: All tests PASS (all three test classes)

- [ ] **Step 5: Run full test suite**

Run: `uv run pytest tests/ -v`
Expected: All tests PASS

- [ ] **Step 6: Commit**

```bash
git add tests/test_validation/test_relationship_checker.py src/agentic_data_contracts/validation/checkers.py
git commit -m "feat: add fan-out risk detection to RelationshipChecker"
```

---

### Task 5: Wire RelationshipChecker into Validator

**Files:**
- Modify: `src/agentic_data_contracts/validation/validator.py`
- Modify: `src/agentic_data_contracts/validation/__init__.py`
- Modify: `tests/test_validation/test_validator.py`

- [ ] **Step 1: Write the failing integration tests**

Add to `tests/test_validation/test_validator.py`:

```python
class TestValidatorWithSemanticSource:
    """Tests Validator integration with SemanticSource for relationship checking."""

    def test_validator_without_semantic_source_works(self, fixtures_dir: Path) -> None:
        contract = DataContract.from_yaml(fixtures_dir / "valid_contract.yml")
        validator = Validator(contract)
        result = validator.validate(
            "SELECT id FROM analytics.orders WHERE tenant_id = 'acme'"
        )
        assert not result.blocked

    def test_validator_with_semantic_source_emits_warnings(self, fixtures_dir: Path) -> None:
        contract = DataContract.from_yaml(fixtures_dir / "valid_contract.yml")
        source = YamlSource(fixtures_dir / "relationships_checker.yml")
        validator = Validator(contract, semantic_source=source)
        # Join orders -> customers without required filter (status)
        result = validator.validate(
            "SELECT o.id, c.name FROM analytics.orders o"
            " JOIN analytics.customers c ON o.customer_id = c.id"
            " WHERE o.tenant_id = 'acme'"
        )
        assert not result.blocked  # warnings only, never blocks
        assert any("status" in w for w in result.warnings)

    def test_validator_with_semantic_source_no_warnings_when_correct(self, fixtures_dir: Path) -> None:
        contract = DataContract.from_yaml(fixtures_dir / "valid_contract.yml")
        source = YamlSource(fixtures_dir / "relationships_checker.yml")
        validator = Validator(contract, semantic_source=source)
        result = validator.validate(
            "SELECT o.id, c.name FROM analytics.orders o"
            " JOIN analytics.customers c ON o.customer_id = c.id"
            " WHERE o.tenant_id = 'acme' AND o.status != 'cancelled'"
        )
        assert not result.blocked
        assert result.warnings == []
```

Note: You will need to add the import for `YamlSource` at the top of the test file:

```python
from agentic_data_contracts.semantic.yaml_source import YamlSource
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/test_validation/test_validator.py::TestValidatorWithSemanticSource -v`
Expected: FAIL — `Validator.__init__()` does not accept `semantic_source` parameter

- [ ] **Step 3: Update Validator to accept SemanticSource**

In `src/agentic_data_contracts/validation/validator.py`, add the import:

```python
from agentic_data_contracts.semantic.base import SemanticSource
```

And add the import for `RelationshipChecker`:

```python
from agentic_data_contracts.validation.checkers import (
    BlockedColumnsChecker,
    CheckResult,
    MaxJoinsChecker,
    NoSelectStarChecker,
    OperationBlocklistChecker,
    RelationshipChecker,
    RequiredFilterChecker,
    RequireLimitChecker,
    ResultCheckRunner,
    TableAllowlistChecker,
    extract_tables,
)
```

Update the `Validator.__init__` signature:

```python
    def __init__(
        self,
        contract: DataContract,
        dialect: str | None = None,
        explain_adapter: ExplainAdapter | None = None,
        sql_normalizer: SqlNormalizer | None = None,
        semantic_source: SemanticSource | None = None,
    ) -> None:
        self.contract = contract
        self.dialect = dialect
        self.explain_adapter = explain_adapter
        self.sql_normalizer = sql_normalizer
        self._relationship_checker: RelationshipChecker | None = None
        if semantic_source is not None:
            rels = semantic_source.get_relationships()
            if rels:
                self._relationship_checker = RelationshipChecker(rels)
        self._build_checkers()
```

Update the `validate` method — add relationship checking after the query checkers loop (before the EXPLAIN block). Insert after the `for enforcement, table_scope, checker in self._query_checkers:` loop ends:

```python
        # Relationship advisory checks (warnings only)
        if self._relationship_checker is not None:
            rel_warnings = self._relationship_checker.check_joins(ast)
            warnings.extend(rel_warnings)
```

- [ ] **Step 4: Update __init__.py exports**

In `src/agentic_data_contracts/validation/__init__.py`, add `RelationshipChecker` to both the import and `__all__`:

Add to the import block:
```python
    RelationshipChecker,
```

Add to `__all__`:
```python
    "RelationshipChecker",
```

- [ ] **Step 5: Run the integration tests**

Run: `uv run pytest tests/test_validation/test_validator.py::TestValidatorWithSemanticSource -v`
Expected: All 3 tests PASS

- [ ] **Step 6: Run the full test suite**

Run: `uv run pytest tests/ -v`
Expected: All tests PASS

- [ ] **Step 7: Run linting and type checking**

Run: `uv run ruff check src/ tests/ && uv run ruff format src/ tests/ && ty check`
Expected: No errors

- [ ] **Step 8: Commit**

```bash
git add src/agentic_data_contracts/validation/validator.py src/agentic_data_contracts/validation/__init__.py tests/test_validation/test_validator.py
git commit -m "feat: wire RelationshipChecker into Validator via optional semantic_source"
```

---

### Task 6: Final verification and cleanup

**Files:**
- None (verification only)

- [ ] **Step 1: Run the complete test suite**

Run: `uv run pytest tests/ -v`
Expected: All tests PASS

- [ ] **Step 2: Run pre-commit hooks**

Run: `prek run --all-files`
Expected: All hooks PASS

- [ ] **Step 3: Review the diff since main**

Run: `git log --oneline main..HEAD`
Expected: 5 commits:
1. `test: add failing tests for RelationshipChecker join-key correctness`
2. `feat: add RelationshipChecker with join-key correctness detection`
3. `feat: add required-filter detection to RelationshipChecker`
4. `feat: add fan-out risk detection to RelationshipChecker`
5. `feat: wire RelationshipChecker into Validator via optional semantic_source`
