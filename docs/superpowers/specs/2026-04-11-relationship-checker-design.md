# RelationshipChecker Design Spec

## Summary

A new `RelationshipChecker` that validates SQL JOIN clauses against declared relationships from the `SemanticSource`. It produces **warnings only** — never blocks queries. Inspired by airlayer's join-graph validation but inverted: we validate agent-written SQL against declared relationships rather than generating SQL from a schema.

## Motivation

Currently, relationships defined in semantic YAML are rendered into the agent's system prompt as guidance but never enforced at validation time. This means:

- An agent can join on incorrect columns without any signal
- Required filters on sensitive joins go unenforced
- Fan-out risks from 1:N joins corrupt aggregations silently

The RelationshipChecker closes this gap with an advisory approach — flagging potential issues without blocking legitimate queries.

## Design Decisions

| Decision | Choice | Rationale |
|----------|--------|-----------|
| Enforcement level | Warnings only | Hard blocks cause false positives; LLMs are smart enough to self-correct from warnings |
| Undeclared joins | Silent (no opinion) | Users can't exhaustively declare all relationships; silence avoids noise |
| Fan-out detection | Simple (aggregation + 1:N join = warn) | LLMs can reason from a simple signal; complex detection has diminishing returns |
| Data source | SemanticSource passed into Validator | Avoids duplicating relationship data into contracts |
| Scope | Only fires on declared relationships | No opinion on joins between tables without relationship definitions |

## Architecture

### Component: `RelationshipChecker`

**Location:** `src/agentic_data_contracts/validation/checkers.py` (alongside existing checkers)

**Protocol compliance:** Implements `Checker` protocol via `check_ast(ast, *args) -> CheckResult`

**Constructor:**

```python
class RelationshipChecker:
    def __init__(self, relationships: list[Relationship]) -> None:
        # Build lookup: (table_a, table_b) -> list[Relationship]
        self._relationship_map: dict[tuple[str, str], list[Relationship]] = ...
```

### Integration: Validator changes

**File:** `src/agentic_data_contracts/validation/validator.py`

```python
class Validator:
    def __init__(
        self,
        contract: DataContract,
        semantic_source: SemanticSource | None = None,  # NEW
        ...
    ):
        ...
        self._relationship_checker = (
            RelationshipChecker(semantic_source.get_relationships())
            if semantic_source and semantic_source.get_relationships()
            else None
        )
```

In `validate()`, after blocking checkers run:

```python
if self._relationship_checker is not None:
    rel_warnings = self._relationship_checker.check_joins(ast)
    warnings.extend(rel_warnings)
```

Note: The checker exposes a `check_joins(ast) -> list[str]` method for the Validator to call, separate from the protocol's `check_ast()`. This is because the checker produces multiple warnings (one per problematic join) rather than a single pass/fail CheckResult.

## Detection Logic

### 1. Join-Key Correctness

**Trigger:** Agent joins two tables that have a declared relationship, but uses different columns than declared.

**Logic:**
1. Extract all JOIN clauses from AST (including implicit joins in WHERE with `=`)
2. For each join, identify the table pair and join columns
3. Look up the table pair in `_relationship_map`
4. If found, compare the join columns against `relationship.from_` and `relationship.to`
5. If columns don't match → emit warning

**Example warning:**
> "Join `orders → customers` uses column `email` but declared relationship specifies `customer_id → id`"

### 2. Required-Filter Enforcement

**Trigger:** Agent joins along a relationship that has `required_filter` set, but the query's WHERE clause doesn't include that condition.

**Logic:**
1. For each matched relationship with a non-null `required_filter`
2. Parse the `required_filter` string to extract referenced columns
3. Check if those columns appear in the query's WHERE clause
4. If not present → emit warning

**Example warning:**
> "Join `orders → customers` has required_filter `status != 'cancelled'` but query does not filter on `status`"

**Note:** We check for column presence in WHERE, not exact expression matching. This is intentionally loose — the agent might express the filter differently (e.g., `status = 'active'` instead of `status != 'cancelled'`). The warning says "you should filter on `status`" — it's up to the agent to decide how.

### 3. Fan-Out Risk Detection

**Trigger:** Query contains an aggregation function AND joins across a `one_to_many` relationship.

**Logic:**
1. Check if the query contains any aggregation functions (SUM, COUNT, AVG, MIN, MAX, etc.)
2. Check if any matched relationship has `type == "one_to_many"`
3. If both conditions are true → emit warning

**Example warning:**
> "Query aggregates across a one_to_many join (`orders → order_items`). Results may be inflated by row multiplication."

**Future refinement:** Could detect which side the aggregation is on — aggregating the "many" side is fine (`COUNT(order_items.id)`), aggregating the "one" side is risky (`SUM(orders.amount)`). Deferred for now.

## Table Matching Strategy

Relationships use `schema.table.column` format (e.g., `analytics.orders.customer_id`). Agent SQL may reference tables as:
- Bare name: `orders`
- Schema-qualified: `analytics.orders`
- Aliased: `FROM orders o JOIN customers c ON o.customer_id = c.id`

The checker will:
1. Build an alias map from the AST (alias → actual table name)
2. Resolve aliases in JOIN conditions to actual table names
3. Match table names case-insensitively
4. Match with or without schema prefix (strip schema from relationship definition for comparison)

## Data Flow

```
Validator.__init__(contract, semantic_source)
    → RelationshipChecker(semantic_source.get_relationships())
        → builds _relationship_map: {(table, table): [Relationship, ...]}

Validator.validate(sql)
    → parse SQL to AST
    → run blocking checkers (table allowlist, operation blocklist)
    → RelationshipChecker.check_joins(ast)
        → extract JOINs from AST
        → resolve aliases
        → for each join pair:
            → lookup in _relationship_map
            → if not found: skip
            → if found: check key correctness, required_filter, fan-out
        → return list of warning strings
    → append to ValidationResult.warnings
```

## Test Plan

### Unit Tests (`tests/test_validation/test_relationship_checker.py`)

1. **Join-key correctness:**
   - Correct join key → no warning
   - Wrong join key → warning emitted
   - Join on undeclared relationship → no warning (silent)

2. **Required-filter enforcement:**
   - Filter present → no warning
   - Filter absent → warning emitted
   - Relationship without required_filter → no warning

3. **Fan-out detection:**
   - Aggregation + one_to_many join → warning
   - No aggregation + one_to_many → no warning
   - Aggregation + many_to_one → no warning
   - Aggregation + one_to_one → no warning

4. **Table matching:**
   - Bare table name matches schema-qualified relationship
   - Aliased table resolves correctly
   - Case-insensitive matching works

5. **Integration with Validator:**
   - Validator accepts optional SemanticSource
   - Warnings appear in ValidationResult.warnings
   - No SemanticSource → checker not instantiated, no errors

### Fixture

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
```

## Files Changed

| File | Change |
|------|--------|
| `src/agentic_data_contracts/validation/checkers.py` | Add `RelationshipChecker` class |
| `src/agentic_data_contracts/validation/validator.py` | Add optional `semantic_source` param, wire up checker |
| `tests/test_validation/test_relationship_checker.py` | New test file |
| `tests/fixtures/relationships_checker.yml` | New fixture |

## Non-Goals

- No blocking enforcement (warnings only)
- No opinion on undeclared joins
- No multi-hop path validation (single-edge only)
- No exact expression matching for required_filter (column presence only)
- No sophisticated fan-out analysis (simple detection only)
