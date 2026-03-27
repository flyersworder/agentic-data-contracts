# Scalable Semantic Discovery — Design Spec

**Date:** 2026-03-27
**Status:** Draft

## Problem

At scale (50+ schemas, 200+ KPIs), dumping all metric definitions into the system prompt is impractical. The agent needs a way to discover relevant metrics efficiently without burning context window.

## Changes

### 1. `domains` field in YAML schema

Optional grouping in `SemanticConfig`:

```yaml
semantic:
  domains:
    acquisition: [CAC, CPA, CPL, click_through_rate]
    retention: [churn_rate, LTV, retention_30d]
    attribution: [ROAS, first_touch_revenue]
```

Schema change in `core/schema.py`:
```python
class SemanticConfig(BaseModel):
    # ... existing fields ...
    domains: dict[str, list[str]] = Field(default_factory=dict)
```

Backwards compatible — empty dict when not specified.

### 2. `to_system_prompt()` accepts optional `SemanticSource`

`DataContract.to_system_prompt(semantic_source: SemanticSource | None = None)`

When a semantic source is provided:
- If `domains` defined: group metric names by domain with one-line descriptions
- If no domains: flat list of metric names with one-line descriptions
- Always append: "Use the lookup_metric tool to get the SQL definition before computing any KPI."

When no semantic source: current behavior (just the file path pointer).

Output format with domains:
```
### Available Metrics (use lookup_metric for full SQL definitions)

**Acquisition:** CAC — Customer acquisition cost, CPA — Cost per acquisition
**Retention:** churn_rate — Monthly churn rate, LTV — Customer lifetime value

Use the lookup_metric tool to get the SQL definition before computing any KPI.
```

Output format without domains:
```
### Available Metrics (use lookup_metric for full SQL definitions)
- total_revenue — Total revenue from completed orders
- active_customers — Count of customers with recent orders

Use the lookup_metric tool to get the SQL definition before computing any KPI.
```

### 3. `lookup_metric` fuzzy fallback

Current: exact match only, returns `None` on miss.

New behavior in `SemanticSource` implementations:
1. Try exact match on `name`
2. If no match, search all metrics where `query` appears as substring in `name` or `description` (case-insensitive)
3. Return top matches (up to 5)

Add method to `SemanticSource` protocol:
```python
def search_metrics(self, query: str) -> list[MetricDefinition]: ...
```

`lookup_metric` tool changes:
- Exact match → return full definition (current behavior)
- No exact match → call `search_metrics(query)` → return candidates list

### 4. `list_metrics` domain filter

Add optional `domain` parameter:

```python
list_metrics()                      # all metrics
list_metrics(domain="acquisition")  # only metrics in that domain
```

Domain filtering happens in the tool, not the semantic source — the source returns all metrics, the tool filters by domain using `contract.schema.semantic.domains`.

## Files Modified

- `src/agentic_data_contracts/core/schema.py` — add `domains` field
- `src/agentic_data_contracts/core/contract.py` — update `to_system_prompt()` signature
- `src/agentic_data_contracts/semantic/base.py` — add `search_metrics` to protocol
- `src/agentic_data_contracts/semantic/yaml_source.py` — implement `search_metrics`
- `src/agentic_data_contracts/semantic/dbt.py` — implement `search_metrics`
- `src/agentic_data_contracts/semantic/cube.py` — implement `search_metrics`
- `src/agentic_data_contracts/tools/factory.py` — update `lookup_metric` + `list_metrics` tools, pass semantic source to `to_system_prompt()`
- `tests/` — tests for all changes
- `tests/fixtures/valid_contract.yml` — add domains example

## Backwards Compatibility

All changes are additive:
- `domains` defaults to empty dict
- `to_system_prompt()` without args works identically to before
- `lookup_metric` exact match behavior unchanged
- `list_metrics` without domain arg returns all metrics
