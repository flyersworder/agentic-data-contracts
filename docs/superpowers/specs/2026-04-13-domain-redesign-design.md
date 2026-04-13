# Domain Redesign: First-Class Business Domains

**Date:** 2026-04-13
**Status:** Approved

## Problem

The current `domains` field (`dict[str, list[str]]`) is a flat mapping of domain name to metric names. It provides no business context to the agent, has no dedicated lookup tool, and performs no validation that referenced metrics exist. Domains are an important business concept — they represent distinct areas of a business (revenue, engagement, operations) and carry definitions, caveats, and vocabulary that the agent needs to answer domain-specific questions correctly.

## Goals

1. Give the agent business context for each domain via a description field
2. Keep the system prompt compact via a summary field (progressive disclosure)
3. Add a `lookup_domain` tool so agents can retrieve full domain context on demand
4. Validate domain references (metrics, tables) at load time
5. Optionally associate tables with domains for navigational convenience

## Non-Goals

- Auto-extracting domains from dbt tags / Cube folders (future enhancement)
- Adding a `domain` field to `MetricDefinition` on the semantic source side
- Cross-domain relationship mapping

## Design

### 1. Pydantic Model (`core/schema.py`)

Add a new `Domain` model and change `SemanticConfig.domains` from `dict[str, list[str]]` to `list[Domain]`:

```python
class Domain(BaseModel):
    name: str
    summary: str                                         # one-liner for system prompt
    description: str                                     # full business context for lookup_domain
    metrics: list[str] = Field(default_factory=list)
    tables: list[str] = Field(default_factory=list)      # optional, fully qualified "schema.table"
```

This is a breaking change to the YAML format. The project is pre-1.0, so this is acceptable.

### 2. YAML Format

Before:

```yaml
domains:
  revenue: [total_revenue]
  engagement: [active_customers]
```

After:

```yaml
domains:
  - name: revenue
    summary: "Financial metrics: bookings, billings, recognized revenue"
    description: >
      Revenue metrics track recognized revenue from completed orders.
      Revenue is recognized at fulfillment, not at booking.
      Excludes refunds and chargebacks unless explicitly stated.
    metrics: [total_revenue, avg_order_value, mrr]
    tables: [analytics.orders, analytics.invoices]

  - name: engagement
    summary: "Customer activity, retention, and churn patterns"
    description: >
      Customer engagement measures active usage patterns.
      A customer is "active" if they performed at least one
      qualifying action in the trailing 30-day window.
    metrics: [active_customers, churn_rate]
```

### 3. `lookup_domain` Tool (New — Tool 12)

**Input:** `name` (string, required)

**Behavior:**
- Exact match by domain name first
- Fuzzy fallback using `thefuzz` (consistent with `lookup_metric`)
- Returns full domain context with metric descriptions pulled from the semantic source

**Output:**

```json
{
  "name": "revenue",
  "summary": "Financial metrics: bookings, billings, recognized revenue",
  "description": "Revenue metrics track recognized revenue from completed orders...",
  "metrics": [
    {"name": "total_revenue", "description": "Total revenue from completed orders"},
    {"name": "avg_order_value", "description": "Average order value in USD"}
  ],
  "tables": ["analytics.orders", "analytics.invoices"]
}
```

Metrics include `name` + `description` (not SQL expressions) — enough for the agent to pick the right metric, then call `lookup_metric` for the SQL definition.

When no semantic source is configured, metrics are returned as names only (no descriptions).

### 4. System Prompt Rendering (`core/prompt.py`)

When domains are defined, render a compact domain index instead of the current metric listing:

```xml
<available_domains>
  <domain name="revenue" summary="Financial metrics: bookings, billings, recognized revenue" metric_count="3" />
  <domain name="engagement" summary="Customer activity, retention, and churn patterns" metric_count="2" />
  <hint>Use lookup_domain("...") for business context, then lookup_metric("...") for SQL definitions.</hint>
</available_domains>
```

This replaces the current 4-branch rendering logic (compact × domains matrix) with a single path when domains exist. When no domains are defined, the existing metric-only rendering stays as-is for backward compatibility.

### 5. `list_metrics` Tool Adaptation

The existing `list_metrics(domain=...)` filter continues to work. The internal lookup changes from `domains.get(domain_filter, [])` (dict lookup) to finding a `Domain` object by name and using its `metrics` list.

### 6. Contract Helper (`core/contract.py`)

Add a method to find a domain by name with fuzzy fallback:

```python
def get_domain(self, name: str) -> Domain | None:
    """Find domain by exact name, or None."""
    for d in self.schema.semantic.domains:
        if d.name == name:
            return d
    return None
```

Fuzzy matching lives in the tool layer (consistent with `lookup_metric` which does fuzzy search at the tool level, not the contract level).

### 7. Validation

At contract load time, when a semantic source is available, surface warnings (not errors) for:
- Domain referencing a metric name not found in the semantic source
- Domain referencing a table not in `allowed_tables`

Validation is soft — the contract still loads, but issues are surfaced early. This validation runs in the tool factory (`create_tools`) where both contract and semantic source are available. Warnings are logged but do not prevent tool creation.

### 8. Agent Workflow

```
System prompt → agent sees domain index (name + summary + metric_count)
    ↓
User asks: "How is revenue trending?"
    ↓
Agent calls: lookup_domain(name="revenue")
    → gets: full description + metrics with descriptions + tables
    ↓
Agent calls: lookup_metric(name="total_revenue")
    → gets: SQL expression, source_model, filters
    ↓
Agent builds query using the metric SQL against the domain's tables
    ↓
Agent calls: validate_query → run_query
```

## Files Changed

| File | Change |
|---|---|
| `src/agentic_data_contracts/core/schema.py` | Add `Domain` model, change `SemanticConfig.domains` type |
| `src/agentic_data_contracts/core/contract.py` | Add `get_domain()` helper method |
| `src/agentic_data_contracts/core/prompt.py` | Add `_render_domains()`, update `_render_metrics()` to defer when domains exist |
| `src/agentic_data_contracts/tools/factory.py` | Add `lookup_domain` tool, update `list_metrics` domain lookup, update tool count to 12 |
| `tests/fixtures/valid_contract.yml` | Update to new domain YAML format |
| `tests/test_core/test_system_prompt_metrics.py` | Update domain rendering tests |
| `tests/test_core/test_scalability.py` | Update domain count tests |
| `tests/test_core/test_prompt_renderers.py` | Update if domain-related |
| `tests/test_tools/test_semantic_tools.py` | Add `lookup_domain` tests, update `list_metrics` domain tests |

## Migration

This is a breaking change to the YAML contract format. The project is pre-1.0 (v0.8.0), so no backward compatibility shim is needed. Existing contracts must update their `domains` section from dict format to list-of-objects format.
