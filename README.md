# agentic-data-contracts

[![PyPI version](https://img.shields.io/pypi/v/agentic-data-contracts.svg)](https://pypi.org/project/agentic-data-contracts/)
[![CI](https://github.com/flyersworder/agentic-data-contracts/actions/workflows/ci.yml/badge.svg)](https://github.com/flyersworder/agentic-data-contracts/actions/workflows/ci.yml)
[![Python 3.12+](https://img.shields.io/badge/python-3.12%2B-blue.svg)](https://www.python.org/downloads/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

**YAML-first, domain-driven data governance for AI agents.**

`agentic-data-contracts` takes a domain-driven approach to AI agent governance: instead of letting agents figure out your data landscape by trial and error, you teach them your business domains, metrics, and rules upfront — in YAML. The agent starts by understanding *what* a business domain means, then discovers *which* metrics to use, then builds queries that comply with your governance rules. All enforced automatically at query time via SQL validation powered by [sqlglot](https://github.com/tobymao/sqlglot).

**Why domain-driven?** AI agents querying databases face three problems: **resource runaway** (unbounded compute, endless retries, cost overruns), **semantic inconsistency** (wrong tables, missing filters, ad-hoc metric definitions), and **lack of business context** (the agent doesn't know what "revenue" means in *your* company). This library addresses all three with a single YAML contract that combines governance rules with business domain knowledge.

**Works with:** [Claude Agent SDK](https://github.com/anthropics/claude-agent-sdk-python) (primary target), or any Python agent framework. Optionally integrates with [ai-agent-contracts](https://pypi.org/project/ai-agent-contracts/) for formal resource governance.

## How It Works

The agent follows a domain-driven workflow — understanding business context before writing SQL:

```
1. Agent receives: "How is revenue trending?"
2. lookup_domain("revenue")     → "Revenue is recognized at fulfillment, not booking"
3. lookup_metric("total_revenue") → SUM(amount) FILTER (WHERE status = 'completed')
4. Agent writes SQL using the metric definition
5. inspect_query(sql)           → {"valid": true, "estimated_cost_usd": 0.0, ...}
6. run_query(sql)               → results returned
```

Governance rules are enforced automatically at query time:

```
Agent: "SELECT * FROM analytics.orders"
  -> BLOCKED (no SELECT * — specify explicit columns)

Agent: "SELECT order_id, amount FROM analytics.orders"
  -> BLOCKED (missing required filter: tenant_id)

Agent: "SELECT order_id, amount FROM analytics.orders WHERE tenant_id = 'acme'"
  -> PASSED + WARN (consider using semantic revenue definition)
```

The contract defines the domains, metrics, and rules. The library enforces them — before the query ever reaches the database.

## Installation

```bash
uv add agentic-data-contracts
# or
pip install agentic-data-contracts
```

With optional database adapters:

```bash
uv add "agentic-data-contracts[duckdb]"      # DuckDB
uv add "agentic-data-contracts[bigquery]"    # BigQuery
uv add "agentic-data-contracts[snowflake]"   # Snowflake
uv add "agentic-data-contracts[postgres]"    # PostgreSQL
uv add "agentic-data-contracts[agent-sdk]"   # Claude Agent SDK integration
```

## Quick Start

### 1. Write a YAML contract

```yaml
# contract.yml
version: "1.0"
name: revenue-analysis

semantic:
  source:
    type: yaml
    path: "./semantic.yml"
  allowed_tables:
    - schema: analytics
      description: "Curated analytics tables — prefer for reporting"
      preferred: true
      tables: ["*"]          # all tables in schema (discovered from database)
    - schema: marketing
      tables: [campaigns]    # or list specific tables
  forbidden_operations: [DELETE, DROP, TRUNCATE, UPDATE, INSERT]
  domains:
    - name: revenue
      summary: "Financial metrics from completed orders"
      description: >
        Revenue is recognized at fulfillment, not at booking.
        Excludes refunds and chargebacks unless stated.
      metrics: [total_revenue]
  rules:
    - name: tenant_isolation
      description: "All queries must filter by tenant_id"
      enforcement: block
      query_check:
        required_filter: tenant_id
    - name: no_select_star
      description: "Must specify explicit columns"
      enforcement: block
      query_check:
        no_select_star: true

resources:
  cost_limit_usd: 5.00
  max_retries: 3
  token_budget: 50000

temporal:
  max_duration_seconds: 300
```

### 2. Load the contract and create tools

```python
from agentic_data_contracts import DataContract, create_tools
from agentic_data_contracts.adapters.duckdb import DuckDBAdapter

dc = DataContract.from_yaml("contract.yml")
adapter = DuckDBAdapter("analytics.duckdb")

# Semantic source is auto-loaded from contract config (source.type + source.path)
tools = create_tools(dc, adapter=adapter)
```

### 3. Use with the Claude Agent SDK (requires `claude-agent-sdk>=0.1.52`)

```python
import asyncio
from agentic_data_contracts import create_sdk_mcp_server
from claude_agent_sdk import (
    ClaudeAgentOptions,
    AssistantMessage,
    TextBlock,
    query,
)

# One-liner: wraps all 9 tools and bundles into an SDK MCP server
server = create_sdk_mcp_server(dc, adapter=adapter)

options = ClaudeAgentOptions(
    model="claude-sonnet-4-6",
    system_prompt=f"You are a revenue analytics assistant.\n\n{dc.to_system_prompt()}",
    mcp_servers={"dc": server},
    **dc.to_sdk_config(),  # token_budget → task_budget, max_retries → max_turns
)

async def run(prompt: str) -> None:
    async for message in query(prompt=prompt, options=options):
        if isinstance(message, AssistantMessage):
            for block in message.content:
                if isinstance(block, TextBlock):
                    print(block.text)

asyncio.run(run("What was total revenue by region in Q1 2025?"))
```

### 4. Or use the tools directly (no SDK required)

```python
import asyncio

async def demo() -> None:
    # Inspect a query without executing. Response is structured JSON.
    inspect = next(t for t in tools if t.name == "inspect_query")
    result = await inspect.callable(
        {"sql": "SELECT id, amount FROM analytics.orders WHERE tenant_id = 'acme'"}
    )
    print(result["content"][0]["text"])
    # {"valid": true, "violations": [], "warnings": [], "log_messages": [],
    #  "schema_valid": true, "explain_errors": [], "pending_result_checks": [...]}

    # Blocked query
    result = await inspect.callable({"sql": "SELECT * FROM analytics.orders"})
    print(result["content"][0]["text"])
    # {"valid": false,
    #  "violations": ["SELECT * is not allowed — specify explicit columns", ...],
    #  "warnings": [], ...}

asyncio.run(demo())
```

## The 9 Tools

| Tool | Description |
|------|-------------|
| `describe_table` | Get full column details for an allowed table |
| `preview_table` | Preview sample rows from an allowed table |
| `list_metrics` | List metric definitions, optionally filtered by domain, tier, or indicator_kind |
| `lookup_metric` | Get a metric definition (SQL, tier, indicator_kind, impacts, impacted_by); fuzzy search fallback when no exact match |
| `lookup_domain` | Get full domain context (description, metrics, tables); fuzzy search fallback |
| `lookup_relationships` | Look up join paths for a table; finds multi-hop paths when given a target table |
| `trace_metric_impacts` | Walk the metric-impact graph upstream (drivers) or downstream (affected metrics) from a starting metric |
| `inspect_query` | Validate a SQL query and estimate its cost via EXPLAIN without executing |
| `run_query` | Validate and execute a SQL query, returning results |

## Domain-Driven Agent Workflow

The core design principle: **agents should understand the business domain before writing SQL.** Instead of dumping table schemas and hoping for the best, the contract teaches the agent your business vocabulary through progressive disclosure:

```
1. Domain context     →  "What does 'revenue' mean here?"
2. Metric definitions →  "How is 'total_revenue' calculated?"
3. Query execution    →  "Run the validated SQL"
```

### Defining domains

Each domain carries a description that teaches the agent your business rules — things the SQL alone can't express:

```yaml
semantic:
  domains:
    - name: acquisition
      summary: "Customer acquisition costs and conversion metrics"
      description: >
        Acquisition metrics track the cost and efficiency of
        acquiring new customers across all channels.
        CAC is calculated using fully-loaded cost, not just ad spend.
      metrics: [CAC, CPA, CPL, click_through_rate]
    - name: retention
      summary: "Customer retention, churn, and lifetime value"
      description: >
        Retention metrics measure how well we keep customers.
        Churn is measured on a 30-day rolling window.
        A customer is "active" if they had at least one qualifying
        action in the window.
      metrics: [churn_rate, LTV, retention_30d]
```

### How the agent uses domains

The system prompt gives the agent a compact domain index. When a user asks a domain-specific question, the agent explores progressively:

```
lookup_domain("acquisition")        → business context + metric descriptions
lookup_metric("CAC")                → SQL expression, source table, filters
lookup_metric("acquisition cost")   → fuzzy match, returns [CAC, CPA] as candidates
list_metrics(domain="retention")    → all metrics in the retention domain
```

This means the agent knows that "revenue is recognized at fulfillment, not at booking" *before* it writes a single line of SQL — reducing hallucinated metrics and incorrect calculations.

### Why progressive disclosure works

This pattern — compact index in the prompt, detailed context on demand — is the same philosophy validated by agent skill systems, MCP tool servers, and RAG architectures. Instead of overloading the agent's context window with everything upfront, you give it just enough to know *where to look*, then let it pull details when needed. The result is better token efficiency, more focused reasoning, and fewer hallucinations from context overload.

## Contract Rules

Rules are enforced at three levels:

- **`block`** — query is rejected and an error is returned to the agent
- **`warn`** — query proceeds and a `WARNINGS:` preamble is prepended to the `run_query` response (also in `inspect_query` under `warnings`)
- **`log`** — query proceeds and a `LOG:` preamble is prepended to the `run_query` response (also in `inspect_query` under `log_messages`); rules at this level are omitted from the system prompt so the agent can't adapt behavior to avoid triggering them

Each rule carries a `query_check` (pre-execution) or `result_check` (post-execution) block. Rules with neither are advisory — they appear in the system prompt but don't enforce anything. Every rule can be scoped to a specific table or applied globally.

**Built-in query checks** (pre-execution, validated against SQL AST):

| Check | Description |
|-------|-------------|
| `required_filter` | Require a column in WHERE clause (e.g., `tenant_id`) |
| `no_select_star` | Forbid `SELECT *` — require explicit columns |
| `blocked_columns` | Forbid specific columns in SELECT (e.g., PII) |
| `require_limit` | Require a LIMIT clause |
| `max_joins` | Cap the number of JOINs |

**Built-in result checks** (post-execution, validated against query output):

| Check | Description |
|-------|-------------|
| `min_value` / `max_value` | Numeric bounds on a column's values |
| `not_null` | Column must not contain nulls |
| `min_rows` / `max_rows` | Row count bounds on the result set |

Example with table scoping and both check types:

```yaml
rules:
  - name: tenant_isolation
    description: "Orders must filter by tenant_id"
    enforcement: block
    table: "analytics.orders"      # only applies to this table
    query_check:
      required_filter: tenant_id

  - name: hide_pii
    description: "Do not select PII columns from customers"
    enforcement: block
    table: "analytics.customers"
    query_check:
      blocked_columns: [ssn, email, phone]

  - name: wau_sanity
    description: "WAU should not exceed world population"
    enforcement: warn
    table: "analytics.user_metrics"
    result_check:
      column: wau
      max_value: 8_000_000_000

  - name: no_negative_revenue
    description: "Revenue must not be negative"
    enforcement: block
    result_check:
      column: revenue
      min_value: 0
```

## Semantic Sources

A semantic source provides metric, table schema, and relationship metadata to the agent. Paths are resolved relative to the contract file's directory (not the process CWD).

**YAML** (built-in):
```yaml
# semantic.yml
metrics:
  - name: total_revenue
    description: "Total revenue from completed orders"
    sql_expression: "SUM(amount) FILTER (WHERE status = 'completed')"
    source_model: analytics.orders
    domains: [revenue]                 # optional — see "Metric Impacts" below
    tier: [north_star, department_kpi] # optional — north_star / department_kpi / team_kpi
    indicator_kind: lagging            # optional — leading | lagging

tables:
  - schema: analytics
    table: orders
    columns:
      - name: id
        type: INTEGER
      - name: amount
        type: DECIMAL
      - name: tenant_id
        type: VARCHAR
```

`tier`, `indicator_kind`, and `domains` are all optional. For dbt and Cube sources, these fields live under the metric's `meta:` block and are read through the same field names.

**dbt** — point to a `manifest.json`:
```yaml
semantic:
  source:
    type: dbt
    path: "./dbt/manifest.json"
```

**Cube** — point to a Cube schema file:
```yaml
semantic:
  source:
    type: cube
    path: "./cube/schema.yml"
```

## Table Relationships

Define join paths so the agent knows how to combine tables correctly:

```yaml
# semantic.yml
relationships:
  - from: analytics.orders.customer_id
    to: analytics.customers.id
    type: many_to_one
    description: >
      Join orders to customers for region-level breakdowns.
      Every order has exactly one customer.

  - from: analytics.bdg_attribution.contact_id
    to: analytics.contacts.contact_id
    type: many_to_one
    description: "Bridge table — filter to avoid fan-out from multiple attribution records."
    required_filter: "attribution_model = 'last_touch_attribution'"
```

| Field | Required | Description |
|-------|----------|-------------|
| `from` / `to` | Yes | Fully qualified column references (`schema.table.column`) |
| `type` | No | Cardinality: `many_to_one` (default), `one_to_one`, `many_to_many` |
| `description` | No | Free-text context for the agent (join guidance, caveats, data quality notes) |
| `required_filter` | No | SQL condition that **must** be applied when using this join (e.g., bridge table disambiguation) |

The agent sees these in its system prompt and uses them to write correct JOINs instead of guessing from column names.

### Relationship Validation

When a `SemanticSource` is passed to the `Validator`, declared relationships are actively validated against the agent's SQL:

| Check | Trigger | Warning |
|-------|---------|---------|
| **Join-key correctness** | Agent joins on wrong columns for a declared relationship | "uses `email` but declared relationship specifies `customer_id → id`" |
| **Required-filter missing** | Join has `required_filter` but WHERE clause doesn't include it | "has required filter `status != 'cancelled'` but query does not filter on: status" |
| **Fan-out risk** | Aggregation (SUM, COUNT, etc.) across a `one_to_many` join | "Results may be inflated by row multiplication" |

All relationship checks are **advisory only** (warnings, never blocks). Undeclared joins are silently ignored — the checker only validates relationships you've explicitly defined.

## Metric Impacts

Table relationships tell the agent *how to join*. Metric impacts tell the agent *what drives what* — the causal / economic graph between KPIs. When an agent is asked "why did revenue drop?", an impact graph lets it walk upstream to the drivers (conversion rate, active customers, traffic) rather than blindly querying revenue again. When it's asked to recommend an action, it can cite verified evidence rather than hand-waving.

Declare impacts at the top level of the semantic YAML, alongside `metrics:` and `relationships:`:

```yaml
# semantic.yml
metric_impacts:
  - from: active_customers
    to: total_revenue
    direction: positive           # positive | negative
    confidence: verified          # verified | correlated | hypothesized
    evidence: "A/B test exp-042 (Q3 2025), +3.2% revenue lift, p<0.01"
    description: "Retained customers drive repeat purchases."
```

| Field | Required | Description |
|-------|----------|-------------|
| `from` / `to` | Yes | Metric names (must match a metric declared in the same contract) |
| `direction` | No | `positive` (default) or `negative` |
| `confidence` | No | `hypothesized` (default), `correlated`, or `verified` — lets the agent prioritize backed-up drivers over hunches |
| `evidence` | No | Free text — study reference, A/B test ID, anything the agent should quote when making a recommendation |
| `description` | No | Optional elaboration |

Edges are directional. There's no `domains` field on the edge itself: an impact surfaces whenever either endpoint is in the agent's active domain, so cross-domain drivers (Checkout → Revenue) get discovered for free.

### How the agent uses impacts

`lookup_metric` surfaces an enriched response: each metric carries `impacts` (outgoing edges) and `impacted_by` (incoming edges), each rendered as a one-line citation string:

```
"positive impact on total_revenue (verified): A/B test exp-042 (Q3 2025), +3.2% revenue lift, p<0.01"
```

The agent can quote this verbatim in its answer — structured enough to reason over, readable enough to paste.

`trace_metric_impacts` walks the graph via BFS:

```python
await trace.callable({
    "metric_name": "total_revenue",
    "direction": "upstream",     # upstream = drivers, downstream = affected
    "max_depth": 2,
})
# Returns: {"edges": [{"depth": 1, "from": "active_customers", "to": "total_revenue",
#                       "direction": "positive", "confidence": "verified",
#                       "evidence": "A/B test exp-042..."}]}
```

Impacts declared in contract YAML reference metric names regardless of where the metric itself is defined, so this works even for dbt and Cube-sourced metrics — neither semantic layer has a native causal-graph concept. Unknown metric references in `metric_impacts` emit a warning at tool-creation time (same pattern as domain validation).

## Custom Prompt Rendering

The system prompt is generated by a `PromptRenderer`. The default `ClaudePromptRenderer` produces XML-structured output optimized for Claude models:

```python
dc = DataContract.from_yaml("contract.yml")
print(dc.to_system_prompt())  # XML output, optimized for Claude
```

For other models (GPT-4, Gemini, Llama), implement the `PromptRenderer` protocol:

```python
from agentic_data_contracts import PromptRenderer, DataContract

class MarkdownRenderer:
    def render(self, contract, semantic_source=None):
        tables = "\n".join(f"- {t}" for t in contract.allowed_table_names())
        return f"## {contract.name}\n\nAllowed tables:\n{tables}"

dc = DataContract.from_yaml("contract.yml")
print(dc.to_system_prompt(renderer=MarkdownRenderer()))
```

## Scaling to Large Organizations

Tested for 200+ tables, 300+ metrics, 50+ relationships across multiple schemas.

| Concern | How it scales |
|---|---|
| **System prompt size** | With domains: compact index (name + summary + count). Without domains: >20 metrics auto-switches to count. >30 relationships: per-table join counts with `lookup_relationships` hint |
| **Relationship lookup** | `lookup_relationships(table=...)` returns joins for a table on demand. With `target_table`, finds shortest multi-hop join path via BFS (up to 3 hops) |
| **Wildcard schemas** | `tables: ["*"]` discovers tables from the database. Resolution is cached — no repeated queries |
| **Metric lookup** | Fuzzy search via `thefuzz` (C++ backed) — sub-millisecond even with 1000+ metrics |
| **SQL validation** | Set-based allowlist check — O(1) per table reference regardless of allowlist size |

## Resource Limits

```yaml
resources:
  cost_limit_usd: 5.00          # max estimated query cost
  max_retries: 3                 # max blocked queries per session
  token_budget: 50000            # max tokens consumed
  max_query_time_seconds: 30     # max wall-clock query time
  max_rows_scanned: 1000000      # max rows an EXPLAIN may estimate
```

## Optional Dependencies

| Extra | Package | Purpose |
|-------|---------|---------|
| `duckdb` | `duckdb` | DuckDB adapter |
| `bigquery` | `google-cloud-bigquery` | BigQuery adapter |
| `snowflake` | `snowflake-connector-python` | Snowflake adapter |
| `postgres` | `psycopg2-binary` | PostgreSQL adapter |
| `agent-sdk` | `claude-agent-sdk` | Claude Agent SDK integration |
| `agent-contracts` | `ai-agent-contracts>=0.2.0` | ai-agent-contracts bridge |

## Optional: Formal Governance with ai-agent-contracts

The library works standalone with lightweight enforcement. Install [`ai-agent-contracts`](https://pypi.org/project/ai-agent-contracts/) to upgrade to the formal governance framework:

```bash
pip install "agentic-data-contracts[agent-contracts]"
```

```python
from agentic_data_contracts.bridge.compiler import compile_to_contract

contract = compile_to_contract(dc)  # YAML → formal 7-tuple Contract
```

**What you get with the bridge:**

| Concern | Standalone | With ai-agent-contracts |
|---|---|---|
| Resource tracking | Manual counters | Formal `ResourceConstraints` with auto-enforcement |
| Rule violations | Exception + retry | `TerminationCondition` with contract state machine |
| Success evaluation | Log-based | Weighted `SuccessCriterion` scoring, LLM judge support |
| Contract lifecycle | None | `DRAFTED → ACTIVE → FULFILLED / VIOLATED / TERMINATED` |
| Framework support | Claude Agent SDK | + LiteLLM, LangChain, LangGraph, Google ADK |
| Multi-agent | Single agent | Coordination patterns (sequential, parallel, hierarchical) |

**When to use it:** formal audit trails, success scoring, multi-agent coordination, or integration with non-Claude agent frameworks.

## Example

See [`examples/revenue_agent/`](examples/revenue_agent/) for a complete working example with a DuckDB database, YAML semantic source, and Claude Agent SDK integration.

```bash
uv run python examples/revenue_agent/setup_db.py
uv run python examples/revenue_agent/agent.py "What was Q1 revenue by region?"
```

## Architecture

See [`docs/architecture.md`](docs/architecture.md) for the full design spec covering the layered architecture, YAML schema, validation pipeline, tool design, semantic sources, database adapters, and the optional `ai-agent-contracts` bridge.

## License

MIT
