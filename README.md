# agentic-data-contracts

[![PyPI version](https://img.shields.io/pypi/v/agentic-data-contracts.svg)](https://pypi.org/project/agentic-data-contracts/)
[![CI](https://github.com/flyersworder/agentic-data-contracts/actions/workflows/ci.yml/badge.svg)](https://github.com/flyersworder/agentic-data-contracts/actions/workflows/ci.yml)
[![Python 3.12+](https://img.shields.io/badge/python-3.12%2B-blue.svg)](https://www.python.org/downloads/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

**Stop your AI agents from running wild on your data.**

`agentic-data-contracts` lets data engineers define governance contracts in YAML — what tables an agent may query, which operations are forbidden, what resource limits apply — and enforces them automatically at query time via SQL validation powered by [sqlglot](https://github.com/tobymao/sqlglot).

**Why?** AI agents querying databases face two problems: **resource runaway** (unbounded compute, endless retries, cost overruns) and **semantic inconsistency** (wrong tables, missing filters, ad-hoc metric definitions). This library addresses both with a single YAML contract.

**Works with:** [Claude Agent SDK](https://github.com/anthropics/claude-agent-sdk-python) (primary target), or any Python agent framework. Optionally integrates with [ai-agent-contracts](https://pypi.org/project/ai-agent-contracts/) for formal resource governance.

## How It Works

```
Agent: "SELECT * FROM analytics.orders"
  -> BLOCKED (no SELECT * — specify explicit columns)

Agent: "SELECT order_id, amount FROM analytics.orders"
  -> BLOCKED (missing required filter: tenant_id)

Agent: "SELECT order_id, amount FROM analytics.orders WHERE tenant_id = 'acme'"
  -> PASSED + WARN (consider using semantic revenue definition)

Agent: "DELETE FROM analytics.orders WHERE id = 1"
  -> BLOCKED (forbidden operation: DELETE)
```

The contract defines the rules. The library enforces them — before the query ever reaches the database.

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
      tables: [orders, customers, subscriptions]
  forbidden_operations: [DELETE, DROP, TRUNCATE, UPDATE, INSERT]
  rules:
    - name: tenant_isolation
      description: "All queries must filter by tenant_id"
      enforcement: block
      filter_column: tenant_id
    - name: no_select_star
      description: "Must specify explicit columns"
      enforcement: block

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

### 3. Use with the Claude Agent SDK (requires `claude-agent-sdk>=0.1.51`)

```python
import asyncio
from claude_agent_sdk import (
    ClaudeAgentOptions,
    AssistantMessage,
    TextBlock,
    create_sdk_mcp_server,
    query,
)

server = create_sdk_mcp_server(name="data-contracts", version="1.0.0", tools=tools)

# Contract limits map to SDK options (token_budget → task_budget, max_retries → max_turns)
sdk_config = dc.to_sdk_config()

options = ClaudeAgentOptions(
    model="claude-sonnet-4-6",
    system_prompt=f"You are a revenue analytics assistant.\n\n{dc.to_system_prompt()}",
    mcp_servers={"dc": server},
    allowed_tools=[f"mcp__dc__{t.name}" for t in tools],
    **sdk_config,
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
    # Validate a query without executing
    validate = next(t for t in tools if t.name == "validate_query")
    result = await validate.callable(
        {"sql": "SELECT id, amount FROM analytics.orders WHERE tenant_id = 'acme'"}
    )
    print(result["content"][0]["text"])
    # VALID — Query passed all checks.

    # Blocked query
    result = await validate.callable({"sql": "SELECT * FROM analytics.orders"})
    print(result["content"][0]["text"])
    # BLOCKED — Violations:
    # - SELECT * is not allowed — specify explicit columns

asyncio.run(demo())
```

## The 10 Tools

| Tool | Description |
|------|-------------|
| `list_schemas` | List all allowed database schemas from the contract |
| `list_tables` | List allowed tables, optionally filtered by schema |
| `describe_table` | Get full column details for an allowed table |
| `preview_table` | Preview sample rows from an allowed table |
| `list_metrics` | List metric definitions, optionally filtered by domain |
| `lookup_metric` | Get a metric definition; fuzzy search fallback when no exact match |
| `validate_query` | Validate a SQL query against contract rules without executing |
| `query_cost_estimate` | Estimate cost and row count via EXPLAIN |
| `run_query` | Validate and execute a SQL query, returning results |
| `get_contract_info` | Get the full contract: rules, limits, and session status |

## Contract Rules

Rules are enforced at three levels:

- **`block`** — query is rejected and an error is returned to the agent
- **`warn`** — query proceeds but a warning is included in the response
- **`log`** — violation is recorded but not surfaced to the agent

Built-in checkers enforce:
- **Table allowlist** — only tables listed in `allowed_tables` may be queried
- **Operation blocklist** — `forbidden_operations` (DELETE, DROP, etc.) are rejected
- **Required filters** — rules with `filter_column` require a matching WHERE clause
- **No SELECT \*** — queries must name explicit columns

## Semantic Sources

A semantic source provides metric and table schema metadata to the agent.

**YAML** (built-in):
```yaml
# semantic.yml
metrics:
  - name: total_revenue
    description: "Total revenue from completed orders"
    sql_expression: "SUM(amount) FILTER (WHERE status = 'completed')"
    source_model: analytics.orders

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

## Scalable Metric Discovery

For large data lakes with hundreds of KPIs, group metrics by domain and let the agent discover them efficiently:

```yaml
semantic:
  domains:
    acquisition: [CAC, CPA, CPL, click_through_rate]
    retention: [churn_rate, LTV, retention_30d]
    attribution: [ROAS, first_touch_revenue]
```

The system prompt gets a compact index (names + descriptions grouped by domain). The agent uses `lookup_metric` for full SQL definitions — with fuzzy fallback when it doesn't know the exact name:

```
lookup_metric("CAC")                → exact match, full definition
lookup_metric("acquisition cost")   → fuzzy match, returns [CAC, CPA] as candidates
list_metrics(domain="retention")    → only retention metrics
```

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
