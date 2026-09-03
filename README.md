# agentic-data-contracts

[![PyPI version](https://img.shields.io/pypi/v/agentic-data-contracts.svg)](https://pypi.org/project/agentic-data-contracts/)
[![PyPI downloads](https://img.shields.io/pepy/dt/agentic-data-contracts)](https://pypistats.org/packages/agentic-data-contracts)
[![CI](https://github.com/flyersworder/agentic-data-contracts/actions/workflows/ci.yml/badge.svg)](https://github.com/flyersworder/agentic-data-contracts/actions/workflows/ci.yml)
[![Python 3.12+](https://img.shields.io/badge/python-3.12%2B-blue.svg)](https://www.python.org/downloads/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

**YAML-first, domain-driven data governance for AI agents.**

You teach agents your business domains, metrics, and governance rules upfront — in YAML — instead of letting them reverse-engineer your data landscape by trial and error. The agent learns *what* a domain means, discovers *which* metrics to use, then writes queries that are validated against your rules at query time (via [sqlglot](https://github.com/tobymao/sqlglot)) before anything reaches the database.

### Highlights

- **Governed, not guessed** — the agent uses *your* metric definitions (`SUM(amount) FILTER (WHERE status = 'completed')`), not an ad-hoc query it invented.
- **Bad SQL blocked before execution** — forbidden operations, disallowed tables, missing tenant filters, `SELECT *`, unbounded scans — caught by static analysis plus an optional EXPLAIN dry-run.
- **Validate a whole corpus, not just live queries** — re-check a verified-examples database against the contract *and* its certified answers in CI (or a metric's arithmetic identity), catching drift when the contract or warehouse schema changes and a compliant query that quietly returns the wrong number.
- **Business context first** — domain descriptions, metric ownership, freshness, and a metric graph (causal *and* arithmetic) guide the agent before it writes a line of SQL.
- **Resource governance built in** — per-session cost, retry, row, and token budgets, and wall-clock limits.
- **Per-caller row/column security** — allow/deny tables and filter values by principal, for multi-user bots.
- **Framework-agnostic** — plain-function tools for the Claude Agent SDK, LangChain/deepagents, Pydantic AI, or no framework at all.
- **Bring your own semantics** — read metrics from dbt, Cube, Apache Ossie, or inline YAML.

### Without a contract vs. with one

| A raw agent on your warehouse | With `agentic-data-contracts` |
|---|---|
| Invents `revenue = SUM(amount)` — silently wrong (counts refunds, cancelled orders) | Uses the governed definition: `SUM(amount) FILTER (WHERE status = 'completed')` |
| `SELECT *`, cross-tenant reads, unbounded scans | Blocked at query time — explicit columns, required `tenant_id`, row caps enforced |
| Loops on retries with no cost ceiling | Per-session retry / cost / token budgets |
| "Why did revenue drop?" → guesses | Walks the metric graph: arithmetic decomposition first, then causal drivers |

### Measured, not asserted

That table is the claim; [`experiments/dabstep-contract-eval/`](experiments/dabstep-contract-eval/) is the test of it. A four-arm ablation on [DABStep](https://huggingface.co/datasets/adyen/DABstep): 401 tasks × 4 arms × 4 model families = **6,416 graded runs**, scored by DABStep's own scorer. Accuracy on the hard split; every contrast is a paired McNemar on the same tasks.

| arm | agent sees | glm-5.3-flash | deepseek-v4-flash | gpt-5.6-sol | Claude Sonnet 5 |
|---|---|---:|---:|---:|---:|
| **contract** | this library's tools + the contract | **55.1%** | **56.6%** | **77.4%** | **68.4%** |
| `contract_hollow` | the same tools, domain content stripped | 19.3% | 22.6% | 51.2% | 38.0% |
| `manual_prompt` | the vendor manual verbatim in the prompt | 22.9% | 42.9% | 50.3% | 23.8% |
| `schema_only` | table and column names only | 13.9% | 22.6% | 37.0% | 22.9% |

- **The gain is the content, not the scaffolding.** `contract_hollow` runs identical tooling with the domain knowledge removed — that is what separates the two, and the content term dominates on every model tested.
- **Governance held.** Across the ungoverned arms the models submitted 166 mutating SQL statements on 26 tasks. Both governed arms submitted zero.
- The contract was written from the vendor's manual alone and **frozen, digest-pinned, before any benchmark question was read.**

Scope, stated plainly: the effect concentrates in the domain-specific task bucket (the non-fee bucket shows no contract advantage on any model), golds are reconstructed for 401 of 450 tasks, and this is one benchmark at k=1. Full results, every caveat, and all 6,416 transcripts: **[`FINDINGS.md`](experiments/dabstep-contract-eval/FINDINGS.md)**.

**Works with:** any Python agent framework — first-class helpers for the [Claude Agent SDK](https://github.com/anthropics/claude-agent-sdk-python), [LangChain](https://github.com/langchain-ai/langchain) / [deepagents](https://github.com/langchain-ai/deepagents), and [Pydantic AI](https://github.com/pydantic/pydantic-ai), plus a framework-free path (the tools are plain async functions). Optionally integrates with [ai-agent-contracts](https://pypi.org/project/ai-agent-contracts/) for formal resource governance.

> **See it running:** [three example agents](#examples) — `revenue_agent` (finance), `growth_agent` (experimentation), `ops_agent` (SRE) — each runs end-to-end in demo mode with no API key.

## Table of Contents

- [How It Works](#how-it-works)
- [Installation](#installation)
- [Quick Start](#quick-start)
- [The 9 Tools](#the-9-tools)
- [Domain-Driven Agent Workflow](#domain-driven-agent-workflow)
- [Contract Rules](#contract-rules)
- [Semantic Sources](#semantic-sources)
- [Table Relationships](#table-relationships)
- [Metric Impacts](#metric-impacts) (incl. [decomposition & drill dimensions](#metric-decomposition-and-drill-dimensions))
- [Custom Prompt Rendering](#custom-prompt-rendering)
- [Consumer-authored sections (extras)](#consumer-authored-sections-extras)
- [Scaling to Large Organizations](#scaling-to-large-organizations)
- [Resource Limits](#resource-limits)
- [Optional Dependencies](#optional-dependencies)
- [Formal Governance with ai-agent-contracts](#optional-formal-governance-with-ai-agent-contracts)
- [Examples](#examples)
- [FAQ](#faq)
- [Architecture](#architecture)

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

**Two files, two responsibilities.** The **contract** (`contract.yml`) defines *governance* — allowed tables, rules, resource limits, and domain *catalog metadata* (what a domain means, who owns it, when it was last reviewed). The **semantic source** (`semantic.yml`, or dbt/Cube) defines the *metrics themselves* — their SQL, source tables, and which domain each belongs to.

Domain membership is **metric-first**: a metric declares the domains it belongs to (`domains: [...]`), and the contract's `Domain` block never lists its metrics. The grain that matters — the metric — owns the relationship; the domain is just a label it points at. `lookup_domain` reconstructs a domain's members at query time by reverse-looking-up the metrics that declare it, so the two files never drift. The library enforces all of this — before the query ever reaches the database.

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
uv add "agentic-data-contracts[langchain]"   # LangChain / deepagents integration
uv add "agentic-data-contracts[pydantic-ai]" # Pydantic AI integration
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
      allowed_principals: [alice@co.com, bob@co.com]  # only these may query marketing.campaigns
  forbidden_operations:
    [DELETE, DROP, TRUNCATE, UPDATE, INSERT, CREATE, ALTER, MERGE, GRANT, REVOKE, COPY]
  domains:
    - name: revenue
      summary: "Financial metrics from completed orders"
      description: >
        Revenue is recognized at fulfillment, not at booking.
        Excludes refunds and chargebacks unless stated.
      # Membership is metric-first: metrics declare their domains in the
      # semantic source (see "Semantic Sources"), so no metric list here.
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
    - name: pii_columns_redacted_for_juniors
      description: "Junior analysts may not select PII columns from analytics.users"
      enforcement: block
      table: analytics.users
      blocked_principals: [security_admin@co.com]   # everyone except security_admin
      query_check:
        blocked_columns: [ssn, dob, email]

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

### Per-Caller Access Control (Optional)

When different callers should see different subsets of a contract's tables, pass `caller_principal` to `create_tools`. Use a static string for single-user sessions (e.g. Chainlit), or a zero-arg callable when identity changes per request (e.g. a Webex room bot serving multiple users from one long-lived process):

```python
from agentic_data_contracts import DataContract, create_tools

dc = DataContract.from_yaml("contract.yml")

# Chainlit app (one user per session)
tools = create_tools(dc, adapter=adapter, caller_principal="alice@co.com")

# Webex bot (multiple users per bot instance, identity per message)
import contextvars
current_sender: contextvars.ContextVar[str | None] = contextvars.ContextVar("sender", default=None)
tools = create_tools(dc, adapter=adapter, caller_principal=lambda: current_sender.get())
# Handler sets current_sender before invoking the agent for each message.
```

The resolver is called per-query, not cached, so one long-lived `Validator` can serve different callers sequentially. Fail-closed: any `allowed_principals` or `blocked_principals` field on a table requires the caller to be identified — an anonymous caller is treated as unauthenticated and denied.

`Principal` and `resolve_principal` are available from the package root for integrators typing their own middleware:

```python
from agentic_data_contracts import Principal, resolve_principal
```

> **Known limitation:** `to_system_prompt()` lists all declared tables in the contract without filtering by principal. Query-time gating remains authoritative (denied queries never reach the database), but the agent may still be told about tables the current caller cannot access and can waste retry budget (`resources.max_retries`) on queries that will be blocked. Principal-aware prompt rendering is a candidate future feature — file an issue if your deployment needs it.

#### Per-Rule Principal Scoping

Individual `SemanticRule` entries accept the same `allowed_principals` / `blocked_principals` pair (mutually exclusive at load time). When a rule carries either field, it is skipped at validate-time for callers outside the scope. This works across every rule kind — `blocked_columns`, `required_filter`, `no_select_star`, `max_joins`, and `result_check`:

```yaml
rules:
  # Block selecting `ssn` for everyone except the security admin.
  - name: redact_ssn
    enforcement: block
    table: pii.users
    blocked_principals: [security_admin@co.com]
    query_check:
      blocked_columns: [ssn]

  # Only the on-call engineer is held to the 60-second timeout result-check.
  - name: oncall_query_budget
    enforcement: warn
    table: prod.events
    allowed_principals: [oncall@co.com]
    result_check:
      max_rows: 1_000_000
```

Same fail-closed contract as per-table scoping: a rule with `allowed_principals` or `blocked_principals` set requires the caller to be identified — anonymous callers are out of scope and the rule is skipped (it does not silently downgrade to "applies to everyone"). This lets you express things like "Alice may not select `ssn` from `pii.users`, but Bob may" directly in YAML, without splitting tables into per-principal views.

### 3. Framework integrations

Contract-aware tools are plain async functions, so they drop into any framework — expand the one you use.

<details>
<summary><b>Claude Agent SDK</b> — requires <code>claude-agent-sdk</code> 0.2.96+</summary>

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

#### Layer Anthropic's `data` plugin on top (governed analyst skills)

The Agent SDK can load [knowledge-work plugins](https://github.com/anthropics/knowledge-work-plugins) alongside your governed tools, so the agent gets the `data` plugin's analyst *skills* (`validate-data`, `statistical-analysis`, `explore-data`, `sql-queries`, …) while every query it runs is still enforced by your contract. The skills are tool-agnostic — they drive "whatever warehouse tool is connected," which in your process is your governed in-process server.

The one rule that makes this safe: **suppress the plugin's bundled `.mcp.json` warehouse servers** so the agent can't bypass the contract. `strict_mcp_config=True` does exactly that — it uses *only* the servers you pass in `mcp_servers`.

```python
import dataclasses

server = create_sdk_mcp_server(dc, adapter=adapter)

opts_kwargs = {
    "model": "claude-sonnet-4-6",
    "mcp_servers": {"dc": server},                  # the ONLY data path
    "allowed_tools": [f"mcp__dc__{t.name}" for t in tools],
}

# Feature-detect SDK support, then overlay the plugin's skills.
fields = {f.name for f in dataclasses.fields(ClaudeAgentOptions)}
if {"plugins", "skills", "strict_mcp_config"} <= fields:
    opts_kwargs["plugins"] = [{"type": "local", "path": "/path/to/knowledge-work-plugins/data"}]
    opts_kwargs["skills"] = ["validate-data", "statistical-analysis", "explore-data", "sql-queries"]
    opts_kwargs["strict_mcp_config"] = True         # ← ignore the plugin's warehouse .mcp.json
    opts_kwargs["system_prompt"] = {                # skills need the claude_code harness
        "type": "preset", "preset": "claude_code",
        "append": dc.to_system_prompt(),            # your governance, appended
    }

options = ClaudeAgentOptions(**opts_kwargs)
```

Notes:

- **Curate the skill list — do not use `skills="all"`.** `data-context-extractor` is deliberately omitted: it generates a parallel semantic skill that competes with your contract as the source of metric truth. The viz/dashboard skills (`create-viz`, `build-dashboard`) need code-execution tools you may not want to grant.
- **Set metric precedence in your prompt** (e.g. "resolve metrics via `lookup_metric`/`lookup_domain` before writing SQL") so the plugin's "just write a query" instinct doesn't undercut your governed semantic layer.
- All three [examples](#examples) ship this wiring behind an opt-in `DATA_PLUGIN_PATH` env var — `growth_agent/agent.py` is the canonical, fully-commented template.

</details>

<details>
<summary><b>LangChain / deepagents</b> — requires <code>langchain</code> 1.2.17+</summary>

```python
from agentic_data_contracts import create_langchain_tools, ContractMiddleware
from deepagents import create_deep_agent

# `dc` and `adapter` are from the previous example.
# Returns list[BaseTool] — drop in anywhere LangChain accepts tools.
tools = create_langchain_tools(dc, adapter=adapter)

# Enforcement is auto-applied: session limits and BLOCKED envelopes from the
# underlying tools surface as ToolMessage(status="error"). Running inside an
# agent graph, each tool also reads the run's token usage, so a declared
# token_budget binds here without the middleware. Pair with ContractMiddleware
# (and apply_middleware=False) for graph-level interception.
agent = create_deep_agent(tools=tools)
```

Install: `pip install "agentic-data-contracts[langchain]"`. For graph-level enforcement instead of in-tool — note the **shared session**, without which the middleware enforces against one budget while `run_query` reports `tokens_remaining` from another:

```python
from agentic_data_contracts.core.session import ContractSession

session = ContractSession(dc)
tools = create_langchain_tools(
    dc, adapter=adapter, session=session, apply_middleware=False
)
agent = create_deep_agent(
    tools=tools,
    middleware=[ContractMiddleware(dc, adapter=adapter, session=session)],
)
```

</details>

<details>
<summary><b>Pydantic AI</b> — requires <code>pydantic-ai-slim</code> 2.0.0+</summary>

```python
from agentic_data_contracts import create_pydantic_ai_tools
from pydantic_ai import Agent

# `dc` and `adapter` are from the previous example.
# Returns list[pydantic_ai.Tool] — drop into Agent(tools=...).
tools = create_pydantic_ai_tools(dc, adapter=adapter)
agent = Agent("anthropic:claude-sonnet-4-6", tools=tools)
```

Enforcement is auto-applied in-tool: a blocked query (bad SQL, forbidden
operation, missing required filter) raises `ModelRetry` so the model rewrites
and retries, while session-limit exhaustion raises a terminal
`ContractSessionLimitError` that ends the run. Install:
`pip install "agentic-data-contracts[pydantic-ai]"`.

**One shared agent for many users.** For a multi-user service, build the `Agent`
**once** and pass each user's state via `deps` — `create_pydantic_ai_toolset`
rebuilds the tools per run, bound to that user's session and principal, so you
don't construct a separate agent (or tools list) per user:

```python
from agentic_data_contracts import ContractDeps, create_pydantic_ai_toolset
from agentic_data_contracts.core.session import ContractSession
from pydantic_ai import Agent

# per_run_step=False: deps are stable within a run, so build the tools once
# per run instead of once per model step.
agent = Agent("anthropic:claude-sonnet-4-6", deps_type=ContractDeps)
agent.toolset(per_run_step=False)(create_pydantic_ai_toolset(dc, adapter=adapter))

# Per user: keep one ContractSession (cumulative limits) and pass it each turn.
result = await agent.run(
    "Revenue by region last quarter",
    deps=ContractDeps(session=user_session, caller_principal="alice@corp.com"),
    message_history=user_history,
)
```

The caller owns each user's `ContractSession` (created once per user, keyed by
user id). Per-user principals drive per-principal table/rule gating, and the same
`ModelRetry` / `ContractSessionLimitError` enforcement applies per user.

Pair with `**contract_run_kwargs(dc, user_session)` on each `run()` to have
Pydantic AI enforce the token budget per model request — see
[Resource Limits](#resource-limits).

</details>

<details>
<summary><b>Directly</b> — no framework required</summary>

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

</details>

## The 9 Tools

| Tool | Description |
|------|-------------|
| `describe_table` | Get full column details for an allowed table |
| `preview_table` | Preview sample rows from an allowed table; returns `{schema, table, columns, rows}` |
| `list_metrics` | List metric definitions, optionally filtered by domain, tier, or indicator_kind; flags `stale` metrics |
| `lookup_metric` | Get a metric definition (SQL, tier, indicator_kind, impacts, impacted_by, decompositions, drill_by, owners, freshness); fuzzy search fallback when no exact match |
| `lookup_domain` | Get full domain context (description, metrics, tables, owners, freshness); fuzzy search fallback |
| `lookup_relationships` | Look up join paths for a table; finds multi-hop paths when given a target table |
| `trace_metric_impacts` | Walk the metric graph upstream (drivers) or downstream (affected) from a metric — across both causal impact edges and arithmetic decomposition edges (a metric's operands are drivers of it), filtered by `kinds` |
| `inspect_query` | Validate a SQL query and estimate its cost via EXPLAIN without executing |
| `run_query` | Validate and execute a SQL query, returning results as `{columns, rows, row_count, session}` |

### Query protocol

`run_query` and `inspect_query` state the workflow rules in their own tool
descriptions, not only in the rendered system prompt:

> When computing a metric, you MUST call lookup_metric first and use its governed
> definition — never invent or adapt a metric formula.

plus, on `run_query`, *"Prefer this tool over any other SQL or data-access path."*

This is deliberate redundancy. The rendered contract prompt is opt-in — if you wire
`create_langchain_tools(...)` or `create_pydantic_ai_toolset(...)` into an agent with
your own system prompt, the contract may never be rendered — but tool descriptions
travel with the tools on every path. A governance rule the host can drop by writing
its own prompt isn't a governance rule. The precedence sentence matters when your
agent also has a generic SQL tool, a warehouse MCP server, or a shell: without it,
nothing tells the model which path is the governed one.

Each sentence appears only when the capability it names exists — ordering when the
semantic source actually has metrics, precedence when an adapter is configured (a
validation-only setup can't execute, so claiming precedence there would point the
agent at a tool that returns an error).

### Result encoding

`run_query` and `preview_table` both return a `columns` list alongside their
`rows`. By default `rows` is a list of **positional arrays** aligned to
`columns`:

```json
{"columns": ["region", "units"], "rows": [["EMEA", 412], ["APAC", 87]]}
```

This costs less than half the tokens of repeating every column name on every
row, and matters more than it looks: tool results stay in the message history
and are re-sent on every subsequent model request, so an oversized result is
paid for repeatedly rather than once.

Pass `row_format="records"` to `create_tools()` (or to any of the ecosystem
wrappers) to get one dict per row instead — the pre-0.31 shape:

```python
tools = create_tools(contract, adapter=adapter, row_format="records")
# {"columns": ["region", "units"],
#  "rows": [{"region": "EMEA", "units": 412}, {"region": "APAC", "units": 87}]}
```

Both renderings coerce values identically, and carry identical information for
distinctly-labelled columns; only the container differs. One case is not
symmetric: a query with duplicate column labels (e.g. `SELECT t.id, u.id FROM
t, t u`) collapses under `records`, since `dict(zip(columns, row))` is
last-value-wins and silently drops one column — `compact`'s positional arrays
keep both. An unrecognised value raises `ValueError` at `create_tools()` time,
not on the first query.

## Domain-Driven Agent Workflow

The core design principle: **agents should understand the business domain before writing SQL.** Instead of dumping table schemas and hoping for the best, the contract teaches the agent your business vocabulary through progressive disclosure:

```
1. Domain context     →  "What does 'revenue' mean here?"
2. Metric definitions →  "How is 'total_revenue' calculated?"
3. Query execution    →  "Run the validated SQL"
```

### Defining domains

A domain in the contract carries **catalog metadata** — a description that teaches the agent your business rules (things the SQL alone can't express), plus ownership and review cadence. It does **not** list its metrics: domain membership is *metric-first*, declared on each metric in the semantic source via `domains: [...]`. The contract describes a domain; the metrics decide which domain they belong to. `lookup_domain` stitches the two together at query time by reverse-looking-up metrics that declare the domain.

```yaml
# contract.yml — catalog metadata only
semantic:
  domains:
    - name: acquisition
      summary: "Customer acquisition costs and conversion metrics"
      description: >
        Acquisition metrics track the cost and efficiency of
        acquiring new customers across all channels.
        CAC is calculated using fully-loaded cost, not just ad spend.
      business_owner: growth-platform   # optional
      last_reviewed: 2026-05-15          # optional — drives staleness detection
    - name: retention
      summary: "Customer retention, churn, and lifetime value"
      description: >
        Retention metrics measure how well we keep customers.
        Churn is measured on a 30-day rolling window.
        A customer is "active" if they had at least one qualifying
        action in the window.
```

```yaml
# semantic.yml — metrics declare their domain membership
metrics:
  - name: CAC
    description: "Fully-loaded customer acquisition cost"
    sql_expression: "..."
    domains: [acquisition]
  - name: churn_rate
    description: "30-day rolling churn"
    sql_expression: "..."
    domains: [retention]
```

### How the agent uses domains

The system prompt gives the agent a compact domain index. When a user asks a domain-specific question, the agent explores progressively:

```
lookup_domain("acquisition")        → business context + metric descriptions
lookup_metric("CAC")                → SQL expression, source table, filters
lookup_metric("acquisition cost")   → fuzzy match, returns [CAC, CPA] as candidates
list_metrics(domain="retention")    → all metrics in the retention domain
```

**Navigation is bidirectional — two doors into the same graph.** Top-down: a user asks about a *domain*, the agent reads its context, then drills into the metrics. Bottom-up (the common case, since queries usually name a metric): the agent looks up a *metric*, and `lookup_metric` returns that metric's `domains`, which it can follow into `lookup_domain` for the business context. Because membership lives on the metric, the metric path is always a first-class on-ramp to the domain.

Either way, the agent knows that "revenue is recognized at fulfillment, not at booking" *before* it writes a single line of SQL — reducing hallucinated metrics and incorrect calculations.

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
    business_owner: revenue-platform   # optional — team that owns the definition
    operational_owner: data-eng-finance # optional — team that owns data health
    last_reviewed: 2026-05-15          # optional — drives staleness detection

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

`tier`, `indicator_kind`, and `domains` are all optional. For dbt and Cube sources, these fields live under the metric's `meta:` block and are read through the same field names. For Ossie, they live in the model's `custom_extensions` block (see [Apache Ossie](#apache-ossie) below).

**Ownership & review cadence (optional).** `business_owner` / `operational_owner` (always *teams*, not individuals — owners outlive any one person) and `last_reviewed` declare who owns a metric's definition vs. its data health, and when it was last vetted. `last_reviewed` feeds `DataContract.find_stale()` (see [Governance Staleness](docs/architecture.md#governance-staleness)) and surfaces in `lookup_metric` / `lookup_domain` as a `stale` flag so the agent can disclose drift at query time. The same three fields are accepted on a `domain`. They are read from the **YAML source** and from an **Ossie** model's `custom_extensions` block; dbt/Cube metrics default to unset.

**dbt** — point to a `manifest.json`:
```yaml
semantic:
  source:
    type: dbt
    path: "./dbt/manifest.json"
```

dbt's built-in `relationships` schema test compiles into the manifest as a test node — `DbtSource` projects each one into a `Relationship`, resolving the owner via `attached_node` (manifest v12+) and the referenced model via `depends_on.nodes`. Tests with non-`relationships` types (`not_null`, `unique`, custom tests) and tests that can't be resolved are silently ignored. Three optional knobs read from the test's `meta:` block (matching how `tier` / `domains` are read on metrics):

```yaml
# In your dbt schema.yml
models:
  - name: orders
    columns:
      - name: customer_id
        tests:
          - relationships:
              to: ref('customers')
              field: id
              meta:
                preferred: true
                required_filter: "status != 'cancelled'"
                relationship_type: many_to_one  # default; one_to_one / many_to_many also accepted
```

**Cube** — point to a Cube schema file:
```yaml
semantic:
  source:
    type: cube
    path: "./cube/schema.yml"
```

Each cube's `joins:` block projects into `Relationship` instances. The parser handles the single-equality form `{CUBE}.col1 = {Other}.col2` (in either direction); the `from` side is always the column on the cube declaring the join, regardless of how the SQL was written. Cube's `relationship` enum (`belongsTo`, `hasOne`, `hasMany`, plus the snake_case aliases `many_to_one` / `one_to_one` / `one_to_many`) maps to the canonical `Relationship.type`. Reads from each join's `meta:` block:

```yaml
# In your Cube schema
cubes:
  - name: Orders
    sql_table: analytics.orders
    joins:
      - name: Users
        sql: "{CUBE}.customer_id = {Users}.id"
        relationship: belongsTo
        meta:
          preferred: true
          required_filter: "status != 'cancelled'"
          relationship_type: many_to_one  # optional override
```

Joins whose SQL doesn't match the single-equality pattern (composite keys with `AND`-chained equalities) or whose target cube can't be resolved by name are skipped silently — fall back to declaring those in your contract YAML via `YamlSource`.

### Apache Ossie

[Apache Ossie (incubating)](https://ossie.apache.org/) — formerly Open Semantic Interchange — is the vendor-neutral spec for exchanging semantic models across analytics, AI, and BI tools. Point at a model file:

```yaml
semantic:
  source:
    type: ossie
    path: "./semantic/model.yml"
```

Ossie standardises what a metric *is*; this library enforces what an agent may *do* with it. So the spec is a strict subset of the vocabulary here — an Ossie `Metric` carries only `name`, `expression`, `description`, `datatype`, and `ai_context`.

| Ossie | Read as |
| --- | --- |
| `datasets[].source` + `fields[]` | Table schemas. A three-part `database.schema.table` is keyed on its trailing `schema.table`; a query-backed dataset registers no table |
| `metrics[].expression.dialects[]` | `sql_expression`, resolved deterministically — your `dialect=` first, then `ANSI_SQL`, then `Ossie_SQL_2026`, then the first declared entry |
| `relationships[]` | `Relationship`, with cardinality *derived from both sides* — see below |
| `ai_context` (anywhere) | Carried into `get_extras()["ossie_ai_context"]`, keyed by model then entity kind, never interpreted |
| `custom_extensions[]` | Ours read as real vocabulary; every other vendor's carried into `get_extras()["ossie_custom_extensions"]`, keyed by model then vendor |

**Cardinality.** Ossie never writes the join type down; it is implied by which endpoints are keys. The spec documents `to` as the one side, but nothing validates that — and trusting it is not free, because `RelationshipChecker` fires its fan-out warning only on `one_to_many`. So both sides are checked:

| `from_columns` a key | `to_columns` a key | Type |
| --- | --- | --- |
| ✓ | ✓ | `one_to_one` |
| ✗ | ✓ | `many_to_one` |
| ✓ | ✗ | `one_to_many` |
| ✗ | ✗ | `many_to_many` |

Keys are optional in Ossie, and their absence is not evidence of fan-out — when the `to` dataset declares no keys at all, the spec's declaration stands and the join reads as `many_to_one`.

Composite-key relationships are **skipped with a warning** rather than split into one edge per column pair — our `Relationship` has single-column endpoints, and splitting would assert two joins that are each individually wrong. Declare those in a `YamlSource` overlay.

**Governance fields.** Ownership, review dates, tiers, decompositions, and the metric-impact graph have no home in the Ossie spec, and every `$def` in its JSON Schema sets `additionalProperties: false` — so they cannot be added as extra keys. They ride in the spec's own escape hatch, `custom_extensions`, under this project's vendor name:

```yaml
    custom_extensions:
      - vendor_name: AGENTIC_DATA_CONTRACTS
        data: |
          {
            "metrics": {
              "total_sales": {
                "business_owner": "revenue-analytics",
                "operational_owner": "data-platform",
                "last_reviewed": "2026-05-01",
                "tier": ["gold"],
                "domains": ["sales"],
                "decompositions": [
                  {"operator": "sum", "operands": ["total_profit", "total_cost"]}
                ],
                "drill_by": [
                  {"dimension": "region", "column": "public.customer.c_region"}
                ]
              }
            },
            "metric_impacts": [
              {"from": "total_cost", "to": "total_profit",
               "direction": "negative", "confidence": "verified",
               "evidence": "Margin bridge reconciled 2026-Q1."}
            ]
          }
```

Ossie stores extension payloads as a JSON *string*, so `data` is JSON nested inside YAML (a plain YAML mapping is accepted too). Restored decompositions and `drill_by` go through the same validators as `YamlSource`, failing loudly at load, and a bare `"tier": "gold"` is promoted to a list exactly as `YamlSource` does. Another vendor's malformed payload is carried verbatim and logged rather than raised on — a neighbour's typo must not stop your contract from being enforced.

`expected_extras` does not apply to an Ossie source: its extras are synthesized by the parser under fixed keys, not authored as free top-level sections. Declaring it on a non-`yaml` source logs a warning rather than being silently dropped.

> The spec is at `0.2.0.dev0` with no tagged releases yet, so expect churn. `dialect` is deliberately treated as an opaque string and never validated against the enum, since the accepted expression-language proposal adds `Ossie_SQL_2026` and makes it the default.

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

  # When multiple parallel join paths exist between the same pair of tables
  # (role-playing dimensions, multi-role FKs), mark the canonical one
  # `preferred: true`. The agent sees `preferred="true"` in the prompt and
  # `lookup_relationships` returns preferred edges first.
  - from: analytics.orders.customer_id
    to: analytics.users.id
    type: many_to_one
    description: "Customer who placed the order — canonical user join."
    preferred: true
  - from: analytics.orders.sales_rep_id
    to: analytics.users.id
    type: many_to_one
    description: "Salesperson who closed the order."
```

| Field | Required | Description |
|-------|----------|-------------|
| `from` / `to` | Yes | Fully qualified column references (`schema.table.column`) |
| `type` | No | Cardinality: `many_to_one` (default), `one_to_one`, `many_to_many` |
| `description` | No | Free-text context for the agent (join guidance, caveats, data quality notes) |
| `required_filter` | No | SQL condition that **must** be applied when using this join (e.g., bridge table disambiguation) |
| `preferred` | No | Mark the canonical join when alternatives exist (defaults to `false`). Surfaces as `preferred="true"` in the prompt, floats the edge to the front of `lookup_relationships` direct-lookup output, and biases multi-hop BFS path-finding toward it. Leave unset for role-playing peers (e.g. `order_date` vs `ship_date`) where no single path is canonical. |

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

`trace_metric_impacts` serializes at most 200 edges per call, nearest (BFS) first; a graph holding more than that adds a `note` naming the true total and how many are shown. A cycle-closing edge (`c -> a` when the graph has `a -> b -> c -> a`) is reported once rather than treated as an error — the graph genuinely closes.

### Metric decomposition and drill dimensions

Impact edges are *causal* — evidential and non-exhaustive. A metric can also declare its *arithmetic identity*: how its value is exactly reconstructed from other metrics. Declare `decompositions` on the metric itself:

```yaml
metrics:
  - name: total_revenue
    sql_expression: "SUM(amount)"
    decompositions:
      - operator: product            # sum | product | ratio | difference
        operands: [paying_customers, arpu]
    drill_by:
      - {dimension: region, column: analytics.dim_customer.region}
```

An identity decomposition is *exhaustive and exact* — nothing else can move `total_revenue` except `paying_customers` and `arpu` — so an agent diagnosing a change can localize it deterministically before reaching for the speculative impact edges. `ratio` and `difference` are binary; `sum` and `product` take two or more operands. Decompositions are validated at load time: every operand must resolve to a declared metric, and the identity edges must form a DAG (a metric cannot transitively decompose into itself). A metric with no decomposition is a valid leaf.

**Operand units and precision are yours to get right.** The library folds operand values with the declared operator and has no view of either. A conversion rate declared as a rounded percentage (`ROUND(100.0 * n / d, 1)`) makes a `product` identity false by ~100× — declare the identity in fraction units. The quieter trap is precision: an operand carried at three decimals leaves a ~0.03% residual, which [`reconcile_decomposition`](#validating-a-verified-examples-corpus) reports identically to ETL drift. Widen its `rel_tol` to the precision the operands actually have; the default `1e-4` assumes they are exact.

**Declaring where the cross term goes.** When an agent attributes a *change* in
`total_revenue` to its factors, the `ΔC·ΔP` cross term has to land somewhere —
and every placement sums correctly, so two reports built on different
placements are indistinguishable. A pilot across 16 sessions found three
distinct placements on identical data, with a 13.5% swing on the headline
contribution and the narrative conclusion flipping between them. That makes the
placement a non-inferable business fact, so declare it:

```yaml
decomposition_convention:
  convention: split_evenly       # source-wide default

metrics:
  - name: activations
    decompositions:
      - operator: product
        operands: [volume, rate]
        convention: fold_into    # explicit | split_evenly | fold_into, overrides the default
        convention_operand: rate # required for fold_into
```

| Convention | Cross term | Known as |
|---|---|---|
| `explicit` | reported on its own line, attributed to no factor | — |
| `split_evenly` | divided equally among the operands | Shapley, at two operands |
| `fold_into` | absorbed entirely by `convention_operand` | Laspeyres / Paasche, at two operands |

Only `product` and `ratio` have a cross term; declaring a convention on `sum`
or `difference` raises at load. The default is resolved onto each decomposition
when the source loads, so a frozen contract states its effective convention
outright. Both `lookup_metric` and `trace_metric_impacts` carry the declaration
to the agent.

`drill_by` lists the dimensional cuts a metric can be sliced by, in priority order (`schema.table.column`) — the exhaustive `GROUP BY` axes ("revenue by region") that dominate weekly-review diagnosis. Its column reference is soft-validated: a malformed shape raises, but a reference to a table the contract hasn't declared is allowed (schemas are optional).

Decomposition operands become identity edges in the same graph as impacts, so `trace_metric_impacts` walks both. Its `kinds` argument selects which — `identity` (arithmetic), `influence` (causal), or `all` (default) — and every returned edge is tagged with its `kind`:

```python
await trace.callable({
    "metric_name": "total_revenue",
    "direction": "upstream",       # operands drive their parent
    "kinds": "identity",           # walk the arithmetic skeleton first
})
# Returns edges like: {"depth": 1, "from": "arpu", "to": "total_revenue",
#                      "kind": "identity", "operator": "product",
#                      "convention": "split_evenly"}
```

Both kinds answer `direction` the same way — every edge points from a driver to
what it affects — so one pair declared as both an impact edge and a
decomposition operand comes back under one direction, as two edges. An identity
walk that finds nothing where edges exist on the other side says so in a `note`
rather than returning a bare `[]`.

Today `decompositions` / `drill_by` are declared directly in YAML contracts or in an Ossie model's `custom_extensions`; dbt/Cube extraction and a variance-diagnosis tool are deferred.

## Validating a verified-examples corpus

If you keep a corpus of known-good `question → SQL` examples — the kind an analytics agent accumulates from real sessions and promotes through review — `validate_examples` re-checks each example's SQL against a contract using the *same* two-layer `Validator` that gates live queries. The corpus stays entirely yours (your repo, your format, your review flow); the library never stores, loads, or executes it — it contributes exactly one verb, *validate*.

```python
from agentic_data_contracts import DataContract
from agentic_data_contracts.validation import VerifiedExample, validate_examples

contract = DataContract.from_yaml("contract.yml")
examples = [VerifiedExample.from_dict(row) for row in load_your_yaml()]  # you own the load step

report = validate_examples(examples, contract, explain_adapter=adapter)  # adapter → live EXPLAIN
if not report.ok:
    print(report.summary())   # markdown, ready to post as an MR comment
    # in CI: sys.exit(1)
```

Each example lands in exactly one `status` — `valid` (statically contract-checked and passed), `violation` (a check rejected it), `unverified` (the engine planned it but policy couldn't be statically checked — see below), or `unchecked` (no verdict possible) — with two flags, `contract_checked` and `engine_checked`, recording *what* was verified. `report.ok` is a **safe gate**: it is True only when *every* example is `valid`, so `if not report.ok: sys.exit(1)` fails on violations, unchecked, *and* unverified rows (test `report.violations` directly for a laxer gate). Two uses of the same call:

- **MR gate** — validate the corpus in CI *before* a human reviews it; fail on `not report.ok`, so the human is no longer the only check.
- **Drift sweep** — re-run against a *changed* contract; `report.violations` are the examples the change just broke. With an `explain_adapter`, the live EXPLAIN also catches a dropped or renamed column that static checks can't see.

It confirms an example is still *allowed, well-formed, and plannable against the current schema* — never that it still returns the right answer, because it **never executes** the SQL. Result correctness is the second pass's job, [below](#asserting-the-certified-answer-not-just-compliance); whether an agent can reach that SQL from the contract at all is the [third's](#evaluating-agent-conformance). For SQL an engine parses but sqlglot cannot (e.g. Denodo/VDP), a parse failure falls back to the engine's own planner; those pass as plannable but policy-unverified, flagged in `report.unverified_compliance`. See [`examples/revenue_agent/verify_examples.py`](examples/revenue_agent/verify_examples.py) for a runnable, DuckDB-backed demo.

### Asserting the certified answer, not just compliance

`validate_examples` proves an example is *allowed* — it never proves the SQL is *right*. A query with every table permitted, the tenant filter present, and explicit columns can still sum the wrong rows and pass with `status: "valid"`. To close that gap, an example can carry the certified answer alongside its SQL, and a second pass, `check_example_answers`, executes just the compliant, asserted rows and compares:

```yaml
- id: acme-completed-revenue
  question: "total completed revenue for acme"
  sql: SELECT SUM(amount) FROM analytics.orders WHERE tenant_id = 'acme' AND status = 'completed'
  expected: 10700.00     # the certified answer
  rel_tol: 0.001          # optional, overrides the call-level default for this row
  abs_tol: 0.0            # optional, likewise
  time_scoped: false       # optional; see "relative time windows" below
```

```python
from agentic_data_contracts.validation import check_example_answers

report = validate_examples(examples, contract, explain_adapter=adapter)
answers = check_example_answers(report, adapter=adapter)  # a DIFFERENT adapter type — see below

if not (report.ok and answers.ok):
    print(report.summary())
    print(answers.summary())
    # in CI: sys.exit(1)
```

Note what `answers.ok` means before you paste that gate in: it is True only when at least one assertion was actually *checked* **and** every checked one matched. An **empty** answer report is therefore False, not True — a gate that quietly stopped asserting anything fails rather than passing. The consequence to expect: adopt the recipe before any row carries an `expected` and the build goes red on a corpus with nothing wrong with it. Add the first assertion, or leave `answers.ok` out of the gate until you do.

`check_example_answers` takes `report` — the output of `validate_examples` — not the raw examples. That is deliberate, not incidental: it means the pipeline cannot hand it unvalidated SQL. A row that violates the tenant-filter rule is precisely the query that must not be sent to the warehouse to see what it returns, so a row is executed only when it is `status == "valid"` **and** declares an `expected` or `expected_rows`; everything else (a violation, an unverified or unchecked row, or a valid row with no assertion) produces no result at all. Note the two functions also take different adapter *kinds*: `validate_examples` takes an `ExplainAdapter` (plans only, never runs a query), while `check_example_answers` takes a `DatabaseAdapter` (executes) — the execute-capable adapter enters the pipeline only at this second, already-filtered stage.

Each result lands in exactly one `status` — `match`, `mismatch`, `unassertable`, or `error`. What a `mismatch` carries depends on which assertion it came from: a scalar `expected` populates `expected` / `actual` / `abs_diff` / `rel_diff`, while a breakdown leaves all four `None` and reports through `expected_rows`, `actual_row_count`, and `row_differences` instead. **A SQL statement using a relative time window is refused, not executed**: `WHERE created_at >= CURRENT_DATE - 30` degrades correctly as fixture data ages, so the certified answer would too, for a reason the corpus author never touched. The checker scans for that before running anything — `CURRENT_DATE` / `CURRENT_TIMESTAMP` and friends, plus function-call spellings like `NOW()`, `GETDATE()`, and `TODAY()` — and marks the row `unassertable` when it finds one. Set `time_scoped: true` once you've confirmed the window is pinned some other way (e.g. the SQL binds explicit dates from application code) to run it anyway.

The default tolerance is deliberately tight — `rel_tol=1e-9`, `abs_tol=0.0` — because a certified answer is meant to be *the* number, not an approximation; the default absorbs only floating-point representation noise. Widen `rel_tol` / `abs_tol` per example when the certified answer itself has limited precision — e.g. it was read off a dashboard that rounds to whole dollars — rather than loosening the call-level default for the whole corpus.

One more consequence worth knowing before you write `expected: 0`: the tolerance's relative term is `rel_tol * abs(expected)`, so at `expected == 0` it's always zero and only the absolute term can pass a near-miss. A row asserting "zero failed orders in Q1" matches only an *exact* zero unless you also set an `abs_tol`.

**`expected_rows`: certifying a breakdown, not just a scalar.** A certified answer isn't always a single number — revenue by region, a top-N list, any `GROUP BY` has nowhere to put its answer in `expected`. `expected_rows` is `expected`'s sibling for exactly that shape, and a row declares one or the other, never both:

```yaml
- id: revenue-by-region
  question: "total revenue by region"
  sql: >
    SELECT c.region, SUM(o.amount) AS revenue
    FROM analytics.orders o JOIN analytics.customers c ON o.customer_id = c.id
    WHERE o.tenant_id = 'acme' AND o.status = 'completed'
    GROUP BY c.region
  expected_rows:
    - [EMEA, 5000.00]
    - [APAC, 3000.00]
    - [AMER, 2700.00]

- id: top-3-regions
  ordered: true                # order IS the answer
  expected_rows:
    - [EMEA, 5000.00]
    - [APAC, 3000.00]
    - [AMER, 2700.00]
```

Row identity is inferred from the cells themselves, not declared: a non-numeric cell is a key (the group), a numeric cell is a value (the measurement). Comparison pairs rows by key and is **unordered by default** — a `GROUP BY` without an `ORDER BY` has no guaranteed row order, so a correct query should never fail on the order it happens to come back in. Set `ordered: true` when order *is* the answer (a top-N list); comparison then pairs rows by position instead of by key.

A certified answer of "no rows" — the shape of every data-quality invariant ("no orders without a tenant", "no negative amounts") — is written with `expected`, not `expected_rows: []` (which is rejected as almost certainly a mistake, not a valid assertion): `SELECT COUNT(*) FROM ... WHERE ...` with `expected: 0`.

Its sibling `reconcile_decomposition(...)` applies the same CI-first, contract-relative spirit to a metric's declared arithmetic identity, executing the `decompositions` above against live data to assert the identity still holds within tolerance. Its default `rel_tol=1e-4` assumes the operands are exact — see [operand units and precision](#metric-decomposition-and-drill-dimensions) when one of them is a rounded percentage or carries limited decimals.

`attribute_change(...)` applies a declared convention to measured values —
values in, contributions out, no database access, because *when* each value was
measured is the caller's business:

```python
from agentic_data_contracts.validation import attribute_change

result = attribute_change(
    contract.load_semantic_source().get_metric("activations"),
    before={"volume": 10_000, "rate": 0.35},
    after={"volume": 15_000, "rate": 0.45},
)
result.contributions   # {"volume": 1750.0, "rate": 1500.0} under fold_into: rate
result.interaction     # 500.0 — the raw residual, so the placement is auditable
```

Its sibling `check_attribution(...)` scores a *reported* breakdown against the
declared convention. Its intended caller is an **eval harness** measuring
whether an agent follows the contract — not CI and not production, since a
reported breakdown exists only inside a written answer.

### Evaluating agent conformance

Both passes above check SQL a human already got right. `validate_examples` asks whether the certified query is still *allowed and plannable*; `check_example_answers` whether it still *returns the right number*. Neither involves an agent, so neither can see the third way a contract decays. Rename a metric, trim a domain description, delete the sentence that explained which order status counts as revenue — enforcement is untouched and the certified SQL still returns `10700.00`, so both passes stay green, while an agent reading that contract can no longer find its way to the query. The contract stopped *teaching*, and nothing said so.

`evaluate_conformance` is the third pass over the same corpus: can an agent reproduce the certified answer from the contract alone, through the governed path? The progression is *enforceable* → *accurate* → *teachable*, and only the last of the three degrades silently today.

The library never runs your agent. You wire the agent the way you already do, give it one `ContractSession` per question with a `ToolRecorder` attached, and hand back what the session recorded:

```python
from agentic_data_contracts import DataContract
from agentic_data_contracts.core.session import ContractSession
from agentic_data_contracts.tools.factory import create_tools
from agentic_data_contracts.validation import (
    Attempt,
    ToolRecorder,
    VerifiedExample,
    evaluate_conformance,
)

examples = [VerifiedExample.from_dict(row) for row in load_your_yaml()]  # you own the load step
corpus = [ex for ex in examples if ex.question]  # a row with no question cannot be evaluated

attempts = []
for example in corpus:
    # One recorder per row: a ToolRecorder serves exactly one attempt and
    # refuses a second read, so two questions can never merge their call logs.
    session = ContractSession(contract, recorder=ToolRecorder())
    tools = {
        t.name: t.callable
        for t in create_tools(
            contract,
            adapter=adapter,
            semantic_source=semantic,
            session=session,
            caller_principal=example.principal,  # a row's principal, or None
        )
    }

    final_text = await your_agent_loop(example.question, tools)   # your agent, your framework

    attempts.append(Attempt.from_session(example, session, final_text=final_text))

report = evaluate_conformance(attempts)   # pure: no network, no database, no model
print(report.summary())                   # markdown, ready to post as a PR comment
```

`evaluate_conformance` itself is pure and synchronous — it scores recorded `Attempt`s and nothing else. Everything expensive and nondeterministic happens in *your* loop, above the call, which is what makes the verdict logic testable without a model and reproducible from a saved run. `Attempt.from_session` also carries off `session.cost_usd` and the recorder's own elapsed time, so a run can be costed. A runnable end-to-end demo with a scripted stand-in agent (no API key, no network) is [`examples/revenue_agent/evaluate_conformance.py`](examples/revenue_agent/evaluate_conformance.py).

**Two orthogonal axes, five states each.** The `answer` axis scores the number: `match`, `mismatch`, `unassertable` (the agent's answering query used a relative time window, so the certified answer decays against it), `skipped` (the row certified no `expected`, **or** certified a breakdown via `expected_rows` — see below), `error`. The `protocol` axis scores the path: `followed`, `violated`, `contaminated`, `not_applicable` (the row activated no protocol rule), `unchecked`. A third field, `answer_source` (`declared` / `sole_scalar` / `last_scalar` / `none`), records *how* the answered number was picked and stays separate from the verdict, so a row that matched on an ambiguously-selected `last_scalar` still reports `answer="match"` and is still excluded from `ok` — the verdict and the evidence for it are different fields.

**`evaluate_conformance` grades a certified breakdown only when the host declares it.** Set `final_rows` and `final_columns` on the `Attempt` — the breakdown counterpart to `final_answer` — and pass 3 scores it through the same `compare_rows` pass 2 uses, with tolerance and naming which group differed:

```python
attempt = Attempt.from_session(
    example, session,
    final_rows=[["Europe", 7200.00], ["North America", 2700.00]],
    final_columns=["region", "revenue"],
)
```

The host declares rather than the library inferring, for the same reason `final_answer` exists: `_select_answer` picks a scalar by clustering candidates and *marks the guess* when it is ambiguous, and there is no equally honest inference for a breakdown — choosing whichever query happened to match would let a lucky drill-down pass. Leave them unset and the row reports `answer="skipped"` and passes, exactly as before; note that this means the protocol axis is graded but the numbers are not.

Wiring `final_rows` uniformly is safe: on a row certified with a scalar `expected` it is ignored, and scalar selection runs as usual. A declared breakdown that reached no successful `run_query` reports `protocol="contaminated"` — pass 3 asks whether the answer came *through the governed path*, and one that never queried did not.

**Tolerances are not symmetric with pass 2.** `check_example_answers` accepts corpus-wide `rel_tol`/`abs_tol` overrides that apply to any row setting none of its own, but `evaluate_conformance(attempts)` takes no such override and falls back straight to the library default for an unset row — so the same corpus can `match` in pass 2 and `mismatch` in pass 3 unless the tolerance is set per-row: a row's own `rel_tol`/`abs_tol` apply identically to both passes.

The distinction that makes the gate mean anything is between **nothing to judge** and **couldn't judge**. `skipped` and `not_applicable` *pass*: no assertion was made, no rule was activated, and there is nothing to hold against the contract. `error` and `unchecked` *fail*: something was supposed to be judged and the evaluation could not do it. Conflating those two is the classic evaluation bug — a suite reporting a serene green because every single case was quietly skipped. For the same reason `report.ok` is False on an **empty** report: a harness that stopped producing attempts fails rather than passes.

**`expects_metrics` activates the protocol rule.** The metric-consultation check only judges rows that declare which metric definitions should have been consulted before answering:

```yaml
- id: acme-completed-revenue
  question: "total completed revenue for acme"
  sql: SELECT SUM(amount) FROM analytics.orders WHERE tenant_id = 'acme' AND status = 'completed'
  expected: 10700.00
  expects_metrics: [total_revenue]   # lookup_metric must precede the answering query
```

A row that names nothing lands on `not_applicable` and passes. That is deliberate, not laziness: the output of this pass is meant to be read as *evidence about the contract's prose* — a `violated` row is an argument for rewriting a definition or a domain description. A guessed violation would therefore become wrongly-rewritten contract text, which is worse than the finding never existing. The rule fires only where a human said in the corpus what the right path was.

**The closed world, and the `contaminated` status.** An agent in production may hold tools this library never created — a generic SQL tool, a warehouse MCP server, a shell, a retriever. The recorder cannot see any of them, which makes "never called `lookup_metric`" ambiguous between a real prose gap and the agent simply going around the contract. The evaluator therefore assumes a **closed world**: the eval runs the agent with the contract toolset and nothing else. That is legitimate precisely because it is an eval and not production — "can an agent answer this from the contract alone?" is not a question you can ask in an open world.

The requirement is enforced by **derived evidence, never by assertion**. You are not asked to promise a closed world. An answer declared with zero successful `run_query` calls proves by construction that the number came from somewhere else, and the row is marked `contaminated`. If you *do* have full framework logs, `Attempt.foreign_tool_calls` accepts the names of non-contract tools that were available; it is used only to mark a row `contaminated` and is never input to any other verdict, keeping arbitrary trace formats out of the reasoning path. The **documented limit**: an agent that used `run_query` but drew its business context from a foreign retriever leaves no detectable trace at all. These findings are trustworthy in proportion to how closed the eval world actually was.

**A blocked query is not a conformance failure.** Run the demo and you will see rows whose SQL the contract rejected counted among the *passes*. That is correct, and it is the point of keeping the axes orthogonal. A row certifying no `expected` and activating no protocol rule gives pass 3 nothing to judge, whatever happened to its SQL — legality is pass 1's job, and it already failed that row there. Blocked attempts are still recorded and surface in `reasons` as friction — the demo's blocked rows report `"1 blocked run_query attempt(s) with none accepted"`, and an attempt that recovered reports `"before an accepted one"` (or `"after an accepted one"` for a later drill-down, since the wording is derived from call order). That is diagnostic signal about how hard the contract was to obey, not a verdict. Each pass gates on what it can actually see, and the gate is composed:

```python
report = validate_examples(examples, contract, explain_adapter=explain_adapter)
answers = check_example_answers(report, adapter=adapter)
conformance = evaluate_conformance(attempts)

if not (report.ok and answers.ok and conformance.ok):
    sys.exit(1)
```

**One known limitation on the SDK and middleware paths.** `create_sdk_mcp_server` and the `contract_middleware` decorator both check the session budget *before* calling the inner tool closure, and the recorder lives inside that closure. So when a session budget is exhausted on one of those two paths, the blocked envelope is returned with **no tool call recorded at all**. The attempt's call log is then short for a reason nothing in it explains, and if you also declare a `final_answer`, the row is attributed `contaminated` — implying the agent went around the contract, when in fact it was cut off by its own budget. On those paths, pass `error=` to `Attempt.from_session` when you observe a budget block; an `error` attempt is scored `error` / `unchecked` and fails honestly instead of being mis-diagnosed.

**Wiring it into CI.** Pass 3 differs in *kind* from the first two. It runs an agent: it costs money per invocation, needs a credential, takes minutes, and is **nondeterministic** — the same contract can pass and fail on consecutive runs from sampling alone. Wired as a hard all-must-match gate on every PR it will flake, and a flaky gate gets disabled, which costs you the signal entirely.

| Pass | Trigger | Gate |
|---|---|---|
| 1 `validate_examples` | every PR | hard, all-must-pass |
| 2 `check_example_answers` | every PR | hard, all-must-pass |
| 3 `evaluate_conformance` | path-filtered on contract / semantic YAML changes, plus nightly | threshold over repeats via `pass_rate()`, or advisory with `summary()` posted as a PR comment |

Run each question several times and `by_example()` groups the repeats — keyed on the example's `id`, falling back to its `question`, never on the positional label — so a verdict can be a *measurement* ("7 of 10 runs found the metric") rather than a single sample of a stochastic process.

**Corpus readiness.** `VerifiedExample.question` has always been optional and non-load-bearing, so most existing corpora do not populate it. Adopting this pass therefore starts there: fill in `question` on the rows you want evaluated, and `expects_metrics` on the rows where the path matters. Rows without a question are skipped and counted, never treated as errors — an unevaluatable corpus stays visible instead of silently shrinking the run.

## Custom Prompt Rendering

The system prompt is generated by a `PromptRenderer`. The default `XmlPromptRenderer` produces XML-structured output — XML tags mark section boundaries, and any frontier model reads them:

```python
dc = DataContract.from_yaml("contract.yml")
print(dc.to_system_prompt())  # XML output, works with any frontier model
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

### Detail thresholds

`XmlPromptRenderer` degrades its own output above two counts, to keep a large
contract from dominating the prompt:

| Above | The prompt loses | The agent recovers it with |
|---|---|---|
| `metric_detail_threshold` (default 20) | per-metric names and descriptions, replaced by a count | `list_metrics()` / `lookup_metric("...")` |
| `relationship_detail_threshold` (default 30) | per-relationship `<from>` / `<to>` join keys, replaced by per-table `join_count` | `lookup_relationships(table="...")` |

Both are constructor parameters, and `None` disables degradation entirely:

```python
# Render every join key inline, however many there are.
dc.to_system_prompt(source, renderer=XmlPromptRenderer(relationship_detail_threshold=None))
```

The suppressed content is not small — on a 52-relationship contract the join
keys are roughly 3x the whole `<table_relationships>` fragment. So when either
threshold trips, the renderer logs at `INFO` naming the count, the threshold,
and the parameter that turns it off, and the rendered block says what it
dropped rather than looking complete. Omitting an argument reads the
`METRIC_DETAIL_THRESHOLD` / `RELATIONSHIP_DETAIL_THRESHOLD` class attributes, so
a subclass that overrides them still works.

### Consumer-authored sections (extras)

`YamlSource` interprets five top-level keys — `metrics`, `tables`,
`relationships`, `metric_impacts`, `decomposition_convention` (exported as
`SEMANTIC_KEYS`). Every other
top-level key is carried verbatim as an *extra*: reachable, portable, and
renderable, but never interpreted.

```yaml
# semantic.yml
metrics: []

column_hints:
  - table: analytics.orders
    prefer: order_total
    over: total            # does not exist on this view
    reason: Verified against the warehouse on 2026-03-14.

join_paths:
  - name: leaf_to_root
    description: Traverse the product hierarchy from leaf to root.
    path:
      - {from: dim_leaf, to: dim_mid, "on": mid_id}
      - {from: dim_mid, to: dim_root, "on": root_id}
```

```python
source = YamlSource(
    "semantic.yml",
    expected_extras={"column_hints", "join_paths"},   # anything else raises
)
prompt = contract.to_system_prompt(
    source,
    renderer=XmlPromptRenderer(extra_sections=["column_hints", "join_paths"]),
)
```

Without `expected_extras`, uninterpreted keys are logged at WARNING and carried
anyway, so no existing contract stops loading. Pass a collection to turn a typo —
`relationship:` for `relationships:` — into a load-time error instead of a
silently deleted section.

When the contract loads the source for you (`DataContract.from_yaml` →
`load_semantic_source()`), declare the sections in the contract instead — there
is no `YamlSource` call of your own to pass the argument to:

```yaml
semantic:
  source:
    type: yaml
    path: ./semantic.yml
    expected_extras: [column_hints, join_paths]
```

`expected_extras: []` is strict mode (any uninterpreted key raises); omitting the
key keeps the warn-and-carry default. The setting applies on both load paths —
the external `path` and the frozen `inline` snapshot — so declaring your sections
does not stop working the moment the contract is frozen. It is ignored for `dbt`
and `cube` sources, which have no extras concept.

`expected_extras` is a policy about *reading* the source, not part of what the
agent sees, so it is deliberately excluded from `contract_canonical_bytes`:
adding, changing, or removing it never moves `contract_digest`. The trade is that
it does not travel with a published contract — a consumer rehydrating your frozen
bytes falls back to warn-and-carry and sees the WARNING you silenced. That is the
right way round: the declaration is a lint on *your* authoring, and buying the
consumer's silence with a digest change would invalidate every attestation
pinned to the contract.

The exclusion is scoped to the content-addressed bytes, not to the field, so an
ordinary `model_dump()` still carries it and a local round trip keeps your
declaration. Only what gets published drops it.

Nothing renders unless you name it in `extra_sections`, so a section kept for
internal bookkeeping never reaches a prompt. Naming a section the source does not
carry warns rather than failing quietly. A mapping value replaces the default
YAML-in-a-tag formatter with your own:

```python
XmlPromptRenderer(extra_sections={"join_paths": my_renderer})
```

**Two things to know before you rely on this.**

Extras are part of the contract's identity. They travel through
`freeze_semantic_source()` into `contract_canonical_bytes`, so editing a hint's
prose changes `contract_digest` and invalidates any ARD attestation pinned to it.
That is deliberate — extras are resident prompt text, so they shape agent
behaviour, and a digest that ignored them would verify something different from
what the agent actually saw. It does mean hint prose lives under the same change
discipline as `forbidden_operations`.

Extras are never interpreted. There is no schema validation and no staleness
detection — the `last_reviewed` machinery that flags stale metrics does not apply
here, because applying it would mean reading the content. If your hints need
shape or freshness guarantees, that lint belongs on your side.

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

`token_budget` is the one limit this library cannot measure on its own — the
tokens are spent by the *model* between tool calls. It is fed from the host
framework's own counter, which the **Pydantic AI** adapter (`ctx.usage`) and
both **LangChain** paths read — `create_langchain_tools` and
`ContractMiddleware` alike, each from the run's own message history. The
remaining paths (`contract_middleware` and the Claude Agent SDK) receive an
`args` dict and nothing else; they warn at wiring time if your contract declares
a budget, so an inert budget is never silent.

With LangChain you no longer need the middleware just to make a budget bind.
Wiring both is still fine — they derive the same per-conversation key from the
same run, so usage is counted once, not twice — but **share one session between
them** if you do. Each otherwise builds its own, and a split pair enforces
against one while reporting `tokens_remaining` from the other:

```python
from agentic_data_contracts.core.session import ContractSession

session = ContractSession(dc)
tools = create_langchain_tools(dc, adapter=adapter, session=session,
                               apply_middleware=False)
middleware = ContractMiddleware(dc, adapter=adapter, session=session)
```

To enforce it elsewhere, feed the session yourself:

```python
session.observe_tokens(my_client.cumulative_tokens, scope="my-run-id")
```

Two limits worth knowing. Usage is observed just before the limit check, so a
budget the current turn has already blown stops *that* call — but an agent that
exhausts its budget and then stops calling tools is never interrupted, because
nothing runs to check. And a sub-agent or subgraph inherits its parent's
`thread_id`, so its own usage is under-counted rather than added; the budget
binds on the parent's spend.

**On Pydantic AI the budget is enforced properly**, by letting the framework
check it on every model request — which never needs a tool call to happen:

```python
from agentic_data_contracts import contract_run_kwargs

result = await agent.run(
    prompt,
    deps=ContractDeps(session=session),
    **contract_run_kwargs(dc, session),   # -> usage= and usage_limits=
)
```

That returns both halves together because they are only correct together: one
`RunUsage` carried across every turn for this session, plus the contract's
budget as a flat ceiling. Pydantic AI's own counter then sees *every* request,
including the answer generation the tools never observe, so nothing has to be
estimated. Two things follow, both measured on a 500-token budget:

- **True spend settles rather than growing.** 600 against a 500-token budget,
  and *flat* however many turns run. Deriving each run's allowance from the
  session's own tally instead is linear in turns — 1100 at 6 turns, 1700 at 12,
  2500 at 20 — because that tally never sees what a run spends after its last
  tool call.
- **A refused turn costs nothing**, because the check runs *before* the request
  is issued.

`max_retries` is deliberately **not** mapped onto `request_limit`: ours counts
blocked queries, theirs counts LLM calls.

Keep catching `ContractSessionLimitError` alongside
`pydantic_ai.exceptions.UsageLimitExceeded` — the session still enforces
`max_retries`, `cost_limit_usd` and `max_duration_seconds`, and can be fed from
outside the run. Sequential turns only: one counter per session is shared
mutable state.

## Optional Dependencies

| Extra | Package | Purpose |
|-------|---------|---------|
| `duckdb` | `duckdb>=1.1.1` | DuckDB adapter (the one adapter that ships) |
| `bigquery` | `google-cloud-bigquery>=3.7.0` | Driver only — for a `DatabaseAdapter` you write |
| `snowflake` | `snowflake-connector-python>=3.14.1` | Driver only — for a `DatabaseAdapter` you write |
| `postgres` | `psycopg2-binary>=2.9.10` | Driver only — for a `DatabaseAdapter` you write |
| `agent-sdk` | `claude-agent-sdk>=0.2.96`, `mcp>=1.23.0` | Claude Agent SDK integration |
| `langchain` | `langchain-core>=1.3.3`, `langchain>=1.2.17`, `langgraph>=1.1.10` | LangChain / deepagents integration |
| `pydantic-ai` | `pydantic-ai-slim[anthropic]>=2.0.0` | Pydantic AI integration |
| `agent-contracts` | `ai-agent-contracts>=0.3.1` | ai-agent-contracts bridge |

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

## Examples

Three end-to-end working examples, each demonstrating a different governance archetype. All three run in demo mode without the Claude Agent SDK installed — DuckDB is used for the sample data and the tools are exercised directly.

| Example | Archetype | Governance patterns it teaches |
|---|---|---|
| [`examples/revenue_agent/`](examples/revenue_agent/) | Finance / lagging KPIs / audit-strict | Tenant isolation, `hypothesized` impact edges, north-star metric tier, undefined-metric policy recipe, **the full extras loop** — `column_hints` + `join_paths` in `semantic.yml`, `expected_extras` in `contract.yml`, `XmlPromptRenderer(extra_sections=...)` in the agent, and a live typo-refusal |
| [`examples/growth_agent/`](examples/growth_agent/) | Experimentation / leading indicators | `verified` / `correlated` / `hypothesized` metric impacts with real-ish A/B evidence, time-bounded events rule, `log`-level PII audit invisible to the agent, stale-review detection, **`preferred: true` on the canonical `events.user_id → users.id` join** (alongside a non-preferred `events.referrer_user_id → users.id` for referral-mechanics questions) |
| [`examples/ops_agent/`](examples/ops_agent/) | SRE reliability / real-time dashboards | `blocked_columns` for PII, two `log`-level audit rules (governance trail), `require_limit` + `max_joins` caps, **negative-direction** metric impact (DORA pattern), aggressive resource limits, **`blocked_principals` on `sre.deploys`** (try `--caller intern@co.com` to see a per-table principal denial) |

Run any of them:

```bash
uv run python examples/revenue_agent/agent.py "What was Q1 revenue by region?"
uv run python examples/growth_agent/agent.py  "Which onboarding variant lifted activation?"
uv run python examples/ops_agent/agent.py     "What's our MTTR by severity this week?"
```

Each example directory contains four files:
- `contract.yml` — governance rules, allowed tables, resource limits
- `semantic.yml` — metrics, relationships, metric impacts
- `setup_db.py` — sample DuckDB data (auto-created on first run)
- `agent.py` — runnable demo with a Claude Agent SDK path plus a fallback that exercises the tools directly

`revenue_agent` additionally ships a **verified-examples validation** demo — `verified_examples.yml` (an external corpus) and `verify_examples.py`, which re-checks it against the contract with a live DuckDB EXPLAIN (valid, static violations, a schema-drift catch only the dry-run finds, and the same SQL diverging by principal):

```bash
uv run python examples/revenue_agent/verify_examples.py
```

Reading all three gives you a complete tour of the library's design space: different enforcement levels (`block` / `warn` / `log`), different impact confidences and directions, and resource profiles tuned for very different user-latency expectations.

All three `agent.py` files also carry the [`data`-plugin skills overlay](#layer-anthropics-data-plugin-on-top-governed-analyst-skills) behind an opt-in `DATA_PLUGIN_PATH` env var (off by default, so the examples run with zero external setup):

```bash
git clone https://github.com/anthropics/knowledge-work-plugins /tmp/kwp
DATA_PLUGIN_PATH=/tmp/kwp/data \
    uv run python examples/growth_agent/agent.py "Which onboarding variant lifted activation?"
```

## FAQ

**Do I need `ai-agent-contracts`?** No. The library works standalone with lightweight enforcement (session counters, cost/retry/token budgets). Install [`ai-agent-contracts`](#optional-formal-governance-with-ai-agent-contracts) only if you want the formal 7-tuple contract, weighted success scoring, or multi-agent coordination.

**Which databases are supported?** Any, via the `DatabaseAdapter` protocol. A **DuckDB** adapter ships in the box; the `bigquery`, `snowflake`, and `postgres` [extras](#optional-dependencies) install the driver only, so you implement the (small) adapter against your warehouse's client. Layer 1 validation (static SQL analysis) runs even with no adapter configured; a database adapter adds the EXPLAIN dry-run and query execution.

**Does it execute my SQL?** Only `run_query` does, and only after validation passes (plus an optional EXPLAIN dry-run). `inspect_query` validates without executing, and forbidden operations (DELETE/DROP/UPDATE/…) are blocked before they ever reach the database.

**Do I have to use dbt or Cube?** No. Author metrics inline in a `semantic.yml` with `YamlSource`. dbt (`manifest.json`), Cube, and Apache Ossie are supported if you already have them — the agent-facing behavior is identical regardless of source.

**Does it work without the Claude Agent SDK?** Yes. The tools are plain async functions usable from LangChain/deepagents, Pydantic AI, or directly; the example agents fall back to a no-SDK demo mode.

**Is it production-ready?** It's pre-1.0 and actively evolving. Every breaking change is documented in [`CHANGELOG.md`](CHANGELOG.md) with migration notes.

## Architecture

See [`docs/architecture.md`](docs/architecture.md) for the full design spec covering the layered architecture, YAML schema, validation pipeline, tool design, semantic sources, database adapters, and the optional `ai-agent-contracts` bridge.

## License

MIT
