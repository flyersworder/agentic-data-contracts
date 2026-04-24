# Agentic Data Contracts — Architecture

**Date:** 2026-04-17
**Status:** Implemented (v0.11.0)
**Author:** Qing Ye + Claude

## Problem Statement

Data/analytics engineers face two problems with AI agents querying their data:

1. **Resource runaway** — agents burn unbounded compute, loop endlessly on retries, exceed cost ceilings
2. **Semantic inconsistency** — agents compute metrics differently across runs, query wrong tables, ignore established definitions

No single existing tool addresses both. Semantic layers (dbt metrics, Cube) handle consistency but not resource governance. Agent frameworks (LangChain, Claude Agent SDK) provide execution but not data-specific governance.

**Inspiration:** Robert Yi's LinkedIn post on "agentic contract layers" for analytics — arguing that agents need a central authority governing how data logic is consumed.

## Scope Changes from v1

| Aspect | v1 spec | v2 spec |
|---|---|---|
| Form factor | Python library tightly coupled to `agent-contracts` | Reusable library with optional `ai-agent-contracts` dependency |
| Primary target runtime | Generic (LiteLLM, LangChain) | Claude Agent SDK (but framework-agnostic) |
| `ai-agent-contracts` | Required dependency | Optional — upgrades enforcement when installed |
| Dependency management | pip | uv |
| Database interaction | Validation only | Full tool set: validate, execute, describe, preview |
| Tool surface | Validator callback | 9 agent tools (factory + middleware) |

## Design Decisions

| Decision | Choice | Rationale |
|---|---|---|
| Target user | Data/analytics engineer | Feels the pain most, already thinks in contracts (dbt, schema tests) |
| Primary runtime | Claude Agent SDK | Concrete target, growing ecosystem, but tools are plain functions usable anywhere |
| `ai-agent-contracts` | Optional dependency | Lowers barrier to entry; library works standalone with lightweight enforcement |
| Database support | Adapter protocol | Clean interface, any database can be plugged in |
| Semantic governance | Reference-based | Point to external source of truth (dbt, Cube), don't replicate it |
| Developer experience | YAML-first | Data engineers live in YAML; zero Python knowledge required to define a contract |
| Enforcement | Configurable per-rule | `block` / `warn` / `log` per rule |
| Tool delivery | Factory + middleware | Quick start via factory, composable via middleware |
| Dependency management | uv | Modern, fast, lockfile-based |

## Architecture

### Overview

```
data_contract.yml    (data engineer writes this)
       │
       ▼
 ┌─────────────────┐
 │ DataContract     │  Parsed YAML (Pydantic model)
 │   .semantic      │
 │   .resources     │
 │   .temporal      │
 │   .rules         │
 └────────┬────────┘
          │
    ┌─────┴──────┐
    │             │
    ▼             ▼
Standalone    Bridge (optional)
Mode          ┌─────────────────┐
    │         │ ai-agent-contracts│
    │         │ Contract 7-tuple │
    │         └────────┬────────┘
    │                  │
    ▼                  ▼
 ┌──────────────────────┐
 │ create_tools()        │  9 agent tools
 │ contract_middleware()  │  BYO tool wrapper
 │ ContractSession       │  Enforcement tracking
 └──────────────────────┘
          │
          ▼
  Claude Agent SDK agent
  (or any Python agent framework)
```

### YAML Schema

```yaml
# data_contract.yml
version: "1.0"
name: revenue-analysis

# Where the semantic definitions live (external source of truth)
semantic:
  source:
    type: dbt                          # dbt | cube | yaml | custom
    path: "./dbt/manifest.json"        # resolved relative to contract file

  # What the agent is allowed to access
  allowed_tables:
    - schema: analytics
      description: "Curated analytics tables — prefer for reporting"
      preferred: true                  # agent should prefer this schema
      tables: [orders, customers, subscriptions]
    - schema: raw
      tables: []                       # empty = nothing from this schema

  # What the agent must NOT do
  forbidden_operations: [DELETE, DROP, TRUNCATE, UPDATE, INSERT]

  # Business domains — provide context for domain-specific questions
  domains:
    - name: revenue
      summary: "Revenue and financial metrics from completed orders"
      description: >
        Revenue metrics track recognized revenue from completed orders.
        Revenue is recognized at fulfillment, not at booking.
      metrics: [total_revenue, gross_margin]
    - name: engagement
      summary: "Customer activity and retention patterns"
      description: >
        Customer engagement measures active usage patterns
        and retention over time.
      metrics: [active_customers, churn_rate]

  # Governance rules (per-rule enforcement)
  # Each rule has a query_check (pre-execution) or result_check (post-execution)
  # Rules with neither are advisory (shown in prompt only)
  rules:
    - name: tenant_isolation
      description: "All queries must include a WHERE tenant_id = filter"
      enforcement: block               # block | warn | log
      query_check:
        required_filter: tenant_id

    - name: use_approved_metrics
      description: "Revenue calculations must use the semantic layer definition"
      enforcement: warn                # advisory — no check block

    - name: no_select_star
      description: "Queries must specify explicit columns, no SELECT *"
      enforcement: block
      query_check:
        no_select_star: true

# Resource governance
resources:
  cost_limit_usd: 5.00
  max_query_time_seconds: 30
  max_retries: 3
  max_rows_scanned: 1_000_000
  token_budget: 50_000

# Time governance
temporal:
  max_duration_seconds: 300

# What counts as success
success_criteria:
  - name: query_uses_semantic_definitions
    weight: 0.4
  - name: results_are_reproducible
    weight: 0.3
  - name: output_includes_methodology
    weight: 0.3
```

## Core Layer

The core layer handles contract loading, Pydantic models, and lightweight self-contained enforcement. Dependencies: `pydantic`, `pyyaml` only.

### DataContract

```python
from agentic_data_contracts import DataContract

dc = DataContract.from_yaml("data_contract.yml")

# Generate contract section for the system prompt
contract_prompt = dc.to_system_prompt()
# Returns a section listing allowed tables, forbidden operations, active rules, semantic guidance

# Users compose their own system prompt and append the contract section:
system_prompt = f"""You are an analytics assistant for Acme Corp.
Always be concise and include methodology notes.

{contract_prompt}
"""
```

### Governance Staleness

YAML-level business assertions — `domain.description`, `metric_impact.evidence` — rot silently when the business changes. Both models carry an optional `last_reviewed: date` field, and `DataContract.find_stale()` flags any artefact whose timestamp is missing or older than a threshold (default 90 days).

```python
dc = DataContract.from_yaml("data_contract.yml")
source = dc.load_semantic_source()
findings = dc.find_stale(source, threshold_days=90)
for f in findings:
    print(f.kind, f.name, f.age_days)
```

Missing timestamps report as stale (`age_days=None`) — otherwise adoption is optional and defeats the forcing function. During rollout, filter by `f.age_days is not None` to grandfather in un-reviewed entries. The detector is a pure function suitable for direct use in a pytest assertion or CI check.

### Principal Resolver

Per-table access control is built on a thin resolver abstraction that normalises `caller_principal` into the identity string used for allowlist comparisons.

```python
from agentic_data_contracts import Principal, resolve_principal

# Type alias — matches the keyword-only parameter on Validator and create_tools
Principal = str | Callable[[], str | None] | None

# Normalises to the current string (calls the callable if needed)
current: str | None = resolve_principal(principal)
```

**How it works:**

- `str` — returned as-is; suitable for single-user sessions (Chainlit, one session per authenticated user).
- `Callable[[], str | None]` — called per-query, not cached; the callable typically reads a `contextvars.ContextVar` set by the message handler for each incoming request. This allows one long-lived `Validator` instance to serve a Webex room bot where different users send messages concurrently.
- `None` — resolver returns `None`; all `*_principals` restrictions are fail-closed (caller treated as unauthenticated and denied).

**Two-tier empty-string handling:** `resolve_principal` passes through an empty string without normalisation. `DataContract.allowed_table_names_for("")` treats an empty string as unauthenticated — same as `None` — so callers should canonicalize identities before passing them in.

`allowed_principals` and `blocked_principals` on `AllowedTable` are mutually exclusive (validated at YAML load time). Principals are opaque strings compared by exact equality — no normalisation is performed inside the library. See the feature spec for the full truth table covering all combinations of principal resolver value, `allowed_principals`, and `blocked_principals`.

### ContractSession (Lightweight Enforcement)

When `ai-agent-contracts` is NOT installed, `ContractSession` provides self-contained enforcement:

- **Retry count** — incremented on each failed query attempt, checked against `max_retries`
- **Token usage** — tracked via callback, checked against `token_budget`
- **Wall-clock duration** — lazy start on first `check_limits()` call (not at construction), checked against `max_duration_seconds`. Can be reset via `reset_timer()` for frameworks that manage their own idle timeouts.
- **Cost estimate** — if EXPLAIN adapter returns cost info, checked against `cost_limit_usd`

These are simple counters/timers with guard checks before each tool call. No formal state machine.

When `ai-agent-contracts` IS installed, enforcement is delegated to the formal framework via the bridge layer (see below).

## Validation Layer

Three-phase validation architecture. Dependencies: `sqlglot`.

### Phase 1: Query Checks (pre-execution, always available)

```python
class Checker(Protocol):
    def check_ast(self, ast: Expression, *args) -> CheckResult: ...
```

SQL is parsed once into a sqlglot AST. The Validator passes the AST to all applicable checkers, respecting table scoping.

**Structural checkers** (from top-level config):

| Checker | What it validates |
|---|---|
| `TableAllowlistChecker` | All referenced tables are in `allowed_tables`, filtered per `caller_principal` if supplied |
| `OperationBlocklistChecker` | No forbidden SQL operations (DELETE, DROP, etc.) |

**Rule-based query checkers** (from `query_check` blocks):

| Check | Checker | What it validates |
|---|---|---|
| `required_filter` | `RequiredFilterChecker` | Required WHERE column present in a non-tautological predicate |
| `no_select_star` | `NoSelectStarChecker` | No `SELECT *` statements |
| `blocked_columns` | `BlockedColumnsChecker` | Forbidden columns not in SELECT |
| `require_limit` | `RequireLimitChecker` | LIMIT clause present |
| `max_joins` | `MaxJoinsChecker` | JOIN count within limit |

`CheckResult` contains: `passed: bool`, `severity: block | warn | log`, `message: str`.

The validator runs all applicable checkers and aggregates results — any `block` result stops execution, `warn` results are prepended to the `run_query` response as a `WARNINGS:` preamble, `log` results are prepended as a `LOG:` preamble (also exposed via `inspect_query` under `warnings` and `log_messages`). `log`-level rules are omitted from the system prompt so the agent can't adapt behavior to avoid triggering them.

Rules that cannot be statically checked (e.g., "use semantic layer definition for revenue") become advisory rules — they appear in the system prompt but don't enforce anything. They can also be used as `SuccessCriterion` for post-hoc evaluation.

### Relationship Advisory Checks (optional, requires semantic source)

When a `SemanticSource` is passed to the `Validator`, the `RelationshipChecker` validates JOINs against declared relationships after Phase 1 completes (and only if the query is not already blocked).

| Check | What it validates |
|---|---|
| `RelationshipChecker` (join-key) | JOIN columns match declared `from`/`to` references |
| `RelationshipChecker` (required-filter) | `required_filter` column present in WHERE with a non-tautological predicate |
| `RelationshipChecker` (fan-out) | No aggregation across `one_to_many` joins |

All relationship checks produce **warnings only** — they never block queries. Undeclared joins (table pairs with no relationship definition) are silently ignored.

The checker does not implement the `Checker` protocol. It exposes `check_joins(ast) -> list[str]` which returns multiple independent warnings rather than a single pass/fail `CheckResult`.

### Layer 2: EXPLAIN Dry-Run (optional, requires database adapter)

```python
class ExplainAdapter(Protocol):
    def explain(self, sql: str) -> ExplainResult: ...

# ExplainResult:
#   estimated_cost_usd: float | None
#   estimated_rows: int | None
#   schema_valid: bool
#   errors: list[str]
```

| Database | Method | Returns |
|---|---|---|
| BigQuery | `jobs.query(dry_run=True)` | Bytes processed → cost |
| Snowflake | `EXPLAIN` | Estimated rows/partitions |
| Postgres | `EXPLAIN` (no ANALYZE) | Row estimates |
| DuckDB | `EXPLAIN` | Row estimates |

### Phase 3: Result Checks (post-execution, from `result_check` blocks)

After a query executes successfully, `run_query` calls `validator.validate_results()` to check the actual output against `result_check` rules.

**Built-in result checks:**

| Check | What it validates |
|---|---|
| `min_value` / `max_value` | Numeric column values within bounds |
| `not_null` | Column contains no null values |
| `min_rows` / `max_rows` | Result set row count within bounds |

If a result check with `enforcement: block` fails, the query data is **discarded** — the agent sees only the violation message (with actual violating values for debugging). If `enforcement: warn`, the data is returned with warnings prepended.

### Validation Flow

```
SQL string
  → sqlglot.parse(sql, dialect=contract.dialect) — parse once
  → Phase 1: structural checkers + rule-based query_check checkers (table-scoped)
  → any block? → return ValidationResult(blocked=True, reasons=[...])
  → Relationship checks (if semantic_source provided, warnings only)
  → Phase 2 available? → explain adapter
  → cost/rows exceed limits? → return ValidationResult(blocked=True, reasons=[...])
  → record estimated cost in session
  → execute query
  → Phase 3: result_check rules against actual output (table-scoped)
  → any block? → discard data, return violation
  → any warn? → prepend WARNINGS preamble to response
  → any log? → prepend LOG preamble to response
  → return results
```

## Tools Layer (Claude Agent SDK Integration)

Two modes: tool factory for quick starts, middleware for BYO tools.

### 9 Tools

1. **`describe_table(schema, table)`** — Column details from the database adapter
2. **`preview_table(schema, table, limit?)`** — Sample rows
3. **`list_metrics(domain?, tier?, indicator_kind?)`** — Browse metrics with filters
4. **`lookup_metric(metric_name)`** — Full metric definition with SQL and impact edges
5. **`lookup_domain(name)`** — Full domain description with metrics and tables
6. **`lookup_relationships(table, target_table?)`** — Direct joins and multi-hop paths
7. **`trace_metric_impacts(metric_name, direction, max_depth?)`** — BFS over the impact graph
8. **`inspect_query(sql)`** — Static + EXPLAIN check, no execution
9. **`run_query(sql)`** — Validate and execute; response includes remaining session budget

### Natural Agent Workflow

```
list_metrics → lookup_metric → lookup_relationships → describe_table
    → write SQL → inspect_query
    → (if valid) run_query
```

### Tool Factory

```python
from agentic_data_contracts import DataContract, create_tools
from agentic_data_contracts.adapters.duckdb import DuckDBAdapter

dc = DataContract.from_yaml("contract.yml")
adapter = DuckDBAdapter("analytics.duckdb")
tools = create_tools(dc, adapter=adapter)
# Returns all 9 tools as @tool-decorated async functions
# compatible with claude_agent_sdk.create_sdk_mcp_server()

# Per-caller access control (optional)
tools = create_tools(dc, adapter=adapter, caller_principal="alice@co.com")
# Or with a callable for multi-user bots (identity read per-query from a ContextVar):
tools = create_tools(dc, adapter=adapter, caller_principal=lambda: current_sender.get())
```

`create_tools` accepts `caller_principal: Principal = None` and forwards it into the `Validator`. Two of the nine tools are principal-aware: `describe_table` and `preview_table` check `allowed_table_names_for(principal)` before serving a response and return a `"Table X is restricted (caller: 'Y')."` message for inaccessible tables. The remaining seven tools are unchanged — `inspect_query` and `run_query` inherit principal gating through the underlying `Validator`.

Tools are returned as Claude Agent SDK `@tool`-decorated async functions. Each tool accepts `args: dict` and returns `{"content": [{"type": "text", "text": ...}]}`. The caller bundles them into an MCP server:

```python
from claude_agent_sdk import create_sdk_mcp_server, ClaudeAgentOptions

server = create_sdk_mcp_server(name="data-contracts", version="1.0.0", tools=tools)
user_prompt = "You are an analytics assistant for Acme Corp."
system_prompt = f"{user_prompt}\n\n{dc.to_system_prompt()}"

options = ClaudeAgentOptions(
    model="claude-sonnet-4-6",
    system_prompt=system_prompt,
    mcp_servers={"dc": server},
    allowed_tools=[f"mcp__dc__{t.name}" for t in tools],
)
```

### Middleware

```python
from agentic_data_contracts import contract_middleware

@contract_middleware(contract, adapter=adapter)
async def my_custom_query_tool(args: dict) -> dict:
    """Existing query tool with custom logic."""
    result = await my_database.execute(args["sql"])
    return {"content": [{"type": "text", "text": str(result)}]}

# Middleware: intercept sql → validate → block/warn → call wrapped → track session
# Returns a @tool-decorated async function compatible with create_sdk_mcp_server()
```

### Graceful Degradation Without Adapter

| Tool | Without adapter |
|---|---|
| `describe_table`, `preview_table`, `list_metrics`, `lookup_metric`, `lookup_domain`, `lookup_relationships`, `trace_metric_impacts` | Fully functional (contract + semantic source) |
| `run_query` | Fully functional when database adapter is configured |
| `inspect_query` | Layer 1 always runs; EXPLAIN fields populated when adapter is configured |

## Semantic Layer

Reads external semantic definitions so the agent knows *how* metrics are defined.

```python
class SemanticSource(Protocol):
    def get_metrics(self) -> list[MetricDefinition]: ...
    def get_metric(self, name: str) -> MetricDefinition | None: ...
    def get_table_schema(self, schema: str, table: str) -> TableSchema | None: ...
    def search_metrics(self, query: str) -> list[MetricDefinition]: ...
    def get_relationships(self) -> list[Relationship]: ...
    def get_relationships_for_table(self, table: str) -> list[Relationship]: ...
    def get_metric_impacts(self) -> list[MetricImpact]: ...
```

**Fuzzy metric search:** When `lookup_metric` receives a query that doesn't exactly match a metric name, it falls back to `search_metrics()` which uses `thefuzz` (`token_set_ratio` scorer, cutoff 50) to find the best matches by name + description. A shared `fuzzy_search_metrics()` helper in `base.py` provides this logic for all source implementations.

**Metric-impact graph (v0.10.0+):** `get_metric_impacts()` returns directed edges between metrics annotated with `direction`, `confidence`, and `evidence`. The `build_metric_impact_index()` / `walk_metric_impacts()` helpers in `base.py` mirror the `build_relationship_index` / `find_join_path` pattern — dual-keyed index (each edge under both endpoints), cycle-safe BFS traversal, direction disambiguated at walk time. `YamlSource` parses a top-level `metric_impacts:` block; `DbtSource` and `CubeSource` return `[]` (neither system has a native causal-graph concept — impacts live in the contract YAML regardless of where the metric itself comes from).

**Built-in sources:**

| Source | Reads | Extracts |
|---|---|---|
| `DbtSource` | `manifest.json` | Metrics (+ `meta.tier` / `meta.indicator_kind` / `meta.domains`), models, columns |
| `CubeSource` | Cube meta API or schema files | Metrics (+ `meta.tier` / `meta.indicator_kind` / `meta.domains`), dimensions |
| `YamlSource` | Inline YAML definitions | Metric / table / relationship / `metric_impacts` definitions for teams not using dbt/Cube |

`MetricDefinition`: `name`, `description`, `sql_expression`, `source_model`, `filters`, `domains`, `tier`, `indicator_kind`.
`MetricImpact`: `from_metric`, `to_metric`, `direction`, `confidence`, `evidence`, `description`.
`Relationship`: `from_`, `to`, `type`, `description`, `required_filter`.
`TableSchema`: `columns: list[Column]` with name, type, description.

## Database Adapters

```python
class DatabaseAdapter(Protocol):
    def execute(self, sql: str) -> QueryResult: ...
    def explain(self, sql: str) -> ExplainResult: ...
    def describe_table(self, schema: str, table: str) -> TableSchema: ...
    @property
    def dialect(self) -> str: ...  # "bigquery", "snowflake", "postgres", "duckdb"

class SqlNormalizer(Protocol):
    def normalize_sql(self, sql: str) -> str: ...
```

### SQL Normalization for Non-Standard Dialects

Adapters for databases with proprietary SQL extensions (Denodo VQL, Teradata, ClickHouse) can implement `SqlNormalizer` alongside `DatabaseAdapter`. The `Validator` calls `normalize_sql()` before `sqlglot.parse_one()` to rewrite non-standard syntax into a form sqlglot can parse. The original SQL is preserved for `execute()` and `explain()`.

Detection is automatic: `create_tools()` and `contract_middleware()` check `isinstance(adapter, SqlNormalizer)` and wire it into the `Validator` if present. Standard-dialect adapters are unaffected.

**`describe_table` maps to native commands:**

| Database | Command | What you get |
|---|---|---|
| BigQuery | `INFORMATION_SCHEMA.COLUMNS` or `get_table()` | Column names, types, descriptions, partitioning |
| Snowflake | `DESCRIBE TABLE` | Column names, types, nullable, default, comments |
| Postgres | `information_schema.columns` | Column names, types, nullable, defaults, comments |
| DuckDB | `DESCRIBE` or `information_schema.columns` | Column names, types |

Table schemas are cached for the lifetime of a `ContractSession` to avoid repeated round-trips.

Built-in adapters are optional extras:

```toml
[project.optional-dependencies]
bigquery = ["google-cloud-bigquery"]
snowflake = ["snowflake-connector-python"]
postgres = ["psycopg2-binary"]
duckdb = ["duckdb"]
```

## Bridge Layer (Optional `ai-agent-contracts` Integration)

When `ai-agent-contracts` is installed, the bridge upgrades from lightweight enforcement to the formal system.

```python
from agentic_data_contracts.bridge import compile_to_contract

contract_obj = compile_to_contract(data_contract)
# Returns: Contract(I, O, S, R, T, Phi, Psi)
```

### Compilation Mapping

| DataContract field | Compiles to |
|---|---|
| `semantic.rules` (block) | `TerminationCondition` |
| `semantic.rules` (warn) | `SuccessCriterion` (low weight) |
| `semantic.rules` (log) | `Contract.metadata` |
| `resources.*` | `ResourceConstraints` |
| `temporal.*` | `TemporalConstraints` |
| `success_criteria` | `list[SuccessCriterion]` with weights |
| `semantic.source` + `allowed_tables` | `Capabilities.instructions` |

### What Changes at Runtime

| Concern | Without ai-agent-contracts | With ai-agent-contracts |
|---|---|---|
| Retry/token/duration tracking | `ContractSession` counters | `ResourceConstraints` formal enforcement |
| Block rule violation | `ContractViolation` exception | `TerminationCondition` triggers agent stop |
| Warn rule violation | Warning in tool result | `SuccessCriterion` penalty |
| Success evaluation | Manual / log-based | Formal `SuccessCriterion` with weights, supports LLM judge |
| Integration with LangChain, LiteLLM | Not available | Full `Contract` works with all existing integrations |

### Detection Is Automatic

```python
try:
    from agent_contracts import Contract
    AGENT_CONTRACTS_AVAILABLE = True
except ImportError:
    AGENT_CONTRACTS_AVAILABLE = False
```

If `ai-agent-contracts` is installed, `ContractSession` automatically uses formal enforcement. Tools behave the same from the agent's perspective.

## Module Structure

```
agentic-data-contracts/
├── src/agentic_data_contracts/
│   ├── __init__.py              # Public API: DataContract, create_tools, contract_middleware
│   ├── core/
│   │   ├── __init__.py
│   │   ├── schema.py            # Pydantic models for YAML validation
│   │   ├── contract.py          # DataContract class (load, to_system_prompt)
│   │   └── session.py           # ContractSession (lightweight enforcement)
│   ├── validation/
│   │   ├── __init__.py
│   │   ├── validator.py         # Orchestrates checkers, aggregates results
│   │   ├── checkers.py          # Built-in checkers (7 query checkers + ResultCheckRunner)
│   │   └── explain.py           # EXPLAIN adapter orchestration
│   ├── tools/
│   │   ├── __init__.py
│   │   ├── factory.py           # create_tools() — returns 9 tools
│   │   └── middleware.py        # contract_middleware decorator
│   ├── semantic/
│   │   ├── __init__.py
│   │   ├── base.py              # SemanticSource protocol
│   │   ├── dbt.py               # DbtSource
│   │   ├── cube.py              # CubeSource
│   │   └── yaml_source.py       # YamlSource
│   ├── adapters/
│   │   ├── __init__.py
│   │   ├── _normalizer.py       # SqlNormalizer protocol (avoids circular import)
│   │   ├── base.py              # DatabaseAdapter protocol + SqlNormalizer re-export
│   │   ├── bigquery.py          # BigQuery adapter
│   │   ├── snowflake.py         # Snowflake adapter
│   │   ├── postgres.py          # Postgres adapter
│   │   └── duckdb.py            # DuckDB adapter
│   └── bridge/
│       ├── __init__.py
│       └── compiler.py          # DataContract → ai-agent-contracts Contract
├── tests/
│   ├── test_core/
│   ├── test_validation/
│   ├── test_tools/
│   ├── test_semantic/
│   ├── test_adapters/
│   ├── test_bridge/
│   └── fixtures/
│       ├── valid_contract.yml
│       ├── minimal_contract.yml
│       └── sample_dbt_manifest.json
├── examples/
│   └── revenue_agent/
│       ├── contract.yml
│       └── agent.py             # Claude Agent SDK example
├── pyproject.toml
└── README.md
```

## Dependencies

```toml
[project]
dependencies = [
    "sqlglot>=23.0",
    "pydantic>=2.0",
    "pyyaml>=6.0",
]

[project.optional-dependencies]
agent-sdk = ["claude-agent-sdk"]
agent-contracts = ["ai-agent-contracts>=0.1.0"]
bigquery = ["google-cloud-bigquery"]
snowflake = ["snowflake-connector-python"]
postgres = ["psycopg2-binary"]
duckdb = ["duckdb"]
all = [
    "agentic-data-contracts[agent-sdk,agent-contracts,bigquery,snowflake,postgres,duckdb]",
]
```

## Testing Strategy

Six test suites matching the layers:

| Suite | What it tests | Extra dependencies |
|---|---|---|
| `test_core/` | YAML loading, Pydantic validation, ContractSession counters | None |
| `test_validation/` | All 4 checkers, validator orchestration, multi-dialect SQL | None (sqlglot) |
| `test_tools/` | Tool factory, middleware, graceful degradation | None |
| `test_semantic/` | DbtSource parses manifest, YamlSource loads inline defs | None |
| `test_adapters/` | Adapter protocol compliance, DuckDB integration tests | DuckDB |
| `test_bridge/` | Compilation mapping, formal enforcement | ai-agent-contracts |

DuckDB for integration tests — zero setup, runs in CI without credentials.

## End-to-End Example

```python
# examples/revenue_agent/agent.py
import asyncio
from agentic_data_contracts import DataContract, create_tools
from agentic_data_contracts.adapters.duckdb import DuckDBAdapter
from claude_agent_sdk import (
    query, ClaudeAgentOptions, create_sdk_mcp_server,
    AssistantMessage, TextBlock,
)

dc = DataContract.from_yaml("contract.yml")
adapter = DuckDBAdapter("sample_data.duckdb")

# Create contract-aware tools and bundle into MCP server
sdk_tools = create_tools(dc, adapter=adapter)
server = create_sdk_mcp_server(
    name="data-contracts", version="1.0.0", tools=sdk_tools
)

# User's own system prompt + contract rules appended
user_prompt = """You are a revenue analytics assistant for Acme Corp.
Always be concise and include methodology notes in your answers."""

options = ClaudeAgentOptions(
    model="claude-sonnet-4-6",
    system_prompt=f"{user_prompt}\n\n{dc.to_system_prompt()}",
    mcp_servers={"dc": server},
    allowed_tools=[f"mcp__dc__{t.name}" for t in sdk_tools],
)

async def main():
    async for message in query(
        prompt="What was total revenue by region in Q1 2025?",
        options=options,
    ):
        if isinstance(message, AssistantMessage):
            for block in message.content:
                if isinstance(block, TextBlock):
                    print(block.text)

asyncio.run(main())
```

**Runtime behavior:**
```
Agent: "SELECT * FROM analytics.orders"
  -> BLOCKED (no_select_star)

Agent: "SELECT order_id, amount FROM analytics.orders"
  -> BLOCKED (tenant_isolation — missing WHERE tenant_id = ?)

Agent: "SELECT order_id, amount FROM analytics.orders WHERE tenant_id = 'acme'"
  -> PASSED + WARN (consider using semantic revenue definition)

Agent: "SELECT order_id, amount FROM raw.payments WHERE tenant_id = 'acme'"
  -> BLOCKED (raw.payments not in allowed_tables)
```

The example ships with a DuckDB setup script so users can run immediately:
```bash
uv run python examples/revenue_agent/agent.py "What was Q1 revenue by region?"
```

## Future Extensions (Out of Scope for v1)

- CLI tool: `agentic-data-contracts validate contract.yml`
- Claude Code MCP server wrapping the tool set
- dbt plugin: auto-generate contracts from `manifest.json`
- Compliance dashboard / audit reporting
- Contract versioning and migration
- **Principal-aware system prompt rendering** — `to_system_prompt()` currently lists all declared tables regardless of caller. An agent serving Bob may be told about tables Bob can't query. Query-time gating remains authoritative (denied queries never reach the database), but UX could be improved by filtering the rendered prompt to only include tables accessible to the current principal. File an issue if your deployment needs this.
