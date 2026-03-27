# Agentic Data Contracts v2 — Design Spec

**Date:** 2026-03-27
**Status:** Draft
**Author:** Qing Ye + Claude
**Supersedes:** 2026-03-26-agentic-data-contracts-design.md

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
| Tool surface | Validator callback | 10 agent tools (factory + middleware) |

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
 │ create_tools()        │  10 agent tools
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
    path: "./dbt/manifest.json"        # local path or URL

  # What the agent is allowed to access
  allowed_tables:
    - schema: analytics
      tables: [orders, customers, subscriptions]
    - schema: raw
      tables: []                       # empty = nothing from this schema

  # What the agent must NOT do
  forbidden_operations: [DELETE, DROP, TRUNCATE, UPDATE, INSERT]

  # Governance rules (per-rule enforcement)
  rules:
    - name: tenant_isolation
      description: "All queries must include a WHERE tenant_id = filter"
      enforcement: block               # block | warn | log

    - name: use_approved_metrics
      description: "Revenue calculations must use the semantic layer definition"
      enforcement: warn

    - name: no_select_star
      description: "Queries must specify explicit columns, no SELECT *"
      enforcement: block

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

# Generate system prompt for the agent
system_prompt = dc.to_system_prompt()
# Lists allowed tables, forbidden operations, active rules, semantic guidance
```

### ContractSession (Lightweight Enforcement)

When `ai-agent-contracts` is NOT installed, `ContractSession` provides self-contained enforcement:

- **Retry count** — incremented on each failed query attempt, checked against `max_retries`
- **Token usage** — tracked via callback, checked against `token_budget`
- **Wall-clock duration** — start time recorded, checked against `max_duration_seconds`
- **Cost estimate** — if EXPLAIN adapter returns cost info, checked against `cost_limit_usd`

These are simple counters/timers with guard checks before each tool call. No formal state machine.

When `ai-agent-contracts` IS installed, enforcement is delegated to the formal framework via the bridge layer (see below).

## Validation Layer

Two-layer validation architecture. Dependencies: `sqlglot`.

### Layer 1: Static Validation (always available)

```python
class Checker(Protocol):
    def check(self, parsed_sql: Expression, contract: DataContract) -> CheckResult: ...
```

**Built-in checkers:**

| Checker | What it validates |
|---|---|
| `TableAllowlistChecker` | All referenced tables are in `allowed_tables` |
| `OperationBlocklistChecker` | No forbidden SQL operations (DELETE, DROP, etc.) |
| `RequiredFilterChecker` | Required WHERE clauses present (e.g., `tenant_id`) |
| `NoSelectStarChecker` | No `SELECT *` statements |

`CheckResult` contains: `passed: bool`, `severity: block | warn | log`, `message: str`.

The validator runs all applicable checkers and aggregates results — any `block` result stops execution, `warn` results are surfaced to the agent, `log` results are recorded silently.

Rules that cannot be statically checked (e.g., "use semantic layer definition for revenue") become:
- An instruction injected into the agent's context via `to_system_prompt()`
- A post-hoc `SuccessCriterion` for evaluation by LLM judge or human review

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

### Validation Flow

```
SQL string
  → sqlglot.parse(sql, dialect=contract.dialect)
  → Layer 1: run all checkers
  → any block? → return ValidationResult(blocked=True, reasons=[...])
  → Layer 2 available? → explain adapter
  → cost/rows exceed limits? → return ValidationResult(blocked=True, reasons=[...])
  → return ValidationResult(blocked=False, warnings=[...])
```

## Tools Layer (Claude Agent SDK Integration)

Two modes: tool factory for quick starts, middleware for BYO tools.

### 10 Tools in Three Categories

#### Discovery tools (understand what's available)

1. **`list_schemas()`** — Allowed schemas from contract
2. **`list_tables(schema?)`** — Allowed tables with column summary
3. **`describe_table(schema, table)`** — Full column details from database (name, type, description, partitioning)
4. **`preview_table(schema, table, limit=5)`** — Sample rows from a table
5. **`list_metrics()`** — All metrics from semantic source
6. **`lookup_metric(metric_name)`** — Specific metric definition + SQL formula

#### Execution tools (query with governance)

7. **`validate_query(sql)`** — Static + EXPLAIN check, no execution
8. **`query_cost_estimate(sql)`** — Estimated cost/rows (Layer 2 only)
9. **`run_query(sql)`** — Validate → execute → return results

#### Meta tool (self-awareness)

10. **`get_contract_info()`** — Active rules, limits, remaining budget, retries left, elapsed time

### Natural Agent Workflow

```
list_schemas → list_tables → describe_table → preview_table
    → lookup_metric (if needed)
    → write SQL → validate_query → query_cost_estimate
    → run_query
    → get_contract_info (check remaining budget)
```

### Tool Factory

```python
from agentic_data_contracts import DataContract, create_tools
from agentic_data_contracts.adapters.duckdb import DuckDBAdapter

dc = DataContract.from_yaml("contract.yml")
adapter = DuckDBAdapter("analytics.duckdb")
tools = create_tools(dc, adapter=adapter)
# Returns all 10 tools as plain Python functions
```

### Middleware

```python
from agentic_data_contracts import contract_middleware

@contract_middleware(contract, adapter=adapter)
def my_custom_query_tool(sql: str) -> dict:
    """Existing query tool with custom logic."""
    return my_database.execute(sql)

# Middleware: intercept sql → validate → block/warn → call wrapped → track session
```

### Graceful Degradation Without Adapter

| Tool | Without adapter |
|---|---|
| `list_schemas`, `list_tables`, `list_metrics`, `lookup_metric` | Fully functional (contract + semantic source) |
| `validate_query`, `get_contract_info` | Fully functional |
| `describe_table`, `preview_table`, `run_query` | Unavailable (clear error message) |
| `query_cost_estimate` | Returns "unavailable without database adapter" |

## Semantic Layer

Reads external semantic definitions so the agent knows *how* metrics are defined.

```python
class SemanticSource(Protocol):
    def get_metrics(self) -> list[MetricDefinition]: ...
    def get_metric(self, name: str) -> MetricDefinition | None: ...
    def get_table_schema(self, schema: str, table: str) -> TableSchema | None: ...
```

**Built-in sources:**

| Source | Reads | Extracts |
|---|---|---|
| `DbtSource` | `manifest.json` | Metrics, models, columns |
| `CubeSource` | Cube meta API or schema files | Metrics, dimensions |
| `YamlSource` | Inline YAML definitions | Simple metric/table definitions for teams not using dbt/Cube |

`MetricDefinition`: `name`, `description`, `sql_expression`, `source_model`, `filters`.
`TableSchema`: `columns: list[Column]` with name, type, description.

## Database Adapters

```python
class DatabaseAdapter(Protocol):
    def execute(self, sql: str) -> QueryResult: ...
    def explain(self, sql: str) -> ExplainResult: ...
    def describe_table(self, schema: str, table: str) -> TableSchema: ...
    @property
    def dialect(self) -> str: ...  # "bigquery", "snowflake", "postgres", "duckdb"
```

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
│   │   ├── checkers.py          # Built-in checkers (4 checkers)
│   │   └── explain.py           # EXPLAIN adapter orchestration
│   ├── tools/
│   │   ├── __init__.py
│   │   ├── factory.py           # create_tools() — returns 10 tools
│   │   └── middleware.py        # contract_middleware decorator
│   ├── semantic/
│   │   ├── __init__.py
│   │   ├── base.py              # SemanticSource protocol
│   │   ├── dbt.py               # DbtSource
│   │   ├── cube.py              # CubeSource
│   │   └── yaml_source.py       # YamlSource
│   ├── adapters/
│   │   ├── __init__.py
│   │   ├── base.py              # DatabaseAdapter protocol
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
from agentic_data_contracts import DataContract, create_tools
from agentic_data_contracts.adapters.duckdb import DuckDBAdapter
from claude_agent_sdk import Agent

dc = DataContract.from_yaml("contract.yml")
adapter = DuckDBAdapter("sample_data.duckdb")
tools = create_tools(dc, adapter=adapter)

agent = Agent(
    model="claude-sonnet-4-6",
    tools=tools,
    instructions=dc.to_system_prompt(),
)

result = agent.run("What was total revenue by region in Q1 2025?")
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
