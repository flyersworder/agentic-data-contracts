# Agentic Data Contracts — Design Spec

**Date:** 2026-03-26
**Status:** Draft
**Author:** Qing Ye + Claude

## Problem Statement

The `agent-contracts` framework provides formal resource governance for autonomous AI agents, but it lacks an accessible entry point for new users. Data/analytics engineers face two severe problems with AI agents querying their data:

1. **Resource runaway** — agents burn unbounded compute, loop endlessly on retries, exceed cost ceilings
2. **Semantic inconsistency** — agents compute metrics differently across runs, query wrong tables, ignore established definitions

No single existing tool addresses both. Semantic layers (dbt metrics, Cube) handle consistency but not resource governance. Agent frameworks (LangChain, LangGraph) provide execution but not data-specific governance.

**Inspiration:** Robert Yi's LinkedIn post on "agentic contract layers" for analytics — arguing that agents need a central authority governing how data logic is consumed. Our framework addresses the complementary resource dimension; combining both creates a complete governance solution.

## Design Decisions

| Decision | Choice | Rationale |
|---|---|---|
| Target user | Data/analytics engineer | Feels the pain most, already thinks in contracts (dbt, schema tests) |
| Database support | Agnostic | Contract layer, not a database integration |
| Semantic governance | Reference-based | Point to external source of truth (dbt, Cube), don't replicate it |
| Developer experience | YAML-first | Data engineers live in YAML; zero Python knowledge required to define a contract |
| Enforcement | Configurable per-rule | `block` / `warn` / `log` per rule; matches existing strict/lenient pattern |
| Repository | Separate repo (`agentic-data-contracts`) | Different audience, different release cadence; validates core API extensibility |

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
          │  .compile()
          ▼
 ┌─────────────────┐
 │ Contract         │  Existing formal 7-tuple (C = I, O, S, R, T, Phi, Psi)
 │   .capabilities  │  instructions built from semantic rules
 │   .resources     │  ResourceConstraints (direct mapping)
 │   .temporal      │  TemporalConstraints (direct mapping)
 │   .success       │  SuccessCriteria from success_criteria
 │   .termination   │  TerminationConditions from block rules
 │   .metadata      │  source_of_truth path, allowed_tables, etc.
 └─────────────────┘
          │
          ▼
  Works with ALL existing integrations
  (LiteLLM, LangChain, LangGraph, Google ADK)
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

  # Optional: database connection for Layer 2 (EXPLAIN) validation
  # connection:
  #   type: bigquery                    # bigquery | snowflake | postgres | duckdb
  #   project: my-project
  #   dataset: analytics

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

# Resource governance (maps to existing ResourceConstraints)
resources:
  cost_limit_usd: 5.00
  max_query_time_seconds: 30
  max_retries: 3
  max_rows_scanned: 1_000_000
  token_budget: 50_000

# Time governance (maps to existing TemporalConstraints)
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

### Compilation Mapping

| YAML field | Compiles to | How |
|---|---|---|
| `semantic.source` | `Contract.metadata["source_of_truth"]` + `Capabilities.instructions` | Instruction: "Consult {path} for metric definitions" |
| `semantic.allowed_tables` | `Capabilities.instructions` + runtime validation | Instruction listing permitted tables; validator checks SQL |
| `semantic.forbidden_operations` | `Capabilities.instructions` + runtime validation | Instruction + validator rejects forbidden SQL keywords |
| `semantic.rules` (enforcement: block) | `TerminationCondition` | Violation triggers agent termination |
| `semantic.rules` (enforcement: warn) | `SuccessCriterion` (low weight) | Violation logged, doesn't stop execution |
| `semantic.rules` (enforcement: log) | `Contract.metadata` + callback | Recorded for audit only |
| `resources.cost_limit_usd` | `ResourceConstraints.cost_usd` | Direct field mapping |
| `resources.token_budget` | `ResourceConstraints.tokens` | Direct field mapping |
| `resources.max_retries` | `ResourceConstraints.iterations` | Direct field mapping |
| `resources.max_query_time_seconds` | `Contract.metadata` + Layer 2 validation | New data-specific field; enforced via EXPLAIN dry-run |
| `resources.max_rows_scanned` | `Contract.metadata` + Layer 2 validation | New data-specific field; enforced via EXPLAIN dry-run |
| `temporal.*` | `TemporalConstraints` | Direct field mapping |
| `success_criteria` | `list[SuccessCriterion]` | Direct mapping with weights |

### Runtime Validation

Two-layer validation architecture:

```
Agent generates SQL
        │
        ▼
   ┌─────────┐
   │ sqlglot  │  Layer 1: always runs (no DB needed)
   │ (static) │  checks: allowed tables, forbidden ops, required filters
   └────┬─────┘
        │ passes?
        ▼
   ┌──────────┐
   │ EXPLAIN   │  Layer 2: optional (if DB connection available)
   │ (live)    │  checks: schema validity, cost estimate vs budget
   └──────────┘
```

**Layer 1 (sqlglot):** Pure-Python SQL parser supporting every major dialect (BigQuery, Snowflake, Postgres, DuckDB). Always available, zero network dependency. Catches contract violations before touching the database.

**Layer 2 (EXPLAIN):** Optional enhancement when a database connection is configured. Dry-runs the query (BigQuery `--dry_run`, Snowflake `EXPLAIN`, Postgres `EXPLAIN` without `ANALYZE`) to get cost estimates and schema validation. Enables pre-execution enforcement of `max_rows_scanned` and `cost_limit_usd`.

Optional connection config in YAML:
```yaml
semantic:
  connection:                         # optional, enables Layer 2
    type: bigquery                    # bigquery | snowflake | postgres | duckdb
    project: my-project
    dataset: analytics
```

**Built-in rule checkers:**

| Checker | What it validates |
|---|---|
| `TableAllowlistChecker` | All referenced tables are in `allowed_tables` |
| `OperationBlocklistChecker` | No forbidden SQL operations (DELETE, DROP, etc.) |
| `RequiredFilterChecker` | Required WHERE clauses present (e.g., `tenant_id`) |
| `NoSelectStarChecker` | No `SELECT *` statements |

Rules that cannot be statically checked (e.g., "use semantic layer definition for revenue") become:
- An instruction injected into the agent's context via `Capabilities.instructions`
- A post-hoc `SuccessCriterion` for evaluation by LLM judge or human review

**The validator plugs into our existing `EnforcementCallback` system** — registered as a callback that intercepts tool calls containing SQL.

## Module Structure

Separate repository: `agentic-data-contracts`

```
agentic-data-contracts/
├── src/agentic_data_contracts/
│   ├── __init__.py                # Public API exports
│   ├── schema.py                  # Pydantic models for YAML validation
│   ├── compiler.py                # DataContract -> Contract compilation
│   ├── validator.py               # Runtime SQL validation (sqlglot)
│   ├── explain.py                 # Optional Layer 2 (EXPLAIN adapters)
│   └── checkers.py                # Built-in rule checkers
├── tests/
│   ├── test_schema.py
│   ├── test_compiler.py
│   ├── test_validator.py
│   ├── test_checkers.py
│   ├── test_explain.py
│   └── fixtures/
│       ├── valid_contract.yml
│       ├── minimal_contract.yml
│       └── ...
├── examples/
│   └── revenue_agent/
│       ├── revenue_agent_contract.yml
│       └── run_agent.py
├── pyproject.toml
└── README.md
```

### Public API

```python
from agentic_data_contracts import DataContract

# Load from YAML
dc = DataContract.from_yaml("data_contract.yml")

# Compile to standard Contract
contract = dc.compile()

# Get the validator (for runtime SQL checking)
validator = dc.create_validator()

# Use with any existing agent-contracts integration
from agent_contracts.integrations.litellm_wrapper import ContractedLLM
llm = ContractedLLM(contract=contract, callbacks=[validator])
```

### Dependencies

```toml
# pyproject.toml
[project]
dependencies = [
    "agent-contracts>=0.1.0",
    "sqlglot>=23.0",
    "pydantic>=2.0",
    "pyyaml>=6.0",
]

[project.optional-dependencies]
bigquery = ["google-cloud-bigquery"]
snowflake = ["snowflake-connector-python"]
postgres = ["psycopg2-binary"]
```

## Testing Strategy

Three test layers matching the architecture:

### Layer 1: Schema Validation
- Valid full contract parses correctly
- Minimal contract (name + one rule) works
- Invalid YAML rejected with clear error messages
- Unknown fields rejected (strict mode)
- Enforcement values validated (`block` / `warn` / `log` only)

### Layer 2: Compilation
- `enforcement: block` rules produce `TerminationCondition`
- `enforcement: warn` rules produce `SuccessCriterion`
- `enforcement: log` rules populate `metadata` only
- `resources` fields map to `ResourceConstraints` correctly
- `temporal` fields map to `TemporalConstraints` correctly
- `semantic.source` generates correct `Capabilities.instructions`
- Compiled `Contract` is valid and usable with existing integrations

### Layer 3: Runtime Validation
- Query referencing allowed table passes
- Query referencing forbidden table blocked/warned per rule
- `DELETE`/`DROP` statements blocked by `forbidden_operations`
- `SELECT *` caught by `NoSelectStarChecker`
- Missing required filter caught by `RequiredFilterChecker`
- Complex SQL (subqueries, CTEs, joins) — correct table extraction
- Multi-dialect support (BigQuery, Postgres, Snowflake syntax)

### Not tested
- Actual database connections (EXPLAIN adapters mocked)
- LLM behavior (whether agent follows instructions is integration-level)

## End-to-End Example

**Scenario:** AI agent answers revenue questions, governed by a data contract.

**Contract (data engineer writes):**
```yaml
# revenue_agent_contract.yml
version: "1.0"
name: revenue-analysis

semantic:
  source:
    type: dbt
    path: "./dbt/manifest.json"
  allowed_tables:
    - schema: analytics
      tables: [orders, customers, subscriptions]
  forbidden_operations: [DELETE, DROP, TRUNCATE, UPDATE, INSERT]
  rules:
    - name: tenant_isolation
      description: "All queries must filter by tenant_id"
      enforcement: block
    - name: use_semantic_revenue
      description: "Revenue must use the dbt metric definition"
      enforcement: warn
    - name: no_select_star
      description: "Must specify explicit columns"
      enforcement: block

resources:
  cost_limit_usd: 5.00
  token_budget: 50_000
  max_retries: 3

temporal:
  max_duration_seconds: 300
```

**Usage (ML engineer writes):**
```python
from agentic_data_contracts import DataContract
from agent_contracts.integrations.litellm_wrapper import ContractedLLM

dc = DataContract.from_yaml("revenue_agent_contract.yml")
contract = dc.compile()
validator = dc.create_validator()

llm = ContractedLLM(contract=contract, callbacks=[validator])
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

## Prerequisites

Before building this repo, `agent-contracts` must be published to PyPI:
1. Add root-level `LICENSE` file
2. Consider switching from CC-BY-4.0 to MIT/Apache-2.0 (CC-BY-4.0 is not OSI-approved for software)
3. `uv build && uv publish`

## Future Extensions (Out of Scope)

- CLI tool: `agentic-data-contracts validate contract.yml`
- dbt plugin: auto-generate contracts from `manifest.json`
- Compliance dashboard / audit reporting
- Contract versioning and migration
