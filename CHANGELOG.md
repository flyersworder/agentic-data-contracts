# Changelog

All notable changes to this project will be documented in this file.

## [0.3.0] - 2026-03-30

### Added

- **`PromptRenderer` protocol**: New `@runtime_checkable` protocol for custom system prompt formatting. Users can implement `render(contract, semantic_source) -> str` to control how contracts are presented to their model of choice.
- **`ClaudePromptRenderer`**: Built-in XML-structured renderer optimized for Claude models (Sonnet 4.6+). Uses XML tags for structural boundaries, places constraints at the end for better instruction-following, and merges resource/temporal limits into a single section.
- **Custom renderer support**: `to_system_prompt(renderer=MyRenderer())` delegates entirely to a user-provided renderer.
- **Top-level exports**: `from agentic_data_contracts import PromptRenderer, ClaudePromptRenderer`

### Changed

- **Default system prompt format**: `to_system_prompt()` now generates XML output (was Markdown). Pass a custom renderer if you need a different format.
- **`contract.py` simplified**: `to_system_prompt()` is now a thin delegate (~7 lines). All prompt-building logic moved to `core/prompt.py`.

## [0.2.6] - 2026-03-29

### Changed

- **Compact system prompt at scale**: When metrics exceed 20, the system prompt shows domain names with counts (e.g., "acquisition (45)") instead of listing every metric. Reduces prompt from ~6K to ~100 tokens for large metric sets.
- **Paginated `list_tables`**: Added `limit` (default 50) and `offset` parameters for handling schemas with many tables. Response includes `total` count and `next_offset` for pagination.
- **Cached wildcard resolution**: `resolve_tables()` is now idempotent — subsequent calls are no-ops, avoiding redundant database queries.

## [0.2.5] - 2026-03-29

### Added

- **Table relationship metadata**: `Relationship` dataclass and `get_relationships()` on `SemanticSource` protocol for declaring join paths between tables (from/to column + relationship type)
- **Relationships in system prompt**: `to_system_prompt()` includes join paths so the agent knows how to combine tables correctly
- **YamlSource relationships**: Parsed from `relationships` section in semantic YAML files
- DbtSource and CubeSource return empty relationships (ready for future parsing of native join metadata)

## [0.2.4] - 2026-03-29

### Added

- **Wildcard table support**: Use `tables: ["*"]` in `allowed_tables` to allow all tables in a schema, discovered from the database at runtime via `adapter.list_tables()`
- **`DataContract.resolve_tables(adapter)`**: Expands wildcard entries using the database adapter; called automatically by `create_tools()` when an adapter is provided
- **`DatabaseAdapter.list_tables(schema)`**: New protocol method for listing tables in a schema; implemented in `DuckDBAdapter` via `information_schema.tables`

## [0.2.3] - 2026-03-29

### Added

- **SDK MCP server convenience method**: `create_sdk_mcp_server(contract, adapter=...)` wraps all 10 tools with the SDK's `@tool` decorator and bundles them into a ready-to-use MCP server for `ClaudeAgentOptions.mcp_servers`
- **Top-level export**: `from agentic_data_contracts import create_sdk_mcp_server`

### Changed

- **SDK dependency**: Bumped `claude-agent-sdk` minimum to `>=0.1.52`

## [0.2.2] - 2026-03-28

### Added

- **SDK config generation**: `DataContract.to_sdk_config()` maps contract limits to Claude Agent SDK options (`token_budget` → `task_budget`, `max_retries` → `max_turns`)

## [0.2.1] - 2026-03-28

### Added

- **Auto-load semantic source**: `DataContract.load_semantic_source()` reads `source.type` and `source.path` from the contract YAML and instantiates the correct `SemanticSource` (YamlSource, DbtSource, or CubeSource)
- **Zero-config tools**: `create_tools()` auto-loads the semantic source from contract config when none is explicitly passed

## [0.2.0] - 2026-03-28

### Added

- **Scalable semantic discovery**: `domains` field in contract YAML for grouping metrics by business domain (e.g., acquisition, retention, attribution)
- **Fuzzy metric search**: `lookup_metric` now falls back to fuzzy matching via `thefuzz` when no exact match is found, returning ranked candidates
- **Domain-filtered list_metrics**: `list_metrics` tool accepts optional `domain` parameter to filter metrics by domain
- **Metrics in system prompt**: `to_system_prompt()` accepts an optional `SemanticSource` and renders a compact metric index (names + descriptions, grouped by domain)
- **`search_metrics()` protocol method**: Added to `SemanticSource` with shared `fuzzy_search_metrics()` helper using `thefuzz` `token_set_ratio` scorer
- **`thefuzz`** added as core dependency (backed by `rapidfuzz` C++ for performance)

### Fixed

- **EXPLAIN integration**: Validator pipeline now enforces `cost_limit_usd` and `max_rows_scanned` via Layer 2 EXPLAIN dry-run
- **`describe_table` allowlist check**: Tool now rejects tables not in the contract's allowed list
- **`filter_column` field**: Explicit column specification on `SemanticRule` for deterministic required filter detection
- **DuckDB row estimates**: EXPLAIN output parsed for `~N` cardinality estimates
- **TRUNCATE detection**: Fixed sqlglot `TruncateTable` type handling in `OperationBlocklistChecker`
- **Code quality**: CTE extraction O(n²)→O(n), `NoSelectStar` idiom, `preview_table` limit validation, public `Checker` protocol

## [0.1.0] - 2026-03-27

### Added

- **Core layer**: YAML-first data contract schema with Pydantic validation, `DataContract` class with YAML loading and system prompt generation, `ContractSession` for lightweight resource enforcement (retries, tokens, cost, duration)
- **Validation layer**: Four built-in SQL checkers via sqlglot (table allowlist, operation blocklist, required filters, no SELECT *), `Validator` orchestrator with two-layer pipeline (static checkers + optional EXPLAIN dry-run for cost/row enforcement)
- **Tools layer**: `create_tools()` factory producing 10 agent tools (list_schemas, list_tables, describe_table, preview_table, list_metrics, lookup_metric, validate_query, query_cost_estimate, run_query, get_contract_info), `contract_middleware` decorator for wrapping existing tools
- **Semantic layer**: `SemanticSource` protocol with three implementations — `YamlSource`, `DbtSource` (manifest.json), `CubeSource` (Cube schema YAML)
- **Database adapters**: `DatabaseAdapter` protocol with `DuckDB` implementation (execute, explain with row estimate parsing, describe_table)
- **Bridge layer**: Optional `ai-agent-contracts` integration via `compile_to_contract()` mapping YAML contracts to the formal 7-tuple Contract model
- **Example**: Revenue analysis agent with DuckDB, YAML semantic source, and Claude Agent SDK fallback demo mode
- **Developer tooling**: uv for dependency management, prek pre-commit hooks (ruff + ty), 124 tests
