# Changelog

All notable changes to this project will be documented in this file.

## [0.10.0] - 2026-04-17

### Added

- **Metric role metadata**: `MetricDefinition` gains three optional fields — `domains` (list), `tier` (list, e.g. `north_star` / `department_kpi` / `team_kpi`), and `indicator_kind` (`leading` / `lagging`). Lets the agent prioritize north-stars and verified leading indicators, and filter metrics by organizational role. All fields default to empty, so existing fixtures parse unchanged.
- **Metric-impact graph**: New `MetricImpact` dataclass captures directed, annotated edges between metrics — `from_metric`, `to_metric`, `direction` (`positive` / `negative`), `confidence` (`verified` / `correlated` / `hypothesized`), and free-text `evidence` the agent can cite verbatim. Declared via a top-level `metric_impacts:` block in the semantic YAML.
- **`trace_metric_impacts` tool**: New tool (13th) that walks the metric-impact graph via BFS from a starting metric. `direction="upstream"` returns drivers (for root-cause analyses like "why did revenue drop?"); `direction="downstream"` returns affected metrics (for "what does this KPI move?"). Each edge in the response carries direction, confidence, and evidence for grounded reasoning. `max_depth` is clamped to `[1, 10]` to prevent runaway walks.
- **`build_metric_impact_index()` and `walk_metric_impacts()` helpers**: Standalone functions in `semantic.base` mirroring the `build_relationship_index` / `find_join_path` pattern. Dual-keyed index (each edge stored under both endpoints); walker disambiguates direction at traversal time. Cycle-safe via visited tracking; self-loops are deduplicated by the index builder.
- **`get_metric_impacts()` on `SemanticSource` protocol**: New method returning `list[MetricImpact]`. `YamlSource` parses from the `metric_impacts:` block; `DbtSource` / `CubeSource` return `[]` — neither system has a native causal-graph concept, so impacts are declared in the contract YAML regardless of where metrics themselves come from.
- **Metric role metadata from dbt / Cube `meta`**: `DbtSource` and `CubeSource` now read `tier`, `indicator_kind`, and `domains` from each metric's `meta` dict. String values for `tier` / `domains` are coerced to single-element lists consistently across all three sources (YAML, dbt, Cube), so writing `tier: north_star` works the same as `tier: [north_star]`.
- **Metric-impact validation warnings**: `create_tools()` emits `logger.warning` at tool-creation time if any `metric_impacts` edge references an unknown metric name. Mirrors the existing domain-reference validation.

### Changed

- **Tool count**: Factory now produces 13 tools (was 12), adding `trace_metric_impacts`.
- **`lookup_metric` response shape**: Enriched with `domains`, `tier`, `indicator_kind`, `impacts` (outgoing edges), and `impacted_by` (incoming edges). Each edge is rendered as a one-line citation string (e.g. `"positive impact on total_revenue (verified): A/B test exp-042, +3.2% lift, p<0.01"`) the agent can quote verbatim. Fields are only included when non-empty, keeping responses compact for metrics with no impact data.
- **`list_metrics` filters**: Gains optional `tier` and `indicator_kind` arguments alongside the existing `domain` filter. Entries include `tier` and `indicator_kind` when set.
- **`list_metrics` domain semantics**: Domain filtering now uses a union of contract-side `Domain.metrics` and metric-side self-declared `metric.domains`. A metric that self-declares a domain is discoverable via the filter even if the contract's `Domain.metrics` list doesn't include it.
- **Factory tool descriptions**: `lookup_metric` and `list_metrics` descriptions now advertise the new fields and filters so the agent knows when to use them.

### Breaking

- **`SemanticSource` protocol extension**: The `@runtime_checkable` Protocol gains a required `get_metric_impacts()` method. Custom third-party `SemanticSource` implementations must add this method (returning `[]` is fine); without it, `isinstance(source, SemanticSource)` returns `False`. Built-in `YamlSource`, `DbtSource`, and `CubeSource` all implement it — no migration required for users who only use the built-in sources.

## [0.9.2] - 2026-04-15

### Fixed

- **Lazy session timer**: `ContractSession` no longer starts its wall-clock timer at construction. The timer now starts on the first `check_limits()` call, so idle time before the user's first interaction does not count against `temporal.max_duration_seconds`. This fixes premature "session expired" errors in long-lived agent setups (Chainlit, Webex bots) where the session object is created well before the first user message. (#16)

### Added

- **`ContractSession.reset_timer()`**: New method that resets the duration timer so it restarts on the next `check_limits()` call. Useful for frameworks with their own idle-timeout mechanisms that want to restart the clock on user activity.

## [0.9.1] - 2026-04-13

### Added

- **Schema `description` field**: Optional description on `AllowedTable` entries, surfaced via `list_schemas` to help agents understand what each schema contains and when to use it.
- **Schema `preferred` flag**: Optional boolean on `AllowedTable` (default `false`), surfaced via `list_schemas` to signal which schema the agent should prefer when similar tables exist across schemas.
- **Example improvements**: Revenue agent example updated with `lookup_domain` and `lookup_metric` demo steps, schema description/preferred in contract, and fixed pre-existing missing `query_check` blocks on `tenant_isolation` and `no_select_star` rules.
- **Domain-driven README**: README reframed around the domain-driven approach — agents understand business domains before writing SQL.

## [0.9.0] - 2026-04-13

### Added

- **First-class business domains**: `domains` redesigned from a flat `dict[str, list[str]]` to a list of `Domain` objects with `name`, `summary`, `description`, `metrics`, and optional `tables`. Domains now carry business context that helps agents understand what a domain means before querying.
- **`lookup_domain` tool**: New tool (12th) that returns full domain context — description, associated metrics with descriptions (enriched from semantic source), and tables. Supports fuzzy matching for domain names, consistent with `lookup_metric`.
- **Compact domain index in system prompt**: When domains are defined, the system prompt renders `<available_domains>` with domain name, summary, and metric count — progressive disclosure that keeps context compact while giving the agent enough to decide which domain to explore.
- **Domain validation warnings**: `create_tools()` now warns at tool creation time if a domain references metrics not found in the semantic source or tables not in `allowed_tables`.
- **Domain summaries in `get_contract_info`**: The `get_contract_info` tool now includes domain names, summaries, and metric counts in its response.
- **`get_domain()` helper**: New method on `DataContract` for exact-match domain lookup by name.

### Changed

- **Tool count**: Factory now produces 12 tools (was 11), adding `lookup_domain`.
- **`list_metrics` domain lookup**: Now uses `DataContract.get_domain()` internally instead of dict lookup.
- **System prompt rendering**: `_render_metrics` simplified to only handle the no-domains case. When domains exist, the new `_render_domains` method takes over with compact domain index rendering.

### Breaking

- **Domain YAML format**: `domains` changed from `dict[str, list[str]]` to `list[Domain]`. Existing contracts must migrate from `domains: { revenue: [metric1] }` to `domains: [{ name: revenue, summary: "...", description: "...", metrics: [metric1] }]`.

## [0.8.0] - 2026-04-12

### Added

- **Lazy-loading relationships**: When a semantic source has more than 30 relationships, the system prompt switches to a compact per-table join-count summary instead of listing every relationship. The agent uses the new `lookup_relationships` tool to fetch details on demand — same progressive-disclosure pattern used for metrics since v0.2.6.
- **`lookup_relationships` tool**: New tool (11th) that returns all relationships involving a given table. When `target_table` is provided, finds the shortest multi-hop join path via BFS (up to 3 hops) — useful when tables are connected through intermediate tables.
- **`get_relationships_for_table()` protocol method**: Added to `SemanticSource` for filtered relationship lookup by table name. Implemented in `YamlSource` with an O(1) index; `DbtSource` and `CubeSource` return empty (ready for future implementation).
- **`build_relationship_index()` helper**: Standalone function in `semantic.base` that builds a `dict[str, list[Relationship]]` index from a relationship list, keyed by table name. Reusable by any `SemanticSource` implementation.
- **`find_join_path()` helper**: BFS shortest-path function that finds a chain of relationships connecting two tables, bounded by `max_hops` (default 3). Returns `None` if no path exists.

### Changed

- **Tool count**: Factory now produces 11 tools (was 10), adding `lookup_relationships`.

## [0.7.1] - 2026-04-11

### Fixed

- **Tools factory now passes `semantic_source` to the Validator**: `create_tools()` was creating the `Validator` without the `semantic_source` parameter, so `RelationshipChecker` never ran through `validate_query` or `run_query`. Relationship warnings now surface correctly in the tools layer.
- **Example SDK fallback**: `agent.py` now catches `AttributeError` alongside `ImportError` when the installed `claude-agent-sdk` version is incompatible, falling back to demo mode instead of crashing.

### Changed

- **Example demo step**: Added a relationship warning demonstration — validates a JOIN query missing the declared `required_filter` to showcase the advisory warning.

## [0.7.0] - 2026-04-11

### Added

- **`RelationshipChecker`**: Advisory validation of SQL JOINs against declared semantic relationships. Produces warnings only — never blocks queries. Silent on undeclared joins. Three detection modes:
  - **Join-key correctness**: Warns when an agent joins two tables that have a declared relationship but uses different columns than specified (e.g., joining on `email` instead of declared `customer_id → id`). Supports both `ON` and `USING` clause syntax.
  - **Required-filter enforcement**: Warns when a join's declared `required_filter` condition is missing from the query's WHERE clause. Checks column presence only (not exact expression), so `status = 'active'` satisfies `required_filter: "status != 'cancelled'"`.
  - **Fan-out risk detection**: Warns when the query aggregates (SUM, COUNT, AVG, etc.) across a `one_to_many` join, which may inflate results due to row multiplication. Only checks top-level SELECT aggregations — subquery aggregations are ignored.
- **`Validator` accepts `semantic_source`**: Optional `SemanticSource` parameter on `Validator.__init__()` enables relationship checking when provided. Fully backward-compatible — omitting it preserves existing behavior.
- **Relationship warnings skip blocked queries**: When a query is already blocked by structural checkers, relationship warnings are suppressed to reduce noise.

## [0.6.0] - 2026-04-09

### Added

- **Relationship `description` field**: Optional free-text description on `Relationship` for communicating join conditions, data quality caveats, or usage guidance to the agent. Rendered as an XML attribute in the system prompt when present.
- **Relationship `required_filter` field**: Optional structured filter condition (e.g., `"attribution_model = 'last_touch'"`) that must be applied when using a relationship. Rendered as a `<required_filter>` element in the system prompt, giving agents a clear, unambiguous signal about mandatory join conditions — especially useful for bridge/junction tables.
- **Contract-relative path resolution**: `DataContract.from_yaml()` now resolves `source.path` relative to the contract file's directory, not the process CWD. This means `path: "./semantic.yml"` in `contracts/contract.yml` correctly loads `contracts/semantic.yml` regardless of where the process runs. Absolute paths and `from_yaml_string()` are unaffected.

### Fixed

- **Example contract**: Removed invalid `filter_column` field from `examples/revenue_agent/contract.yml` (the field was removed in v0.4.0 in favor of `query_check.required_filter`).

## [0.5.0] - 2026-04-04

### Added

- **`SqlNormalizer` protocol**: Optional pre-processing hook for adapters serving non-standard SQL dialects (e.g., Denodo VQL, Teradata). Adapters implement `normalize_sql(sql) -> str` to rewrite proprietary syntax into a form sqlglot can parse, while the original SQL is preserved for `execute()` and `explain()`.
- **Auto-detection in factory and middleware**: When an adapter implements both `DatabaseAdapter` and `SqlNormalizer`, the factory and middleware automatically wire normalization into the `Validator` — no API changes needed.
- **Normalization in `validate_results()`**: Table-scoped result checks now also benefit from SQL normalization, ensuring scoped checks fire correctly for non-standard dialects.
- **Adapter package exports**: `adapters/__init__.py` now re-exports `Column`, `DatabaseAdapter`, `QueryResult`, `SqlNormalizer`, and `TableSchema`.
- **Root export**: `SqlNormalizer` is available via `from agentic_data_contracts import SqlNormalizer`.

## [0.4.0] - 2026-03-31

### Added

- **Unified rule engine**: Rules now support `query_check` (pre-execution) and `result_check` (post-execution) blocks, replacing the old `filter_column` shorthand. All rules live in one `rules` list; the engine determines execution phase automatically.
- **Table scoping**: Every rule can be scoped to a specific table (`table: "schema.table"`) or apply globally (omitted or `"*"`). Pre-execution and post-execution rules both support scoping.
- **5 built-in query checks**: `required_filter`, `no_select_star`, `blocked_columns`, `require_limit`, `max_joins` — all declarative in YAML, no Python needed.
- **6 built-in result checks**: `min_value`/`max_value` (numeric column bounds), `not_null`, `min_rows`/`max_rows` — validated against actual query output post-execution.
- **Advisory rules**: Rules with neither `query_check` nor `result_check` appear in the system prompt as guidance but don't enforce anything.
- **Session cost enforcement**: `run_query` now records estimated cost from EXPLAIN and enforces cumulative `cost_limit_usd` across the session.
- **`validate_results()` on Validator**: New method for post-execution result validation, used transparently inside `run_query`.
- **`validate_query` result check notes**: Output now lists pending result checks that will run at execution time.
- **New checker classes**: `BlockedColumnsChecker`, `RequireLimitChecker`, `MaxJoinsChecker`, `ResultCheckRunner` — all exported from `validation` module.

### Changed

- **Checker protocol**: All checkers now use `check_ast(ast)` instead of `check_sql(sql)`. SQL is parsed once by the Validator and the AST is passed to all checkers.
- **`extract_tables()` utility**: Extracted from `TableAllowlistChecker` into a standalone function for shared use by the Validator's table scoping logic.
- **`ValidationResult`**: Gains `estimated_cost_usd: float | None` field for session cost passthrough from EXPLAIN.
- **Three-phase validation**: Validator now runs query checks (Phase 1) → EXPLAIN (Phase 2) → result checks (Phase 3), up from the previous two-phase pipeline.

### Removed

- **`SemanticRule.filter_column`**: Replaced by `query_check: { required_filter: <column> }`. No backward compatibility — the old field is removed entirely.
- **Heuristic filter detection**: The regex-based `_extract_filter_column()` method that guessed filter columns from rule descriptions is gone. Filters are now explicit in `query_check`.

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
