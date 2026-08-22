# Agentic Data Contracts — Architecture

**Date:** 2026-04-17
**Status:** Implemented
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
| Domain membership | Metric-first | A metric declares its domains (`domains: [...]`); the contract's `Domain` holds only catalog metadata (summary, owners, review cadence) — never a metric list. Single source of truth, no contract↔source drift, and it matches the grain dbt/Cube already use (`meta.domains` per metric) |
| Developer experience | YAML-first | Data engineers live in YAML; zero Python knowledge required to define a contract |
| Enforcement | Configurable per-rule | `block` / `warn` / `log` per rule |
| Tool delivery | Factory + middleware | Quick start via factory, composable via middleware |
| Dependency management | uv | Modern, fast, lockfile-based |
| Consumer-authored semantics | Carried, never interpreted | Every first-class section is *computed on* — relationships are indexed and BFS-walked, decompositions are cycle-checked, drill_by is validated against schemas. A section that is only interpolated into a string is carriage, and carriage gets a generic mechanism (`get_extras`, `extra_sections`) rather than bespoke vocabulary. Generalises the boundary `validation/examples.py` already draws for verified examples |
| Prompt renderer naming | Named by output format | `XmlPromptRenderer`, not `ClaudePromptRenderer`: the class holds no Claude-specific logic, and `PromptRenderer` implementations differ by format (XML vs Markdown), not by vendor |

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
    type: dbt                          # dbt | cube | ossie | yaml | custom
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
  forbidden_operations:
    [DELETE, DROP, TRUNCATE, UPDATE, INSERT, CREATE, ALTER, MERGE, GRANT, REVOKE, COPY]

  # Business domains — catalog metadata (description, owners, review cadence)
  # for domain-specific questions. Membership is metric-first: metrics declare
  # their domains in the semantic source, so there is no metric list here.
  domains:
    - name: revenue
      summary: "Revenue and financial metrics from completed orders"
      description: >
        Revenue metrics track recognized revenue from completed orders.
        Revenue is recognized at fulfillment, not at booking.
    - name: engagement
      summary: "Customer activity and retention patterns"
      description: >
        Customer engagement measures active usage patterns
        and retention over time.

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

YAML-level business assertions — `domain.description`, `metric.sql_expression`, `metric_impact.evidence` — rot silently when the business changes. `Domain`, `MetricDefinition`, and `MetricImpact` each carry an optional `last_reviewed: date` field, and `DataContract.find_stale()` flags any artefact whose timestamp is missing or older than a threshold (default 90 days). Findings come back as three `kind`s — `domain`, `metric`, `metric_impact` — and each carries its owners (`business_owner` / `operational_owner`) in `context` when set, so the report says *who to nag*.

```python
dc = DataContract.from_yaml("data_contract.yml")
source = dc.load_semantic_source()
findings = dc.find_stale(source, threshold_days=90)
for f in findings:
    print(f.kind, f.name, f.age_days, f.context.get("business_owner"))
```

Missing timestamps report as stale (`age_days=None`) — otherwise adoption is optional and defeats the forcing function. During rollout, filter by `f.age_days is not None` to grandfather in un-reviewed entries. The detector is a pure function suitable for direct use in a pytest assertion or CI check.

**Two audiences, two policies.** `find_stale()` is the strict *audit* path above (missing date = stale). The agent-facing tools (`lookup_metric`, `list_metrics`, `lookup_domain`) take the *lenient* view: they surface `last_reviewed` + a `stale` boolean only when a review date is actually set, so contracts that never adopted the field get no false "stale" noise at query time. The stale threshold for the tools is `create_tools(..., staleness_threshold_days=90)`.

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

**Two-tier empty-string handling:** `resolve_principal` passes through an empty string without normalisation. The access-policy layer (`principal_in_scope`, called from both `DataContract.allowed_table_names_for` and the per-rule scope check inside `Validator`) treats an empty string as unauthenticated — same as `None` — so callers should canonicalize identities before passing them in. Splitting the resolver from policy is intentional: the resolver stays neutral, and `principal_in_scope` is the single source of truth for the allow/block-list semantics.

`allowed_principals` and `blocked_principals` on `AllowedTable` are mutually exclusive (validated at YAML load time). Principals are opaque strings compared by exact equality — no normalisation is performed inside the library. The rule of thumb: **any `*_principals` field on a table requires identification** — symmetric for allowlist and blocklist. An unidentified caller (resolver returns `None` or `""`) is always denied for any table that declares either field.

**Per-rule principal scoping** uses the exact same model. `SemanticRule` accepts the same `allowed_principals` / `blocked_principals` pair (also mutually exclusive). Inside `Validator._build_checkers`, each rule's principal scope is captured once at construction time as a `(allowed, blocked)` snapshot and stored on a frozen `_QueryRuleEntry` / `_ResultRuleEntry`. At validate-time the caller is resolved once and rules whose scope excludes them are skipped — same fail-closed contract as table scoping. This generalises to every rule kind (`blocked_columns`, `required_filter`, `no_select_star`, `max_joins`, `result_check`) without touching any individual checker class. Note that `pending_result_check_names()` deliberately returns the full declared list (a *superset* of what runs for any given caller); the only consumer is `run_query` telemetry, and resolving a callable principal there would create a TOCTOU surface.

### ContractSession (Lightweight Enforcement)

When `ai-agent-contracts` is NOT installed, `ContractSession` provides self-contained enforcement:

- **Retry count** — incremented on each failed query attempt, checked against `max_retries`
- **Token usage** — fed by the adapters via `observe_tokens()`, checked against `token_budget` (see below)
- **Wall-clock duration** — lazy start on first `check_limits()` call (not at construction), checked against `max_duration_seconds`. Can be reset via `reset_timer()` for frameworks that manage their own idle timeouts.
- **Cost estimate** — if EXPLAIN adapter returns cost info, checked against `cost_limit_usd`

These are simple counters/timers with guard checks before each tool call. No formal state machine.

**Token usage is the one counter the session cannot produce itself,** because
the tokens are spent by the *model* between tool calls, not by anything this
library runs. It has to be fed from whatever the host framework exposes, and
three paths currently read it:

| Path | Feeds `token_budget`? | Source |
|---|---|---|
| Pydantic AI tools / toolset | Yes | `ctx.usage.total_tokens`, scoped by `ctx.run_id` |
| LangChain `ContractMiddleware` | Yes | `usage_metadata` summed over `request.state["messages"]`, scoped per conversation |
| LangChain `create_langchain_tools` | Yes | the same, from an injected `ToolRuntime` |
| `contract_middleware` | No | its wrapper receives an `args` dict only |
| Claude Agent SDK | No | the tool callable receives an `args` dict only |

The two LangChain rows share one implementation (`_observe_usage`), keyed on
`(state, config)` — the shape both callers can produce. That is deliberate:
fed the same run they derive the same scope key and observe the same cumulative
total, so wiring both does not double-count, and neither can drift into
disagreeing about a session's total.

`create_langchain_tools` gets there by declaring a `runtime: ToolRuntime | None`
parameter on its `StructuredTool` coroutine, which LangGraph's `ToolNode`
injects. Three things about that are easy to get wrong:

- **It never reaches the model.** `infer_schema=False` means the advertised
  schema is the tool's JSON Schema dict verbatim, not the coroutine signature.
- **It is optional on purpose.** Injection only happens inside a graph;
  `await tool.ainvoke({"sql": ...})` is a supported call shape and passes no
  runtime. A required parameter would break every direct caller to catch a
  mis-wiring that degrades to the old behaviour anyway.
- **The annotation is what carries it, not the name.** `ToolNode` triggers on
  a parameter *named* `runtime` **or** annotated `ToolRuntime` — so renaming
  this parameter would keep working, while dropping its annotation would
  silently stop injection. Either way it reads the hints via `get_type_hints()`,
  and since `tools/langchain.py` uses `from __future__ import annotations`, the
  `ToolRuntime` import must stay at module level to resolve. A test drives a
  real `create_agent` for exactly this reason — every test that passes
  `runtime` itself would still pass with injection broken.

Two conversations are told apart by `thread_id`, falling back to the id
LangGraph stamps on the first message. Both under-count on a *collision*: two
conversations sharing the last-resort constant key, or a **nested run
inheriting its parent's `thread_id`** (a sub-agent or subgraph, which
`create_deep_agent` produces), whose shorter history is then dropped whole by
the monotone guard rather than added. Appending the config's `checkpoint_ns`
looks like the fix and is not — it is `tools:<task-id>` and changes on every
tool call, so it would re-accrue the full total each time. Both cases
under-enforce, which is the safe direction, and both beat the nothing this
path observed before v0.35.0.

The two paths that remain blind warn at wiring time, for the same reason
`Validator` warns about unenforceable `forbidden_operations`: a
declared-but-unenforced limit is worse than an absent one, because the contract
reads as protective while permitting the thing it names.

**Both LangChain enforcement pieces should still share one `ContractSession`.**
They each default to constructing their own, and a split pair enforces against
the middleware's session while `run_query` reports `remaining()` from the tools'
— so the model is told it has its full budget no matter what it spent. Build
one session and pass it to both.

`observe_tokens(cumulative, *, scope)` exists because framework counters report
a running total for **their** scope (a Pydantic AI run, a LangGraph thread)
while a `ContractSession` deliberately spans several of them — `ContractDeps`
tells callers to keep one session per user across every turn. Adding the total
on each call multiplies it; assigning it resets the session at each new run.
Only the per-scope delta accrues. `record_tokens()` remains the delta-based
entry point for framework-free callers.

**Known window:** the paths that observe usage do so *before* running
`check_limits()`, so a budget the current turn has already blown stops that
call rather than the next. What no amount of ordering fixes is an agent that
exhausts its budget and then stops calling tools: nothing runs, so nothing
checks. That is inherent to enforcing from inside tools, and identical to how
`max_retries` and `cost_limit_usd` already behave. Now that usage is fed,
`remaining()` reports honest `tokens_remaining` in every `run_query` response,
which lets the model self-regulate.

**Closing the window (Pydantic AI).** `contract_run_kwargs(contract, session)`
returns `{"usage", "usage_limits"}` for `agent.run()`: one `RunUsage` carried
across every turn for that session, plus the contract's `token_budget` as a flat
ceiling. Pydantic AI checks it on every model request — no tool call required —
so the blind case above disappears, and because the counter spans turns it sees
*every* request, including the answer generation the tools never observe.
Nothing is missed, so nothing is estimated. Measured on a 500-token budget:
true spend settles at 600 and stays there however many turns run. Deriving
each run's allowance from the session's own tally instead grows linearly with
turns (1100 at 6, 1700 at 12, 2500 at 20), because that tally never sees what a
run spends after its last tool call. A refused turn also costs nothing, because
`check_before_request` fires before the request is issued.

Three details carry the design:

- **The limit is flat, not the remainder.** The carried counter already holds
  the spend; subtracting it again would take it off twice and lock the run out
  below its budget — the spend is deducted once in the limit and again inside
  the counter.
- **The counter lives in a module-private `WeakKeyDictionary` keyed on the
  session**, not on `ContractSession` — `RunUsage` is a Pydantic AI type and
  `core/` stays framework-agnostic. Weak keys because the caller owns session
  lifetime and this must not be what keeps one alive.
- **The tool wrapper picks its scope by identity**, not a flag: if `ctx.usage`
  *is* the counter registered for this session it accrues under one constant
  key, otherwise it falls back to `run_id`. So a caller who ignores the helper,
  or passes its `usage_limits` without its `usage`, gets plain per-run
  scoping rather than a corrupted tally. Getting a boolean wrong is not a
  failure mode because there is no boolean. Treating a carried counter as
  per-run would inflate the session's tally and block runs still within budget —
  measured at 900 recorded against 600 truly spent, before the identity check
  existed.

`session.tokens_used` still lags mid-run, since the wrapper only observes while
a tool is executing; `contract_run_kwargs` reconciles it at the start of each
run, so between runs — when a caller would read it — it is the true spend.

Callers should catch `pydantic_ai.exceptions.UsageLimitExceeded` **as well as**
`ContractSessionLimitError`, not instead of it: the session still enforces
`max_retries`, `cost_limit_usd` and `max_duration_seconds`, and still fires when
fed from outside the run (a shared session, or a direct `observe_tokens()`
call, on either `apply_middleware` value).

Two things are deliberately not mapped. `max_retries` must **not** become
`request_limit`: ours counts blocked query attempts, theirs counts model
requests, and conflating them would silently redefine existing contracts.
`cost_limit_usd` and `max_duration_seconds` have no equivalent and stay
session-side. It is Pydantic AI only — LangChain has no per-request ceiling and
the SDK path cannot observe usage at all, so the helper lives in a module that
imports only under the `[pydantic-ai]` extra rather than anywhere implying
coverage it lacks.

**Run cancellation is a third deliberate non-adoption (assessed 2026-08-16,
against Pydantic AI 2.31).** Pydantic AI 2.26 added first-party cancellation —
`AgentRun.cancel()`, `RunContext.cancel()`, `RunCancelled` — and a terminal
budget breach looks like exactly the condition it was built for. It is not
adopted, for three reasons:

- **Framework neutrality is a stated property of this library.** A terminal
  breach raises `ContractSessionLimitError` identically on the Agent SDK,
  LangChain and Pydantic AI paths. Routing one adapter through a
  framework-native mechanism would make governance behave differently depending
  on which agent framework the caller happened to pick — the divergence this
  library exists to remove.
- **A governance stop is not a cancellation.** `RunCancelled` reads as *someone
  asked to stop*; a breached budget is *policy refused*. A caller catching
  `RunCancelled` to handle a user pressing Stop would silently swallow a limit
  breach. Distinct meanings need distinct types.
- **It would add a third path, not unify two.** A limit already trips on either
  the tool side (`ContractSessionLimitError`) or the model side
  (`UsageLimitExceeded`), and callers are told above to catch both. Cancellation
  does not collapse that pair; it appends to it.

The third reason assumes the exception escapes `agent.run()` at all, so that
assumption is a test rather than a belief:
`test_terminal_limit_error_escapes_agent_run` runs a breach through Pydantic
AI's own tool-execution machinery and asserts `ContractSessionLimitError`
propagates uncaught. Every other terminal-limit test invokes the tool function
directly, proving only that *our* wrapper raises. Because
`test-pydantic-ai-latest` runs this module against the newest release the floor
allows, a future version that caught non-`ModelRetry` tool exceptions — turning
a breached budget into a retry — would surface there rather than in a user's
install. Revisit this decision if that job ever goes red on it.

When `ai-agent-contracts` IS installed, enforcement is delegated to the formal framework via the bridge layer (see below).

## Validation Layer

Three-phase validation architecture. Dependencies: `sqlglot`.

### Phase 1: Query Checks (pre-execution, always available)

```python
class Checker(Protocol):
    def check_ast(self, ast: Expression, *args) -> CheckResult: ...
```

SQL is parsed once into a sqlglot AST. The Validator passes the AST to all applicable checkers, respecting table and per-rule principal scoping (rules carrying `allowed_principals` / `blocked_principals` are skipped when the resolved caller is out of scope).

**Structural checkers** (from top-level config):

| Checker | What it validates |
|---|---|
| `TableAllowlistChecker` | All referenced tables are in `allowed_tables`, filtered per `caller_principal` if supplied |
| `OperationBlocklistChecker` | No forbidden SQL operations (see `ENFORCEABLE_OPERATIONS` below) |

`OperationBlocklistChecker` only blocks what a contract explicitly lists, and it
can only block operations it recognises. `ENFORCEABLE_OPERATIONS` — DELETE, DROP,
INSERT, UPDATE, TRUNCATE, CREATE, ALTER, MERGE, GRANT, REVOKE, COPY — is
**derived from** `_OPERATION_MAP` rather than written out separately, because a
hand-maintained duplicate would drift from the map and reintroduce exactly the
bug the set exists to surface.

An operation outside that set is *unenforceable*: naming it in
`forbidden_operations` used to parse and store cleanly while enforcing nothing,
so the contract read as protective and permitted the statement. `Validator`
now warns at construction when a contract names one:

```
forbidden_operations names ['CALL', 'VACUUM'], which the operation blocklist
cannot detect — declared but NOT enforced. Enforceable operations: [...]
```

A declared-but-unenforced rule is worse than an absent one, so this fails loud
rather than silent — and deliberately *uncached*, though it fires once per
`Validator`. Memoising a logging side effect means a consumer who calls
`logging.basicConfig()` after building its contracts loses the warning
permanently, which is the opposite of what a fail-loud diagnostic should do. The
five `logger.warning` calls in `create_tools` sit on the identical call path
uncached; if the repetition ever matters, the fix belongs at contract-load time
for all six.

The residue is real, and not purely `exp.Command`:

- `CALL proc()` and vendor-specific DDL parse as a bare `exp.Command` — no
  operation name reaches the blocklist, and the warning cannot fire for them
  either, since the contract never named them.
- `ALTER WAREHOUSE wh RESUME` parses as a bare `exp.Command` on Snowflake, while
  `ALTER TABLE ...` parses as `exp.Alter`. Two statements that read as the same
  operation land on opposite sides of the blocklist. Note also that adding
  `ALTER` to a contract blocks `ALTER SESSION SET TIMEZONE = 'UTC'`, which is
  idiomatic Snowflake session setup — correct, but worth knowing before copying
  the recommended list.
- `SELECT a INTO newtbl FROM t` parses as a plain `exp.Select` (tsql, postgres).
  The operation blocklist never sees a write at all. It is caught by a
  *different* checker — `extract_tables` walks the `Into` node, so
  `TableAllowlistChecker` rejects it unless the target table happens to be
  allowlisted.

That is why the warning exists rather than a claim of completeness, and why
`run_query` carries no `readOnlyHint`.

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

### Batch validation: verified-examples corpus

`validate_examples(examples, contract, ...)` (in `agentic_data_contracts.validation`) runs an **external** corpus of `question → SQL` examples through the *same* `Validator` used for live queries — no parallel checking path. The corpus (a human-reviewed examples database) lives outside the library; the framework only re-validates each `sql`. One `Validator` is built per distinct `example.principal`, so per-principal rules are checked under the right identity, and input order is preserved.

Each example maps to an `ExampleResult` with exactly one `status` — `valid` (Phase 1 static checks ran *and* passed), `violation` (a check rejected it), `unverified` (decision B, below), or `unchecked` (no verdict) — and two flags: `contract_checked` (Phase 1 static checks ran; requires a successful sqlglot parse) and `engine_checked` (Phase 2 EXPLAIN ran). `engine_checked` is reconstructed from the returned `ValidationResult` (`schema_valid` / `estimated_*`) — the only core addition this feature makes to `ValidationResult` is a `parse_error` flag. When sqlglot cannot parse the SQL but an `ExplainAdapter` is present (**decision B**, for engines sqlglot does not model, e.g. Denodo/VDP), the engine is asked to plan it directly; such a pass verifies *plannability* but not *contract policy* (no AST for the static checkers), so it takes status `unverified` (`contract_checked=False`), surfaced in `report.unverified_compliance`. `report.ok` is a safe CI gate: True only when every example is `valid`, so violations, unchecked, *and* unverified rows all fail it. A per-example guard degrades any example whose adapter raises to `unchecked` rather than aborting the batch. Two intended triggers, one call: an authoring-time **MR gate** (Layer 1 only, no warehouse needed) and a contract-change **drift sweep** (with an adapter, so the live EXPLAIN catches schema drift). It never executes the SQL — that is the second pass's job (below), and the sibling `reconcile_decomposition(...)` covers execution-based identity checks.

**Expected-value assertions (v0.44.0+):** `validate_examples` proves an example is *allowed*; it cannot prove the SQL is *right*. A query with every table permitted, the tenant filter present and explicit columns can still sum the wrong rows and pass as `valid`, and nothing downstream distinguishes it from a correct one. `VerifiedExample` therefore gains four optional fields — `expected` (the certified scalar answer), `rel_tol`, `abs_tol`, `time_scoped` — and `check_example_answers(report, *, adapter, dialect=None, sql_normalizer=None, rel_tol=1e-9, abs_tol=0.0)` executes the compliant, asserted rows and compares. It takes the **`ExampleValidationReport`, not the raw examples**, and that is the load-bearing choice: a row that violates the tenant-filter rule is precisely the query that must not be sent to the warehouse to see what it returns, so consuming the report makes the ordering a property of the signature rather than a rule in a docstring. A row is executed only when it is `status == "valid"` **and** declares an `expected`. The adapter *kinds* differ for the same reason — `validate_examples` takes an `ExplainAdapter` (plans only), while the execute-capable `DatabaseAdapter` enters at this second, already-filtered stage.

  Each asserted row lands in one of `match` / `mismatch` / `unassertable` / `error`. The **answer** report has its own `ok`, distinct from the validation `report.ok` discussed above: True only with at least one *checked* assertion and every one a `match`, so an empty answer report fails rather than passing a no-op check. It cannot see an asserted row that failed contract validation — such a row is filtered out before this pass — which is why every documented gate composes the two (`report.ok and answers.ok`) rather than trusting either alone. **A relative time window is refused, not executed** (`unassertable`): an expected value pinned against `WHERE created_at >= CURRENT_DATE - 30` decays on its own, so the row would go red in a month for a reason the corpus author never touched. The scan runs before any execution and needs **two arms**, because sqlglot normalises a spelling to a typed AST node only in the dialects that own it — `NOW()` is `exp.CurrentTimestamp` under postgres but a bare `exp.Anonymous` under duckdb, mysql, snowflake, bigquery, tsql and oracle, while `LOCALTIMESTAMP` and Snowflake's `CURRENT_TIME` are `exp.Localtimestamp` / `exp.Localtime`. Arm one matches the typed nodes; arm two matches `exp.Anonymous` calls by name **whose arguments look like a clock read** — none (`NOW()`), or a single integer literal, which is a fractional-seconds precision spec (`NOW(3)`, `SYSDATE(6)`). Keying on the argument's *kind* rather than its mere presence is what keeps the deterministic `UNIX_TIMESTAMP(created_at)` conversion — whose argument is a column — from being refused alongside the clock reads that share its name, without also letting `NOW(3)` through: `NOW` and `SYSDATE` are exactly the names that never reach arm one. Matching a call rather than any identifier is what keeps a *column* named `now_flag` unflagged. `time_scoped: true` asserts the window is pinned some other way and clears the refusal.

  The default tolerance is `rel_tol=1e-9`, deliberately tighter than `reconcile_decomposition`'s `1e-4`, and the two anchor differently on purpose: a decomposition identity is approximate by construction (operand precision leaves a real residual) and compares two *measurements* with no privileged side, so it anchors on the measured parent; an assertion has a certified reference, so it anchors the relative term on `expected` — which also keeps the tolerance's meaning stable as a query drifts, unlike `math.isclose`'s larger-magnitude anchor. One consequence: at `expected == 0` the relative term vanishes, so a zero-valued assertion matches only exactly unless an `abs_tol` is set. Scalar measurement is shared with reconciliation through `validation/_scalar.py`, so the empty-result / NULL / non-finite / non-scalar semantics have one implementation; a per-example guard degrades a single bad row to `error` rather than aborting the batch, matching `validate_examples`. The framework still never stores, loads or serves examples — the corpus stays external, and this pass only returns verdicts on records handed to it.

## Tools Layer (Claude Agent SDK Integration)

Two modes: tool factory for quick starts, middleware for BYO tools.

### 9 Tools

1. **`describe_table(schema, table)`** — Column details, merging the database adapter's catalog view with authored descriptions from the semantic source (semantic wins; adapter fills gaps)
2. **`preview_table(schema, table, limit?)`** — Sample rows, as `{schema, table, columns, rows}`
3. **`list_metrics(domain?, tier?, indicator_kind?)`** — Browse metrics with filters
4. **`lookup_metric(metric_name)`** — Full metric definition with SQL, impact edges, and any `decompositions` / `drill_by`
5. **`lookup_domain(name)`** — Full domain description with metrics and tables
6. **`lookup_relationships(table, target_table?)`** — Direct joins and multi-hop paths
7. **`trace_metric_impacts(metric_name, direction, max_depth?, kinds?)`** — BFS over the metric graph across both influence and identity (decomposition) edges; `kinds` (`all` / `identity` / `influence`) filters which kind(s) to walk
8. **`inspect_query(sql)`** — Static + EXPLAIN check, no execution
9. **`run_query(sql)`** — Validate and execute; response includes remaining session budget

Both result-returning tools render `rows` according to `create_tools(row_format=...)`:
`"compact"` (the default) emits positional arrays aligned to the `columns` key,
`"records"` emits one dict per row. The rendering is an operator decision, not an
agent-facing tool argument — the two carry identical information, so the model has
no basis on which to choose, and a schema field would cost input tokens on every
request. The value is validated eagerly at `create_tools()` time.

### Query Protocol in the Tool Descriptions

`run_query` and `inspect_query` carry two protocol clauses in their descriptions
(`_PROTOCOL_METRIC_ORDERING`, `_PROTOCOL_PRECEDENCE` in `tools/factory.py`): an
**ordering** rule — when computing a metric, call `lookup_metric` first and use its
governed definition, never invent or adapt a formula — and, on `run_query` only, a
**precedence** claim that it is preferred over any other SQL or data-access path.

The guidance duplicates hints `XmlPromptRenderer` already emits, and the
duplication is deliberate. The rendered prompt is **opt-in**: none of the ecosystem
wrappers inject it, so a host calling `create_langchain_tools` or
`create_pydantic_ai_toolset` and supplying its own system prompt may never render
the contract at all. Descriptions travel with the tools on every path. The two
surfaces do different jobs — the prompt hint aids *discovery* (the tool exists),
the description enforces at *call time* — and only one of them survives a
host that writes its own prompt.

One rule governs both clauses: **a clause appears only when the capability it names
exists.**

- **Ordering is gated on metrics.** `metric_ordering` resolves to `""` when
  `metric_names_set` is empty, so a schema-only contract never points the agent at
  a tool with nothing in it.
- **Precedence is gated on an adapter.** Without one, `run_query` returns "No
  database adapter configured — cannot execute query", so the sentence would be a
  false claim about the tool's capability, steering the agent off a path that works
  and onto one that cannot run. Adapter-less `create_tools()` is a supported
  configuration (validation-only deployments), so this is not a hypothetical.

Both follow the conditional shape `rows_note` established. Two further decisions
shape the text:

- **Narrow trigger.** A broader "before any query" would tax plain exploratory SQL
  with a lookup turn that finds nothing. The guarded failure is a KPI computed from
  an invented formula — SQL that is *authorized* and merely wrong, which is exactly
  the class no checker catches. (The design was informed by
  [Erupt Cube × LLM](https://docs.erupt.xyz/en/topics/cube-llm), which enforces an
  equivalent protocol in its tool descriptions but triggers on any query.)
- **Precedence on `run_query` only,** never `inspect_query`, which executes nothing
  and already argues its own precedence ("before spending retry budget on
  run_query").

Descriptions are re-sent on every model request, so each clause is one sentence.
`tests/test_tools/test_tool_protocol.py` pins the placement, including a scope
guard that the other seven descriptions carry neither clause.

### Error Signalling (`is_error` → MCP `isError`)

Every tool return goes through one of two constructors in `tools/factory.py`:
`_text_response` (success) or `_error_response`, which adds `is_error: True`.
`middleware.py` and `sdk.py` build envelopes of their own and call the same
helper — they used to hand-roll the dict, and that is exactly how
`contract_middleware`'s violations envelope shipped without the flag.
`claude_agent_sdk` reads that key off the envelope and maps it onto MCP's
`isError` — the channel the spec designates for *tool execution errors* a model
can self-correct from. Without it, a governance decision reaches the model as a
successful tool result whose failure is discoverable only by reading the prose.

The rule is **"the tool did not perform the action it advertises"**:

| Group | `is_error` |
|-------|-----------|
| Governance blocks (`BLOCKED —` …) | Yes |
| Access denials (not in allowed tables, restricted for caller) | Yes |
| Misconfiguration (no adapter, no semantic source) | Yes |
| Invalid arguments (bad `direction` / `kinds`) | Yes |
| Lookup found nothing (`Metric 'x' not found.`) | **No** — a valid answer |
| `inspect_query` reporting violations | **No** — that is its job |

The last two rows are the load-bearing exclusions. Flagging a not-found would
tell the model its call failed and distort fuzzy-search recovery; flagging an
inspection that found violations would contradict the tool's entire purpose.

The single-constructor rule is what makes this hold: a new block site gets the
flag by construction rather than by remembering. Tests exercise each block path
individually — `run_query`'s four, `preview_table`'s gated site, and both
`contract_middleware` envelopes — each with a fresh `ContractSession`, since a
shared one exhausts the retry budget and silently reroutes later cases to the
session-limit branch.

The key is additive on every path. `create_pydantic_ai_tools` reads only
`content`; `create_langchain_tools` returns the raw envelope as the `ToolMessage.artifact`
under `response_format="content_and_artifact"`, so `is_error` rides along for
non-`BLOCKED` errors (missing adapter, restricted table, invalid argument). For a
governance block it raises `ToolException` before any artifact is produced, so
the flag is not observable there. Neither wrapper branches on it; both already
signalled errors natively (`ToolException`, `ModelRetry` / the terminal
`ContractSessionLimitError`), which is why this gap was specific to the SDK
path — the only path where the MCP envelope survives as MCP.

**Two error signals now coexist, deliberately.** The wrappers still branch on
`text.startswith("BLOCKED —")` rather than reading `is_error`, so a denial like
`Table x is not in the allowed tables list.` carries the flag but does not become
a `ToolException` / `ModelRetry`. Switching them was considered and rejected: it
is not the behaviour-preserving refactor it appears to be, because `is_error`
covers strictly more than the prefix, so denials, misconfiguration, and invalid
arguments would newly raise on two shipped adapters. And the inner
`_SESSION_LIMIT_MARKER` sniff would have to stay regardless — one boolean cannot
separate recoverable from terminal, which is the distinction those adapters exist
to make. The prefix remains the wrappers' trigger; `is_error` is the MCP-facing
signal. Revisit only with a deliberate decision to widen what the wrappers raise.

### MCP Tool Annotations (SDK path only)

`create_sdk_mcp_server` passes a `readOnlyHint` annotation for the eight tools
that only read, via `_annotations_for(name)`. `run_query` is left
**unannotated** rather than `False`: whether it can write depends on the
contract's `forbidden_operations`, and while the blocklist now covers every
operation in `ENFORCEABLE_OPERATIONS`, `CALL` and vendor-specific DDL still
parse as a bare `exp.Command` and pass unseen. An omitted hint means "unknown"
in MCP; claiming read-only would invite a client to skip a confirmation prompt
it should have shown.

The annotation is built from its **wire representation** —
`ToolAnnotations.model_validate({"readOnlyHint": True})` — not from a
`ToolAnnotations(readOnlyHint=True)` kwarg. The declared floor `mcp>=1.23.0` is
unbounded above, and mcp 2.0 renamed the model attributes to snake_case
(`readOnlyHint` -> `read_only_hint`) while keeping the camelCase alias the
protocol sends. `model_validate` resolves to the field name on 1.x and to the
validation alias on 2.0; the camelCase kwarg happens to work on both too, but
only via alias validation, so it reads as an argument silently discarded and ty
reports it as one. Extend annotations in the same form (add keys to that dict),
and assert on them through `model_dump(by_alias=True)` rather than attribute
access, or the tests pass on one side of the range and fail on the other.

Annotations are an `mcp.types` concept, so they live in `tools/sdk.py` rather
than on the framework-agnostic `ToolDef`, and `mcp.types` is imported lazily so
the module stays importable without the `[agent-sdk]` extra. Per the spec,
clients must treat annotations as untrusted unless the server is trusted — this
is client UX (skipping confirmations on safe tools), not enforcement.

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

### Concurrency & Event-Loop Safety

The tool handlers are `async def`, but the `DatabaseAdapter` / `ExplainAdapter`
protocols are deliberately **synchronous**. To keep the public adapter contract
simple while never stalling the host's asyncio event loop, every blocking
adapter round-trip in the async handlers is offloaded to a worker thread via
`asyncio.to_thread(...)`:

| Tool | Offloaded call |
|---|---|
| `run_query` | `validator.validate` (EXPLAIN dry-run) + `adapter.execute` |
| `inspect_query` | `validator.validate` (EXPLAIN dry-run) |
| `describe_table` | `adapter.describe_table` |
| `preview_table` | `adapter.execute` |

The two graph-/decorator-level enforcement entry points apply the same offload
on their async paths: `contract_middleware`'s async wrapper and the LangChain
`ContractMiddleware.awrap_tool_call` both run `validator.validate` (and its
EXPLAIN dry-run) via `asyncio.to_thread`. The synchronous `wrap_tool_call`
path is left as-is — it is not on an event loop.

This matters when one host process serves multiple concurrent sessions on a
single event loop (e.g. a shared in-process MCP server backing a multi-user
bot): without offloading, a single 30–60s analytical query would freeze every
other coroutine — other sessions' tool calls, health-check probes, and so on.
Offloading keeps the adapter contract unchanged, so existing consumers need no
code changes.

`asyncio.to_thread` uses the event loop's default `ThreadPoolExecutor`.
Concurrent database work is therefore naturally bounded by **the adapter's own
connection pool**, not the thread pool — connections are the real concurrency
gate. Size your adapter's connection pool to the concurrency you want to
support.

Because the offloaded calls now run on worker threads, an adapter must be safe
for concurrent invocation. Implementations backed by a connection pool get this
for free; an adapter that shares a single connection must serialize access to
it. The bundled `DuckDBAdapter` holds a single connection — which is not safe
for concurrent queries — so it guards `execute` / `explain` / `describe_table`
with a `threading.Lock`, serializing on the connection rather than interleaving.
Custom single-connection adapters should do the same.

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

**Metric-impact graph (v0.10.0+):** `get_metric_impacts()` returns directed edges between metrics annotated with `direction`, `confidence`, and `evidence`. The `build_metric_impact_index()` / `walk_metric_impacts()` helpers in `base.py` mirror the `build_relationship_index` / `find_join_path` pattern — dual-keyed index (each edge under both endpoints), cycle-safe BFS traversal, direction disambiguated at walk time. `YamlSource` parses a top-level `metric_impacts:` block; `DbtSource` and `CubeSource` return `[]` (neither system has a native causal-graph concept — impacts live in the contract YAML regardless of where the metric itself comes from); `OssieSource` reads them from its vendor `custom_extensions` block, Ossie having no native concept either (it is on their roadmap as discussion #40).

**dbt relationship parsing (v0.17.0+):** `DbtSource.get_relationships()` projects dbt's built-in `relationships` schema tests into `Relationship` instances. Each test node (`resource_type == "test"`, `test_metadata.name == "relationships"`) carries `kwargs.column_name` and `kwargs.field`; the owner model is resolved via `attached_node` (manifest v12+) and the referenced model via `depends_on.nodes`. The test's `meta:` block supplies `preferred`, `required_filter`, and `relationship_type` (defaulting to `many_to_one`). Tests that can't be resolved (missing `attached_node`, unmodelled dependencies, non-`relationships` test names) are skipped silently rather than raising — manifests are heterogeneous and some tests live on seeds or sources we don't model.

**Cube relationship parsing (v0.18.0+):** `CubeSource.get_relationships()` parses each cube's `joins:` block. The parser builds a `cube_name -> sql_table` map, regexes the single-equality form `{X}.col1 = {Y}.col2` from each join's `sql:` field, and normalises so the `from` side is always the column on the cube declaring the join (independent of which side `{CUBE}` appears on in the SQL). Cube's `relationship` enum (`belongsTo` / `hasOne` / `hasMany` plus `many_to_one` / `one_to_one` / `one_to_many` aliases) maps to canonical `Relationship.type` strings; `meta.relationship_type` overrides. `meta.preferred` and `meta.required_filter` work the same way as `YamlSource` and `DbtSource`. Composite-key joins (multiple `AND`-chained equalities) and joins whose target cube can't be resolved by name are skipped — declare those in contract YAML via `YamlSource` instead.

**Ossie parsing (v0.42.0+):** `OssieSource` reads an [Apache Ossie (incubating)](https://ossie.apache.org/) semantic model — the vendor-neutral spec, formerly Open Semantic Interchange, now governed by the ASF. The spec is a strict subset of this library's vocabulary: an Ossie `Metric` is `name` + multi-dialect `expression` + `description` + `datatype` + `ai_context`, and nothing else. Four decisions define the adapter.

*Table keys drop the database qualifier.* Ossie sources are `database.schema.table`; our keys are two-part because `Relationship` endpoints are `schema.table.column` and `build_relationship_index` recovers the table with a single `rsplit`. A three-part key would make every endpoint four deep and break that contract. Collisions (two databases sharing a schema and table name) are logged. Query-backed datasets register no table at all.

*Cardinality is derived from both sides.* Ossie never writes the join type down — it is implicit in the keys. The spec documents `to` as the one side but nothing validates it, and trusting it is not free: `RelationshipChecker._check_fan_out` fires only on `one_to_many`, so reading a backwards-declared join as `one_to_one` silently disables the row-multiplication warning on an aggregate. The type is therefore read off `(from_columns are a key, to_columns are a key)` → `one_to_one` / `many_to_one` / `one_to_many` / `many_to_many`. Keys are optional in Ossie, though, and absence is not evidence of fan-out: when the `to` dataset declares no keys at all the spec's declaration stands, otherwise every key-less model would flood the fan-out checker with false positives.

*Composite joins are skipped, not split.* Ossie relationships carry parallel `from_columns` / `to_columns` lists; our `Relationship` has single-column endpoints. Emitting one edge per column pair would assert two joins that are each individually wrong, and the join planner walks those edges — so a composite relationship is skipped with a warning naming it, matching the precedent `CubeSource` set for Cube's `AND`-chained joins.

*Governance rides in `custom_extensions`.* Every `$def` in `osi-schema.json` sets `additionalProperties: false` and `version` is a `const`, so ownership, review dates, tiers, `decompositions`, `drill_by`, and the impact graph cannot be added as extra keys. They live under the `AGENTIC_DATA_CONTRACTS` vendor block, whose `data` is a JSON *string* per the spec. Restored `decompositions` / `drill_by` pass through the same `validate_decompositions` / `validate_drill_by` as `YamlSource`. Scalar `tier` / `domains` / `filters` are promoted to one-element lists exactly as `YamlSource` does, since `list("gold")` would otherwise yield `['g', 'o', 'l', 'd']` and then drive tier policy.

Every *other* vendor's block, plus every `ai_context` in the file, is carried through `get_extras()` under `ossie_custom_extensions` / `ossie_ai_context` — the same boundary `YamlSource` draws, where the framework carries extras and renders them on request but never interprets them. Both are keyed by **semantic-model name first**: Ossie's top level is a list and it namespaces entity names per model, so a flat key would silently drop the first of two `customer` datasets or two blocks from the same vendor. Both sections are omitted entirely when empty rather than emitted as `{}`, which would add a noise key to every Ossie contract's canonical bytes and trip a contract declaring `expected_extras` on rehydrate; both are put through the shared `jsonify_extras` (promoted from `yaml_source` alongside `parse_review_date`), so a YAML-native date in an `ai_context` cannot reach `contract_canonical_bytes` unconverted. A foreign vendor's malformed JSON is carried verbatim and logged rather than raised on.

The spec is `0.2.0.dev0` with no tagged releases (apache/ossie#102), and the accepted expression-language proposal introduces an `Ossie_SQL_2026` dialect absent from today's enum. `dialect` is therefore treated as an opaque string and never validated against the enum: a closed check would break on the next spec bump for no benefit. Dialect resolution is deterministic, as the spec requires — caller's choice, then `ANSI_SQL`, then `Ossie_SQL_2026`, then the first declared entry, so a model written only in a vendor dialect still yields an expression rather than a silent empty string.

**Metric-first domain membership (v0.26.0+):** A metric declares the domains it belongs to via `MetricDefinition.domains`, populated from `meta.domains` by every adapter (`YamlSource`, `DbtSource`, `CubeSource`). The contract's `Domain` model carries only catalog metadata (summary, description, owners, `last_reviewed`) and does **not** list its metrics; it sets `extra="forbid"`, so a stale `metrics:` key from a pre-0.26 contract fails loudly at load time instead of being silently dropped. To enumerate a domain's members, callers reverse-look-up with the `metrics_in_domain(metrics, domain_name)` helper in `base.py` (used by `lookup_domain` and the `list_metrics(domain=...)` filter); per-domain `metric_count` in the prompt index and the `lookup_domain` fuzzy fallback is tallied in a single `Counter` pass over the metrics. This makes the metric the single source of truth for membership: the contract and the semantic source cannot disagree, because only one of them states it. (Prior to v0.26.0 the contract also carried a `Domain.metrics` list, reconciled with `metric.domains` by a union shim; that redundancy — and the drift it allowed — has been removed.)

**The catalog is authoritative for which domains exist.** `list_metrics(domain=...)`, `lookup_domain`, and the prompt's `<available_domains>` index all treat the contract's domain catalog as the set of navigable domains — a domain a metric references but the contract does not catalog is not navigable, and `create_tools` logs a warning for it at startup (the metric-first mirror of the old "domain references unknown metric" check). Metrics declare *membership*; the catalog defines *identity* (and supplies the business context `lookup_domain` returns).

**Metric identity decomposition (v0.28.0+):** A metric can declare `decompositions` — arithmetic identity edges describing how it is reconstructed from other metrics via `sum`, `product`, `ratio`, or `difference` (`ratio` and `difference` are binary; `sum` and `product` take two or more operands). `validate_decompositions()` checks operator validity, operand arity, that every operand resolves to a known metric, and that the identity edges form a DAG — a metric cannot transitively decompose into itself — raising loudly at load time on any fault. A metric can also declare `drill_by`: a priority-ordered list of dimensional slice hints (`dimension` name + `schema.table.column`), soft-validated by `validate_drill_by()` — a malformed `schema.table.column` shape always raises, but a reference to a table the contract hasn't declared is skipped rather than rejected, since schemas are optional. Both fields are optional to declare and validated only when present; leaf metrics with neither are untouched. `identity_edges_from_metrics()` flattens every metric's decompositions into `IdentityEdge` instances (`from_metric` = parent, `to_metric` = operand, plus the producing `operator`), which share the `MetricEdge` union with `MetricImpact` (`kind` = `"identity"` vs. `"influence"`). `trace_metric_impacts` walks both edge kinds through the same unified index, and its `kinds` argument (`all` | `identity` | `influence`, default `all`) filters which edge kind(s) the traversal follows. Extraction from dbt/Cube is deferred — today `decompositions`/`drill_by` are declared directly in contract YAML for `YamlSource`, or in an Ossie model's vendor `custom_extensions` block for `OssieSource`. Two follow-ups scoped as Spec B split on the framework's governance/agent boundary: the **reconciliation check shipped in v0.29.0** (see below), while a **variance-diagnosis tool stays out of scope** (see Future Extensions) — agent-owned orchestration the model already performs from the grounding this feature ships.

**Decomposition reconciliation (v0.29.0+):** `reconcile_decomposition(metric, *, parent_sql, operand_sql, adapter, rel_tol=1e-4, abs_tol=0.0, decomposition=0)` — in `validation/reconciliation.py`, exported from `agentic_data_contracts.validation` — executes a metric's declared decomposition against a live `DatabaseAdapter` and asserts the arithmetic identity holds within tolerance. It is a contract-integrity check, the same species as the sqlglot/EXPLAIN checkers and the stale-review detector: it catches an identity gone *false in the data* (ETL drift, a child metric SQL that diverged, a join that skews a population) that leaves every per-query check green. It is **keyed off the declared decomposition** — the contract owns *what* the identity is (operator + operand names, read straight from `decompositions[decomposition]`), while the caller supplies scalar SQL for the parent and each declared operand, owning *how* to measure each over its chosen slice — so no metric executor is assumed (that is the deferred `compose_metric_query`, which would later supply the SQL for free). Malformed input raises `ValueError` (no decomposition, out-of-range index, operand-key mismatch, unknown operator, wrong arity — all validated before any query runs); data conditions are *findings* (`reconciles=False`) — a NULL / empty / non-finite (`NaN` / `inf`) measurement, or a `ratio` zero denominator. `ReconciliationResult.reason` states only the mechanical condition and **never infers the cause** (that interpretation is agent-owned, the same boundary as the deferred diagnosis tool). The default `rel_tol=1e-4` is tight because decompositions are exact identities, so any gap beyond float/`FILTER`/`DISTINCT` noise is a real finding. Its intended home is CI: a hermetic per-PR regression guard plus a live-warehouse nightly drift detector (the run that actually catches drift).

  **Operand units and precision stay the author's responsibility, deliberately.** The check folds the declared operands with the declared operator and has no view of either, and cannot acquire one without expression semantics the semantic layer does not carry. Two ordinary authoring conventions collide there: rate metrics are commonly declared as rounded percentages (`ROUND(100.0 * n / d, 1)`), while `parent = volume × rate` is the canonical `product` decomposition. The scale variant is loud and self-diagnosing — an implied `350,100` against an actual `3,500` needs no help from `reason`. The variant worth documenting is the quiet one: an operand declared at *limited precision* makes the identity approximate **by construction**, and its residual (~0.03% for a rate carried at three decimals) is indistinguishable from the mild ETL drift this check exists to detect. Neither half of the library can close that gap and neither should try — `validate_decompositions()` sees operator, arity, operand resolution and acyclicity, none of which reveal units, and having `reason` volunteer "this looks like a units problem" would be an inference, crossing the never-infer-the-cause boundary the docstring commits to. The lever is already in the signature: `rel_tol` / `abs_tol` are caller-supplied precisely so an identity that is approximate by construction is checked at the precision it actually has. (Reported as #68, out of the #67 pilot; pinned by `TestReconcileOperandUnitsAndPrecision`.)

**Declared attribution convention (v0.43.0+):** A `Decomposition` can declare `convention` (`explicit` | `split_evenly` | `fold_into`) and, for `fold_into`, a `convention_operand` naming which operand absorbs the cross term. This exists because attributing a metric's *change* to its factors has no single right answer: the `ΔC·ΔP` term can be left explicit, split evenly (Shapley), or folded into either factor, and every placement sums to the observed change. A 16-session pilot (#67) found three distinct placements on identical data, a 13.5% span on the headline contribution, and the narrative conclusion flipping between them — with 3 of 16 runs disclosing no convention at all. That is the same species of fact as "active users excludes staff": not derivable from the schema at any level of model intelligence, so it belongs in the contract. The operand is **named rather than positional** because `product` takes two or more operands, so `fold_into_last` would both make operand order load-bearing (it is semantically free for `product` today) and hand the agent an index to count instead of a metric name. `laspeyres` / `paasche` are documented as the analyst-facing names of the two-operand `fold_into` cases, never as schema values — they have no agreed meaning at higher arity, and adopting them would split the vocabulary by operand count. A source-level `decomposition_convention` key (in the YAML source, or the Ossie vendor block) sets a house default, restricted to the conventions that need no operand; it is **resolved onto each cross-term decomposition at load** rather than carried, so the effective value survives `freeze_semantic_source` — which re-serializes from parsed objects — and a frozen contract states its convention outright instead of leaving a consumer to re-derive it. It is deliberately *not* a `SemanticConfig` field: `contract_canonical_bytes` dumps with no `exclude_none`, so a contract-schema key would serialize as `null` for every contract that omits it and move every published ARD digest, and the `_CANONICAL_EXCLUDE` alternative would hide a governance fact from the attestation meant to describe the contract. Both keys are omitted from `dump_semantic_source` and from the tool payloads when unset, so a contract declaring no convention keeps byte-identical canonical bytes. Delivery is through **both** tool channels a decomposition already reaches the agent by — `lookup_metric` and, via `IdentityEdge`, `trace_metric_impacts`; the second is not optional, because that tool's own description tells the agent to "walk 'identity' first to localize the change" for root cause, which *is* the attribution workflow, and handing over the operator without the convention would omit the field governing the answer. This is grounding, not enforcement: an attribution report is prose and never passes a checker the way SQL does. `attribute_change` / `check_attribution` (in `validation/attribution.py`) apply and score a convention as pure arithmetic — values in, contributions out, no adapter, because calendar-vs-cohort window semantics are analytics-domain logic rather than governance. Neither is wired into `create_tools()`: such a tool would fire at the last step to do arithmetic the pilot shows the agent performs correctly (16/16), its only real content is the convention that both channels already deliver, and a tenth tool dilutes attention on the checkers that actually block queries.

**Built-in sources:**

| Source | Reads | Extracts |
|---|---|---|
| `DbtSource` | `manifest.json` | Metrics (+ `meta.tier` / `meta.indicator_kind` / `meta.domains`), models, columns |
| `CubeSource` | Cube meta API or schema files | Metrics (+ `meta.tier` / `meta.indicator_kind` / `meta.domains`), dimensions |
| `OssieSource` | Apache Ossie semantic model (YAML/JSON) | Datasets, fields, relationships, metrics; governance vocabulary from `custom_extensions` |
| `YamlSource` | Inline YAML definitions | Metric / table / relationship / `metric_impacts` definitions for teams not using dbt/Cube |

`MetricDefinition`: `name`, `description`, `sql_expression`, `source_model`, `filters`, `domains`, `tier`, `indicator_kind`, `business_owner`, `operational_owner`, `last_reviewed`, `decompositions`, `drill_by`. `business_owner` / `operational_owner` / `last_reviewed` and `decompositions` / `drill_by` are parsed by `YamlSource`, and by `OssieSource` from its vendor `custom_extensions` block; `DbtSource` / `CubeSource` leave them unset/empty.
`MetricImpact`: `from_metric`, `to_metric`, `direction`, `confidence`, `evidence`, `description`.
`Decomposition`: `operator`, `operands`. `DrillDimension`: `dimension`, `column`. `IdentityEdge`: `from_metric`, `to_metric`, `operator`.
`Relationship`: `from_`, `to`, `type`, `description`, `required_filter`, `preferred`. The `preferred` flag (default `False`) marks the canonical join when alternatives exist between the same table pair. `build_relationship_index` stable-sorts each adjacency list with preferred edges first, so `find_join_path` (BFS) and `get_relationships_for_table` both surface the canonical edge automatically. The flat list returned by `get_relationships()` deliberately keeps declaration order; that list feeds the prompt renderer, which renders `preferred="true"` as a per-edge attribute instead of via reordering.
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
│   │   ├── explain.py           # EXPLAIN adapter orchestration
│   │   ├── examples.py          # Verified-examples corpus: validate_examples + check_example_answers
│   │   ├── reconciliation.py    # reconcile_decomposition (declared identity vs. live data)
│   │   ├── attribution.py       # attribute_change / check_attribution (convention arithmetic)
│   │   └── _scalar.py           # Shared scalar measurement (reconciliation + answer checks)
│   ├── tools/
│   │   ├── __init__.py
│   │   ├── factory.py           # create_tools() — returns 9 tools
│   │   ├── middleware.py        # contract_middleware decorator
│   │   ├── sdk.py               # Claude Agent SDK adapter (create_sdk_mcp_server)
│   │   ├── langchain.py         # LangChain / deepagents adapter (create_langchain_tools)
│   │   └── pydantic_ai.py       # Pydantic AI adapter (create_pydantic_ai_tools; create_pydantic_ai_toolset for one shared Agent across users; contract_run_kwargs for exact per-request budget enforcement)
│   ├── semantic/
│   │   ├── __init__.py
│   │   ├── base.py              # SemanticSource protocol
│   │   ├── dbt.py               # DbtSource
│   │   ├── cube.py              # CubeSource
│   │   ├── ossie.py             # OssieSource (Apache Ossie spec)
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
    "sqlglot>=28.0",   # see the floor comment in pyproject.toml
    "pydantic>=2.0",
    "pyyaml>=6.0",
]

[project.optional-dependencies]
agent-sdk = ["claude-agent-sdk"]
agent-contracts = ["ai-agent-contracts>=0.1.0"]
langchain = ["langchain-core", "langchain", "langgraph"]
pydantic-ai = ["pydantic-ai-slim[anthropic]"]
bigquery = ["google-cloud-bigquery"]
snowflake = ["snowflake-connector-python"]
postgres = ["psycopg2-binary"]
duckdb = ["duckdb"]
all = [
    "agentic-data-contracts[agent-sdk,agent-contracts,langchain,pydantic-ai,bigquery,snowflake,postgres,duckdb]",
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
- **Deterministic named-metric execution (`compose_metric_query`)** — Today the agent reads a metric's `sql_expression` via `lookup_metric`, hand-adapts it, and runs raw SQL through `run_query`. The validation layer catches *unauthorized* SQL (bad tables, forbidden operations, missing filters, `SELECT *`) but **not** *authorized-but-semantically-wrong* SQL: if the agent silently drops an `AND is_staff = false` clause or widens a `> 7d` window while adapting a definition, every checker passes green and the metric is misreported — the "confident wrong answer" failure. A deterministic path would keep the measure/definition **immutable** while letting the agent supply only structured *cuts* (dimensions, filters, grain) that are themselves validated — the Cube/MetricFlow parameterized model. The agent chooses the *what* (which metric, which cuts); it never rewrites the *how* (the measure SQL). The free-form read-and-adapt path stays as the fallback for novel questions with no matching metric.

  Tradeoff (deliberately deferred, not rejected): read-and-adapt is more flexible and its failure rate falls as LLM SQL ability improves. But that improvement shrinks only *composition* risk, not *definition-drift* risk — business definitions like "active users excludes staff" or "activation = first value within 7 days" are **not derivable from the schema at any level of model intelligence**. The semantic layer carries non-inferable facts; deterministic execution keeps those facts non-negotiable. Prioritize when there's demand for guaranteed-consistent metric values (regulatory reporting, exec dashboards) over open-ended exploration.
- **Variance-diagnosis tool (Spec B, out of scope — agent-owned)** — a tool that walks the decomposition, drills by `drill_by`, and attributes a metric's movement across a time window is deliberately *not* a framework concern. The contract already ships the grounding the agent needs (the decomposition identity via `lookup_metric`, the slice hints via `drill_by`, causal candidates via `trace_metric_impacts`); the diagnosis itself is orchestration a capable model performs from those hints, and hardcoding a strategy (identity → drill → influence) into machinery makes a declarative library prescriptive. It also couples the library to time-series execution semantics (calendar vs. cohort windows, seasonality) that are analytics-domain logic, not governance. This is the same boundary drawn for verified examples: the repo is the governance/promotion gate, not the analytics engine — diagnosis stays in the agent. The one piece with genuine framework value, a deterministic variance-attribution kernel (values in → contribution breakdown out, no I/O, no strategy), is a small pure helper that could live here or in the agent's own toolbox.

  **Trigger (revised 2026-08-16).** This was originally gated on the arithmetic "proving to be a real error source in practice" — a condition that cannot fire. Attribution errors do not announce themselves: an agent that folds the `ΔC·ΔP` cross term into the wrong factor returns numbers that are plausible, internally consistent, and confidently presented. Unlike a SQL error (throws) or definition drift (`reconcile_decomposition` catches it), this failure has no surface, so waiting for a bug report shelves the item permanently behind an unfalsifiable condition.

  The observable replacement, which `reconcile_decomposition` can already measure with no new library code — run it at two time points, ask the agent to attribute the movement, then check its claimed contributions against the same `rel_tol`:

  - **Contributions do not sum to the metric's change.** The original concern, now stated so it can be seen. This is a correctness kernel with a fixed default.
  - **They sum, but differ across runs or reports.** The likelier and more consequential failure. The arithmetic is fine; the agent is silently choosing an attribution *convention* — where the cross term goes: left explicit, split evenly (Shapley), or folded into one factor. That makes the convention a non-inferable business fact, and by the same argument this document makes for `compose_metric_query` ("business definitions are not derivable from the schema at any level of model intelligence") it belongs in the contract as declared vocabulary rather than in a helper function.

  Note the two outcomes imply *different artifacts*, and the ~80 lines of arithmetic are not the expensive half — committing to a vocabulary (how a convention is declared, per-metric or per-contract) is, and that half cannot be designed without knowing which failure is real.

  **Resolved (2026-08-18, #67).** The probe was run: 16 sessions, two arms.
  Outcome #1 is **disconfirmed** — 16/16 runs were arithmetically correct, so
  the correctness kernel with a fixed default is not the artifact and that
  branch is closed rather than pending. Outcome #2 **fired** — three distinct
  cross-term placements in the undeclared arm, two in the declared arm, with a
  13.5% span on the headline contribution and the narrative conclusion flipping
  between them. Declaring `decompositions` did not stabilize the convention and
  structurally could not, since it names which factors participate and carries
  nothing about the cross term. v0.43.0 therefore ships the declared vocabulary
  (`convention` / `convention_operand`), delivered through both tool channels an
  attribution question already touches, plus the pure kernel as an importable
  helper. It is **not** wired into `create_tools()`: the tool would fire at the
  last step to do arithmetic the pilot shows the agent performs correctly, and
  its only real content is the convention, which both channels already deliver.
  The diagnosis tool itself stays out of scope, unchanged.
