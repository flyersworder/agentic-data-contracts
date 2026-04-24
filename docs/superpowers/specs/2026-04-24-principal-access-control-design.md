# Per-Table Principal Access Control — Design

**Status:** Draft
**Date:** 2026-04-24
**Scope:** Add per-table allow/blocklist gates keyed on an opaque caller principal (email, Webex ID, employee number, etc.) to `agentic-data-contracts`.

## Motivation

Today the library enforces table-level governance that is uniform for all callers of an agent. A contract says *what tables the agent may query* — not *who, among the humans talking to that agent, may query each table*.

Two deployed agent shapes motivate changing this:

1. **Chainlit app** — one authenticated user per session. Want to vary which tables are reachable by user.
2. **Webex room bot** — a single long-lived bot instance receives messages from multiple users in the same room. Each message carries a Webex person identifier. Want to enforce per-sender access on shared tables.

Neither deployment wants row-level security (handled in the warehouse) or per-rule policy variation (over-engineered). They want a simple per-table bouncer keyed on caller identity.

## Non-goals

- Row-level security / value-bound filters per principal (use warehouse RLS)
- Per-rule principal scoping (different `blocked_columns` by user)
- Wildcard/regex matching on principals (e.g. `*@co.com`)
- Hiding semantic metadata (metrics, domains, relationships) per principal
- Auditing or verifying the principal the agent asserts (out of trust scope)

## Design

### 1. Schema changes (`core/schema.py`)

Two new optional fields on `AllowedTable`, mutually exclusive:

```python
class AllowedTable(BaseModel):
    schema_: str = Field(alias="schema")
    tables: list[str] = Field(default_factory=list)
    description: str | None = None
    preferred: bool = False
    allowed_principals: list[str] | None = None   # NEW
    blocked_principals: list[str] | None = None   # NEW

    @model_validator(mode="after")
    def principals_mutually_exclusive(self) -> Self:
        if self.allowed_principals is not None and self.blocked_principals is not None:
            raise ValueError(
                f"AllowedTable for schema '{self.schema_}' cannot set both "
                f"allowed_principals and blocked_principals — pick one"
            )
        return self
```

**Naming rationale.** `principals` over `emails` because the library performs exact string comparison with no email-specific semantics (no MX lookup, no plus-addressing, no lowercasing). "Principal" is the standard access-control term (NIST, AWS IAM, Kerberos) and correctly signals "opaque identifier, compared exactly." This lets Webex numeric IDs, employee numbers, or JWT subject claims coexist with emails under one field.

**Normalization.** None. The caller is responsible for canonicalizing identifiers. If using emails, lowercase on both sides (YAML and the resolver callable); if using opaque IDs, pass them through unchanged. One rule, zero surprises.

**`None` vs `[]` distinction is meaningful.** `None` means "field not provided → no restriction." `[]` means "explicitly empty list → nobody allowed" (for `allowed_principals`). This lets a contract lock a table down to zero people, e.g. during maintenance.

### 2. Principal resolver (new `core/principal.py`)

Shared helper used by both `Validator` and `create_tools`:

```python
from typing import Callable

Principal = str | Callable[[], "str | None"] | None

def resolve_principal(p: Principal) -> str | None:
    if p is None:
        return None
    return p() if callable(p) else p
```

Accepting `str | Callable` in one parameter lets callers pass:

- **Static string** for the Chainlit case (one session = one user)
- **Zero-arg callable** for the Webex case (typically `lambda: ctx.get()` over a `contextvars.ContextVar` set per incoming message)

No protocol, no provider class, no async. A callable that raises propagates the exception — broken identity wiring should fail loudly, not silently downgrade to "access denied."

### 3. Contract helper (`core/contract.py`)

Single source of truth for the effective allowlist:

```python
def allowed_table_names_for(self, principal: str | None) -> set[str]:
    """Tables the given principal may access.

    - Table with no allowed/blocked_principals → open to all (inc. None)
    - Table with either field set → requires identification (principal=None → denied)
    - allowed_principals set → principal must be in the list
    - blocked_principals set → principal must not be in the list
    """
    result: set[str] = set()
    for at in self.schema.semantic.allowed_tables:
        restricted = (
            at.allowed_principals is not None or at.blocked_principals is not None
        )
        if restricted and principal is None:
            continue
        if at.allowed_principals is not None and principal not in at.allowed_principals:
            continue
        if at.blocked_principals is not None and principal in at.blocked_principals:
            continue
        for t in at.tables:
            result.add(f"{at.schema_}.{t}")
    return result
```

The existing `allowed_table_names()` is preserved. It returns the declared union (ignoring principals) and is still useful for catalog-style discovery in error messages. **Rule:** anywhere enforcement happens, use `_for(principal)`; anywhere discovery happens, use the unscoped version.

### 4. Validator threading (`validation/validator.py`)

`Validator.__init__` gains one keyword-only arg:

```python
def __init__(
    self,
    contract: DataContract,
    dialect: str | None = None,
    explain_adapter: ExplainAdapter | None = None,
    sql_normalizer: SqlNormalizer | None = None,
    semantic_source: SemanticSource | None = None,
    caller_principal: Principal = None,   # NEW
) -> None:
    ...
    self._caller_principal = caller_principal
```

No resolver method on the Validator — it just stores the value and forwards to `TableAllowlistChecker` at construction time:

```python
self._table_checker = (
    TableAllowlistChecker(
        principal_resolver=lambda: resolve_principal(self._caller_principal)
    )
    if semantic.allowed_tables
    else None
)
```

**Critical:** the resolver is called on *every* `validate()`, not cached. This is what makes Webex's "different user per message on the same long-lived validator" work.

### 5. `TableAllowlistChecker` (`validation/checkers.py`)

Accepts a resolver callback (default: always `None`) and emits a two-tier error message that distinguishes undeclared tables from tables restricted by principal:

```python
class TableAllowlistChecker:
    def __init__(
        self,
        principal_resolver: Callable[[], str | None] | None = None,
    ) -> None:
        self._resolve = principal_resolver or (lambda: None)

    def check_ast(self, ast: exp.Expression, contract: DataContract) -> CheckResult:
        principal = self._resolve()
        allowed = contract.allowed_table_names_for(principal)
        declared = set(contract.allowed_table_names())
        referenced = extract_tables(ast)

        undeclared = referenced - declared
        restricted = (referenced - allowed) - undeclared

        if not undeclared and not restricted:
            return CheckResult(passed=True, message="")

        parts: list[str] = []
        if undeclared:
            parts.append(f"Tables not in allowlist: {', '.join(sorted(undeclared))}")
        if restricted:
            who = principal if principal else "<no caller identified>"
            parts.append(
                f"Tables restricted to other principals "
                f"(caller: {who!r}): {', '.join(sorted(restricted))}"
            )
        return CheckResult(passed=False, message="; ".join(parts))
```

**Why split the message:** without it, a developer whose contract restricts `hr.salaries` to `alice@` and tests as `bob@` gets `Tables not in allowlist: hr.salaries` while staring at `hr.salaries` right there in the YAML. The split turns ten minutes of confusion into zero.

**Information disclosure.** Telling `bob@` that `hr.salaries` exists but he can't have it is a deliberate choice. In an internal-agent context the contract author and caller are both trusted, and actionable errors beat information-hiding. If this design is later reused in a zero-trust setting, collapsing the two message segments to a uniform "not in allowlist" is a one-line change.

### 6. Tool factory threading (`tools/factory.py`)

`create_tools()` accepts the same kwarg and forwards to the Validator:

```python
def create_tools(
    contract: DataContract,
    *,
    adapter: DatabaseAdapter | None = None,
    semantic_source: SemanticSource | None = None,
    session: ContractSession | None = None,
    caller_principal: Principal = None,   # NEW
) -> list[ToolDef]:
    ...
    validator = Validator(
        contract,
        dialect=dialect,
        explain_adapter=adapter,
        sql_normalizer=sql_normalizer,
        semantic_source=semantic_source,
        caller_principal=caller_principal,
    )
```

Two tools currently bypass the validator and check the allowlist themselves — they need principal-awareness added:

- `describe_table` (currently factory.py:215)
- `preview_table` (currently factory.py:242)

Both are updated to distinguish "not declared" from "declared but restricted":

```python
async def describe_table(args: dict[str, Any]) -> dict[str, Any]:
    qualified = f"{args.get('schema', '')}.{args.get('table', '')}"

    declared = contract.allowed_table_names()
    if qualified not in declared:
        return _text_response(f"Table {qualified} is not in the allowed tables list.")

    principal = resolve_principal(caller_principal)
    if qualified not in contract.allowed_table_names_for(principal):
        who = principal if principal else "<no caller identified>"
        return _text_response(f"Table {qualified} is restricted; not available to {who!r}.")
    # ... rest unchanged
```

Same pattern in `preview_table`.

**Tools that are explicitly NOT principal-aware:** `list_metrics`, `lookup_metric`, `lookup_domain`, `lookup_relationships`, `trace_metric_impacts`. These describe the semantic model, not query data. Hiding metric definitions per-principal is a different feature — if a metric shouldn't be visible to principal X, don't put it in the contract they load.

## Semantics: the fail-closed truth table

| Table declares | Caller resolves to | Outcome |
|---|---|---|
| neither field | anything (inc. `None`) | ✅ allow |
| `allowed_principals: [alice]` | `"alice"` | ✅ allow |
| `allowed_principals: [alice]` | `"bob"` | ❌ deny |
| `allowed_principals: [alice]` | `None` | ❌ deny (fail closed) |
| `allowed_principals: []` | anything (inc. `None`) | ❌ deny (explicitly empty = nobody) |
| `blocked_principals: [evil]` | `"alice"` | ✅ allow |
| `blocked_principals: [evil]` | `"evil"` | ❌ deny |
| `blocked_principals: [evil]` | `None` | ❌ deny (fail closed) |
| both set | — | 💥 error at contract load time |

**Rule of thumb:** "any principal field on a table means this table requires identification." Symmetric for allowlist and blocklist. The uniform rule is both safer (missing `caller_principal` in an integration fails loud) and simpler to document.

**Trade-off accepted.** The alternative — "blocklist with `None` caller → allow because `None ≠ evil`" — is defensible but creates an asymmetry between allow and block, and lets a misconfigured Webex handler silently bypass `blocked_principals` for all users. If someone genuinely wants "deny these specific people but allow anonymous," that's a separate feature (e.g., an explicit `anonymous_allowed: true` flag) not worth building speculatively.

### Resolver edge cases

| Situation | Library behavior |
|---|---|
| `caller_principal` not passed | resolver returns `None` |
| `caller_principal=""` (empty string) | returns `""` — a distinct principal that won't match anything real (no silent coercion) |
| callable returns `None` | resolver returns `None` |
| callable raises | exception propagates — the tool call fails loudly |

## Worked example

```yaml
# contract.yml
allowed_tables:
  - schema: analytics
    tables: [orders]                      # open
  - schema: hr
    tables: [salaries]
    allowed_principals: [alice@co.com]    # allowlist
  - schema: raw
    tables: [audit_log]
    blocked_principals: [intern@co.com]   # blocklist
```

```python
# Chainlit — static principal
tools = create_tools(contract, caller_principal="alice@co.com")
# alice → all three tables reachable.
# Same contract with caller_principal="bob@co.com":
#   bob → orders ✓, audit_log ✓ (not blocked), salaries ✗ ("caller: 'bob@co.com'")

# Webex — dynamic principal via contextvars
import contextvars
current_sender = contextvars.ContextVar("sender", default=None)
tools = create_tools(contract, caller_principal=lambda: current_sender.get())
# Per-message handler sets current_sender before invoking the agent.
# Message from alice → hr.salaries allowed.
# Message from intern → raw.audit_log blocked.

# No principal configured — fail closed on restricted tables
tools = create_tools(contract)
# orders ✓, salaries ✗ ("caller: '<no caller identified>'"), audit_log ✗ (same)
```

**Sample error messages:**

```
BLOCKED — Violations:
- Tables restricted to other principals (caller: 'bob@co.com'): hr.salaries
```

```
BLOCKED — Violations:
- Tables restricted to other principals (caller: '<no caller identified>'): hr.salaries
```

Quoting the caller in the message is what turns Webex debugging from "did the contextvar get set?" guesswork into a one-line answer.

## Testing plan

Tests follow the project's per-layer convention (`tests/test_core/`, `tests/test_validation/`, `tests/test_tools/`) and TDD discipline (write tests first, implement against them).

### New fixture

`tests/fixtures/principals_contract.yml` — one contract exercising all three modes plus `allowed_principals: []` (nobody). Reused across suites.

### `tests/test_core/test_schema_principals.py`

- `AllowedTable` accepts both new fields
- Both set → `ValidationError` at load time
- `None` vs `[]` preserved through round-trip
- Loading `principals_contract.yml` produces the expected model

### `tests/test_core/test_contract_allowlist_for.py`

Parameterized truth table — each row in the semantics table above becomes a pytest case:

```python
@pytest.mark.parametrize("principal,expected", [
    (None,              {"analytics.orders"}),
    ("alice@co.com",    {"analytics.orders", "hr.salaries", "raw.audit_log"}),
    ("bob@co.com",      {"analytics.orders", "raw.audit_log"}),
    ("intern@co.com",   {"analytics.orders"}),
])
def test_allowed_table_names_for(contract, principal, expected): ...
```

Plus: `allowed_table_names()` (unscoped) still returns the declared union for all four tables.

### `tests/test_core/test_principal_resolver.py`

- Static string passes through
- `None` returns `None`
- Callable returning a string returns that string
- Callable returning `None` returns `None`
- Callable that raises propagates (assert with `pytest.raises`)
- Empty string `""` passes through unchanged

### `tests/test_validation/test_table_allowlist_checker_principals.py`

Explicit coverage of the two-tier error message:

| Scenario | Expected |
|---|---|
| Open table, no principal | pass |
| Allowlist match | pass |
| Allowlist miss (named caller) | `caller: 'bob@co.com'` in message |
| Allowlist miss (no caller) | `caller: '<no caller identified>'` |
| Blocklist hit | `caller: 'intern@co.com'` |
| Mixed undeclared + restricted | both segments, joined by `; ` |
| Empty allowlist | always denied |

### `tests/test_validation/test_validator_principals.py`

- `Validator(..., caller_principal="alice@co.com")` allows `SELECT * FROM hr.salaries`
- Same validator denies `SELECT * FROM sealed.top_secret`
- **Critical test:** `Validator(..., caller_principal=lambda: ctx.get())` where a `contextvars.ContextVar` is flipped between two `validate()` calls on the same validator instance, asserting different outcomes. This is the Webex pattern in a single test.

### `tests/test_tools/test_factory_principals.py`

- `create_tools(..., caller_principal=...)` forwards to Validator (black-box: assert `inspect_query` violations contain the caller)
- `describe_table` denies restricted tables per caller
- `preview_table` denies restricted tables per caller
- `list_metrics`, `lookup_metric`, `lookup_domain`, `lookup_relationships`, `trace_metric_impacts` are explicitly unaffected by principal (negative tests)

### `tests/test_tools/test_factory_principals_webex_scenario.py`

End-to-end simulation — the design's motivating example, in one async test:

```python
async def test_multi_user_session_with_contextvar(contract):
    current_sender: ContextVar[str | None] = ContextVar("sender", default=None)
    tools = create_tools(contract, caller_principal=lambda: current_sender.get())
    inspect = next(t for t in tools if t.name == "inspect_query").callable

    current_sender.set("alice@co.com")
    body = json.loads((await inspect({"sql": "SELECT salary FROM hr.salaries"}))["content"][0]["text"])
    assert body["valid"] is True

    current_sender.set("bob@co.com")
    body = json.loads((await inspect({"sql": "SELECT salary FROM hr.salaries"}))["content"][0]["text"])
    assert body["valid"] is False
    assert "caller: 'bob@co.com'" in body["violations"][0]
```

### DuckDB integration

Extend an existing `test_integration_*` file rather than creating a new layer. Add one test that loads `principals_contract.yml` and confirms a denied `run_query` never reaches DuckDB.

### Out of scope for tests

- Fuzzing email shapes (library does exact string match; nothing to fuzz)
- Performance (resolver is O(1); contract helper is O(n_tables))
- Thread/async concurrency of `contextvars` (Python's responsibility)

## Backwards compatibility

- All new fields default to `None` → existing contracts load unchanged.
- `caller_principal` defaults to `None` on both `Validator` and `create_tools` → existing integrations work with no changes.
- `allowed_table_names()` unchanged; new method `allowed_table_names_for()` added alongside.
- Error messages for **unrestricted** tables unchanged. Principal-related message variants only appear when a contract declares `*_principals`.

## Files touched

| File | Change |
|---|---|
| `src/agentic_data_contracts/core/schema.py` | +2 fields on `AllowedTable` + mutex validator |
| `src/agentic_data_contracts/core/principal.py` | **new** — `Principal` type + `resolve_principal()` |
| `src/agentic_data_contracts/core/contract.py` | +`allowed_table_names_for()` |
| `src/agentic_data_contracts/validation/validator.py` | +`caller_principal` kwarg, wire resolver into checker |
| `src/agentic_data_contracts/validation/checkers.py` | `TableAllowlistChecker` accepts resolver + two-tier message |
| `src/agentic_data_contracts/tools/factory.py` | +`caller_principal` kwarg, principal-aware `describe_table` / `preview_table` |
| `tests/fixtures/principals_contract.yml` | **new** fixture |
| `tests/test_core/test_schema_principals.py` | **new** |
| `tests/test_core/test_contract_allowlist_for.py` | **new** |
| `tests/test_core/test_principal_resolver.py` | **new** |
| `tests/test_validation/test_table_allowlist_checker_principals.py` | **new** |
| `tests/test_validation/test_validator_principals.py` | **new** |
| `tests/test_tools/test_factory_principals.py` | **new** |
| `tests/test_tools/test_factory_principals_webex_scenario.py` | **new** |
| an existing `tests/test_integration_*.py` | +1 test case |

Estimated diff size: ~200–300 lines of source + ~400–500 lines of tests.
