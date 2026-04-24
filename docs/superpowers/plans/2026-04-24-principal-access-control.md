# Per-Table Principal Access Control Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add optional `allowed_principals` / `blocked_principals` fields on `AllowedTable` so contracts can gate individual tables by caller identity (email, Webex ID, employee number — any opaque string), with identity supplied as either a static string or a zero-arg callable.

**Architecture:** Add two optional fields on `AllowedTable` (mutually exclusive at load time). Thread a new `caller_principal: str | Callable[[], str | None] | None` kwarg through `Validator` and `create_tools`. A new `DataContract.allowed_table_names_for(principal)` method returns the effective allowlist per caller. `TableAllowlistChecker` gains a resolver callback and emits a two-tier error message distinguishing undeclared tables from tables restricted to other principals. Fail-closed when a table declares `*_principals` but no identity is supplied. Unit-test each layer; include one end-to-end Webex-style `contextvars`-based test that flips identity mid-session on a shared validator.

**Tech Stack:** Python 3.12+, Pydantic 2, sqlglot, pytest + pytest-asyncio, DuckDB (integration), uv (runner), ruff + ty (pre-commit).

**Spec:** `docs/superpowers/specs/2026-04-24-principal-access-control-design.md`

---

## File Structure

| Path | Status | Responsibility |
|---|---|---|
| `tests/fixtures/principals_contract.yml` | **new** | Test fixture exercising open / allowlist / blocklist / empty-allowlist modes |
| `src/agentic_data_contracts/core/schema.py` | modify | Add `allowed_principals` and `blocked_principals` fields + mutex validator on `AllowedTable` |
| `src/agentic_data_contracts/core/principal.py` | **new** | `Principal` type alias + `resolve_principal()` helper |
| `src/agentic_data_contracts/core/contract.py` | modify | Add `allowed_table_names_for(principal)` method |
| `src/agentic_data_contracts/validation/checkers.py` | modify | `TableAllowlistChecker` gains resolver arg + two-tier error message |
| `src/agentic_data_contracts/validation/validator.py` | modify | Add `caller_principal` kwarg; wire resolver into `TableAllowlistChecker` at construction |
| `src/agentic_data_contracts/tools/factory.py` | modify | Add `caller_principal` kwarg; principal-aware `describe_table` / `preview_table` |
| `tests/test_core/test_schema_principals.py` | **new** | Pydantic layer tests |
| `tests/test_core/test_principal_resolver.py` | **new** | Resolver helper tests |
| `tests/test_core/test_contract_allowlist_for.py` | **new** | Truth table tests for contract helper |
| `tests/test_validation/test_table_allowlist_checker_principals.py` | **new** | Checker error-message and resolver tests |
| `tests/test_validation/test_validator_principals.py` | **new** | Validator threading + contextvars flip test |
| `tests/test_tools/test_factory_principals.py` | **new** | Factory threading + principal-aware tools |
| `tests/test_tools/test_factory_principals_webex_scenario.py` | **new** | End-to-end Webex scenario |
| `tests/test_tools/test_factory.py` | modify | +1 DuckDB-backed principal test |

---

## Task 1: Add test fixture

**Files:**
- Create: `tests/fixtures/principals_contract.yml`

- [ ] **Step 1: Create the fixture**

Write this exact content to `tests/fixtures/principals_contract.yml`:

```yaml
version: "1.0"
name: principals_test_contract

semantic:
  allowed_tables:
    - schema: analytics
      tables: [orders]
    - schema: hr
      tables: [salaries]
      allowed_principals:
        - alice@co.com
    - schema: raw
      tables: [audit_log]
      blocked_principals:
        - intern@co.com
    - schema: sealed
      tables: [top_secret]
      allowed_principals: []
  forbidden_operations: [DELETE, DROP, INSERT, UPDATE]
```

- [ ] **Step 2: Verify it's valid YAML**

Run: `uv run python -c "import yaml; yaml.safe_load(open('tests/fixtures/principals_contract.yml'))"`
Expected: no output, exit 0.

- [ ] **Step 3: Commit**

```bash
git add tests/fixtures/principals_contract.yml
git commit -m "test(fixtures): add principals_contract.yml for per-table principal tests"
```

---

## Task 2: Extend `AllowedTable` schema with principal fields

**Files:**
- Modify: `src/agentic_data_contracts/core/schema.py` (class `AllowedTable` at lines 23–29)
- Test: `tests/test_core/test_schema_principals.py`

- [ ] **Step 1: Write the failing tests**

Create `tests/test_core/test_schema_principals.py`:

```python
from pathlib import Path

import pytest
import yaml
from pydantic import ValidationError

from agentic_data_contracts.core.schema import AllowedTable, DataContractSchema


def test_accepts_allowed_principals() -> None:
    at = AllowedTable.model_validate(
        {"schema": "hr", "tables": ["salaries"], "allowed_principals": ["alice@co.com"]}
    )
    assert at.allowed_principals == ["alice@co.com"]
    assert at.blocked_principals is None


def test_accepts_blocked_principals() -> None:
    at = AllowedTable.model_validate(
        {"schema": "raw", "tables": ["audit_log"], "blocked_principals": ["evil@co.com"]}
    )
    assert at.blocked_principals == ["evil@co.com"]
    assert at.allowed_principals is None


def test_rejects_both_fields_set() -> None:
    with pytest.raises(ValidationError, match="cannot set both"):
        AllowedTable.model_validate(
            {
                "schema": "hr",
                "tables": ["salaries"],
                "allowed_principals": ["alice@co.com"],
                "blocked_principals": ["evil@co.com"],
            }
        )


def test_defaults_are_none() -> None:
    at = AllowedTable.model_validate({"schema": "analytics", "tables": ["orders"]})
    assert at.allowed_principals is None
    assert at.blocked_principals is None


def test_empty_list_preserved() -> None:
    # Explicitly empty list must stay [] (meaning "nobody"), not become None.
    at = AllowedTable.model_validate(
        {"schema": "sealed", "tables": ["top_secret"], "allowed_principals": []}
    )
    assert at.allowed_principals == []


def test_principals_contract_fixture_loads(fixtures_dir: Path) -> None:
    raw = yaml.safe_load((fixtures_dir / "principals_contract.yml").read_text())
    schema = DataContractSchema.model_validate(raw)
    tables = {at.schema_: at for at in schema.semantic.allowed_tables}
    assert tables["analytics"].allowed_principals is None
    assert tables["analytics"].blocked_principals is None
    assert tables["hr"].allowed_principals == ["alice@co.com"]
    assert tables["raw"].blocked_principals == ["intern@co.com"]
    assert tables["sealed"].allowed_principals == []
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/test_core/test_schema_principals.py -v`
Expected: All tests fail with `AttributeError` or pydantic errors about unknown fields.

- [ ] **Step 3: Modify `AllowedTable` in `src/agentic_data_contracts/core/schema.py`**

Replace the existing `AllowedTable` class (lines 23–29) with:

```python
class AllowedTable(BaseModel):
    schema_: str = Field(alias="schema")
    tables: list[str] = Field(default_factory=list)
    description: str | None = None
    preferred: bool = False
    allowed_principals: list[str] | None = None
    blocked_principals: list[str] | None = None

    model_config = {"populate_by_name": True}

    @model_validator(mode="after")
    def principals_mutually_exclusive(self) -> Self:
        if self.allowed_principals is not None and self.blocked_principals is not None:
            raise ValueError(
                f"AllowedTable for schema '{self.schema_}' cannot set both "
                f"allowed_principals and blocked_principals — pick one"
            )
        return self
```

No other changes in `schema.py` are needed — `model_validator` and `Self` are already imported.

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/test_core/test_schema_principals.py -v`
Expected: 6 passed.

- [ ] **Step 5: Run the full schema suite to confirm no regression**

Run: `uv run pytest tests/test_core/test_schema.py -v`
Expected: all existing tests still pass.

- [ ] **Step 6: Commit**

```bash
git add src/agentic_data_contracts/core/schema.py tests/test_core/test_schema_principals.py
git commit -m "feat(core): add allowed_principals and blocked_principals to AllowedTable"
```

---

## Task 3: Add principal resolver module

**Files:**
- Create: `src/agentic_data_contracts/core/principal.py`
- Test: `tests/test_core/test_principal_resolver.py`

- [ ] **Step 1: Write the failing tests**

Create `tests/test_core/test_principal_resolver.py`:

```python
import pytest

from agentic_data_contracts.core.principal import resolve_principal


def test_none_returns_none() -> None:
    assert resolve_principal(None) is None


def test_static_string_returned() -> None:
    assert resolve_principal("alice@co.com") == "alice@co.com"


def test_empty_string_passes_through() -> None:
    # No silent coercion — empty string is a distinct (non-matching) principal.
    assert resolve_principal("") == ""


def test_callable_returning_string() -> None:
    assert resolve_principal(lambda: "bob@co.com") == "bob@co.com"


def test_callable_returning_none() -> None:
    assert resolve_principal(lambda: None) is None


def test_callable_that_raises_propagates() -> None:
    def broken() -> str:
        raise RuntimeError("identity lookup failed")

    with pytest.raises(RuntimeError, match="identity lookup failed"):
        resolve_principal(broken)
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/test_core/test_principal_resolver.py -v`
Expected: ImportError — `principal` module does not exist.

- [ ] **Step 3: Create `src/agentic_data_contracts/core/principal.py`**

```python
"""Principal resolver — normalizes static strings and zero-arg callables."""

from __future__ import annotations

from typing import Callable, Union

Principal = Union[str, Callable[[], "str | None"], None]


def resolve_principal(p: Principal) -> str | None:
    """Resolve a Principal to its current string value (or None).

    - ``None`` → ``None``
    - ``str`` → returned unchanged (no case normalization, no trimming)
    - ``Callable`` → invoked and its return value returned unchanged

    A callable that raises propagates the exception — broken identity
    wiring should fail loudly, not silently downgrade to "no caller".
    """
    if p is None:
        return None
    if callable(p):
        return p()
    return p
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/test_core/test_principal_resolver.py -v`
Expected: 6 passed.

- [ ] **Step 5: Commit**

```bash
git add src/agentic_data_contracts/core/principal.py tests/test_core/test_principal_resolver.py
git commit -m "feat(core): add principal resolver for static-or-callable identity"
```

---

## Task 4: Add `DataContract.allowed_table_names_for(principal)`

**Files:**
- Modify: `src/agentic_data_contracts/core/contract.py` (add method after `allowed_table_names` at line 69)
- Test: `tests/test_core/test_contract_allowlist_for.py`

- [ ] **Step 1: Write the failing tests**

Create `tests/test_core/test_contract_allowlist_for.py`:

```python
from pathlib import Path

import pytest

from agentic_data_contracts.core.contract import DataContract


@pytest.fixture
def contract(fixtures_dir: Path) -> DataContract:
    return DataContract.from_yaml(fixtures_dir / "principals_contract.yml")


@pytest.mark.parametrize(
    "principal,expected",
    [
        (None, {"analytics.orders"}),
        ("alice@co.com", {"analytics.orders", "hr.salaries", "raw.audit_log"}),
        ("bob@co.com", {"analytics.orders", "raw.audit_log"}),
        ("intern@co.com", {"analytics.orders"}),
        ("", {"analytics.orders"}),
    ],
    ids=["none", "alice-allowed", "bob-neither", "intern-blocked", "empty-string"],
)
def test_allowed_table_names_for(
    contract: DataContract, principal: str | None, expected: set[str]
) -> None:
    assert contract.allowed_table_names_for(principal) == expected


def test_sealed_table_never_accessible(contract: DataContract) -> None:
    # allowed_principals: [] means nobody, no matter who asks.
    for principal in [None, "alice@co.com", "bob@co.com", ""]:
        assert "sealed.top_secret" not in contract.allowed_table_names_for(principal)


def test_unscoped_allowed_table_names_unchanged(contract: DataContract) -> None:
    # The old method returns the full declared union, ignoring principals.
    assert set(contract.allowed_table_names()) == {
        "analytics.orders",
        "hr.salaries",
        "raw.audit_log",
        "sealed.top_secret",
    }
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/test_core/test_contract_allowlist_for.py -v`
Expected: `AttributeError: 'DataContract' object has no attribute 'allowed_table_names_for'`.

- [ ] **Step 3: Add the method to `DataContract`**

In `src/agentic_data_contracts/core/contract.py`, insert this method immediately after the existing `allowed_table_names` method (after line 76):

```python
    def allowed_table_names_for(self, principal: str | None) -> set[str]:
        """Return the set of qualified table names the given principal may access.

        Rules:
        - Table with neither allowed_principals nor blocked_principals → open to all.
        - Table with either field set and principal=None → denied (fail-closed).
        - Table with allowed_principals set → principal must be in the list.
        - Table with blocked_principals set → principal must not be in the list.
        """
        result: set[str] = set()
        for entry in self.schema.semantic.allowed_tables:
            restricted = (
                entry.allowed_principals is not None
                or entry.blocked_principals is not None
            )
            if restricted and principal is None:
                continue
            if (
                entry.allowed_principals is not None
                and principal not in entry.allowed_principals
            ):
                continue
            if (
                entry.blocked_principals is not None
                and principal in entry.blocked_principals
            ):
                continue
            for table in entry.tables:
                if table == "*":
                    continue
                result.add(f"{entry.schema_}.{table}")
        return result
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/test_core/test_contract_allowlist_for.py -v`
Expected: 7 passed (5 parametrized + 2 standalone).

- [ ] **Step 5: Run the full contract suite**

Run: `uv run pytest tests/test_core/test_contract.py -v`
Expected: all existing tests still pass.

- [ ] **Step 6: Commit**

```bash
git add src/agentic_data_contracts/core/contract.py tests/test_core/test_contract_allowlist_for.py
git commit -m "feat(core): add allowed_table_names_for to filter allowlist by principal"
```

---

## Task 5: Update `TableAllowlistChecker` for principal awareness + two-tier error

**Files:**
- Modify: `src/agentic_data_contracts/validation/checkers.py` (class `TableAllowlistChecker` at lines 121–133)
- Test: `tests/test_validation/test_table_allowlist_checker_principals.py`

- [ ] **Step 1: Write the failing tests**

Create `tests/test_validation/test_table_allowlist_checker_principals.py`:

```python
from pathlib import Path
from typing import cast

import pytest
import sqlglot

from agentic_data_contracts.core.contract import DataContract
from agentic_data_contracts.validation.checkers import TableAllowlistChecker


def _parse(sql: str) -> sqlglot.exp.Expression:
    return cast(sqlglot.exp.Expression, sqlglot.parse_one(sql))


@pytest.fixture
def contract(fixtures_dir: Path) -> DataContract:
    return DataContract.from_yaml(fixtures_dir / "principals_contract.yml")


def _checker(principal: str | None) -> TableAllowlistChecker:
    return TableAllowlistChecker(principal_resolver=lambda: principal)


class TestOpenTable:
    def test_no_principal_passes(self, contract: DataContract) -> None:
        ast = _parse("SELECT id FROM analytics.orders")
        assert _checker(None).check_ast(ast, contract).passed

    def test_any_principal_passes(self, contract: DataContract) -> None:
        ast = _parse("SELECT id FROM analytics.orders")
        assert _checker("anyone@co.com").check_ast(ast, contract).passed


class TestAllowlist:
    def test_match_passes(self, contract: DataContract) -> None:
        ast = _parse("SELECT salary FROM hr.salaries")
        assert _checker("alice@co.com").check_ast(ast, contract).passed

    def test_miss_named_caller(self, contract: DataContract) -> None:
        ast = _parse("SELECT salary FROM hr.salaries")
        result = _checker("bob@co.com").check_ast(ast, contract)
        assert not result.passed
        assert "restricted to other principals" in result.message
        assert "caller: 'bob@co.com'" in result.message
        assert "hr.salaries" in result.message

    def test_miss_no_caller(self, contract: DataContract) -> None:
        ast = _parse("SELECT salary FROM hr.salaries")
        result = _checker(None).check_ast(ast, contract)
        assert not result.passed
        assert "caller: '<no caller identified>'" in result.message


class TestBlocklist:
    def test_non_blocked_passes(self, contract: DataContract) -> None:
        ast = _parse("SELECT id FROM raw.audit_log")
        assert _checker("alice@co.com").check_ast(ast, contract).passed

    def test_blocked_caller_denied(self, contract: DataContract) -> None:
        ast = _parse("SELECT id FROM raw.audit_log")
        result = _checker("intern@co.com").check_ast(ast, contract)
        assert not result.passed
        assert "caller: 'intern@co.com'" in result.message


class TestUndeclared:
    def test_undeclared_denied(self, contract: DataContract) -> None:
        ast = _parse("SELECT * FROM nowhere.nothing")
        result = _checker("alice@co.com").check_ast(ast, contract)
        assert not result.passed
        assert "Tables not in allowlist" in result.message
        assert "nowhere.nothing" in result.message


class TestMixedErrors:
    def test_undeclared_and_restricted_both_reported(
        self, contract: DataContract
    ) -> None:
        ast = _parse(
            "SELECT s.salary FROM hr.salaries s "
            "JOIN nowhere.nothing n ON s.id = n.id"
        )
        result = _checker("bob@co.com").check_ast(ast, contract)
        assert not result.passed
        assert "Tables not in allowlist: nowhere.nothing" in result.message
        assert "restricted to other principals" in result.message
        assert "hr.salaries" in result.message


class TestEmptyAllowlist:
    def test_empty_allowlist_denies_everyone(self, contract: DataContract) -> None:
        ast = _parse("SELECT * FROM sealed.top_secret")
        for principal in [None, "alice@co.com", "bob@co.com"]:
            result = _checker(principal).check_ast(ast, contract)
            assert not result.passed


class TestBackwardsCompat:
    def test_no_resolver_behaves_as_before(self, contract: DataContract) -> None:
        """Constructing without a resolver = resolver always returns None.

        Restricted tables are then denied (fail-closed), open tables allowed.
        """
        checker = TableAllowlistChecker()
        assert checker.check_ast(
            _parse("SELECT id FROM analytics.orders"), contract
        ).passed
        assert not checker.check_ast(
            _parse("SELECT salary FROM hr.salaries"), contract
        ).passed
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/test_validation/test_table_allowlist_checker_principals.py -v`
Expected: Tests fail — `TableAllowlistChecker.__init__() got an unexpected keyword argument 'principal_resolver'`.

- [ ] **Step 3: Update `TableAllowlistChecker` in `checkers.py`**

Replace the existing class (lines 121–133) with:

```python
class TableAllowlistChecker:
    """Checks referenced tables against the contract's allowlist, filtered by caller principal.

    The principal_resolver is called at check time (not construction), so a
    single long-lived checker can serve different callers across sequential
    queries (e.g. a Webex bot with one user per message).
    """

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

Add this import at the top of `checkers.py` if `Callable` is not already imported (check existing imports at lines 1–16):

```python
from collections.abc import Callable
```

- [ ] **Step 4: Run the new tests to verify they pass**

Run: `uv run pytest tests/test_validation/test_table_allowlist_checker_principals.py -v`
Expected: 11 passed.

- [ ] **Step 5: Run the existing checker suite to check for regressions**

Run: `uv run pytest tests/test_validation/test_checkers.py -v`
Expected: all existing tests still pass. The existing `TableAllowlistChecker()` usages (with no resolver) continue to work; queries against unrestricted tables pass unchanged.

- [ ] **Step 6: Commit**

```bash
git add src/agentic_data_contracts/validation/checkers.py tests/test_validation/test_table_allowlist_checker_principals.py
git commit -m "feat(validation): principal-aware TableAllowlistChecker with two-tier error"
```

---

## Task 6: Thread `caller_principal` through `Validator`

**Files:**
- Modify: `src/agentic_data_contracts/validation/validator.py` (`Validator.__init__` at lines 59–77 and `_build_checkers` at lines 78–154)
- Test: `tests/test_validation/test_validator_principals.py`

- [ ] **Step 1: Write the failing tests**

Create `tests/test_validation/test_validator_principals.py`:

```python
import contextvars
from pathlib import Path

import pytest

from agentic_data_contracts.core.contract import DataContract
from agentic_data_contracts.validation.validator import Validator


@pytest.fixture
def contract(fixtures_dir: Path) -> DataContract:
    return DataContract.from_yaml(fixtures_dir / "principals_contract.yml")


class TestStaticPrincipal:
    def test_alice_can_query_hr(self, contract: DataContract) -> None:
        v = Validator(contract, caller_principal="alice@co.com")
        assert not v.validate("SELECT salary FROM hr.salaries").blocked

    def test_bob_cannot_query_hr(self, contract: DataContract) -> None:
        v = Validator(contract, caller_principal="bob@co.com")
        result = v.validate("SELECT salary FROM hr.salaries")
        assert result.blocked
        assert any("caller: 'bob@co.com'" in r for r in result.reasons)

    def test_no_caller_cannot_query_restricted(self, contract: DataContract) -> None:
        v = Validator(contract)  # no caller_principal
        result = v.validate("SELECT salary FROM hr.salaries")
        assert result.blocked
        assert any("<no caller identified>" in r for r in result.reasons)

    def test_open_table_always_accessible(self, contract: DataContract) -> None:
        v = Validator(contract)
        assert not v.validate("SELECT id FROM analytics.orders").blocked


class TestCallablePrincipal:
    def test_callable_invoked_per_validate(self, contract: DataContract) -> None:
        """The resolver MUST be called each validate(), not cached at init.

        This is the core Webex scenario: one long-lived validator, different
        users per message, with identity held in a contextvars.ContextVar.
        """
        current: contextvars.ContextVar[str | None] = contextvars.ContextVar(
            "current", default=None
        )
        v = Validator(contract, caller_principal=lambda: current.get())

        current.set("alice@co.com")
        assert not v.validate("SELECT salary FROM hr.salaries").blocked

        current.set("bob@co.com")
        result = v.validate("SELECT salary FROM hr.salaries")
        assert result.blocked
        assert any("caller: 'bob@co.com'" in r for r in result.reasons)

        current.set("alice@co.com")
        assert not v.validate("SELECT salary FROM hr.salaries").blocked

    def test_callable_returning_none_fails_closed(
        self, contract: DataContract
    ) -> None:
        v = Validator(contract, caller_principal=lambda: None)
        assert v.validate("SELECT salary FROM hr.salaries").blocked

    def test_callable_that_raises_propagates(self, contract: DataContract) -> None:
        def broken() -> str:
            raise RuntimeError("boom")

        v = Validator(contract, caller_principal=broken)
        with pytest.raises(RuntimeError, match="boom"):
            v.validate("SELECT salary FROM hr.salaries")
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/test_validation/test_validator_principals.py -v`
Expected: All tests fail — `Validator.__init__() got an unexpected keyword argument 'caller_principal'`.

- [ ] **Step 3: Update `Validator.__init__` and `_build_checkers`**

In `src/agentic_data_contracts/validation/validator.py`:

**(a)** Add this import near the top (with the other imports around lines 6–14):

```python
from agentic_data_contracts.core.principal import Principal, resolve_principal
```

**(b)** Update the `__init__` signature and body. Replace lines 59–77 (the `__init__` method) with:

```python
    def __init__(
        self,
        contract: DataContract,
        dialect: str | None = None,
        explain_adapter: ExplainAdapter | None = None,
        sql_normalizer: SqlNormalizer | None = None,
        semantic_source: SemanticSource | None = None,
        caller_principal: Principal = None,
    ) -> None:
        self.contract = contract
        self.dialect = dialect
        self.explain_adapter = explain_adapter
        self.sql_normalizer = sql_normalizer
        self._caller_principal = caller_principal
        self._relationship_checker: RelationshipChecker | None = None
        if semantic_source is not None:
            rels = semantic_source.get_relationships()
            if rels:
                self._relationship_checker = RelationshipChecker(rels)
        self._build_checkers()
```

**(c)** Update the `TableAllowlistChecker` construction inside `_build_checkers` (currently at lines 81–83). Replace:

```python
        self._table_checker = (
            TableAllowlistChecker() if semantic.allowed_tables else None
        )
```

with:

```python
        self._table_checker = (
            TableAllowlistChecker(
                principal_resolver=lambda: resolve_principal(self._caller_principal)
            )
            if semantic.allowed_tables
            else None
        )
```

- [ ] **Step 4: Run the new tests to verify they pass**

Run: `uv run pytest tests/test_validation/test_validator_principals.py -v`
Expected: 7 passed.

- [ ] **Step 5: Run the full validation suite to check for regressions**

Run: `uv run pytest tests/test_validation/ -v`
Expected: all existing tests still pass. The new `caller_principal` kwarg defaults to `None`, preserving existing behavior for unrestricted contracts; existing contracts with only open tables (no `*_principals`) are unaffected.

- [ ] **Step 6: Commit**

```bash
git add src/agentic_data_contracts/validation/validator.py tests/test_validation/test_validator_principals.py
git commit -m "feat(validation): thread caller_principal through Validator"
```

---

## Task 7: Thread `caller_principal` through `create_tools`

**Files:**
- Modify: `src/agentic_data_contracts/tools/factory.py` (`create_tools` signature at lines 112–118, Validator construction at lines 141–147, `describe_table` at lines 211–230, `preview_table` at lines 233–256)
- Test: `tests/test_tools/test_factory_principals.py`

- [ ] **Step 1: Write the failing tests**

Create `tests/test_tools/test_factory_principals.py`:

```python
import json
from pathlib import Path

import pytest

from agentic_data_contracts.adapters.duckdb import DuckDBAdapter
from agentic_data_contracts.core.contract import DataContract
from agentic_data_contracts.tools.factory import create_tools


@pytest.fixture
def contract(fixtures_dir: Path) -> DataContract:
    return DataContract.from_yaml(fixtures_dir / "principals_contract.yml")


@pytest.fixture
def adapter() -> DuckDBAdapter:
    db = DuckDBAdapter(":memory:")
    db.connection.execute(
        """
        CREATE SCHEMA IF NOT EXISTS analytics;
        CREATE SCHEMA IF NOT EXISTS hr;
        CREATE SCHEMA IF NOT EXISTS raw;
        CREATE SCHEMA IF NOT EXISTS sealed;
        CREATE TABLE analytics.orders (id INTEGER, amount DECIMAL(10,2));
        INSERT INTO analytics.orders VALUES (1, 10.00);
        CREATE TABLE hr.salaries (id INTEGER, salary DECIMAL(10,2));
        INSERT INTO hr.salaries VALUES (1, 100000.00);
        CREATE TABLE raw.audit_log (id INTEGER, event VARCHAR);
        INSERT INTO raw.audit_log VALUES (1, 'login');
        CREATE TABLE sealed.top_secret (id INTEGER, payload VARCHAR);
        INSERT INTO sealed.top_secret VALUES (1, 'classified');
        """
    )
    return db


def _tool(tools: list, name: str):
    return next(t for t in tools if t.name == name).callable


@pytest.mark.asyncio
class TestInspectQueryForwarding:
    async def test_alice_inspect_passes(
        self, contract: DataContract, adapter: DuckDBAdapter
    ) -> None:
        tools = create_tools(
            contract, adapter=adapter, caller_principal="alice@co.com"
        )
        inspect = _tool(tools, "inspect_query")
        body = json.loads(
            (await inspect({"sql": "SELECT salary FROM hr.salaries"}))["content"][0][
                "text"
            ]
        )
        assert body["valid"] is True

    async def test_bob_inspect_blocks_with_caller_in_message(
        self, contract: DataContract, adapter: DuckDBAdapter
    ) -> None:
        tools = create_tools(contract, adapter=adapter, caller_principal="bob@co.com")
        inspect = _tool(tools, "inspect_query")
        body = json.loads(
            (await inspect({"sql": "SELECT salary FROM hr.salaries"}))["content"][0][
                "text"
            ]
        )
        assert body["valid"] is False
        assert any("caller: 'bob@co.com'" in v for v in body["violations"])


@pytest.mark.asyncio
class TestDescribeTable:
    async def test_allowed_principal_succeeds(
        self, contract: DataContract, adapter: DuckDBAdapter
    ) -> None:
        tools = create_tools(
            contract, adapter=adapter, caller_principal="alice@co.com"
        )
        describe = _tool(tools, "describe_table")
        text = (await describe({"schema": "hr", "table": "salaries"}))["content"][0][
            "text"
        ]
        body = json.loads(text)
        assert body["schema"] == "hr"
        assert body["table"] == "salaries"

    async def test_restricted_for_other_principal(
        self, contract: DataContract, adapter: DuckDBAdapter
    ) -> None:
        tools = create_tools(contract, adapter=adapter, caller_principal="bob@co.com")
        describe = _tool(tools, "describe_table")
        text = (await describe({"schema": "hr", "table": "salaries"}))["content"][0][
            "text"
        ]
        assert "restricted" in text
        assert "'bob@co.com'" in text

    async def test_restricted_for_unidentified(
        self, contract: DataContract, adapter: DuckDBAdapter
    ) -> None:
        tools = create_tools(contract, adapter=adapter)
        describe = _tool(tools, "describe_table")
        text = (await describe({"schema": "hr", "table": "salaries"}))["content"][0][
            "text"
        ]
        assert "'<no caller identified>'" in text

    async def test_undeclared_table_unchanged_message(
        self, contract: DataContract, adapter: DuckDBAdapter
    ) -> None:
        tools = create_tools(
            contract, adapter=adapter, caller_principal="alice@co.com"
        )
        describe = _tool(tools, "describe_table")
        text = (await describe({"schema": "nope", "table": "nothing"}))["content"][0][
            "text"
        ]
        assert "not in the allowed tables list" in text


@pytest.mark.asyncio
class TestPreviewTable:
    async def test_allowed_principal_succeeds(
        self, contract: DataContract, adapter: DuckDBAdapter
    ) -> None:
        tools = create_tools(
            contract, adapter=adapter, caller_principal="alice@co.com"
        )
        preview = _tool(tools, "preview_table")
        text = (await preview({"schema": "hr", "table": "salaries"}))["content"][0][
            "text"
        ]
        body = json.loads(text)
        # DuckDB returns Decimal; json.dumps(..., default=str) renders it as a string.
        assert body["rows"][0]["salary"] == "100000.00"

    async def test_restricted_for_other_principal(
        self, contract: DataContract, adapter: DuckDBAdapter
    ) -> None:
        tools = create_tools(contract, adapter=adapter, caller_principal="bob@co.com")
        preview = _tool(tools, "preview_table")
        text = (await preview({"schema": "hr", "table": "salaries"}))["content"][0][
            "text"
        ]
        assert "restricted" in text
        assert "'bob@co.com'" in text


@pytest.mark.asyncio
class TestSemanticToolsUnaffected:
    """Explicit negative tests: metric/domain tools ignore caller_principal."""

    async def test_list_metrics_unaffected(
        self, contract: DataContract, adapter: DuckDBAdapter
    ) -> None:
        # Same call, different principals → same output.
        for principal in ["alice@co.com", "bob@co.com", None]:
            tools = create_tools(
                contract, adapter=adapter, caller_principal=principal
            )
            list_metrics = _tool(tools, "list_metrics")
            text = (await list_metrics({}))["content"][0]["text"]
            # principals_contract.yml has no semantic source → this exact reply.
            assert text == "No semantic source configured."


def test_create_tools_accepts_callable_principal(
    contract: DataContract, adapter: DuckDBAdapter
) -> None:
    # Must accept a zero-arg callable (Webex pattern) without raising.
    tools = create_tools(
        contract, adapter=adapter, caller_principal=lambda: "alice@co.com"
    )
    assert len(tools) == 9
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/test_tools/test_factory_principals.py -v`
Expected: All tests fail — `create_tools() got an unexpected keyword argument 'caller_principal'`.

- [ ] **Step 3: Update `create_tools` in `tools/factory.py`**

**(a)** Add import near the top (with the other core imports around line 10–24):

```python
from agentic_data_contracts.core.principal import Principal, resolve_principal
```

**(b)** Update the `create_tools` signature (lines 112–118). Replace with:

```python
def create_tools(
    contract: DataContract,
    *,
    adapter: DatabaseAdapter | None = None,
    semantic_source: SemanticSource | None = None,
    session: ContractSession | None = None,
    caller_principal: Principal = None,
) -> list[ToolDef]:
```

**(c)** Forward the principal to the Validator. Replace the existing Validator construction (lines 141–147) with:

```python
    validator = Validator(
        contract,
        dialect=dialect,
        explain_adapter=adapter,
        sql_normalizer=sql_normalizer,
        semantic_source=semantic_source,
        caller_principal=caller_principal,
    )
```

**(d)** Replace the body of `describe_table` (the function starting at line 211). The new version adds a principal check between the "not declared" check and the adapter call:

```python
    async def describe_table(args: dict[str, Any]) -> dict[str, Any]:
        schema_name = args.get("schema", "")
        table_name = args.get("table", "")
        qualified = f"{schema_name}.{table_name}"
        if qualified not in contract.allowed_table_names():
            return _text_response(
                f"Table {qualified} is not in the allowed tables list."
            )
        principal = resolve_principal(caller_principal)
        if qualified not in contract.allowed_table_names_for(principal):
            who = principal if principal else "<no caller identified>"
            return _text_response(
                f"Table {qualified} is restricted; not available to {who!r}."
            )
        if adapter is None:
            return _text_response(
                f"No database adapter configured — table description unavailable"
                f" for {qualified}."
            )
        ts = adapter.describe_table(schema_name, table_name)
        cols = [
            {"name": c.name, "type": c.type, "nullable": c.nullable} for c in ts.columns
        ]
        return _text_response(
            json.dumps({"schema": schema_name, "table": table_name, "columns": cols})
        )
```

**(e)** Replace the body of `preview_table` (the function starting at line 233). The new version applies the same principal check:

```python
    async def preview_table(args: dict[str, Any]) -> dict[str, Any]:
        schema = args.get("schema", "")
        table = args.get("table", "")
        try:
            limit = max(1, min(int(args.get("limit", 5)), 100))
        except (ValueError, TypeError):
            limit = 5
        qualified = f"{schema}.{table}"
        if qualified not in contract.allowed_table_names():
            return _text_response(
                f"Table {qualified} is not in the allowed tables list."
            )
        principal = resolve_principal(caller_principal)
        if qualified not in contract.allowed_table_names_for(principal):
            who = principal if principal else "<no caller identified>"
            return _text_response(
                f"Table {qualified} is restricted; not available to {who!r}."
            )
        if adapter is None:
            return _text_response(
                "No database adapter configured — preview unavailable."
            )
        # preview_table intentionally uses SELECT * — it's a discovery tool
        # and the table has already been verified against the allowlist above.
        result = adapter.execute(f"SELECT * FROM {qualified} LIMIT {limit}")
        rows = [dict(zip(result.columns, row)) for row in result.rows]
        return _text_response(
            json.dumps({"schema": schema, "table": table, "rows": rows}, default=str)
        )
```

- [ ] **Step 4: Run the new tests to verify they pass**

Run: `uv run pytest tests/test_tools/test_factory_principals.py -v`
Expected: all passed.

- [ ] **Step 5: Run the full tools suite to check for regressions**

Run: `uv run pytest tests/test_tools/ -v`
Expected: all existing tests still pass. `create_tools()` without `caller_principal` still produces 9 working tools; existing `describe_table` / `preview_table` tests (which use unrestricted tables) are unaffected because the new principal check is skipped when `allowed_table_names_for(None) == allowed_table_names()` for open tables.

- [ ] **Step 6: Commit**

```bash
git add src/agentic_data_contracts/tools/factory.py tests/test_tools/test_factory_principals.py
git commit -m "feat(tools): thread caller_principal through create_tools and allowlist tools"
```

---

## Task 8: End-to-end Webex scenario test

**Files:**
- Create: `tests/test_tools/test_factory_principals_webex_scenario.py`

This test is the design's motivating use case in one file — multiple users flowing through the same long-lived `tools` object via a `contextvars.ContextVar`. It should pass immediately after Task 7 if the implementation is correct.

- [ ] **Step 1: Write the test**

Create `tests/test_tools/test_factory_principals_webex_scenario.py`:

```python
import contextvars
import json
from pathlib import Path

import pytest

from agentic_data_contracts.adapters.duckdb import DuckDBAdapter
from agentic_data_contracts.core.contract import DataContract
from agentic_data_contracts.tools.factory import create_tools


@pytest.fixture
def contract(fixtures_dir: Path) -> DataContract:
    return DataContract.from_yaml(fixtures_dir / "principals_contract.yml")


@pytest.fixture
def adapter() -> DuckDBAdapter:
    db = DuckDBAdapter(":memory:")
    db.connection.execute(
        """
        CREATE SCHEMA IF NOT EXISTS analytics;
        CREATE SCHEMA IF NOT EXISTS hr;
        CREATE SCHEMA IF NOT EXISTS raw;
        CREATE TABLE analytics.orders (id INTEGER);
        INSERT INTO analytics.orders VALUES (1);
        CREATE TABLE hr.salaries (id INTEGER, salary DECIMAL(10,2));
        INSERT INTO hr.salaries VALUES (1, 100000.00);
        CREATE TABLE raw.audit_log (id INTEGER);
        INSERT INTO raw.audit_log VALUES (1);
        """
    )
    return db


@pytest.mark.asyncio
async def test_webex_room_multiple_users_one_tool_instance(
    contract: DataContract, adapter: DuckDBAdapter
) -> None:
    """Simulates a Webex room: one long-lived bot, many users, identity per message.

    This is the canonical scenario the callable-principal design was built for.
    One `create_tools()` call; identity flipped via contextvars between messages;
    per-user access rules apply correctly.
    """
    current_sender: contextvars.ContextVar[str | None] = contextvars.ContextVar(
        "current_sender", default=None
    )

    tools = create_tools(
        contract,
        adapter=adapter,
        caller_principal=lambda: current_sender.get(),
    )
    inspect = next(t for t in tools if t.name == "inspect_query").callable

    # Message 1: alice asks about hr.salaries → allowed.
    current_sender.set("alice@co.com")
    body = json.loads(
        (await inspect({"sql": "SELECT salary FROM hr.salaries"}))["content"][0][
            "text"
        ]
    )
    assert body["valid"] is True, body

    # Message 2: bob asks the same thing → blocked, with bob in message.
    current_sender.set("bob@co.com")
    body = json.loads(
        (await inspect({"sql": "SELECT salary FROM hr.salaries"}))["content"][0][
            "text"
        ]
    )
    assert body["valid"] is False
    assert any("caller: 'bob@co.com'" in v for v in body["violations"])

    # Message 3: intern asks about raw.audit_log → blocked (blocklist hit).
    current_sender.set("intern@co.com")
    body = json.loads(
        (await inspect({"sql": "SELECT id FROM raw.audit_log"}))["content"][0]["text"]
    )
    assert body["valid"] is False
    assert any("caller: 'intern@co.com'" in v for v in body["violations"])

    # Message 4: alice again, audit_log → allowed (not in blocklist).
    current_sender.set("alice@co.com")
    body = json.loads(
        (await inspect({"sql": "SELECT id FROM raw.audit_log"}))["content"][0]["text"]
    )
    assert body["valid"] is True, body

    # Message 5: nobody set — should fail closed on restricted tables.
    current_sender.set(None)
    body = json.loads(
        (await inspect({"sql": "SELECT salary FROM hr.salaries"}))["content"][0][
            "text"
        ]
    )
    assert body["valid"] is False
    assert any("<no caller identified>" in v for v in body["violations"])

    # Open table: works regardless of identity.
    for sender in ["alice@co.com", "bob@co.com", "intern@co.com", None]:
        current_sender.set(sender)
        body = json.loads(
            (await inspect({"sql": "SELECT id FROM analytics.orders"}))["content"][0][
                "text"
            ]
        )
        assert body["valid"] is True, (sender, body)
```

- [ ] **Step 2: Run to verify it passes**

Run: `uv run pytest tests/test_tools/test_factory_principals_webex_scenario.py -v`
Expected: 1 passed.

- [ ] **Step 3: Commit**

```bash
git add tests/test_tools/test_factory_principals_webex_scenario.py
git commit -m "test(tools): end-to-end Webex scenario for callable caller_principal"
```

---

## Task 9: DuckDB-backed integration assertion on `run_query`

Confirms a denied `run_query` never reaches DuckDB (i.e., the blocking happens before `adapter.execute`).

**Files:**
- Modify: `tests/test_tools/test_factory.py` (append to the end of the file)

- [ ] **Step 1: Write the test**

Append to `tests/test_tools/test_factory.py`:

```python
@pytest.mark.asyncio
async def test_run_query_principal_denied_never_hits_database(
    fixtures_dir: Path,
) -> None:
    """A principal-denied query must not reach the database.

    Uses a spy that counts execute() calls on top of DuckDBAdapter; asserts
    that a blocked query leaves the count at zero.
    """
    import json

    import duckdb

    from agentic_data_contracts.adapters.duckdb import DuckDBAdapter
    from agentic_data_contracts.core.contract import DataContract
    from agentic_data_contracts.tools.factory import create_tools

    class SpyAdapter(DuckDBAdapter):
        def __init__(self, path: str) -> None:
            super().__init__(path)
            self.execute_calls: int = 0

        def execute(self, sql: str):  # type: ignore[override]
            self.execute_calls += 1
            return super().execute(sql)

    contract = DataContract.from_yaml(fixtures_dir / "principals_contract.yml")
    db = SpyAdapter(":memory:")
    db.connection.execute(
        "CREATE SCHEMA hr; "
        "CREATE TABLE hr.salaries (id INTEGER, salary DECIMAL(10,2)); "
        "INSERT INTO hr.salaries VALUES (1, 100000.00);"
    )

    tools = create_tools(contract, adapter=db, caller_principal="bob@co.com")
    run_query = next(t for t in tools if t.name == "run_query").callable

    response = await run_query({"sql": "SELECT salary FROM hr.salaries"})
    text = response["content"][0]["text"]

    assert "BLOCKED" in text
    assert "caller: 'bob@co.com'" in text
    assert db.execute_calls == 0, (
        f"Expected 0 execute() calls for a principal-denied query, "
        f"got {db.execute_calls}"
    )
```

- [ ] **Step 2: Run to verify it passes**

Run: `uv run pytest tests/test_tools/test_factory.py::test_run_query_principal_denied_never_hits_database -v`
Expected: 1 passed.

- [ ] **Step 3: Run the full test suite as a final check**

Run: `uv run pytest -v`
Expected: all tests pass (existing + new). Also run pre-commit checks:

```bash
uv run ruff check src/ tests/
uv run ruff format --check src/ tests/
uv run ty check
```

If any of these surface issues, fix and re-run before committing.

- [ ] **Step 4: Commit**

```bash
git add tests/test_tools/test_factory.py
git commit -m "test(tools): assert principal-denied run_query never reaches database"
```

---

## Done-Done Checklist

After all 9 tasks complete:

- [ ] Full suite passes: `uv run pytest -v`
- [ ] Ruff + ty clean: `uv run ruff check src/ tests/ && uv run ty check`
- [ ] Nine commits on the branch, each self-contained
- [ ] Spec's truth table is covered by parametrized tests in `test_contract_allowlist_for.py` and scenario tests in `test_table_allowlist_checker_principals.py`
- [ ] Webex scenario test (Task 8) passes — the design's motivating example works end-to-end
- [ ] DuckDB spy test (Task 9) confirms denied queries never execute
- [ ] No changes to any tool other than `describe_table`, `preview_table`, `inspect_query`, `run_query` — semantic tools (`list_metrics`, `lookup_metric`, `lookup_domain`, `lookup_relationships`, `trace_metric_impacts`) are verifiably unaffected

## Out of Scope (Do Not Implement)

- Wildcard / domain matching on principals (`*@co.com`, regex). Exact string match only.
- Per-rule (non-table) principal scoping.
- Row-level filtering based on principal value.
- Principal-aware filtering of semantic metadata (metrics, domains, relationships).
- An `anonymous_allowed: true` escape hatch for blocklist-without-identification.

Any of these would be separate, future features with their own specs.
