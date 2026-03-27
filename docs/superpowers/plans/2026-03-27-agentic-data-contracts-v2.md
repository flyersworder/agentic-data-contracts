# Agentic Data Contracts v2 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a Python library that loads YAML data contracts, validates SQL against them, and provides 10 agent tools for Claude Agent SDK (or any framework), with optional `ai-agent-contracts` integration for formal enforcement.

**Architecture:** Layered library — core (YAML + Pydantic + lightweight enforcement), validation (sqlglot checkers), tools (factory + middleware), semantic (dbt/Cube/YAML sources), adapters (database protocol), bridge (optional ai-agent-contracts compilation). Each layer is independently testable.

**Tech Stack:** Python 3.12+, uv, Pydantic 2, PyYAML, sqlglot, pytest, DuckDB (tests), ruff + ty (linting/types)

---

### Task 1: Project Scaffolding

**Files:**
- Create: `pyproject.toml`
- Create: `src/agentic_data_contracts/__init__.py`
- Create: `src/agentic_data_contracts/core/__init__.py`
- Create: `src/agentic_data_contracts/validation/__init__.py`
- Create: `src/agentic_data_contracts/tools/__init__.py`
- Create: `src/agentic_data_contracts/semantic/__init__.py`
- Create: `src/agentic_data_contracts/adapters/__init__.py`
- Create: `src/agentic_data_contracts/bridge/__init__.py`
- Create: `tests/__init__.py`
- Create: `tests/conftest.py`

- [ ] **Step 1: Initialize uv project**

Run:
```bash
cd /Users/qingye/Documents/agentic-data-contracts
uv init --lib --name agentic-data-contracts
```

- [ ] **Step 2: Configure pyproject.toml**

Replace the generated `pyproject.toml` with:

```toml
[project]
name = "agentic-data-contracts"
version = "0.1.0"
description = "YAML-first data contract governance for AI agents"
readme = "README.md"
requires-python = ">=3.12"
license = "MIT"
dependencies = [
    "sqlglot>=23.0",
    "pydantic>=2.0",
    "pyyaml>=6.0",
]

[project.optional-dependencies]
agent-sdk = ["claude-agent-sdk"]
agent-contracts = ["ai-agent-contracts>=0.2.0"]
bigquery = ["google-cloud-bigquery"]
snowflake = ["snowflake-connector-python"]
postgres = ["psycopg2-binary"]
duckdb = ["duckdb"]
dev = [
    "pytest>=8.0",
    "pytest-asyncio>=0.23",
    "duckdb",
]
all = [
    "agentic-data-contracts[agent-sdk,agent-contracts,bigquery,snowflake,postgres,duckdb]",
]

[build-system]
requires = ["hatchling"]
build-backend = "hatchling.build"

[tool.hatch.build.targets.wheel]
packages = ["src/agentic_data_contracts"]

[tool.pytest.ini_options]
testpaths = ["tests"]
asyncio_mode = "auto"

[tool.ruff]
src = ["src"]
target-version = "py312"

[tool.ruff.lint]
select = ["E", "F", "I", "UP"]

[tool.ty]
src = ["src"]
python-version = "3.12"
```

- [ ] **Step 3: Create package directory structure**

Create all `__init__.py` files for the package:

`src/agentic_data_contracts/__init__.py`:
```python
"""Agentic Data Contracts — YAML-first data contract governance for AI agents."""

from agentic_data_contracts.core.contract import DataContract
from agentic_data_contracts.tools.factory import create_tools
from agentic_data_contracts.tools.middleware import contract_middleware

__all__ = ["DataContract", "create_tools", "contract_middleware"]
```

All other `__init__.py` files (`core/`, `validation/`, `tools/`, `semantic/`, `adapters/`, `bridge/`, `tests/`) are empty files.

`tests/conftest.py`:
```python
from pathlib import Path

import pytest

FIXTURES_DIR = Path(__file__).parent / "fixtures"


@pytest.fixture
def fixtures_dir() -> Path:
    return FIXTURES_DIR
```

- [ ] **Step 4: Create test fixtures**

Create `tests/fixtures/valid_contract.yml`:
```yaml
version: "1.0"
name: revenue-analysis

semantic:
  source:
    type: dbt
    path: "./dbt/manifest.json"
  allowed_tables:
    - schema: analytics
      tables: [orders, customers, subscriptions]
    - schema: raw
      tables: []
  forbidden_operations: [DELETE, DROP, TRUNCATE, UPDATE, INSERT]
  rules:
    - name: tenant_isolation
      description: "All queries must include a WHERE tenant_id = filter"
      enforcement: block
    - name: use_approved_metrics
      description: "Revenue calculations must use the semantic layer definition"
      enforcement: warn
    - name: no_select_star
      description: "Queries must specify explicit columns, no SELECT *"
      enforcement: block

resources:
  cost_limit_usd: 5.00
  max_query_time_seconds: 30
  max_retries: 3
  max_rows_scanned: 1000000
  token_budget: 50000

temporal:
  max_duration_seconds: 300

success_criteria:
  - name: query_uses_semantic_definitions
    weight: 0.4
  - name: results_are_reproducible
    weight: 0.3
  - name: output_includes_methodology
    weight: 0.3
```

Create `tests/fixtures/minimal_contract.yml`:
```yaml
version: "1.0"
name: basic-query

semantic:
  allowed_tables:
    - schema: public
      tables: [users]
  forbidden_operations: [DELETE, DROP]
  rules: []
```

- [ ] **Step 5: Install dependencies**

Run:
```bash
uv sync --all-extras
```

Expected: Resolves and installs all dependencies including dev extras.

- [ ] **Step 6: Verify project structure**

Run:
```bash
uv run python -c "import agentic_data_contracts; print('OK')"
```

Expected: Will fail with ImportError (core.contract doesn't exist yet). That's expected — confirms the package is found but the module is missing.

- [ ] **Step 7: Commit**

```bash
git add pyproject.toml uv.lock src/ tests/
git commit -m "chore: scaffold project with uv, package structure, and test fixtures"
```

---

### Task 2: Core Layer — Pydantic Schema Models

**Files:**
- Create: `src/agentic_data_contracts/core/schema.py`
- Create: `tests/test_core/__init__.py`
- Create: `tests/test_core/test_schema.py`

- [ ] **Step 1: Write the failing tests**

`tests/test_core/test_schema.py`:
```python
from pathlib import Path

import pytest
import yaml

from agentic_data_contracts.core.schema import (
    AllowedTable,
    DataContractSchema,
    ResourceConfig,
    SemanticConfig,
    SemanticRule,
    SemanticSource,
    SuccessCriterionConfig,
    TemporalConfig,
)


def test_full_contract_parses(fixtures_dir: Path) -> None:
    raw = yaml.safe_load((fixtures_dir / "valid_contract.yml").read_text())
    schema = DataContractSchema.model_validate(raw)
    assert schema.name == "revenue-analysis"
    assert schema.version == "1.0"
    assert len(schema.semantic.allowed_tables) == 2
    assert schema.semantic.allowed_tables[0].schema_ == "analytics"
    assert schema.semantic.allowed_tables[0].tables == ["orders", "customers", "subscriptions"]
    assert schema.resources is not None
    assert schema.resources.cost_limit_usd == 5.00
    assert schema.resources.max_retries == 3
    assert schema.temporal is not None
    assert schema.temporal.max_duration_seconds == 300
    assert len(schema.success_criteria) == 3
    assert schema.success_criteria[0].weight == pytest.approx(0.4)


def test_minimal_contract_parses(fixtures_dir: Path) -> None:
    raw = yaml.safe_load((fixtures_dir / "minimal_contract.yml").read_text())
    schema = DataContractSchema.model_validate(raw)
    assert schema.name == "basic-query"
    assert schema.semantic.source is None
    assert schema.resources is None
    assert schema.temporal is None
    assert schema.success_criteria == []


def test_invalid_enforcement_rejected() -> None:
    with pytest.raises(Exception):
        SemanticRule(
            name="bad",
            description="bad rule",
            enforcement="crash",  # type: ignore[arg-type]
        )


def test_enforcement_values() -> None:
    for val in ("block", "warn", "log"):
        rule = SemanticRule(name="test", description="test", enforcement=val)  # type: ignore[arg-type]
        assert rule.enforcement == val


def test_success_criteria_weight_validation() -> None:
    with pytest.raises(Exception):
        SuccessCriterionConfig(name="bad", weight=1.5)
    with pytest.raises(Exception):
        SuccessCriterionConfig(name="bad", weight=-0.1)


def test_allowed_table_empty_tables() -> None:
    t = AllowedTable(schema_="raw", tables=[])
    assert t.tables == []
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/test_core/test_schema.py -v`
Expected: FAIL with `ModuleNotFoundError` — `core.schema` doesn't exist yet.

- [ ] **Step 3: Implement schema models**

`src/agentic_data_contracts/core/schema.py`:
```python
"""Pydantic models for YAML data contract validation."""

from __future__ import annotations

from enum import Enum
from typing import Literal

from pydantic import BaseModel, Field


class Enforcement(str, Enum):
    BLOCK = "block"
    WARN = "warn"
    LOG = "log"


class SemanticSource(BaseModel):
    type: str  # dbt | cube | yaml | custom
    path: str


class AllowedTable(BaseModel):
    schema_: str = Field(alias="schema")
    tables: list[str] = Field(default_factory=list)

    model_config = {"populate_by_name": True}


class SemanticRule(BaseModel):
    name: str
    description: str
    enforcement: Enforcement


class SemanticConfig(BaseModel):
    source: SemanticSource | None = None
    allowed_tables: list[AllowedTable] = Field(default_factory=list)
    forbidden_operations: list[str] = Field(default_factory=list)
    rules: list[SemanticRule] = Field(default_factory=list)


class ResourceConfig(BaseModel):
    cost_limit_usd: float | None = None
    max_query_time_seconds: float | None = None
    max_retries: int | None = None
    max_rows_scanned: int | None = None
    token_budget: int | None = None


class TemporalConfig(BaseModel):
    max_duration_seconds: float | None = None


class SuccessCriterionConfig(BaseModel):
    name: str
    weight: float = Field(ge=0.0, le=1.0, default=1.0)


class DataContractSchema(BaseModel):
    version: str = "1.0"
    name: str
    semantic: SemanticConfig
    resources: ResourceConfig | None = None
    temporal: TemporalConfig | None = None
    success_criteria: list[SuccessCriterionConfig] = Field(default_factory=list)
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/test_core/test_schema.py -v`
Expected: All 6 tests PASS.

- [ ] **Step 5: Commit**

```bash
git add src/agentic_data_contracts/core/schema.py tests/test_core/
git commit -m "feat: add Pydantic schema models for YAML contract validation"
```

---

### Task 3: Core Layer — DataContract Class

**Files:**
- Create: `src/agentic_data_contracts/core/contract.py`
- Create: `tests/test_core/test_contract.py`

- [ ] **Step 1: Write the failing tests**

`tests/test_core/test_contract.py`:
```python
from pathlib import Path

from agentic_data_contracts.core.contract import DataContract


def test_from_yaml(fixtures_dir: Path) -> None:
    dc = DataContract.from_yaml(fixtures_dir / "valid_contract.yml")
    assert dc.name == "revenue-analysis"
    assert len(dc.schema.semantic.allowed_tables) == 2
    assert dc.schema.resources is not None
    assert dc.schema.resources.cost_limit_usd == 5.00


def test_from_yaml_minimal(fixtures_dir: Path) -> None:
    dc = DataContract.from_yaml(fixtures_dir / "minimal_contract.yml")
    assert dc.name == "basic-query"
    assert dc.schema.resources is None


def test_from_yaml_string(fixtures_dir: Path) -> None:
    text = (fixtures_dir / "valid_contract.yml").read_text()
    dc = DataContract.from_yaml_string(text)
    assert dc.name == "revenue-analysis"


def test_to_system_prompt(fixtures_dir: Path) -> None:
    dc = DataContract.from_yaml(fixtures_dir / "valid_contract.yml")
    prompt = dc.to_system_prompt()
    assert "analytics.orders" in prompt
    assert "analytics.customers" in prompt
    assert "DELETE" in prompt
    assert "tenant_isolation" in prompt
    assert "no_select_star" in prompt
    assert "cost_limit_usd" in prompt or "5.0" in prompt


def test_to_system_prompt_composable(fixtures_dir: Path) -> None:
    dc = DataContract.from_yaml(fixtures_dir / "valid_contract.yml")
    user_prompt = "You are an analytics assistant."
    combined = f"{user_prompt}\n\n{dc.to_system_prompt()}"
    assert combined.startswith("You are an analytics assistant.")
    assert "analytics.orders" in combined


def test_allowed_table_names(fixtures_dir: Path) -> None:
    dc = DataContract.from_yaml(fixtures_dir / "valid_contract.yml")
    names = dc.allowed_table_names()
    assert "analytics.orders" in names
    assert "analytics.customers" in names
    assert "analytics.subscriptions" in names
    # raw schema has empty tables, so no raw.* entries
    assert not any(n.startswith("raw.") for n in names)


def test_block_rules(fixtures_dir: Path) -> None:
    dc = DataContract.from_yaml(fixtures_dir / "valid_contract.yml")
    block_rules = dc.block_rules()
    assert len(block_rules) == 2
    names = [r.name for r in block_rules]
    assert "tenant_isolation" in names
    assert "no_select_star" in names


def test_warn_rules(fixtures_dir: Path) -> None:
    dc = DataContract.from_yaml(fixtures_dir / "valid_contract.yml")
    warn_rules = dc.warn_rules()
    assert len(warn_rules) == 1
    assert warn_rules[0].name == "use_approved_metrics"
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/test_core/test_contract.py -v`
Expected: FAIL with `ImportError`.

- [ ] **Step 3: Implement DataContract**

`src/agentic_data_contracts/core/contract.py`:
```python
"""DataContract — loads YAML, provides accessors and system prompt generation."""

from __future__ import annotations

from pathlib import Path

import yaml

from agentic_data_contracts.core.schema import (
    DataContractSchema,
    Enforcement,
    SemanticRule,
)


class DataContract:
    """Main entry point: load a YAML data contract and interact with it."""

    def __init__(self, schema: DataContractSchema) -> None:
        self.schema = schema

    @property
    def name(self) -> str:
        return self.schema.name

    @classmethod
    def from_yaml(cls, path: str | Path) -> DataContract:
        text = Path(path).read_text()
        return cls.from_yaml_string(text)

    @classmethod
    def from_yaml_string(cls, text: str) -> DataContract:
        raw = yaml.safe_load(text)
        schema = DataContractSchema.model_validate(raw)
        return cls(schema=schema)

    def allowed_table_names(self) -> list[str]:
        names: list[str] = []
        for entry in self.schema.semantic.allowed_tables:
            for table in entry.tables:
                names.append(f"{entry.schema_}.{table}")
        return names

    def block_rules(self) -> list[SemanticRule]:
        return [r for r in self.schema.semantic.rules if r.enforcement == Enforcement.BLOCK]

    def warn_rules(self) -> list[SemanticRule]:
        return [r for r in self.schema.semantic.rules if r.enforcement == Enforcement.WARN]

    def log_rules(self) -> list[SemanticRule]:
        return [r for r in self.schema.semantic.rules if r.enforcement == Enforcement.LOG]

    def to_system_prompt(self) -> str:
        sections: list[str] = []
        sections.append("## Data Contract: " + self.name)

        # Allowed tables
        table_names = self.allowed_table_names()
        if table_names:
            sections.append("\n### Allowed Tables\nYou may ONLY query these tables:")
            for name in table_names:
                sections.append(f"- {name}")

        # Forbidden operations
        if self.schema.semantic.forbidden_operations:
            ops = ", ".join(self.schema.semantic.forbidden_operations)
            sections.append(f"\n### Forbidden Operations\nYou must NEVER use: {ops}")

        # Rules
        block = self.block_rules()
        warn = self.warn_rules()
        if block or warn:
            sections.append("\n### Governance Rules")
            for rule in block:
                sections.append(f"- **MUST** (violation blocks execution): {rule.description}")
            for rule in warn:
                sections.append(f"- **SHOULD** (violation produces warning): {rule.description}")

        # Resource limits
        res = self.schema.resources
        if res:
            sections.append("\n### Resource Limits")
            if res.cost_limit_usd is not None:
                sections.append(f"- Max cost: ${res.cost_limit_usd:.2f}")
            if res.max_retries is not None:
                sections.append(f"- Max retries: {res.max_retries}")
            if res.token_budget is not None:
                sections.append(f"- Token budget: {res.token_budget:,}")
            if res.max_query_time_seconds is not None:
                sections.append(f"- Max query time: {res.max_query_time_seconds}s")
            if res.max_rows_scanned is not None:
                sections.append(f"- Max rows scanned: {res.max_rows_scanned:,}")

        # Temporal limits
        if self.schema.temporal and self.schema.temporal.max_duration_seconds:
            sections.append(
                f"\n### Time Limit\n- Max session duration: {self.schema.temporal.max_duration_seconds}s"
            )

        # Semantic source
        if self.schema.semantic.source:
            src = self.schema.semantic.source
            sections.append(
                f"\n### Semantic Source\nConsult {src.path} ({src.type}) for metric definitions before computing metrics."
            )

        return "\n".join(sections)
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/test_core/test_contract.py -v`
Expected: All 8 tests PASS.

- [ ] **Step 5: Commit**

```bash
git add src/agentic_data_contracts/core/contract.py tests/test_core/test_contract.py
git commit -m "feat: add DataContract class with YAML loading and system prompt generation"
```

---

### Task 4: Core Layer — ContractSession (Lightweight Enforcement)

**Files:**
- Create: `src/agentic_data_contracts/core/session.py`
- Create: `tests/test_core/test_session.py`

- [ ] **Step 1: Write the failing tests**

`tests/test_core/test_session.py`:
```python
import time
from pathlib import Path

import pytest

from agentic_data_contracts.core.contract import DataContract
from agentic_data_contracts.core.session import ContractSession, LimitExceededError


def test_session_tracks_retries(fixtures_dir: Path) -> None:
    dc = DataContract.from_yaml(fixtures_dir / "valid_contract.yml")
    session = ContractSession(dc)
    assert session.retries == 0
    session.record_retry()
    session.record_retry()
    assert session.retries == 2


def test_session_blocks_on_max_retries(fixtures_dir: Path) -> None:
    dc = DataContract.from_yaml(fixtures_dir / "valid_contract.yml")
    session = ContractSession(dc)
    # max_retries is 3 in valid_contract.yml
    session.record_retry()
    session.record_retry()
    session.record_retry()
    with pytest.raises(LimitExceededError, match="retries"):
        session.check_limits()


def test_session_tracks_tokens(fixtures_dir: Path) -> None:
    dc = DataContract.from_yaml(fixtures_dir / "valid_contract.yml")
    session = ContractSession(dc)
    session.record_tokens(10000)
    session.record_tokens(20000)
    assert session.tokens_used == 30000


def test_session_blocks_on_token_budget(fixtures_dir: Path) -> None:
    dc = DataContract.from_yaml(fixtures_dir / "valid_contract.yml")
    session = ContractSession(dc)
    # token_budget is 50000
    session.record_tokens(50001)
    with pytest.raises(LimitExceededError, match="token"):
        session.check_limits()


def test_session_tracks_cost(fixtures_dir: Path) -> None:
    dc = DataContract.from_yaml(fixtures_dir / "valid_contract.yml")
    session = ContractSession(dc)
    session.record_cost(2.50)
    session.record_cost(1.50)
    assert session.cost_usd == pytest.approx(4.0)


def test_session_blocks_on_cost_limit(fixtures_dir: Path) -> None:
    dc = DataContract.from_yaml(fixtures_dir / "valid_contract.yml")
    session = ContractSession(dc)
    # cost_limit_usd is 5.00
    session.record_cost(5.01)
    with pytest.raises(LimitExceededError, match="cost"):
        session.check_limits()


def test_session_elapsed_seconds(fixtures_dir: Path) -> None:
    dc = DataContract.from_yaml(fixtures_dir / "valid_contract.yml")
    session = ContractSession(dc)
    assert session.elapsed_seconds >= 0.0


def test_session_no_limits_when_none_configured(fixtures_dir: Path) -> None:
    dc = DataContract.from_yaml(fixtures_dir / "minimal_contract.yml")
    session = ContractSession(dc)
    # No resources configured — nothing should block
    session.record_retry()
    session.record_retry()
    session.record_retry()
    session.record_retry()
    session.record_tokens(999999)
    session.record_cost(999.0)
    session.check_limits()  # Should not raise


def test_session_remaining_budget(fixtures_dir: Path) -> None:
    dc = DataContract.from_yaml(fixtures_dir / "valid_contract.yml")
    session = ContractSession(dc)
    session.record_tokens(10000)
    session.record_cost(1.50)
    session.record_retry()
    info = session.remaining()
    assert info["retries_remaining"] == 2
    assert info["tokens_remaining"] == 40000
    assert info["cost_remaining_usd"] == pytest.approx(3.50)
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/test_core/test_session.py -v`
Expected: FAIL with `ImportError`.

- [ ] **Step 3: Implement ContractSession**

`src/agentic_data_contracts/core/session.py`:
```python
"""ContractSession — lightweight enforcement via counters and timers."""

from __future__ import annotations

import time
from typing import Any

from agentic_data_contracts.core.contract import DataContract


class LimitExceededError(Exception):
    """Raised when a contract resource limit is exceeded."""


class ContractSession:
    """Tracks enforcement state for a single agent run."""

    def __init__(self, contract: DataContract) -> None:
        self.contract = contract
        self.retries: int = 0
        self.tokens_used: int = 0
        self.cost_usd: float = 0.0
        self._start_time: float = time.monotonic()

    @property
    def elapsed_seconds(self) -> float:
        return time.monotonic() - self._start_time

    def record_retry(self) -> None:
        self.retries += 1

    def record_tokens(self, count: int) -> None:
        self.tokens_used += count

    def record_cost(self, amount: float) -> None:
        self.cost_usd += amount

    def check_limits(self) -> None:
        res = self.contract.schema.resources
        if res is None:
            return

        if res.max_retries is not None and self.retries >= res.max_retries:
            raise LimitExceededError(
                f"Max retries exceeded: {self.retries} >= {res.max_retries}"
            )

        if res.token_budget is not None and self.tokens_used > res.token_budget:
            raise LimitExceededError(
                f"Token budget exceeded: {self.tokens_used} > {res.token_budget}"
            )

        if res.cost_limit_usd is not None and self.cost_usd > res.cost_limit_usd:
            raise LimitExceededError(
                f"Cost limit exceeded: ${self.cost_usd:.2f} > ${res.cost_limit_usd:.2f}"
            )

        temporal = self.contract.schema.temporal
        if temporal and temporal.max_duration_seconds is not None:
            if self.elapsed_seconds > temporal.max_duration_seconds:
                raise LimitExceededError(
                    f"Duration exceeded: {self.elapsed_seconds:.1f}s > {temporal.max_duration_seconds}s"
                )

    def remaining(self) -> dict[str, Any]:
        res = self.contract.schema.resources
        result: dict[str, Any] = {
            "elapsed_seconds": round(self.elapsed_seconds, 1),
        }
        if res:
            if res.max_retries is not None:
                result["retries_remaining"] = res.max_retries - self.retries
            if res.token_budget is not None:
                result["tokens_remaining"] = res.token_budget - self.tokens_used
            if res.cost_limit_usd is not None:
                result["cost_remaining_usd"] = res.cost_limit_usd - self.cost_usd

        temporal = self.contract.schema.temporal
        if temporal and temporal.max_duration_seconds is not None:
            result["seconds_remaining"] = round(
                temporal.max_duration_seconds - self.elapsed_seconds, 1
            )
        return result
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/test_core/test_session.py -v`
Expected: All 9 tests PASS.

- [ ] **Step 5: Commit**

```bash
git add src/agentic_data_contracts/core/session.py tests/test_core/test_session.py
git commit -m "feat: add ContractSession for lightweight resource enforcement"
```

---

### Task 5: Validation Layer — Checkers

**Files:**
- Create: `src/agentic_data_contracts/validation/checkers.py`
- Create: `tests/test_validation/__init__.py`
- Create: `tests/test_validation/test_checkers.py`

- [ ] **Step 1: Write the failing tests**

`tests/test_validation/test_checkers.py`:
```python
from pathlib import Path

import pytest

from agentic_data_contracts.core.contract import DataContract
from agentic_data_contracts.validation.checkers import (
    CheckResult,
    NoSelectStarChecker,
    OperationBlocklistChecker,
    RequiredFilterChecker,
    TableAllowlistChecker,
)


@pytest.fixture
def contract(fixtures_dir: Path) -> DataContract:
    return DataContract.from_yaml(fixtures_dir / "valid_contract.yml")


class TestTableAllowlistChecker:
    def test_allowed_table_passes(self, contract: DataContract) -> None:
        result = TableAllowlistChecker().check_sql(
            "SELECT id FROM analytics.orders WHERE tenant_id = 'x'", contract
        )
        assert result.passed

    def test_forbidden_table_blocked(self, contract: DataContract) -> None:
        result = TableAllowlistChecker().check_sql(
            "SELECT id FROM raw.payments", contract
        )
        assert not result.passed
        assert "raw.payments" in result.message

    def test_unknown_table_blocked(self, contract: DataContract) -> None:
        result = TableAllowlistChecker().check_sql(
            "SELECT id FROM secret.data", contract
        )
        assert not result.passed

    def test_subquery_tables_checked(self, contract: DataContract) -> None:
        result = TableAllowlistChecker().check_sql(
            "SELECT * FROM (SELECT id FROM secret.data) t", contract
        )
        assert not result.passed

    def test_join_tables_checked(self, contract: DataContract) -> None:
        result = TableAllowlistChecker().check_sql(
            "SELECT o.id FROM analytics.orders o JOIN analytics.customers c ON o.id = c.id",
            contract,
        )
        assert result.passed

    def test_cte_tables_checked(self, contract: DataContract) -> None:
        result = TableAllowlistChecker().check_sql(
            "WITH cte AS (SELECT id FROM analytics.orders) SELECT id FROM cte",
            contract,
        )
        assert result.passed


class TestOperationBlocklistChecker:
    def test_select_passes(self, contract: DataContract) -> None:
        result = OperationBlocklistChecker().check_sql(
            "SELECT id FROM analytics.orders", contract
        )
        assert result.passed

    def test_delete_blocked(self, contract: DataContract) -> None:
        result = OperationBlocklistChecker().check_sql(
            "DELETE FROM analytics.orders WHERE id = 1", contract
        )
        assert not result.passed
        assert "DELETE" in result.message

    def test_drop_blocked(self, contract: DataContract) -> None:
        result = OperationBlocklistChecker().check_sql(
            "DROP TABLE analytics.orders", contract
        )
        assert not result.passed

    def test_insert_blocked(self, contract: DataContract) -> None:
        result = OperationBlocklistChecker().check_sql(
            "INSERT INTO analytics.orders (id) VALUES (1)", contract
        )
        assert not result.passed

    def test_update_blocked(self, contract: DataContract) -> None:
        result = OperationBlocklistChecker().check_sql(
            "UPDATE analytics.orders SET id = 1", contract
        )
        assert not result.passed


class TestNoSelectStarChecker:
    def test_explicit_columns_pass(self, contract: DataContract) -> None:
        result = NoSelectStarChecker().check_sql(
            "SELECT id, name FROM analytics.orders", contract
        )
        assert result.passed

    def test_select_star_blocked(self, contract: DataContract) -> None:
        result = NoSelectStarChecker().check_sql(
            "SELECT * FROM analytics.orders", contract
        )
        assert not result.passed
        assert "SELECT *" in result.message

    def test_select_star_in_subquery_blocked(self, contract: DataContract) -> None:
        result = NoSelectStarChecker().check_sql(
            "SELECT id FROM (SELECT * FROM analytics.orders) t", contract
        )
        assert not result.passed


class TestRequiredFilterChecker:
    def test_filter_present_passes(self, contract: DataContract) -> None:
        checker = RequiredFilterChecker(required_filters=["tenant_id"])
        result = checker.check_sql(
            "SELECT id FROM analytics.orders WHERE tenant_id = 'acme'", contract
        )
        assert result.passed

    def test_filter_missing_blocked(self, contract: DataContract) -> None:
        checker = RequiredFilterChecker(required_filters=["tenant_id"])
        result = checker.check_sql(
            "SELECT id FROM analytics.orders WHERE id = 1", contract
        )
        assert not result.passed
        assert "tenant_id" in result.message

    def test_no_where_clause_blocked(self, contract: DataContract) -> None:
        checker = RequiredFilterChecker(required_filters=["tenant_id"])
        result = checker.check_sql(
            "SELECT id FROM analytics.orders", contract
        )
        assert not result.passed

    def test_filter_in_subquery_passes(self, contract: DataContract) -> None:
        checker = RequiredFilterChecker(required_filters=["tenant_id"])
        result = checker.check_sql(
            "SELECT id FROM (SELECT id FROM analytics.orders WHERE tenant_id = 'x') t",
            contract,
        )
        assert result.passed
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/test_validation/test_checkers.py -v`
Expected: FAIL with `ImportError`.

- [ ] **Step 3: Implement checkers**

`src/agentic_data_contracts/validation/checkers.py`:
```python
"""Built-in SQL checkers using sqlglot."""

from __future__ import annotations

from dataclasses import dataclass

import sqlglot
from sqlglot import exp

from agentic_data_contracts.core.contract import DataContract


@dataclass
class CheckResult:
    passed: bool
    severity: str  # "block" | "warn" | "log"
    message: str


class TableAllowlistChecker:
    """Checks that all referenced tables are in the contract's allowed_tables."""

    def check_sql(
        self, sql: str, contract: DataContract, dialect: str | None = None
    ) -> CheckResult:
        allowed = set(contract.allowed_table_names())
        try:
            parsed = sqlglot.parse_one(sql, dialect=dialect)
        except sqlglot.errors.ParseError as e:
            return CheckResult(passed=False, severity="block", message=f"SQL parse error: {e}")

        referenced_tables = self._extract_tables(parsed)
        disallowed = referenced_tables - allowed
        if disallowed:
            return CheckResult(
                passed=False,
                severity="block",
                message=f"Tables not in allowlist: {', '.join(sorted(disallowed))}",
            )
        return CheckResult(passed=True, severity="block", message="")

    def _extract_tables(self, expression: exp.Expression) -> set[str]:
        tables: set[str] = set()
        for table in expression.find_all(exp.Table):
            # Skip CTEs (they are not real tables)
            if isinstance(table.parent, exp.CTE):
                continue
            parts = []
            if table.db:
                parts.append(table.db)
            if table.name:
                parts.append(table.name)
            full_name = ".".join(parts)
            # Skip CTE references (tables that match CTE names)
            cte_names = {
                cte.alias for cte in expression.find_all(exp.CTE) if cte.alias
            }
            if full_name and full_name not in cte_names:
                tables.add(full_name)
        return tables


class OperationBlocklistChecker:
    """Checks that the SQL statement type is not in forbidden_operations."""

    _OPERATION_MAP: dict[type[exp.Expression], str] = {
        exp.Delete: "DELETE",
        exp.Drop: "DROP",
        exp.Insert: "INSERT",
        exp.Update: "UPDATE",
    }

    def check_sql(
        self, sql: str, contract: DataContract, dialect: str | None = None
    ) -> CheckResult:
        forbidden = {op.upper() for op in contract.schema.semantic.forbidden_operations}
        try:
            parsed = sqlglot.parse_one(sql, dialect=dialect)
        except sqlglot.errors.ParseError as e:
            return CheckResult(passed=False, severity="block", message=f"SQL parse error: {e}")

        for expr_type, op_name in self._OPERATION_MAP.items():
            if isinstance(parsed, expr_type) and op_name in forbidden:
                return CheckResult(
                    passed=False,
                    severity="block",
                    message=f"Forbidden operation: {op_name}",
                )

        # Check for TRUNCATE (sqlglot may parse as Command)
        if "TRUNCATE" in forbidden and isinstance(parsed, exp.Command):
            if parsed.this and str(parsed.this).upper() == "TRUNCATE":
                return CheckResult(
                    passed=False,
                    severity="block",
                    message="Forbidden operation: TRUNCATE",
                )

        return CheckResult(passed=True, severity="block", message="")


class NoSelectStarChecker:
    """Checks that no SELECT * appears anywhere in the query."""

    def check_sql(
        self, sql: str, contract: DataContract, dialect: str | None = None
    ) -> CheckResult:
        try:
            parsed = sqlglot.parse_one(sql, dialect=dialect)
        except sqlglot.errors.ParseError as e:
            return CheckResult(passed=False, severity="block", message=f"SQL parse error: {e}")

        for star in parsed.find_all(exp.Star):
            return CheckResult(
                passed=False,
                severity="block",
                message="SELECT * is not allowed — specify explicit columns",
            )
        return CheckResult(passed=True, severity="block", message="")


class RequiredFilterChecker:
    """Checks that required WHERE filters are present in the query."""

    def __init__(self, required_filters: list[str]) -> None:
        self.required_filters = required_filters

    def check_sql(
        self, sql: str, contract: DataContract, dialect: str | None = None
    ) -> CheckResult:
        try:
            parsed = sqlglot.parse_one(sql, dialect=dialect)
        except sqlglot.errors.ParseError as e:
            return CheckResult(passed=False, severity="block", message=f"SQL parse error: {e}")

        # Collect all column references in WHERE clauses
        where_columns: set[str] = set()
        for where in parsed.find_all(exp.Where):
            for col in where.find_all(exp.Column):
                where_columns.add(col.name.lower())

        missing = [f for f in self.required_filters if f.lower() not in where_columns]
        if missing:
            return CheckResult(
                passed=False,
                severity="block",
                message=f"Missing required filters: {', '.join(missing)}",
            )
        return CheckResult(passed=True, severity="block", message="")
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/test_validation/test_checkers.py -v`
Expected: All 17 tests PASS.

- [ ] **Step 5: Commit**

```bash
git add src/agentic_data_contracts/validation/checkers.py tests/test_validation/
git commit -m "feat: add SQL validation checkers (table allowlist, operation blocklist, required filters, no SELECT *)"
```

---

### Task 6: Validation Layer — Validator Orchestrator

**Files:**
- Create: `src/agentic_data_contracts/validation/validator.py`
- Create: `tests/test_validation/test_validator.py`

- [ ] **Step 1: Write the failing tests**

`tests/test_validation/test_validator.py`:
```python
from pathlib import Path

import pytest

from agentic_data_contracts.core.contract import DataContract
from agentic_data_contracts.validation.validator import ValidationResult, Validator


@pytest.fixture
def contract(fixtures_dir: Path) -> DataContract:
    return DataContract.from_yaml(fixtures_dir / "valid_contract.yml")


@pytest.fixture
def validator(contract: DataContract) -> Validator:
    return Validator(contract)


def test_valid_query_passes(validator: Validator) -> None:
    result = validator.validate(
        "SELECT id, amount FROM analytics.orders WHERE tenant_id = 'acme'"
    )
    assert not result.blocked
    assert result.reasons == []


def test_forbidden_table_blocks(validator: Validator) -> None:
    result = validator.validate("SELECT id FROM raw.payments WHERE tenant_id = 'x'")
    assert result.blocked
    assert any("raw.payments" in r for r in result.reasons)


def test_select_star_blocks(validator: Validator) -> None:
    result = validator.validate(
        "SELECT * FROM analytics.orders WHERE tenant_id = 'x'"
    )
    assert result.blocked
    assert any("SELECT *" in r for r in result.reasons)


def test_missing_filter_blocks(validator: Validator) -> None:
    result = validator.validate("SELECT id FROM analytics.orders")
    assert result.blocked
    assert any("tenant_id" in r for r in result.reasons)


def test_delete_blocks(validator: Validator) -> None:
    result = validator.validate("DELETE FROM analytics.orders WHERE tenant_id = 'x'")
    assert result.blocked
    assert any("DELETE" in r for r in result.reasons)


def test_multiple_violations_all_reported(validator: Validator) -> None:
    result = validator.validate("SELECT * FROM raw.payments")
    assert result.blocked
    # Should report multiple issues: forbidden table, SELECT *, missing filter
    assert len(result.reasons) >= 2


def test_warnings_returned(validator: Validator) -> None:
    result = validator.validate(
        "SELECT id FROM analytics.orders WHERE tenant_id = 'x'"
    )
    assert not result.blocked
    # The warn rules (use_approved_metrics) produce warnings via system prompt,
    # not via static validation — so no warnings from the validator here.
    # Warnings come from EXPLAIN layer if configured.
    assert result.warnings == []


def test_minimal_contract_permissive(fixtures_dir: Path) -> None:
    dc = DataContract.from_yaml(fixtures_dir / "minimal_contract.yml")
    validator = Validator(dc)
    result = validator.validate("SELECT * FROM public.users")
    # minimal contract has no rules, so SELECT * and no filter is fine
    assert not result.blocked
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/test_validation/test_validator.py -v`
Expected: FAIL with `ImportError`.

- [ ] **Step 3: Implement Validator**

`src/agentic_data_contracts/validation/validator.py`:
```python
"""Validator — orchestrates checkers and aggregates results."""

from __future__ import annotations

from dataclasses import dataclass, field

from agentic_data_contracts.core.contract import DataContract
from agentic_data_contracts.core.schema import Enforcement
from agentic_data_contracts.validation.checkers import (
    CheckResult,
    NoSelectStarChecker,
    OperationBlocklistChecker,
    RequiredFilterChecker,
    TableAllowlistChecker,
)


@dataclass
class ValidationResult:
    blocked: bool
    reasons: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)
    log_messages: list[str] = field(default_factory=list)


class Validator:
    """Runs all applicable checkers against a SQL query."""

    def __init__(
        self, contract: DataContract, dialect: str | None = None
    ) -> None:
        self.contract = contract
        self.dialect = dialect
        self._checkers = self._build_checkers()

    def _build_checkers(self) -> list[tuple[str, object]]:
        checkers: list[tuple[str, object]] = []
        semantic = self.contract.schema.semantic

        if semantic.allowed_tables:
            checkers.append(("block", TableAllowlistChecker()))

        if semantic.forbidden_operations:
            checkers.append(("block", OperationBlocklistChecker()))

        # Build required filters from block rules that mention filter patterns
        required_filters: list[str] = []
        for rule in self.contract.block_rules():
            name_lower = rule.name.lower()
            # Convention: rules named *_isolation with a column name prefix
            # are treated as required filter rules
            if "isolation" in name_lower or "filter" in name_lower:
                # Extract the column name from the rule name (e.g., tenant_isolation -> tenant_id)
                # Or from description if it mentions a column
                col = self._extract_filter_column(rule.description)
                if col:
                    required_filters.append(col)

        if required_filters:
            checkers.append(("block", RequiredFilterChecker(required_filters=required_filters)))

        # Check if no_select_star rule exists
        for rule in self.contract.schema.semantic.rules:
            if "select_star" in rule.name.lower() or "select *" in rule.description.lower():
                checkers.append((rule.enforcement.value, NoSelectStarChecker()))
                break

        return checkers

    def _extract_filter_column(self, description: str) -> str | None:
        """Extract column name from rule description like 'must filter by tenant_id'."""
        import re

        # Look for common patterns: "filter by X", "WHERE X =", "include X"
        patterns = [
            r"filter\s+(?:by\s+)?(\w+)",
            r"WHERE\s+(\w+)\s*=",
            r"include\s+(?:a\s+)?(?:WHERE\s+)?(\w+)",
        ]
        for pattern in patterns:
            match = re.search(pattern, description, re.IGNORECASE)
            if match:
                return match.group(1)
        return None

    def validate(self, sql: str) -> ValidationResult:
        reasons: list[str] = []
        warnings: list[str] = []
        log_messages: list[str] = []

        for severity, checker in self._checkers:
            result: CheckResult = checker.check_sql(sql, self.contract, self.dialect)  # type: ignore[union-attr]
            if not result.passed:
                if severity == "block":
                    reasons.append(result.message)
                elif severity == "warn":
                    warnings.append(result.message)
                else:
                    log_messages.append(result.message)

        return ValidationResult(
            blocked=len(reasons) > 0,
            reasons=reasons,
            warnings=warnings,
            log_messages=log_messages,
        )
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/test_validation/test_validator.py -v`
Expected: All 8 tests PASS.

- [ ] **Step 5: Commit**

```bash
git add src/agentic_data_contracts/validation/validator.py tests/test_validation/test_validator.py
git commit -m "feat: add Validator orchestrator for aggregating checker results"
```

---

### Task 7: Validation Layer — EXPLAIN Adapters

**Files:**
- Create: `src/agentic_data_contracts/validation/explain.py`
- Create: `tests/test_validation/test_explain.py`

- [ ] **Step 1: Write the failing tests**

`tests/test_validation/test_explain.py`:
```python
from agentic_data_contracts.validation.explain import ExplainAdapter, ExplainResult


def test_explain_result_creation() -> None:
    result = ExplainResult(
        estimated_cost_usd=1.50,
        estimated_rows=50000,
        schema_valid=True,
        errors=[],
    )
    assert result.estimated_cost_usd == 1.50
    assert result.estimated_rows == 50000
    assert result.schema_valid
    assert result.errors == []


def test_explain_result_with_errors() -> None:
    result = ExplainResult(
        estimated_cost_usd=None,
        estimated_rows=None,
        schema_valid=False,
        errors=["Column 'foo' not found"],
    )
    assert not result.schema_valid
    assert len(result.errors) == 1


def test_explain_adapter_is_protocol() -> None:
    """ExplainAdapter is a Protocol — verify it's importable and usable as a type hint."""

    class FakeAdapter:
        def explain(self, sql: str) -> ExplainResult:
            return ExplainResult(
                estimated_cost_usd=0.01,
                estimated_rows=100,
                schema_valid=True,
                errors=[],
            )

    adapter: ExplainAdapter = FakeAdapter()
    result = adapter.explain("SELECT 1")
    assert result.schema_valid
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/test_validation/test_explain.py -v`
Expected: FAIL with `ImportError`.

- [ ] **Step 3: Implement EXPLAIN types**

`src/agentic_data_contracts/validation/explain.py`:
```python
"""EXPLAIN dry-run types and protocol."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Protocol, runtime_checkable


@dataclass
class ExplainResult:
    estimated_cost_usd: float | None
    estimated_rows: int | None
    schema_valid: bool
    errors: list[str] = field(default_factory=list)


@runtime_checkable
class ExplainAdapter(Protocol):
    def explain(self, sql: str) -> ExplainResult: ...
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/test_validation/test_explain.py -v`
Expected: All 3 tests PASS.

- [ ] **Step 5: Commit**

```bash
git add src/agentic_data_contracts/validation/explain.py tests/test_validation/test_explain.py
git commit -m "feat: add EXPLAIN adapter protocol and ExplainResult dataclass"
```

---

### Task 8: Database Adapters — Protocol and DuckDB

**Files:**
- Create: `src/agentic_data_contracts/adapters/base.py`
- Create: `src/agentic_data_contracts/adapters/duckdb.py`
- Create: `tests/test_adapters/__init__.py`
- Create: `tests/test_adapters/test_duckdb.py`

- [ ] **Step 1: Write the failing tests**

`tests/test_adapters/test_duckdb.py`:
```python
import pytest

from agentic_data_contracts.adapters.base import (
    Column,
    DatabaseAdapter,
    QueryResult,
    TableSchema,
)
from agentic_data_contracts.adapters.duckdb import DuckDBAdapter


@pytest.fixture
def adapter() -> DuckDBAdapter:
    db = DuckDBAdapter(":memory:")
    db.connection.execute(
        """
        CREATE SCHEMA IF NOT EXISTS analytics;
        CREATE TABLE analytics.orders (
            id INTEGER,
            amount DECIMAL(10,2),
            tenant_id VARCHAR
        );
        INSERT INTO analytics.orders VALUES (1, 100.00, 'acme'), (2, 200.00, 'acme');
        """
    )
    return db


def test_adapter_implements_protocol(adapter: DuckDBAdapter) -> None:
    assert isinstance(adapter, DatabaseAdapter)


def test_dialect(adapter: DuckDBAdapter) -> None:
    assert adapter.dialect == "duckdb"


def test_execute(adapter: DuckDBAdapter) -> None:
    result = adapter.execute("SELECT id, amount FROM analytics.orders ORDER BY id")
    assert isinstance(result, QueryResult)
    assert len(result.rows) == 2
    assert result.columns == ["id", "amount"]
    assert result.rows[0][0] == 1


def test_explain(adapter: DuckDBAdapter) -> None:
    result = adapter.explain("SELECT id FROM analytics.orders")
    assert result.schema_valid
    assert result.errors == []


def test_explain_invalid_sql(adapter: DuckDBAdapter) -> None:
    result = adapter.explain("SELECT nonexistent FROM analytics.orders")
    assert not result.schema_valid
    assert len(result.errors) > 0


def test_describe_table(adapter: DuckDBAdapter) -> None:
    schema = adapter.describe_table("analytics", "orders")
    assert isinstance(schema, TableSchema)
    assert len(schema.columns) == 3
    col_names = [c.name for c in schema.columns]
    assert "id" in col_names
    assert "amount" in col_names
    assert "tenant_id" in col_names


def test_describe_table_types(adapter: DuckDBAdapter) -> None:
    schema = adapter.describe_table("analytics", "orders")
    col_map = {c.name: c for c in schema.columns}
    assert "INTEGER" in col_map["id"].type.upper()
    assert "VARCHAR" in col_map["tenant_id"].type.upper()
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/test_adapters/test_duckdb.py -v`
Expected: FAIL with `ImportError`.

- [ ] **Step 3: Implement adapter base and DuckDB adapter**

`src/agentic_data_contracts/adapters/base.py`:
```python
"""Database adapter protocol and shared types."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Protocol, runtime_checkable

from agentic_data_contracts.validation.explain import ExplainResult


@dataclass
class Column:
    name: str
    type: str
    description: str = ""
    nullable: bool = True


@dataclass
class TableSchema:
    columns: list[Column] = field(default_factory=list)


@dataclass
class QueryResult:
    columns: list[str]
    rows: list[tuple[Any, ...]]
    row_count: int = 0

    def __post_init__(self) -> None:
        if self.row_count == 0:
            self.row_count = len(self.rows)


@runtime_checkable
class DatabaseAdapter(Protocol):
    def execute(self, sql: str) -> QueryResult: ...
    def explain(self, sql: str) -> ExplainResult: ...
    def describe_table(self, schema: str, table: str) -> TableSchema: ...
    @property
    def dialect(self) -> str: ...
```

`src/agentic_data_contracts/adapters/duckdb.py`:
```python
"""DuckDB database adapter."""

from __future__ import annotations

from functools import lru_cache

import duckdb

from agentic_data_contracts.adapters.base import Column, QueryResult, TableSchema
from agentic_data_contracts.validation.explain import ExplainResult


class DuckDBAdapter:
    """Database adapter for DuckDB."""

    def __init__(self, database: str = ":memory:") -> None:
        self.connection = duckdb.connect(database)

    @property
    def dialect(self) -> str:
        return "duckdb"

    def execute(self, sql: str) -> QueryResult:
        result = self.connection.execute(sql)
        columns = [desc[0] for desc in result.description]
        rows = result.fetchall()
        return QueryResult(columns=columns, rows=rows)

    def explain(self, sql: str) -> ExplainResult:
        try:
            self.connection.execute(f"EXPLAIN {sql}")
            return ExplainResult(
                estimated_cost_usd=None,
                estimated_rows=None,
                schema_valid=True,
                errors=[],
            )
        except duckdb.Error as e:
            return ExplainResult(
                estimated_cost_usd=None,
                estimated_rows=None,
                schema_valid=False,
                errors=[str(e)],
            )

    def describe_table(self, schema: str, table: str) -> TableSchema:
        rows = self.connection.execute(
            """
            SELECT column_name, data_type, is_nullable
            FROM information_schema.columns
            WHERE table_schema = ? AND table_name = ?
            ORDER BY ordinal_position
            """,
            [schema, table],
        ).fetchall()
        columns = [
            Column(
                name=row[0],
                type=row[1],
                nullable=row[2] == "YES",
            )
            for row in rows
        ]
        return TableSchema(columns=columns)
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/test_adapters/test_duckdb.py -v`
Expected: All 7 tests PASS.

- [ ] **Step 5: Commit**

```bash
git add src/agentic_data_contracts/adapters/ tests/test_adapters/
git commit -m "feat: add DatabaseAdapter protocol and DuckDB implementation"
```

---

### Task 9: Semantic Layer — Protocol and YamlSource

**Files:**
- Create: `src/agentic_data_contracts/semantic/base.py`
- Create: `src/agentic_data_contracts/semantic/yaml_source.py`
- Create: `tests/test_semantic/__init__.py`
- Create: `tests/test_semantic/test_yaml_source.py`
- Create: `tests/fixtures/semantic_source.yml`

- [ ] **Step 1: Create the semantic YAML fixture**

`tests/fixtures/semantic_source.yml`:
```yaml
metrics:
  - name: total_revenue
    description: "Total revenue from completed orders"
    sql_expression: "SUM(amount) FILTER (WHERE status = 'completed')"
    source_model: analytics.orders
    filters:
      - "status = 'completed'"

  - name: active_customers
    description: "Count of customers with at least one order in the last 90 days"
    sql_expression: "COUNT(DISTINCT customer_id)"
    source_model: analytics.customers
    filters:
      - "last_order_date >= CURRENT_DATE - INTERVAL '90 days'"

tables:
  - schema: analytics
    table: orders
    columns:
      - name: id
        type: INTEGER
        description: "Primary key"
      - name: amount
        type: DECIMAL
        description: "Order total in USD"
      - name: tenant_id
        type: VARCHAR
        description: "Tenant identifier for multi-tenancy"
      - name: status
        type: VARCHAR
        description: "Order status: pending, completed, cancelled"
```

- [ ] **Step 2: Write the failing tests**

`tests/test_semantic/test_yaml_source.py`:
```python
from pathlib import Path

import pytest

from agentic_data_contracts.semantic.base import MetricDefinition, SemanticSource
from agentic_data_contracts.semantic.yaml_source import YamlSource


@pytest.fixture
def source(fixtures_dir: Path) -> YamlSource:
    return YamlSource(fixtures_dir / "semantic_source.yml")


def test_source_implements_protocol(source: YamlSource) -> None:
    assert isinstance(source, SemanticSource)


def test_get_metrics(source: YamlSource) -> None:
    metrics = source.get_metrics()
    assert len(metrics) == 2
    names = [m.name for m in metrics]
    assert "total_revenue" in names
    assert "active_customers" in names


def test_get_metric(source: YamlSource) -> None:
    metric = source.get_metric("total_revenue")
    assert metric is not None
    assert metric.name == "total_revenue"
    assert "SUM(amount)" in metric.sql_expression
    assert metric.source_model == "analytics.orders"


def test_get_metric_not_found(source: YamlSource) -> None:
    metric = source.get_metric("nonexistent")
    assert metric is None


def test_get_table_schema(source: YamlSource) -> None:
    schema = source.get_table_schema("analytics", "orders")
    assert schema is not None
    assert len(schema.columns) == 4
    col_names = [c.name for c in schema.columns]
    assert "id" in col_names
    assert "amount" in col_names


def test_get_table_schema_not_found(source: YamlSource) -> None:
    schema = source.get_table_schema("analytics", "nonexistent")
    assert schema is None
```

- [ ] **Step 3: Run tests to verify they fail**

Run: `uv run pytest tests/test_semantic/test_yaml_source.py -v`
Expected: FAIL with `ImportError`.

- [ ] **Step 4: Implement semantic base and YamlSource**

`src/agentic_data_contracts/semantic/base.py`:
```python
"""Semantic source protocol and shared types."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Protocol, runtime_checkable

from agentic_data_contracts.adapters.base import Column, TableSchema


@dataclass
class MetricDefinition:
    name: str
    description: str
    sql_expression: str
    source_model: str = ""
    filters: list[str] = field(default_factory=list)


@runtime_checkable
class SemanticSource(Protocol):
    def get_metrics(self) -> list[MetricDefinition]: ...
    def get_metric(self, name: str) -> MetricDefinition | None: ...
    def get_table_schema(self, schema: str, table: str) -> TableSchema | None: ...
```

`src/agentic_data_contracts/semantic/yaml_source.py`:
```python
"""YAML-based semantic source for teams not using dbt or Cube."""

from __future__ import annotations

from pathlib import Path

import yaml

from agentic_data_contracts.adapters.base import Column, TableSchema
from agentic_data_contracts.semantic.base import MetricDefinition


class YamlSource:
    """Loads metric and table definitions from a YAML file."""

    def __init__(self, path: str | Path) -> None:
        raw = yaml.safe_load(Path(path).read_text())
        self._metrics = [
            MetricDefinition(
                name=m["name"],
                description=m.get("description", ""),
                sql_expression=m.get("sql_expression", ""),
                source_model=m.get("source_model", ""),
                filters=m.get("filters", []),
            )
            for m in raw.get("metrics", [])
        ]
        self._tables: dict[str, TableSchema] = {}
        for t in raw.get("tables", []):
            key = f"{t['schema']}.{t['table']}"
            self._tables[key] = TableSchema(
                columns=[
                    Column(
                        name=c["name"],
                        type=c.get("type", ""),
                        description=c.get("description", ""),
                    )
                    for c in t.get("columns", [])
                ]
            )

    def get_metrics(self) -> list[MetricDefinition]:
        return list(self._metrics)

    def get_metric(self, name: str) -> MetricDefinition | None:
        for m in self._metrics:
            if m.name == name:
                return m
        return None

    def get_table_schema(self, schema: str, table: str) -> TableSchema | None:
        return self._tables.get(f"{schema}.{table}")
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `uv run pytest tests/test_semantic/test_yaml_source.py -v`
Expected: All 6 tests PASS.

- [ ] **Step 6: Commit**

```bash
git add src/agentic_data_contracts/semantic/ tests/test_semantic/ tests/fixtures/semantic_source.yml
git commit -m "feat: add SemanticSource protocol and YamlSource implementation"
```

---

### Task 10: Semantic Layer — DbtSource

**Files:**
- Create: `src/agentic_data_contracts/semantic/dbt.py`
- Create: `tests/test_semantic/test_dbt.py`
- Create: `tests/fixtures/sample_dbt_manifest.json`

- [ ] **Step 1: Create the dbt manifest fixture**

`tests/fixtures/sample_dbt_manifest.json`:
```json
{
  "metadata": {
    "dbt_schema_version": "https://schemas.getdbt.com/dbt/manifest/v12/manifest.json",
    "generated_at": "2026-01-01T00:00:00Z"
  },
  "nodes": {
    "model.project.orders": {
      "unique_id": "model.project.orders",
      "name": "orders",
      "schema": "analytics",
      "resource_type": "model",
      "columns": {
        "id": {
          "name": "id",
          "description": "Primary key",
          "data_type": "INTEGER"
        },
        "amount": {
          "name": "amount",
          "description": "Order total in USD",
          "data_type": "DECIMAL"
        },
        "tenant_id": {
          "name": "tenant_id",
          "description": "Tenant identifier",
          "data_type": "VARCHAR"
        }
      },
      "description": "All orders"
    }
  },
  "metrics": {
    "metric.project.total_revenue": {
      "unique_id": "metric.project.total_revenue",
      "name": "total_revenue",
      "label": "Total Revenue",
      "description": "Sum of all completed order amounts",
      "type": "simple",
      "type_params": {
        "measure": {
          "name": "total_amount",
          "expr": "SUM(amount)",
          "filter": "status = 'completed'"
        }
      },
      "model": "ref('orders')",
      "filters": [
        {"field": "status", "operator": "=", "value": "'completed'"}
      ]
    }
  }
}
```

- [ ] **Step 2: Write the failing tests**

`tests/test_semantic/test_dbt.py`:
```python
from pathlib import Path

import pytest

from agentic_data_contracts.semantic.base import SemanticSource
from agentic_data_contracts.semantic.dbt import DbtSource


@pytest.fixture
def source(fixtures_dir: Path) -> DbtSource:
    return DbtSource(fixtures_dir / "sample_dbt_manifest.json")


def test_source_implements_protocol(source: DbtSource) -> None:
    assert isinstance(source, SemanticSource)


def test_get_metrics(source: DbtSource) -> None:
    metrics = source.get_metrics()
    assert len(metrics) == 1
    assert metrics[0].name == "total_revenue"


def test_get_metric(source: DbtSource) -> None:
    metric = source.get_metric("total_revenue")
    assert metric is not None
    assert "SUM(amount)" in metric.sql_expression
    assert metric.description == "Sum of all completed order amounts"


def test_get_metric_not_found(source: DbtSource) -> None:
    assert source.get_metric("nonexistent") is None


def test_get_table_schema(source: DbtSource) -> None:
    schema = source.get_table_schema("analytics", "orders")
    assert schema is not None
    assert len(schema.columns) == 3
    col_names = [c.name for c in schema.columns]
    assert "id" in col_names
    assert "amount" in col_names


def test_get_table_schema_not_found(source: DbtSource) -> None:
    assert source.get_table_schema("analytics", "nonexistent") is None
```

- [ ] **Step 3: Run tests to verify they fail**

Run: `uv run pytest tests/test_semantic/test_dbt.py -v`
Expected: FAIL with `ImportError`.

- [ ] **Step 4: Implement DbtSource**

`src/agentic_data_contracts/semantic/dbt.py`:
```python
"""dbt manifest.json semantic source."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from agentic_data_contracts.adapters.base import Column, TableSchema
from agentic_data_contracts.semantic.base import MetricDefinition


class DbtSource:
    """Loads metric and table definitions from a dbt manifest.json."""

    def __init__(self, path: str | Path) -> None:
        raw = json.loads(Path(path).read_text())
        self._metrics = self._parse_metrics(raw.get("metrics", {}))
        self._tables = self._parse_models(raw.get("nodes", {}))

    def _parse_metrics(self, metrics: dict[str, Any]) -> list[MetricDefinition]:
        result: list[MetricDefinition] = []
        for metric in metrics.values():
            sql_expr = ""
            type_params = metric.get("type_params", {})
            measure = type_params.get("measure", {})
            if isinstance(measure, dict):
                sql_expr = measure.get("expr", "")

            filters: list[str] = []
            for f in metric.get("filters", []):
                if isinstance(f, dict):
                    filters.append(
                        f"{f.get('field', '')} {f.get('operator', '')} {f.get('value', '')}"
                    )

            result.append(
                MetricDefinition(
                    name=metric["name"],
                    description=metric.get("description", ""),
                    sql_expression=sql_expr,
                    source_model=metric.get("model", ""),
                    filters=filters,
                )
            )
        return result

    def _parse_models(self, nodes: dict[str, Any]) -> dict[str, TableSchema]:
        tables: dict[str, TableSchema] = {}
        for node in nodes.values():
            if node.get("resource_type") != "model":
                continue
            schema_name = node.get("schema", "")
            table_name = node.get("name", "")
            key = f"{schema_name}.{table_name}"
            columns = [
                Column(
                    name=col["name"],
                    type=col.get("data_type", ""),
                    description=col.get("description", ""),
                )
                for col in node.get("columns", {}).values()
            ]
            tables[key] = TableSchema(columns=columns)
        return tables

    def get_metrics(self) -> list[MetricDefinition]:
        return list(self._metrics)

    def get_metric(self, name: str) -> MetricDefinition | None:
        for m in self._metrics:
            if m.name == name:
                return m
        return None

    def get_table_schema(self, schema: str, table: str) -> TableSchema | None:
        return self._tables.get(f"{schema}.{table}")
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `uv run pytest tests/test_semantic/test_dbt.py -v`
Expected: All 6 tests PASS.

- [ ] **Step 6: Commit**

```bash
git add src/agentic_data_contracts/semantic/dbt.py tests/test_semantic/test_dbt.py tests/fixtures/sample_dbt_manifest.json
git commit -m "feat: add DbtSource for reading metrics and tables from dbt manifest.json"
```

---

### Task 11: Tools Layer — Tool Factory (10 tools)

**Files:**
- Create: `src/agentic_data_contracts/tools/factory.py`
- Create: `tests/test_tools/__init__.py`
- Create: `tests/test_tools/test_factory.py`

- [ ] **Step 1: Write the failing tests**

`tests/test_tools/test_factory.py`:
```python
import json
from pathlib import Path

import pytest

from agentic_data_contracts.adapters.duckdb import DuckDBAdapter
from agentic_data_contracts.core.contract import DataContract
from agentic_data_contracts.semantic.yaml_source import YamlSource
from agentic_data_contracts.tools.factory import create_tools


@pytest.fixture
def contract(fixtures_dir: Path) -> DataContract:
    return DataContract.from_yaml(fixtures_dir / "valid_contract.yml")


@pytest.fixture
def adapter() -> DuckDBAdapter:
    db = DuckDBAdapter(":memory:")
    db.connection.execute(
        """
        CREATE SCHEMA IF NOT EXISTS analytics;
        CREATE TABLE analytics.orders (
            id INTEGER, amount DECIMAL(10,2), tenant_id VARCHAR
        );
        INSERT INTO analytics.orders VALUES (1, 100.00, 'acme'), (2, 200.00, 'acme');
        CREATE TABLE analytics.customers (id INTEGER, name VARCHAR, tenant_id VARCHAR);
        CREATE TABLE analytics.subscriptions (id INTEGER, plan VARCHAR, tenant_id VARCHAR);
        """
    )
    return db


@pytest.fixture
def semantic(fixtures_dir: Path) -> YamlSource:
    return YamlSource(fixtures_dir / "semantic_source.yml")


def test_create_tools_returns_10_tools(
    contract: DataContract, adapter: DuckDBAdapter, semantic: YamlSource
) -> None:
    tools = create_tools(contract, adapter=adapter, semantic_source=semantic)
    assert len(tools) == 10


def test_create_tools_without_adapter(
    contract: DataContract, semantic: YamlSource
) -> None:
    tools = create_tools(contract, semantic_source=semantic)
    assert len(tools) == 10


def test_tool_names(
    contract: DataContract, adapter: DuckDBAdapter, semantic: YamlSource
) -> None:
    tools = create_tools(contract, adapter=adapter, semantic_source=semantic)
    names = [t.name for t in tools]
    assert "list_schemas" in names
    assert "list_tables" in names
    assert "describe_table" in names
    assert "preview_table" in names
    assert "list_metrics" in names
    assert "lookup_metric" in names
    assert "validate_query" in names
    assert "query_cost_estimate" in names
    assert "run_query" in names
    assert "get_contract_info" in names


@pytest.mark.asyncio
async def test_list_schemas(
    contract: DataContract, adapter: DuckDBAdapter, semantic: YamlSource
) -> None:
    tools = create_tools(contract, adapter=adapter, semantic_source=semantic)
    tool = next(t for t in tools if t.name == "list_schemas")
    result = await tool.callable({})
    text = result["content"][0]["text"]
    assert "analytics" in text


@pytest.mark.asyncio
async def test_list_tables(
    contract: DataContract, adapter: DuckDBAdapter, semantic: YamlSource
) -> None:
    tools = create_tools(contract, adapter=adapter, semantic_source=semantic)
    tool = next(t for t in tools if t.name == "list_tables")
    result = await tool.callable({})
    text = result["content"][0]["text"]
    assert "orders" in text
    assert "customers" in text


@pytest.mark.asyncio
async def test_describe_table_with_adapter(
    contract: DataContract, adapter: DuckDBAdapter, semantic: YamlSource
) -> None:
    tools = create_tools(contract, adapter=adapter, semantic_source=semantic)
    tool = next(t for t in tools if t.name == "describe_table")
    result = await tool.callable({"schema": "analytics", "table": "orders"})
    text = result["content"][0]["text"]
    assert "id" in text
    assert "amount" in text


@pytest.mark.asyncio
async def test_describe_table_without_adapter(
    contract: DataContract, semantic: YamlSource
) -> None:
    tools = create_tools(contract, semantic_source=semantic)
    tool = next(t for t in tools if t.name == "describe_table")
    result = await tool.callable({"schema": "analytics", "table": "orders"})
    text = result["content"][0]["text"]
    assert "unavailable" in text.lower() or "no database" in text.lower()


@pytest.mark.asyncio
async def test_validate_query_passes(
    contract: DataContract, adapter: DuckDBAdapter, semantic: YamlSource
) -> None:
    tools = create_tools(contract, adapter=adapter, semantic_source=semantic)
    tool = next(t for t in tools if t.name == "validate_query")
    result = await tool.callable(
        {"sql": "SELECT id, amount FROM analytics.orders WHERE tenant_id = 'acme'"}
    )
    text = result["content"][0]["text"]
    assert "pass" in text.lower() or "valid" in text.lower()


@pytest.mark.asyncio
async def test_validate_query_blocked(
    contract: DataContract, adapter: DuckDBAdapter, semantic: YamlSource
) -> None:
    tools = create_tools(contract, adapter=adapter, semantic_source=semantic)
    tool = next(t for t in tools if t.name == "validate_query")
    result = await tool.callable({"sql": "SELECT * FROM analytics.orders"})
    text = result["content"][0]["text"]
    assert "block" in text.lower() or "violation" in text.lower()


@pytest.mark.asyncio
async def test_run_query_valid(
    contract: DataContract, adapter: DuckDBAdapter, semantic: YamlSource
) -> None:
    tools = create_tools(contract, adapter=adapter, semantic_source=semantic)
    tool = next(t for t in tools if t.name == "run_query")
    result = await tool.callable(
        {"sql": "SELECT id, amount FROM analytics.orders WHERE tenant_id = 'acme'"}
    )
    text = result["content"][0]["text"]
    assert "100" in text  # first order amount


@pytest.mark.asyncio
async def test_run_query_blocked(
    contract: DataContract, adapter: DuckDBAdapter, semantic: YamlSource
) -> None:
    tools = create_tools(contract, adapter=adapter, semantic_source=semantic)
    tool = next(t for t in tools if t.name == "run_query")
    result = await tool.callable({"sql": "DELETE FROM analytics.orders"})
    text = result["content"][0]["text"]
    assert "block" in text.lower() or "violation" in text.lower()


@pytest.mark.asyncio
async def test_get_contract_info(
    contract: DataContract, adapter: DuckDBAdapter, semantic: YamlSource
) -> None:
    tools = create_tools(contract, adapter=adapter, semantic_source=semantic)
    tool = next(t for t in tools if t.name == "get_contract_info")
    result = await tool.callable({})
    text = result["content"][0]["text"]
    assert "revenue-analysis" in text


@pytest.mark.asyncio
async def test_lookup_metric(
    contract: DataContract, adapter: DuckDBAdapter, semantic: YamlSource
) -> None:
    tools = create_tools(contract, adapter=adapter, semantic_source=semantic)
    tool = next(t for t in tools if t.name == "lookup_metric")
    result = await tool.callable({"metric_name": "total_revenue"})
    text = result["content"][0]["text"]
    assert "total_revenue" in text
    assert "SUM(amount)" in text


@pytest.mark.asyncio
async def test_preview_table(
    contract: DataContract, adapter: DuckDBAdapter, semantic: YamlSource
) -> None:
    tools = create_tools(contract, adapter=adapter, semantic_source=semantic)
    tool = next(t for t in tools if t.name == "preview_table")
    result = await tool.callable({"schema": "analytics", "table": "orders"})
    text = result["content"][0]["text"]
    assert "100" in text or "acme" in text
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/test_tools/test_factory.py -v`
Expected: FAIL with `ImportError`.

- [ ] **Step 3: Implement tool factory**

`src/agentic_data_contracts/tools/factory.py`:
```python
"""Tool factory — creates 10 agent tools from a DataContract."""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any

from agentic_data_contracts.adapters.base import DatabaseAdapter
from agentic_data_contracts.core.contract import DataContract
from agentic_data_contracts.core.session import ContractSession
from agentic_data_contracts.semantic.base import SemanticSource
from agentic_data_contracts.validation.validator import Validator


@dataclass
class ToolDef:
    """A tool definition compatible with Claude Agent SDK's @tool decorator."""

    name: str
    description: str
    input_schema: dict[str, Any]
    callable: Any  # async function(args: dict) -> dict


def _text_response(text: str) -> dict[str, Any]:
    return {"content": [{"type": "text", "text": text}]}


def create_tools(
    contract: DataContract,
    *,
    adapter: DatabaseAdapter | None = None,
    semantic_source: SemanticSource | None = None,
    session: ContractSession | None = None,
) -> list[ToolDef]:
    """Create 10 agent tools from a DataContract."""

    if session is None:
        session = ContractSession(contract)

    dialect = adapter.dialect if adapter else None
    validator = Validator(contract, dialect=dialect)

    # --- Discovery tools ---

    async def list_schemas(args: dict[str, Any]) -> dict[str, Any]:
        schemas = sorted({
            entry.schema_
            for entry in contract.schema.semantic.allowed_tables
            if entry.tables  # skip schemas with empty tables list
        })
        return _text_response(json.dumps({"schemas": schemas}, indent=2))

    async def list_tables(args: dict[str, Any]) -> dict[str, Any]:
        schema_filter = args.get("schema")
        tables: list[dict[str, Any]] = []
        for entry in contract.schema.semantic.allowed_tables:
            if schema_filter and entry.schema_ != schema_filter:
                continue
            for table in entry.tables:
                info: dict[str, Any] = {"schema": entry.schema_, "table": table}
                # Try semantic source for column info
                if semantic_source:
                    ts = semantic_source.get_table_schema(entry.schema_, table)
                    if ts:
                        info["columns"] = [c.name for c in ts.columns]
                tables.append(info)
        return _text_response(json.dumps({"tables": tables}, indent=2))

    async def describe_table(args: dict[str, Any]) -> dict[str, Any]:
        schema_name = args["schema"]
        table_name = args["table"]
        if adapter:
            ts = adapter.describe_table(schema_name, table_name)
            cols = [
                {"name": c.name, "type": c.type, "description": c.description, "nullable": c.nullable}
                for c in ts.columns
            ]
            return _text_response(json.dumps({"columns": cols}, indent=2))
        return _text_response("Unavailable: no database adapter configured.")

    async def preview_table(args: dict[str, Any]) -> dict[str, Any]:
        schema_name = args["schema"]
        table_name = args["table"]
        limit = args.get("limit", 5)
        if not adapter:
            return _text_response("Unavailable: no database adapter configured.")
        # Validate the table is allowed
        full_name = f"{schema_name}.{table_name}"
        if full_name not in contract.allowed_table_names():
            return _text_response(f"Table {full_name} is not in the allowed tables list.")
        result = adapter.execute(f"SELECT * FROM {schema_name}.{table_name} LIMIT {limit}")
        rows = [dict(zip(result.columns, row)) for row in result.rows]
        return _text_response(json.dumps({"columns": result.columns, "rows": rows, "count": len(rows)}, indent=2, default=str))

    async def list_metrics(args: dict[str, Any]) -> dict[str, Any]:
        if not semantic_source:
            return _text_response("No semantic source configured.")
        metrics = semantic_source.get_metrics()
        data = [
            {"name": m.name, "description": m.description, "source_model": m.source_model}
            for m in metrics
        ]
        return _text_response(json.dumps({"metrics": data}, indent=2))

    async def lookup_metric(args: dict[str, Any]) -> dict[str, Any]:
        metric_name = args["metric_name"]
        if not semantic_source:
            return _text_response("No semantic source configured.")
        metric = semantic_source.get_metric(metric_name)
        if not metric:
            return _text_response(f"Metric '{metric_name}' not found.")
        data = {
            "name": metric.name,
            "description": metric.description,
            "sql_expression": metric.sql_expression,
            "source_model": metric.source_model,
            "filters": metric.filters,
        }
        return _text_response(json.dumps(data, indent=2))

    # --- Execution tools ---

    async def validate_query(args: dict[str, Any]) -> dict[str, Any]:
        sql = args["sql"]
        result = validator.validate(sql)
        if result.blocked:
            return _text_response(
                f"BLOCKED — Violations:\n" + "\n".join(f"- {r}" for r in result.reasons)
            )
        parts = ["VALID — Query passes all contract checks."]
        if result.warnings:
            parts.append("Warnings:\n" + "\n".join(f"- {w}" for w in result.warnings))
        return _text_response("\n".join(parts))

    async def query_cost_estimate(args: dict[str, Any]) -> dict[str, Any]:
        sql = args["sql"]
        if not adapter:
            return _text_response("Unavailable: no database adapter configured for cost estimation.")
        explain_result = adapter.explain(sql)
        data: dict[str, Any] = {"schema_valid": explain_result.schema_valid}
        if explain_result.estimated_cost_usd is not None:
            data["estimated_cost_usd"] = explain_result.estimated_cost_usd
        if explain_result.estimated_rows is not None:
            data["estimated_rows"] = explain_result.estimated_rows
        if explain_result.errors:
            data["errors"] = explain_result.errors
        return _text_response(json.dumps(data, indent=2))

    async def run_query(args: dict[str, Any]) -> dict[str, Any]:
        sql = args["sql"]

        # Check session limits first
        try:
            session.check_limits()
        except Exception as e:
            return _text_response(f"BLOCKED — Session limit exceeded: {e}")

        # Validate
        result = validator.validate(sql)
        if result.blocked:
            session.record_retry()
            return _text_response(
                f"BLOCKED — Violations:\n" + "\n".join(f"- {r}" for r in result.reasons)
            )

        if not adapter:
            return _text_response("Unavailable: no database adapter configured.")

        # Execute
        try:
            qr = adapter.execute(sql)
        except Exception as e:
            session.record_retry()
            return _text_response(f"Query execution failed: {e}")

        rows = [dict(zip(qr.columns, row)) for row in qr.rows]
        output = {
            "columns": qr.columns,
            "rows": rows,
            "row_count": qr.row_count,
        }
        if result.warnings:
            output["warnings"] = result.warnings
        return _text_response(json.dumps(output, indent=2, default=str))

    # --- Meta tool ---

    async def get_contract_info(args: dict[str, Any]) -> dict[str, Any]:
        info: dict[str, Any] = {
            "contract_name": contract.name,
            "allowed_tables": contract.allowed_table_names(),
            "forbidden_operations": contract.schema.semantic.forbidden_operations,
            "rules": [
                {"name": r.name, "description": r.description, "enforcement": r.enforcement.value}
                for r in contract.schema.semantic.rules
            ],
        }
        res = contract.schema.resources
        if res:
            info["resource_limits"] = {
                "cost_limit_usd": res.cost_limit_usd,
                "max_retries": res.max_retries,
                "token_budget": res.token_budget,
                "max_query_time_seconds": res.max_query_time_seconds,
                "max_rows_scanned": res.max_rows_scanned,
            }
        info["session"] = session.remaining()
        return _text_response(json.dumps(info, indent=2))

    return [
        ToolDef(
            name="list_schemas",
            description="List database schemas available under this data contract",
            input_schema={"type": "object", "properties": {}},
            callable=list_schemas,
        ),
        ToolDef(
            name="list_tables",
            description="List allowed tables, optionally filtered by schema",
            input_schema={
                "type": "object",
                "properties": {
                    "schema": {"type": "string", "description": "Filter by schema name"},
                },
            },
            callable=list_tables,
        ),
        ToolDef(
            name="describe_table",
            description="Get full column details (name, type, description) for a table",
            input_schema={
                "type": "object",
                "properties": {
                    "schema": {"type": "string", "description": "Schema name"},
                    "table": {"type": "string", "description": "Table name"},
                },
                "required": ["schema", "table"],
            },
            callable=describe_table,
        ),
        ToolDef(
            name="preview_table",
            description="Get sample rows from a table",
            input_schema={
                "type": "object",
                "properties": {
                    "schema": {"type": "string", "description": "Schema name"},
                    "table": {"type": "string", "description": "Table name"},
                    "limit": {"type": "integer", "description": "Number of rows (default 5)", "default": 5},
                },
                "required": ["schema", "table"],
            },
            callable=preview_table,
        ),
        ToolDef(
            name="list_metrics",
            description="List all metrics defined in the semantic source",
            input_schema={"type": "object", "properties": {}},
            callable=list_metrics,
        ),
        ToolDef(
            name="lookup_metric",
            description="Look up a specific metric definition including SQL formula",
            input_schema={
                "type": "object",
                "properties": {
                    "metric_name": {"type": "string", "description": "Name of the metric to look up"},
                },
                "required": ["metric_name"],
            },
            callable=lookup_metric,
        ),
        ToolDef(
            name="validate_query",
            description="Check if a SQL query passes all contract rules without executing it",
            input_schema={
                "type": "object",
                "properties": {
                    "sql": {"type": "string", "description": "SQL query to validate"},
                },
                "required": ["sql"],
            },
            callable=validate_query,
        ),
        ToolDef(
            name="query_cost_estimate",
            description="Estimate the cost and row count of a SQL query without executing it",
            input_schema={
                "type": "object",
                "properties": {
                    "sql": {"type": "string", "description": "SQL query to estimate"},
                },
                "required": ["sql"],
            },
            callable=query_cost_estimate,
        ),
        ToolDef(
            name="run_query",
            description="Validate and execute a SQL query, returning results",
            input_schema={
                "type": "object",
                "properties": {
                    "sql": {"type": "string", "description": "SQL query to execute"},
                },
                "required": ["sql"],
            },
            callable=run_query,
        ),
        ToolDef(
            name="get_contract_info",
            description="Get active contract rules, resource limits, and remaining budget",
            input_schema={"type": "object", "properties": {}},
            callable=get_contract_info,
        ),
    ]
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/test_tools/test_factory.py -v`
Expected: All 16 tests PASS.

- [ ] **Step 5: Commit**

```bash
git add src/agentic_data_contracts/tools/factory.py tests/test_tools/
git commit -m "feat: add tool factory producing 10 contract-aware agent tools"
```

---

### Task 12: Tools Layer — Middleware

**Files:**
- Create: `src/agentic_data_contracts/tools/middleware.py`
- Create: `tests/test_tools/test_middleware.py`

- [ ] **Step 1: Write the failing tests**

`tests/test_tools/test_middleware.py`:
```python
from pathlib import Path

import pytest

from agentic_data_contracts.adapters.duckdb import DuckDBAdapter
from agentic_data_contracts.core.contract import DataContract
from agentic_data_contracts.core.session import ContractSession
from agentic_data_contracts.tools.middleware import contract_middleware


@pytest.fixture
def contract(fixtures_dir: Path) -> DataContract:
    return DataContract.from_yaml(fixtures_dir / "valid_contract.yml")


@pytest.fixture
def adapter() -> DuckDBAdapter:
    db = DuckDBAdapter(":memory:")
    db.connection.execute(
        """
        CREATE SCHEMA IF NOT EXISTS analytics;
        CREATE TABLE analytics.orders (id INTEGER, amount DECIMAL(10,2), tenant_id VARCHAR);
        INSERT INTO analytics.orders VALUES (1, 100.00, 'acme');
        """
    )
    return db


@pytest.mark.asyncio
async def test_middleware_allows_valid_query(
    contract: DataContract, adapter: DuckDBAdapter
) -> None:
    @contract_middleware(contract, adapter=adapter)
    async def my_query(args: dict) -> dict:
        result = adapter.execute(args["sql"])
        rows = [dict(zip(result.columns, row)) for row in result.rows]
        return {"content": [{"type": "text", "text": str(rows)}]}

    result = await my_query(
        {"sql": "SELECT id, amount FROM analytics.orders WHERE tenant_id = 'acme'"}
    )
    text = result["content"][0]["text"]
    assert "100" in text


@pytest.mark.asyncio
async def test_middleware_blocks_invalid_query(
    contract: DataContract, adapter: DuckDBAdapter
) -> None:
    @contract_middleware(contract, adapter=adapter)
    async def my_query(args: dict) -> dict:
        return {"content": [{"type": "text", "text": "should not reach here"}]}

    result = await my_query({"sql": "SELECT * FROM analytics.orders"})
    text = result["content"][0]["text"]
    assert "BLOCKED" in text
    assert "should not reach here" not in text


@pytest.mark.asyncio
async def test_middleware_tracks_session(
    contract: DataContract, adapter: DuckDBAdapter
) -> None:
    session = ContractSession(contract)

    @contract_middleware(contract, adapter=adapter, session=session)
    async def my_query(args: dict) -> dict:
        return {"content": [{"type": "text", "text": "ok"}]}

    # This will be blocked, incrementing retry count
    await my_query({"sql": "DELETE FROM analytics.orders"})
    assert session.retries == 1


@pytest.mark.asyncio
async def test_middleware_checks_session_limits(
    contract: DataContract, adapter: DuckDBAdapter
) -> None:
    session = ContractSession(contract)
    # Exhaust retries (max_retries=3)
    session.record_retry()
    session.record_retry()
    session.record_retry()

    @contract_middleware(contract, adapter=adapter, session=session)
    async def my_query(args: dict) -> dict:
        return {"content": [{"type": "text", "text": "ok"}]}

    result = await my_query(
        {"sql": "SELECT id FROM analytics.orders WHERE tenant_id = 'x'"}
    )
    text = result["content"][0]["text"]
    assert "limit" in text.lower() or "exceeded" in text.lower()
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/test_tools/test_middleware.py -v`
Expected: FAIL with `ImportError`.

- [ ] **Step 3: Implement middleware**

`src/agentic_data_contracts/tools/middleware.py`:
```python
"""Contract middleware — wraps existing tool functions with contract enforcement."""

from __future__ import annotations

import functools
from typing import Any, Callable, Coroutine

from agentic_data_contracts.adapters.base import DatabaseAdapter
from agentic_data_contracts.core.contract import DataContract
from agentic_data_contracts.core.session import ContractSession, LimitExceededError
from agentic_data_contracts.validation.validator import Validator


def contract_middleware(
    contract: DataContract,
    *,
    adapter: DatabaseAdapter | None = None,
    session: ContractSession | None = None,
) -> Callable[
    [Callable[..., Coroutine[Any, Any, dict[str, Any]]]],
    Callable[..., Coroutine[Any, Any, dict[str, Any]]],
]:
    """Decorator that wraps an async tool function with contract enforcement.

    The wrapped function must accept args: dict with a "sql" key.
    """
    if session is None:
        session = ContractSession(contract)

    dialect = adapter.dialect if adapter else None
    validator = Validator(contract, dialect=dialect)

    def decorator(
        fn: Callable[..., Coroutine[Any, Any, dict[str, Any]]],
    ) -> Callable[..., Coroutine[Any, Any, dict[str, Any]]]:
        @functools.wraps(fn)
        async def wrapper(args: dict[str, Any]) -> dict[str, Any]:
            # Check session limits
            try:
                session.check_limits()
            except LimitExceededError as e:
                return {"content": [{"type": "text", "text": f"BLOCKED — Session limit exceeded: {e}"}]}

            sql = args.get("sql", "")
            if sql:
                result = validator.validate(sql)
                if result.blocked:
                    session.record_retry()
                    return {
                        "content": [
                            {
                                "type": "text",
                                "text": "BLOCKED — Violations:\n"
                                + "\n".join(f"- {r}" for r in result.reasons),
                            }
                        ]
                    }

            return await fn(args)

        return wrapper

    return decorator
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/test_tools/test_middleware.py -v`
Expected: All 4 tests PASS.

- [ ] **Step 5: Commit**

```bash
git add src/agentic_data_contracts/tools/middleware.py tests/test_tools/test_middleware.py
git commit -m "feat: add contract_middleware decorator for wrapping BYO tools"
```

---

### Task 13: Bridge Layer — Optional ai-agent-contracts Compilation

**Files:**
- Create: `src/agentic_data_contracts/bridge/compiler.py`
- Create: `tests/test_bridge/__init__.py`
- Create: `tests/test_bridge/test_compiler.py`

- [ ] **Step 1: Write the failing tests**

`tests/test_bridge/test_compiler.py`:
```python
from pathlib import Path

import pytest

from agentic_data_contracts.core.contract import DataContract

try:
    from agent_contracts import (
        Capabilities,
        Contract,
        ResourceConstraints,
        SuccessCriterion,
        TemporalConstraints,
        TerminationCondition,
    )

    from agentic_data_contracts.bridge.compiler import compile_to_contract

    HAS_AGENT_CONTRACTS = True
except ImportError:
    HAS_AGENT_CONTRACTS = False

pytestmark = pytest.mark.skipif(
    not HAS_AGENT_CONTRACTS,
    reason="ai-agent-contracts not installed",
)


@pytest.fixture
def dc(fixtures_dir: Path) -> DataContract:
    return DataContract.from_yaml(fixtures_dir / "valid_contract.yml")


def test_compile_returns_contract(dc: DataContract) -> None:
    contract = compile_to_contract(dc)
    assert isinstance(contract, Contract)
    assert contract.name == "revenue-analysis"


def test_compile_resources(dc: DataContract) -> None:
    contract = compile_to_contract(dc)
    assert contract.resources.cost_usd == 5.00
    assert contract.resources.tokens == 50000
    assert contract.resources.iterations == 3


def test_compile_temporal(dc: DataContract) -> None:
    contract = compile_to_contract(dc)
    assert contract.temporal.max_duration is not None
    assert contract.temporal.max_duration.total_seconds() == 300


def test_compile_block_rules_become_termination_conditions(dc: DataContract) -> None:
    contract = compile_to_contract(dc)
    assert len(contract.termination_conditions) >= 2
    types = [tc.type for tc in contract.termination_conditions]
    assert "contract_rule_violation" in types


def test_compile_warn_rules_become_success_criteria(dc: DataContract) -> None:
    contract = compile_to_contract(dc)
    # Should have warn rules + explicit success_criteria
    warn_criteria = [
        sc for sc in contract.success_criteria if sc.name == "use_approved_metrics"
    ]
    assert len(warn_criteria) == 1


def test_compile_success_criteria_weights(dc: DataContract) -> None:
    contract = compile_to_contract(dc)
    named = {sc.name: sc for sc in contract.success_criteria}
    assert "query_uses_semantic_definitions" in named
    assert named["query_uses_semantic_definitions"].weight == pytest.approx(0.4)


def test_compile_capabilities_instructions(dc: DataContract) -> None:
    contract = compile_to_contract(dc)
    assert contract.capabilities is not None
    assert contract.capabilities.instructions is not None
    assert "analytics.orders" in contract.capabilities.instructions


def test_compile_metadata(dc: DataContract) -> None:
    contract = compile_to_contract(dc)
    assert "source_of_truth" in contract.metadata or "allowed_tables" in contract.metadata
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/test_bridge/test_compiler.py -v`
Expected: Either skipped (if ai-agent-contracts not installed) or FAIL with `ImportError` on `bridge.compiler`.

- [ ] **Step 3: Implement bridge compiler**

`src/agentic_data_contracts/bridge/compiler.py`:
```python
"""Bridge layer — compiles DataContract to ai-agent-contracts Contract."""

from __future__ import annotations

from datetime import timedelta

from agent_contracts import (
    Capabilities,
    Contract,
    ResourceConstraints,
    SuccessCriterion,
    TemporalConstraints,
    TerminationCondition,
)

from agentic_data_contracts.core.contract import DataContract
from agentic_data_contracts.core.schema import Enforcement


def compile_to_contract(dc: DataContract) -> Contract:
    """Compile a DataContract into an ai-agent-contracts Contract."""

    # Resources
    res = dc.schema.resources
    resources = ResourceConstraints(
        cost_usd=res.cost_limit_usd if res else None,
        tokens=res.token_budget if res else None,
        iterations=res.max_retries if res else None,
    )

    # Temporal
    temporal_cfg = dc.schema.temporal
    temporal = TemporalConstraints(
        max_duration=(
            timedelta(seconds=temporal_cfg.max_duration_seconds)
            if temporal_cfg and temporal_cfg.max_duration_seconds
            else None
        ),
    )

    # Termination conditions from block rules
    termination_conditions: list[TerminationCondition] = []
    for rule in dc.block_rules():
        termination_conditions.append(
            TerminationCondition(
                type="contract_rule_violation",
                condition=f"Rule '{rule.name}': {rule.description}",
                priority=1,
            )
        )

    # Success criteria from warn rules + explicit success_criteria
    success_criteria: list[SuccessCriterion] = []
    for rule in dc.warn_rules():
        success_criteria.append(
            SuccessCriterion(
                name=rule.name,
                condition=rule.description,
                weight=0.3,
                required=False,
            )
        )
    for sc in dc.schema.success_criteria:
        success_criteria.append(
            SuccessCriterion(
                name=sc.name,
                condition=sc.name,
                weight=sc.weight,
                required=False,
            )
        )

    # Capabilities with instructions
    instructions = dc.to_system_prompt()
    capabilities = Capabilities(instructions=instructions)

    # Metadata
    metadata: dict[str, object] = {
        "allowed_tables": dc.allowed_table_names(),
        "forbidden_operations": dc.schema.semantic.forbidden_operations,
    }
    if dc.schema.semantic.source:
        metadata["source_of_truth"] = dc.schema.semantic.source.path

    # Log rules go to metadata only
    for rule in dc.log_rules():
        metadata[f"log_rule_{rule.name}"] = rule.description

    return Contract(
        id=f"data-contract-{dc.name}",
        name=dc.name,
        resources=resources,
        temporal=temporal,
        capabilities=capabilities,
        termination_conditions=termination_conditions,
        success_criteria=success_criteria,
        metadata=metadata,
    )
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/test_bridge/test_compiler.py -v`
Expected: All 8 tests PASS (or all SKIPPED if ai-agent-contracts is not installed).

- [ ] **Step 5: Install ai-agent-contracts and re-run if tests were skipped**

Run:
```bash
uv add --optional agent-contracts "ai-agent-contracts>=0.2.0"
uv sync --all-extras
uv run pytest tests/test_bridge/test_compiler.py -v
```

Expected: All 8 tests PASS.

- [ ] **Step 6: Commit**

```bash
git add src/agentic_data_contracts/bridge/ tests/test_bridge/
git commit -m "feat: add bridge layer compiling DataContract to ai-agent-contracts Contract"
```

---

### Task 14: Semantic Layer — CubeSource (Stub)

**Files:**
- Create: `src/agentic_data_contracts/semantic/cube.py`
- Create: `tests/test_semantic/test_cube.py`
- Create: `tests/fixtures/sample_cube_schema.yml`

- [ ] **Step 1: Create the Cube fixture**

`tests/fixtures/sample_cube_schema.yml`:
```yaml
cubes:
  - name: Orders
    sql_table: analytics.orders
    measures:
      - name: total_revenue
        sql: "SUM(amount)"
        type: sum
        description: "Total revenue from all orders"
    dimensions:
      - name: tenant_id
        sql: "tenant_id"
        type: string
      - name: status
        sql: "status"
        type: string
    columns:
      - name: id
        type: INTEGER
        description: "Primary key"
      - name: amount
        type: DECIMAL
        description: "Order total"
      - name: tenant_id
        type: VARCHAR
        description: "Tenant identifier"
```

- [ ] **Step 2: Write the failing tests**

`tests/test_semantic/test_cube.py`:
```python
from pathlib import Path

import pytest

from agentic_data_contracts.semantic.base import SemanticSource
from agentic_data_contracts.semantic.cube import CubeSource


@pytest.fixture
def source(fixtures_dir: Path) -> CubeSource:
    return CubeSource(fixtures_dir / "sample_cube_schema.yml")


def test_source_implements_protocol(source: CubeSource) -> None:
    assert isinstance(source, SemanticSource)


def test_get_metrics(source: CubeSource) -> None:
    metrics = source.get_metrics()
    assert len(metrics) == 1
    assert metrics[0].name == "total_revenue"
    assert "SUM(amount)" in metrics[0].sql_expression


def test_get_metric(source: CubeSource) -> None:
    metric = source.get_metric("total_revenue")
    assert metric is not None
    assert metric.description == "Total revenue from all orders"


def test_get_metric_not_found(source: CubeSource) -> None:
    assert source.get_metric("nonexistent") is None


def test_get_table_schema(source: CubeSource) -> None:
    schema = source.get_table_schema("analytics", "orders")
    assert schema is not None
    col_names = [c.name for c in schema.columns]
    assert "id" in col_names
    assert "amount" in col_names


def test_get_table_schema_not_found(source: CubeSource) -> None:
    assert source.get_table_schema("analytics", "nonexistent") is None
```

- [ ] **Step 3: Run tests to verify they fail**

Run: `uv run pytest tests/test_semantic/test_cube.py -v`
Expected: FAIL with `ImportError`.

- [ ] **Step 4: Implement CubeSource**

`src/agentic_data_contracts/semantic/cube.py`:
```python
"""Cube schema YAML semantic source."""

from __future__ import annotations

from pathlib import Path

import yaml

from agentic_data_contracts.adapters.base import Column, TableSchema
from agentic_data_contracts.semantic.base import MetricDefinition


class CubeSource:
    """Loads metric and table definitions from a Cube schema YAML file."""

    def __init__(self, path: str | Path) -> None:
        raw = yaml.safe_load(Path(path).read_text())
        self._metrics: list[MetricDefinition] = []
        self._tables: dict[str, TableSchema] = {}

        for cube in raw.get("cubes", []):
            sql_table = cube.get("sql_table", "")

            # Parse measures as metrics
            for measure in cube.get("measures", []):
                self._metrics.append(
                    MetricDefinition(
                        name=measure["name"],
                        description=measure.get("description", ""),
                        sql_expression=measure.get("sql", ""),
                        source_model=sql_table,
                    )
                )

            # Parse table schema from columns
            if sql_table and "." in sql_table:
                columns = [
                    Column(
                        name=c["name"],
                        type=c.get("type", ""),
                        description=c.get("description", ""),
                    )
                    for c in cube.get("columns", [])
                ]
                self._tables[sql_table] = TableSchema(columns=columns)

    def get_metrics(self) -> list[MetricDefinition]:
        return list(self._metrics)

    def get_metric(self, name: str) -> MetricDefinition | None:
        for m in self._metrics:
            if m.name == name:
                return m
        return None

    def get_table_schema(self, schema: str, table: str) -> TableSchema | None:
        return self._tables.get(f"{schema}.{table}")
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `uv run pytest tests/test_semantic/test_cube.py -v`
Expected: All 6 tests PASS.

- [ ] **Step 6: Commit**

```bash
git add src/agentic_data_contracts/semantic/cube.py tests/test_semantic/test_cube.py tests/fixtures/sample_cube_schema.yml
git commit -m "feat: add CubeSource for reading metrics and tables from Cube schema YAML"
```

---

### Task 15: Public API Exports and Full Test Suite

**Files:**
- Modify: `src/agentic_data_contracts/__init__.py`
- Create: `tests/test_public_api.py`

- [ ] **Step 1: Write the failing tests**

`tests/test_public_api.py`:
```python
def test_top_level_imports() -> None:
    from agentic_data_contracts import (
        DataContract,
        contract_middleware,
        create_tools,
    )

    assert DataContract is not None
    assert create_tools is not None
    assert contract_middleware is not None


def test_core_imports() -> None:
    from agentic_data_contracts.core.contract import DataContract
    from agentic_data_contracts.core.schema import DataContractSchema
    from agentic_data_contracts.core.session import ContractSession, LimitExceededError

    assert DataContract is not None
    assert DataContractSchema is not None
    assert ContractSession is not None
    assert LimitExceededError is not None


def test_validation_imports() -> None:
    from agentic_data_contracts.validation.checkers import (
        CheckResult,
        NoSelectStarChecker,
        OperationBlocklistChecker,
        RequiredFilterChecker,
        TableAllowlistChecker,
    )
    from agentic_data_contracts.validation.explain import ExplainAdapter, ExplainResult
    from agentic_data_contracts.validation.validator import ValidationResult, Validator

    assert CheckResult is not None
    assert Validator is not None


def test_adapter_imports() -> None:
    from agentic_data_contracts.adapters.base import (
        Column,
        DatabaseAdapter,
        QueryResult,
        TableSchema,
    )

    assert DatabaseAdapter is not None
    assert Column is not None


def test_semantic_imports() -> None:
    from agentic_data_contracts.semantic.base import MetricDefinition, SemanticSource
    from agentic_data_contracts.semantic.yaml_source import YamlSource

    assert SemanticSource is not None
    assert YamlSource is not None


def test_tools_imports() -> None:
    from agentic_data_contracts.tools.factory import ToolDef, create_tools
    from agentic_data_contracts.tools.middleware import contract_middleware

    assert ToolDef is not None
    assert create_tools is not None
    assert contract_middleware is not None
```

- [ ] **Step 2: Run tests to verify they pass**

Run: `uv run pytest tests/test_public_api.py -v`
Expected: All 6 tests PASS.

- [ ] **Step 3: Run the full test suite**

Run: `uv run pytest -v`
Expected: All tests PASS (bridge tests may be skipped if ai-agent-contracts not installed).

- [ ] **Step 4: Run linting and type checking**

Run:
```bash
uv run ruff check src/ tests/
uv run ruff format --check src/ tests/
ty check
```

Fix any issues that come up.

- [ ] **Step 5: Commit**

```bash
git add tests/test_public_api.py src/agentic_data_contracts/__init__.py
git commit -m "feat: add public API tests and verify all imports"
```

---

### Task 16: Example — Revenue Agent with DuckDB

**Files:**
- Create: `examples/revenue_agent/contract.yml`
- Create: `examples/revenue_agent/semantic.yml`
- Create: `examples/revenue_agent/setup_db.py`
- Create: `examples/revenue_agent/agent.py`

- [ ] **Step 1: Create the example contract**

`examples/revenue_agent/contract.yml`:
```yaml
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
    - name: use_semantic_revenue
      description: "Revenue calculations must use the dbt metric definition"
      enforcement: warn
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

- [ ] **Step 2: Create the semantic source**

`examples/revenue_agent/semantic.yml`:
```yaml
metrics:
  - name: total_revenue
    description: "Total revenue from completed orders"
    sql_expression: "SUM(amount) FILTER (WHERE status = 'completed')"
    source_model: analytics.orders
    filters:
      - "status = 'completed'"

  - name: revenue_by_region
    description: "Revenue broken down by customer region"
    sql_expression: "SUM(o.amount) GROUP BY c.region"
    source_model: analytics.orders
    filters:
      - "o.status = 'completed'"

tables:
  - schema: analytics
    table: orders
    columns:
      - name: id
        type: INTEGER
        description: "Order ID"
      - name: customer_id
        type: INTEGER
        description: "FK to customers"
      - name: amount
        type: DECIMAL
        description: "Order total in USD"
      - name: status
        type: VARCHAR
        description: "pending, completed, cancelled"
      - name: tenant_id
        type: VARCHAR
        description: "Tenant identifier"
      - name: created_at
        type: DATE
        description: "Order date"

  - schema: analytics
    table: customers
    columns:
      - name: id
        type: INTEGER
        description: "Customer ID"
      - name: name
        type: VARCHAR
        description: "Customer name"
      - name: region
        type: VARCHAR
        description: "Geographic region"
      - name: tenant_id
        type: VARCHAR
        description: "Tenant identifier"
```

- [ ] **Step 3: Create the database setup script**

`examples/revenue_agent/setup_db.py`:
```python
"""Set up a sample DuckDB database for the revenue agent example."""

import duckdb


def setup(db_path: str = "sample_data.duckdb") -> None:
    conn = duckdb.connect(db_path)
    conn.execute("CREATE SCHEMA IF NOT EXISTS analytics")
    conn.execute(
        """
        CREATE OR REPLACE TABLE analytics.customers (
            id INTEGER, name VARCHAR, region VARCHAR, tenant_id VARCHAR
        )
        """
    )
    conn.execute(
        """
        INSERT INTO analytics.customers VALUES
        (1, 'Alice Corp', 'North America', 'acme'),
        (2, 'Bob Ltd', 'Europe', 'acme'),
        (3, 'Charlie Inc', 'Asia Pacific', 'acme'),
        (4, 'Diana GmbH', 'Europe', 'acme'),
        (5, 'Eve SA', 'North America', 'acme')
        """
    )
    conn.execute(
        """
        CREATE OR REPLACE TABLE analytics.orders (
            id INTEGER, customer_id INTEGER, amount DECIMAL(10,2),
            status VARCHAR, tenant_id VARCHAR, created_at DATE
        )
        """
    )
    conn.execute(
        """
        INSERT INTO analytics.orders VALUES
        (1, 1, 1500.00, 'completed', 'acme', '2025-01-15'),
        (2, 2, 2300.00, 'completed', 'acme', '2025-01-20'),
        (3, 3, 800.00, 'completed', 'acme', '2025-02-01'),
        (4, 1, 1200.00, 'completed', 'acme', '2025-02-15'),
        (5, 4, 3100.00, 'completed', 'acme', '2025-03-01'),
        (6, 5, 950.00, 'pending', 'acme', '2025-03-10'),
        (7, 2, 1800.00, 'completed', 'acme', '2025-03-15'),
        (8, 3, 2100.00, 'cancelled', 'acme', '2025-03-20')
        """
    )
    conn.execute(
        """
        CREATE OR REPLACE TABLE analytics.subscriptions (
            id INTEGER, customer_id INTEGER, plan VARCHAR, tenant_id VARCHAR
        )
        """
    )
    conn.execute(
        """
        INSERT INTO analytics.subscriptions VALUES
        (1, 1, 'enterprise', 'acme'),
        (2, 2, 'pro', 'acme'),
        (3, 3, 'starter', 'acme')
        """
    )
    conn.close()
    print(f"Sample database created at {db_path}")


if __name__ == "__main__":
    setup()
```

- [ ] **Step 4: Create the agent script**

`examples/revenue_agent/agent.py`:
```python
"""Revenue analysis agent — demonstrates agentic-data-contracts with Claude Agent SDK.

Usage:
    # First, set up the sample database:
    uv run python examples/revenue_agent/setup_db.py

    # Then run the agent:
    uv run python examples/revenue_agent/agent.py "What was Q1 revenue by region?"

Requires: claude-agent-sdk (`uv add claude-agent-sdk` or `pip install claude-agent-sdk`)
"""

from __future__ import annotations

import asyncio
import sys
from pathlib import Path

from agentic_data_contracts import DataContract, create_tools
from agentic_data_contracts.adapters.duckdb import DuckDBAdapter
from agentic_data_contracts.semantic.yaml_source import YamlSource

EXAMPLE_DIR = Path(__file__).parent


def main() -> None:
    prompt = " ".join(sys.argv[1:]) if len(sys.argv) > 1 else "What was total revenue by region in Q1 2025?"

    # Load contract and semantic source
    dc = DataContract.from_yaml(EXAMPLE_DIR / "contract.yml")
    semantic = YamlSource(EXAMPLE_DIR / "semantic.yml")

    # Set up DuckDB with sample data
    db_path = EXAMPLE_DIR / "sample_data.duckdb"
    if not db_path.exists():
        from examples.revenue_agent.setup_db import setup
        setup(str(db_path))
    adapter = DuckDBAdapter(str(db_path))

    # Create tools
    tools = create_tools(dc, adapter=adapter, semantic_source=semantic)

    # Try to use Claude Agent SDK if available
    try:
        asyncio.run(_run_with_sdk(dc, tools, prompt))
    except ImportError:
        print("claude-agent-sdk not installed. Showing tool demo instead.\n")
        asyncio.run(_run_demo(tools, prompt))


async def _run_with_sdk(dc: DataContract, tools: list, prompt: str) -> None:
    from claude_agent_sdk import (
        AssistantMessage,
        ClaudeAgentOptions,
        TextBlock,
        create_sdk_mcp_server,
        query,
    )

    server = create_sdk_mcp_server(name="data-contracts", version="1.0.0", tools=tools)

    user_prompt = """You are a revenue analytics assistant for Acme Corp.
Always be concise and include methodology notes in your answers."""

    options = ClaudeAgentOptions(
        model="claude-sonnet-4-6",
        system_prompt=f"{user_prompt}\n\n{dc.to_system_prompt()}",
        mcp_servers={"dc": server},
        allowed_tools=[f"mcp__dc__{t.name}" for t in tools],
    )

    async for message in query(prompt=prompt, options=options):
        if isinstance(message, AssistantMessage):
            for block in message.content:
                if isinstance(block, TextBlock):
                    print(block.text)


async def _run_demo(tools: list, prompt: str) -> None:
    """Fallback demo showing tools working without the SDK."""
    print(f"Query: {prompt}\n")

    # Show available schemas
    tool = next(t for t in tools if t.name == "list_schemas")
    result = await tool.callable({})
    print("=== Available Schemas ===")
    print(result["content"][0]["text"])

    # Show available tables
    tool = next(t for t in tools if t.name == "list_tables")
    result = await tool.callable({})
    print("\n=== Available Tables ===")
    print(result["content"][0]["text"])

    # Validate a good query
    tool = next(t for t in tools if t.name == "validate_query")
    sql = "SELECT c.region, SUM(o.amount) as revenue FROM analytics.orders o JOIN analytics.customers c ON o.customer_id = c.id WHERE o.tenant_id = 'acme' AND o.status = 'completed' AND o.created_at BETWEEN '2025-01-01' AND '2025-03-31' GROUP BY c.region"
    result = await tool.callable({"sql": sql})
    print(f"\n=== Validate Query ===")
    print(result["content"][0]["text"])

    # Run it
    tool = next(t for t in tools if t.name == "run_query")
    result = await tool.callable({"sql": sql})
    print(f"\n=== Query Results ===")
    print(result["content"][0]["text"])

    # Show a blocked query
    tool = next(t for t in tools if t.name == "validate_query")
    result = await tool.callable({"sql": "SELECT * FROM analytics.orders"})
    print(f"\n=== Blocked Query ===")
    print(result["content"][0]["text"])

    # Show contract info
    tool = next(t for t in tools if t.name == "get_contract_info")
    result = await tool.callable({})
    print(f"\n=== Contract Info ===")
    print(result["content"][0]["text"])


if __name__ == "__main__":
    main()
```

- [ ] **Step 5: Test the example**

Run:
```bash
uv run python examples/revenue_agent/setup_db.py
uv run python examples/revenue_agent/agent.py
```

Expected: Prints the tool demo output showing schemas, tables, validated query, results, blocked query, and contract info.

- [ ] **Step 6: Commit**

```bash
git add examples/
git commit -m "feat: add revenue agent example with DuckDB and contract enforcement"
```

---

### Task 17: Final Verification

- [ ] **Step 1: Run full test suite**

Run: `uv run pytest -v --tb=short`
Expected: All tests PASS.

- [ ] **Step 2: Run linting and formatting**

Run:
```bash
uv run ruff check src/ tests/ examples/
uv run ruff format --check src/ tests/ examples/
```

Expected: No issues.

- [ ] **Step 3: Run type checking**

Run: `ty check`
Expected: No errors (warnings are acceptable for dynamic patterns).

- [ ] **Step 4: Run the example end-to-end**

Run: `uv run python examples/revenue_agent/agent.py "Show me Q1 2025 revenue by region"`
Expected: Prints results showing revenue by region.

- [ ] **Step 5: Verify pre-commit hooks work**

Run: `prek run --all-files`
Expected: All hooks pass.

- [ ] **Step 6: Push to remote**

Run: `git push origin main`
