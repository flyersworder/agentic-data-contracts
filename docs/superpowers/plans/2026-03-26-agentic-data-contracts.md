# Agentic Data Contracts Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a separate `agentic-data-contracts` Python package that lets data engineers define YAML-based governance contracts for AI agents querying databases, compiling to the `agent-contracts` framework.

**Architecture:** YAML contract → Pydantic schema validation → compile to `agent_contracts.Contract` → runtime SQL validation via sqlglot. Two validation layers: static SQL parsing (always) and optional EXPLAIN dry-run (when DB connection configured).

**Tech Stack:** Python 3.12+, Pydantic v2, PyYAML, sqlglot, agent-contracts (dependency), pytest, uv

---

## File Structure

```
agentic-data-contracts/
├── src/agentic_data_contracts/
│   ├── __init__.py          # Public API: DataContract, DataContractValidator
│   ├── schema.py            # Pydantic models validating YAML structure
│   ├── compiler.py          # DataContract class: from_yaml() + compile()
│   ├── checkers.py          # Built-in SQL rule checkers using sqlglot
│   ├── validator.py         # DataContractValidator: runtime SQL checking
│   └── explain.py           # Optional EXPLAIN adapters (Layer 2)
├── tests/
│   ├── test_schema.py       # YAML parsing and validation tests
│   ├── test_compiler.py     # Compilation to Contract tests
│   ├── test_checkers.py     # Individual SQL checker tests
│   ├── test_validator.py    # Runtime validation integration tests
│   ├── test_explain.py      # EXPLAIN adapter tests (mocked)
│   └── fixtures/
│       ├── valid_full.yml
│       ├── valid_minimal.yml
│       ├── invalid_missing_name.yml
│       ├── invalid_bad_enforcement.yml
│       └── valid_with_connection.yml
├── examples/
│   └── revenue_agent/
│       ├── contract.yml
│       └── run_agent.py
├── pyproject.toml
├── LICENSE
└── README.md
```

---

### Task 1: Repository Scaffolding

**Files:**
- Create: `agentic-data-contracts/pyproject.toml`
- Create: `agentic-data-contracts/src/agentic_data_contracts/__init__.py`
- Create: `agentic-data-contracts/LICENSE`

- [ ] **Step 1: Create the repository directory and src layout**

```bash
mkdir -p ~/Documents/agentic-data-contracts/src/agentic_data_contracts
mkdir -p ~/Documents/agentic-data-contracts/tests/fixtures
mkdir -p ~/Documents/agentic-data-contracts/examples/revenue_agent
```

- [ ] **Step 2: Create pyproject.toml**

Create `~/Documents/agentic-data-contracts/pyproject.toml`:

```toml
[project]
name = "agentic-data-contracts"
version = "0.1.0"
description = "YAML-first governance contracts for AI agents querying databases, built on agent-contracts"
readme = "README.md"
requires-python = ">=3.12"
authors = [{ name = "Qing", email = "qingye779@gmail.com" }]
license = { text = "Apache-2.0" }
keywords = ["ai-agents", "data-contracts", "governance", "analytics", "dbt", "semantic-layer"]
classifiers = [
    "Development Status :: 3 - Alpha",
    "Intended Audience :: Developers",
    "Programming Language :: Python :: 3",
    "Programming Language :: Python :: 3.12",
    "Programming Language :: Python :: 3.13",
    "Topic :: Scientific/Engineering :: Artificial Intelligence",
    "Topic :: Database",
]

dependencies = [
    "agent-contracts>=0.1.0",
    "sqlglot>=23.0",
    "pydantic>=2.0",
    "pyyaml>=6.0",
]

[project.urls]
Homepage = "https://github.com/flyersworder/agentic-data-contracts"
Repository = "https://github.com/flyersworder/agentic-data-contracts"
Issues = "https://github.com/flyersworder/agentic-data-contracts/issues"

[project.optional-dependencies]
bigquery = ["google-cloud-bigquery"]
snowflake = ["snowflake-connector-python"]
postgres = ["psycopg2-binary"]

[dependency-groups]
dev = [
    "pre-commit>=4.0.0",
    "ruff>=0.8.0",
    "mypy>=1.13.0",
    "pytest>=8.3.0",
    "pytest-cov>=6.0.0",
]

[build-system]
requires = ["uv_build>=0.9.2,<0.10.0"]
build-backend = "uv_build"

[tool.ruff]
target-version = "py312"
line-length = 100

[tool.ruff.lint]
select = ["E", "W", "F", "I", "N", "UP", "B", "C4", "SIM", "TCH", "RUF"]
ignore = ["E501"]

[tool.ruff.format]
quote-style = "double"
indent-style = "space"

[tool.mypy]
python_version = "3.12"
warn_return_any = true
warn_unused_configs = true
disallow_untyped_defs = true
disallow_incomplete_defs = true
check_untyped_defs = true

[tool.pytest.ini_options]
testpaths = ["tests"]
python_files = ["test_*.py"]
python_classes = ["Test*"]
python_functions = ["test_*"]
addopts = [
    "--strict-markers",
    "--strict-config",
    "--cov=agentic_data_contracts",
    "--cov-report=term-missing",
]
```

- [ ] **Step 3: Create __init__.py with version**

Create `~/Documents/agentic-data-contracts/src/agentic_data_contracts/__init__.py`:

```python
"""Agentic Data Contracts: YAML-first governance for AI agents querying databases."""

__version__ = "0.1.0"
```

- [ ] **Step 4: Create LICENSE file**

Create `~/Documents/agentic-data-contracts/LICENSE` with the Apache-2.0 license text.

- [ ] **Step 5: Initialize git and install dependencies**

```bash
cd ~/Documents/agentic-data-contracts
git init
uv sync
```

- [ ] **Step 6: Verify the package installs**

```bash
cd ~/Documents/agentic-data-contracts
uv pip install -e .
python -c "import agentic_data_contracts; print(agentic_data_contracts.__version__)"
```

Expected: `0.1.0`

- [ ] **Step 7: Commit**

```bash
git add pyproject.toml src/ LICENSE
git commit -m "chore: scaffold agentic-data-contracts package"
```

---

### Task 2: YAML Schema (Pydantic Models)

**Files:**
- Create: `src/agentic_data_contracts/schema.py`
- Create: `tests/test_schema.py`
- Create: `tests/fixtures/valid_full.yml`
- Create: `tests/fixtures/valid_minimal.yml`
- Create: `tests/fixtures/invalid_missing_name.yml`
- Create: `tests/fixtures/invalid_bad_enforcement.yml`
- Create: `tests/fixtures/valid_with_connection.yml`

- [ ] **Step 1: Create test fixtures**

Create `tests/fixtures/valid_full.yml`:

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
      description: "All queries must filter by tenant_id"
      enforcement: block
    - name: use_approved_metrics
      description: "Revenue must use semantic layer definition"
      enforcement: warn
    - name: no_select_star
      description: "Must specify explicit columns"
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

Create `tests/fixtures/valid_minimal.yml`:

```yaml
version: "1.0"
name: simple-query-agent

semantic:
  allowed_tables:
    - schema: public
      tables: [users]
```

Create `tests/fixtures/invalid_missing_name.yml`:

```yaml
version: "1.0"
semantic:
  allowed_tables:
    - schema: public
      tables: [users]
```

Create `tests/fixtures/invalid_bad_enforcement.yml`:

```yaml
version: "1.0"
name: bad-contract

semantic:
  rules:
    - name: test_rule
      description: "A rule"
      enforcement: explode
```

Create `tests/fixtures/valid_with_connection.yml`:

```yaml
version: "1.0"
name: connected-agent

semantic:
  source:
    type: bigquery
    path: "project.dataset"
  allowed_tables:
    - schema: analytics
      tables: [events]
  connection:
    type: bigquery
    project: my-project
    dataset: analytics
```

- [ ] **Step 2: Write failing tests for schema parsing**

Create `tests/test_schema.py`:

```python
"""Tests for YAML schema parsing and validation."""

from pathlib import Path

import pytest
import yaml

from agentic_data_contracts.schema import (
    ConnectionConfig,
    DataContractSchema,
    EnforcementLevel,
    ResourceConfig,
    RuleConfig,
    SemanticConfig,
    SourceConfig,
    SuccessCriterionConfig,
    TableGroup,
    TemporalConfig,
)

FIXTURES = Path(__file__).parent / "fixtures"


class TestDataContractSchemaParsing:
    """Test YAML parsing into Pydantic models."""

    def test_parse_full_contract(self) -> None:
        raw = yaml.safe_load((FIXTURES / "valid_full.yml").read_text())
        schema = DataContractSchema(**raw)

        assert schema.version == "1.0"
        assert schema.name == "revenue-analysis"
        assert schema.semantic is not None
        assert schema.semantic.source is not None
        assert schema.semantic.source.type == "dbt"
        assert schema.semantic.source.path == "./dbt/manifest.json"
        assert len(schema.semantic.allowed_tables) == 2
        assert schema.semantic.allowed_tables[0].schema_ == "analytics"
        assert schema.semantic.allowed_tables[0].tables == ["orders", "customers", "subscriptions"]
        assert schema.semantic.forbidden_operations == ["DELETE", "DROP", "TRUNCATE", "UPDATE", "INSERT"]
        assert len(schema.semantic.rules) == 3
        assert schema.semantic.rules[0].name == "tenant_isolation"
        assert schema.semantic.rules[0].enforcement == EnforcementLevel.BLOCK
        assert schema.semantic.rules[1].enforcement == EnforcementLevel.WARN

    def test_parse_minimal_contract(self) -> None:
        raw = yaml.safe_load((FIXTURES / "valid_minimal.yml").read_text())
        schema = DataContractSchema(**raw)

        assert schema.name == "simple-query-agent"
        assert schema.semantic is not None
        assert schema.semantic.source is None
        assert schema.semantic.rules == []
        assert schema.resources is None
        assert schema.temporal is None
        assert schema.success_criteria == []

    def test_parse_contract_with_connection(self) -> None:
        raw = yaml.safe_load((FIXTURES / "valid_with_connection.yml").read_text())
        schema = DataContractSchema(**raw)

        assert schema.semantic.connection is not None
        assert schema.semantic.connection.type == "bigquery"
        assert schema.semantic.connection.project == "my-project"
        assert schema.semantic.connection.dataset == "analytics"

    def test_parse_resources(self) -> None:
        raw = yaml.safe_load((FIXTURES / "valid_full.yml").read_text())
        schema = DataContractSchema(**raw)

        assert schema.resources is not None
        assert schema.resources.cost_limit_usd == 5.00
        assert schema.resources.max_query_time_seconds == 30
        assert schema.resources.max_retries == 3
        assert schema.resources.max_rows_scanned == 1_000_000
        assert schema.resources.token_budget == 50_000

    def test_parse_temporal(self) -> None:
        raw = yaml.safe_load((FIXTURES / "valid_full.yml").read_text())
        schema = DataContractSchema(**raw)

        assert schema.temporal is not None
        assert schema.temporal.max_duration_seconds == 300

    def test_parse_success_criteria(self) -> None:
        raw = yaml.safe_load((FIXTURES / "valid_full.yml").read_text())
        schema = DataContractSchema(**raw)

        assert len(schema.success_criteria) == 3
        assert schema.success_criteria[0].name == "query_uses_semantic_definitions"
        assert schema.success_criteria[0].weight == 0.4


class TestDataContractSchemaValidation:
    """Test validation errors for invalid contracts."""

    def test_missing_name_rejected(self) -> None:
        raw = yaml.safe_load((FIXTURES / "invalid_missing_name.yml").read_text())
        with pytest.raises(Exception):
            DataContractSchema(**raw)

    def test_invalid_enforcement_rejected(self) -> None:
        raw = yaml.safe_load((FIXTURES / "invalid_bad_enforcement.yml").read_text())
        with pytest.raises(Exception):
            DataContractSchema(**raw)

    def test_negative_cost_rejected(self) -> None:
        with pytest.raises(Exception):
            ResourceConfig(cost_limit_usd=-1.0)

    def test_negative_token_budget_rejected(self) -> None:
        with pytest.raises(Exception):
            ResourceConfig(token_budget=-100)

    def test_empty_name_rejected(self) -> None:
        with pytest.raises(Exception):
            DataContractSchema(version="1.0", name="")

    def test_enforcement_values_are_exhaustive(self) -> None:
        assert set(EnforcementLevel) == {
            EnforcementLevel.BLOCK,
            EnforcementLevel.WARN,
            EnforcementLevel.LOG,
        }


class TestTableGroup:
    """Test table group model."""

    def test_qualified_names(self) -> None:
        tg = TableGroup(schema_="analytics", tables=["orders", "customers"])
        assert tg.qualified_names() == ["analytics.orders", "analytics.customers"]

    def test_empty_tables(self) -> None:
        tg = TableGroup(schema_="raw", tables=[])
        assert tg.qualified_names() == []
```

- [ ] **Step 3: Run tests to verify they fail**

```bash
cd ~/Documents/agentic-data-contracts
pytest tests/test_schema.py -v
```

Expected: FAIL — `ModuleNotFoundError: No module named 'agentic_data_contracts.schema'`

- [ ] **Step 4: Implement schema.py**

Create `src/agentic_data_contracts/schema.py`:

```python
"""Pydantic models for validating data contract YAML structure."""

from enum import Enum

from pydantic import BaseModel, Field, field_validator


class EnforcementLevel(str, Enum):
    """How a rule violation is handled."""

    BLOCK = "block"
    WARN = "warn"
    LOG = "log"


class SourceConfig(BaseModel):
    """Reference to an external source of truth."""

    type: str = Field(description="Source type: dbt | cube | yaml | custom")
    path: str = Field(description="Path or URI to the source")


class TableGroup(BaseModel):
    """A schema and its permitted tables."""

    schema_: str = Field(alias="schema", description="Database schema name")
    tables: list[str] = Field(default_factory=list, description="Permitted table names")

    model_config = {"populate_by_name": True}

    def qualified_names(self) -> list[str]:
        """Return fully qualified table names (schema.table)."""
        return [f"{self.schema_}.{table}" for table in self.tables]


class RuleConfig(BaseModel):
    """A governance rule with enforcement level."""

    name: str = Field(description="Rule identifier")
    description: str = Field(description="Human-readable rule description")
    enforcement: EnforcementLevel = Field(description="block | warn | log")


class ConnectionConfig(BaseModel):
    """Optional database connection for EXPLAIN-based validation."""

    type: str = Field(description="Database type: bigquery | snowflake | postgres | duckdb")
    project: str | None = None
    dataset: str | None = None
    host: str | None = None
    port: int | None = None
    database: str | None = None


class SemanticConfig(BaseModel):
    """Semantic governance configuration."""

    source: SourceConfig | None = None
    allowed_tables: list[TableGroup] = Field(default_factory=list)
    forbidden_operations: list[str] = Field(default_factory=list)
    rules: list[RuleConfig] = Field(default_factory=list)
    connection: ConnectionConfig | None = None


class ResourceConfig(BaseModel):
    """Resource constraints for data queries."""

    cost_limit_usd: float | None = None
    max_query_time_seconds: int | None = None
    max_retries: int | None = None
    max_rows_scanned: int | None = None
    token_budget: int | None = None

    @field_validator("cost_limit_usd")
    @classmethod
    def cost_must_be_non_negative(cls, v: float | None) -> float | None:
        if v is not None and v < 0:
            msg = "cost_limit_usd must be non-negative"
            raise ValueError(msg)
        return v

    @field_validator("token_budget")
    @classmethod
    def tokens_must_be_non_negative(cls, v: int | None) -> int | None:
        if v is not None and v < 0:
            msg = "token_budget must be non-negative"
            raise ValueError(msg)
        return v


class TemporalConfig(BaseModel):
    """Temporal constraints."""

    max_duration_seconds: int


class SuccessCriterionConfig(BaseModel):
    """A success criterion with weight."""

    name: str
    weight: float = 1.0


class DataContractSchema(BaseModel):
    """Root schema for a data contract YAML file."""

    version: str = "1.0"
    name: str = Field(min_length=1, description="Contract name")
    semantic: SemanticConfig = Field(default_factory=SemanticConfig)
    resources: ResourceConfig | None = None
    temporal: TemporalConfig | None = None
    success_criteria: list[SuccessCriterionConfig] = Field(default_factory=list)

    model_config = {"extra": "forbid"}
```

- [ ] **Step 5: Run tests to verify they pass**

```bash
cd ~/Documents/agentic-data-contracts
pytest tests/test_schema.py -v
```

Expected: All tests PASS

- [ ] **Step 6: Commit**

```bash
git add src/agentic_data_contracts/schema.py tests/test_schema.py tests/fixtures/
git commit -m "feat: add YAML schema validation with Pydantic models"
```

---

### Task 3: Compiler (DataContract → Contract)

**Files:**
- Create: `src/agentic_data_contracts/compiler.py`
- Create: `tests/test_compiler.py`

- [ ] **Step 1: Write failing tests for compilation**

Create `tests/test_compiler.py`:

```python
"""Tests for DataContract compilation to agent_contracts.Contract."""

from datetime import timedelta
from pathlib import Path

import pytest

from agent_contracts import (
    Contract,
    Capabilities,
    ResourceConstraints,
    SuccessCriterion,
    TemporalConstraints,
    TerminationCondition,
)
from agentic_data_contracts.compiler import DataContract

FIXTURES = Path(__file__).parent / "fixtures"


class TestDataContractFromYaml:
    """Test loading from YAML files."""

    def test_load_full_contract(self) -> None:
        dc = DataContract.from_yaml(FIXTURES / "valid_full.yml")
        assert dc.schema.name == "revenue-analysis"

    def test_load_minimal_contract(self) -> None:
        dc = DataContract.from_yaml(FIXTURES / "valid_minimal.yml")
        assert dc.schema.name == "simple-query-agent"

    def test_load_nonexistent_file_raises(self) -> None:
        with pytest.raises(FileNotFoundError):
            DataContract.from_yaml("nonexistent.yml")

    def test_load_from_string(self) -> None:
        yaml_str = """
version: "1.0"
name: inline-contract
semantic:
  allowed_tables:
    - schema: public
      tables: [users]
"""
        dc = DataContract.from_yaml_string(yaml_str)
        assert dc.schema.name == "inline-contract"


class TestCompileResources:
    """Test resource constraint compilation."""

    def test_cost_usd_maps(self) -> None:
        dc = DataContract.from_yaml(FIXTURES / "valid_full.yml")
        contract = dc.compile()
        assert contract.resource_constraints.cost_usd == 5.00

    def test_token_budget_maps(self) -> None:
        dc = DataContract.from_yaml(FIXTURES / "valid_full.yml")
        contract = dc.compile()
        assert contract.resource_constraints.tokens == 50_000

    def test_max_retries_maps_to_iterations(self) -> None:
        dc = DataContract.from_yaml(FIXTURES / "valid_full.yml")
        contract = dc.compile()
        assert contract.resource_constraints.iterations == 3

    def test_data_specific_fields_in_metadata(self) -> None:
        dc = DataContract.from_yaml(FIXTURES / "valid_full.yml")
        contract = dc.compile()
        assert contract.metadata["max_query_time_seconds"] == 30
        assert contract.metadata["max_rows_scanned"] == 1_000_000

    def test_no_resources_gives_defaults(self) -> None:
        dc = DataContract.from_yaml(FIXTURES / "valid_minimal.yml")
        contract = dc.compile()
        assert contract.resource_constraints is not None


class TestCompileTemporal:
    """Test temporal constraint compilation."""

    def test_max_duration_maps(self) -> None:
        dc = DataContract.from_yaml(FIXTURES / "valid_full.yml")
        contract = dc.compile()
        assert contract.temporal_constraints.max_duration == timedelta(seconds=300)

    def test_no_temporal_gives_none(self) -> None:
        dc = DataContract.from_yaml(FIXTURES / "valid_minimal.yml")
        contract = dc.compile()
        assert contract.temporal_constraints is None


class TestCompileSemanticRules:
    """Test semantic rule compilation."""

    def test_block_rules_become_termination_conditions(self) -> None:
        dc = DataContract.from_yaml(FIXTURES / "valid_full.yml")
        contract = dc.compile()

        block_conditions = [
            tc for tc in contract.termination_conditions
            if tc.type == "data_contract_violation"
        ]
        # tenant_isolation and no_select_star are both block rules
        assert len(block_conditions) == 2
        condition_strs = [tc.condition for tc in block_conditions]
        assert any("tenant_isolation" in str(c) for c in condition_strs)
        assert any("no_select_star" in str(c) for c in condition_strs)

    def test_warn_rules_become_success_criteria(self) -> None:
        dc = DataContract.from_yaml(FIXTURES / "valid_full.yml")
        contract = dc.compile()

        # use_approved_metrics is a warn rule; plus 3 explicit success_criteria
        warn_criteria = [
            sc for sc in contract.success_criteria
            if "use_approved_metrics" in sc.name
        ]
        assert len(warn_criteria) == 1

    def test_success_criteria_compiled(self) -> None:
        dc = DataContract.from_yaml(FIXTURES / "valid_full.yml")
        contract = dc.compile()

        criteria_names = [sc.name for sc in contract.success_criteria]
        assert "query_uses_semantic_definitions" in criteria_names
        assert "results_are_reproducible" in criteria_names
        assert "output_includes_methodology" in criteria_names


class TestCompileCapabilities:
    """Test capabilities and instructions compilation."""

    def test_source_of_truth_in_instructions(self) -> None:
        dc = DataContract.from_yaml(FIXTURES / "valid_full.yml")
        contract = dc.compile()

        assert contract.capabilities is not None
        assert "./dbt/manifest.json" in contract.capabilities.instructions

    def test_allowed_tables_in_instructions(self) -> None:
        dc = DataContract.from_yaml(FIXTURES / "valid_full.yml")
        contract = dc.compile()

        assert "analytics.orders" in contract.capabilities.instructions
        assert "analytics.customers" in contract.capabilities.instructions

    def test_forbidden_operations_in_instructions(self) -> None:
        dc = DataContract.from_yaml(FIXTURES / "valid_full.yml")
        contract = dc.compile()

        assert "DELETE" in contract.capabilities.instructions
        assert "DROP" in contract.capabilities.instructions

    def test_source_of_truth_in_metadata(self) -> None:
        dc = DataContract.from_yaml(FIXTURES / "valid_full.yml")
        contract = dc.compile()

        assert contract.metadata["source_of_truth"] == {
            "type": "dbt",
            "path": "./dbt/manifest.json",
        }

    def test_allowed_tables_in_metadata(self) -> None:
        dc = DataContract.from_yaml(FIXTURES / "valid_full.yml")
        contract = dc.compile()

        allowed = contract.metadata["allowed_tables"]
        assert "analytics.orders" in allowed


class TestCompileContract:
    """Test the compiled Contract is valid and usable."""

    def test_compiled_contract_is_contract_instance(self) -> None:
        dc = DataContract.from_yaml(FIXTURES / "valid_full.yml")
        contract = dc.compile()
        assert isinstance(contract, Contract)

    def test_contract_id_contains_name(self) -> None:
        dc = DataContract.from_yaml(FIXTURES / "valid_full.yml")
        contract = dc.compile()
        assert "revenue-analysis" in contract.contract_id

    def test_minimal_contract_compiles(self) -> None:
        dc = DataContract.from_yaml(FIXTURES / "valid_minimal.yml")
        contract = dc.compile()
        assert isinstance(contract, Contract)
        assert contract.capabilities is not None
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
cd ~/Documents/agentic-data-contracts
pytest tests/test_compiler.py -v
```

Expected: FAIL — `ModuleNotFoundError: No module named 'agentic_data_contracts.compiler'`

- [ ] **Step 3: Implement compiler.py**

Create `src/agentic_data_contracts/compiler.py`:

```python
"""DataContract: load from YAML and compile to agent_contracts.Contract."""

from datetime import timedelta
from pathlib import Path
from typing import Any

import yaml
from agent_contracts import (
    Capabilities,
    Contract,
    ResourceConstraints,
    SuccessCriterion,
    TemporalConstraints,
    TerminationCondition,
)

from agentic_data_contracts.schema import (
    DataContractSchema,
    EnforcementLevel,
)


class DataContract:
    """A data-domain contract that compiles to agent_contracts.Contract.

    Usage:
        dc = DataContract.from_yaml("data_contract.yml")
        contract = dc.compile()
    """

    def __init__(self, schema: DataContractSchema) -> None:
        self.schema = schema

    @classmethod
    def from_yaml(cls, path: str | Path) -> "DataContract":
        """Load a data contract from a YAML file."""
        path = Path(path)
        if not path.exists():
            msg = f"Contract file not found: {path}"
            raise FileNotFoundError(msg)
        raw = yaml.safe_load(path.read_text())
        schema = DataContractSchema(**raw)
        return cls(schema)

    @classmethod
    def from_yaml_string(cls, yaml_str: str) -> "DataContract":
        """Load a data contract from a YAML string."""
        raw = yaml.safe_load(yaml_str)
        schema = DataContractSchema(**raw)
        return cls(schema)

    def compile(self) -> Contract:
        """Compile this data contract into an agent_contracts.Contract."""
        return Contract(
            contract_id=f"data-contract-{self.schema.name}",
            capabilities=self._compile_capabilities(),
            resource_constraints=self._compile_resources(),
            temporal_constraints=self._compile_temporal(),
            success_criteria=self._compile_success_criteria(),
            termination_conditions=self._compile_termination_conditions(),
            metadata=self._compile_metadata(),
        )

    def _compile_capabilities(self) -> Capabilities:
        """Build Capabilities with instructions from semantic config."""
        instructions_parts: list[str] = []
        semantic = self.schema.semantic

        if semantic.source:
            instructions_parts.append(
                f"IMPORTANT: Consult the {semantic.source.type} source of truth "
                f"at '{semantic.source.path}' for all metric definitions."
            )

        allowed_tables = self._get_all_allowed_tables()
        if allowed_tables:
            table_list = ", ".join(allowed_tables)
            instructions_parts.append(
                f"You may ONLY query the following tables: {table_list}. "
                "Do not reference any other tables."
            )

        if semantic.forbidden_operations:
            ops = ", ".join(semantic.forbidden_operations)
            instructions_parts.append(
                f"FORBIDDEN SQL operations: {ops}. Never use these."
            )

        for rule in semantic.rules:
            if rule.enforcement == EnforcementLevel.WARN:
                instructions_parts.append(
                    f"GUIDELINE ({rule.name}): {rule.description}"
                )
            elif rule.enforcement == EnforcementLevel.BLOCK:
                instructions_parts.append(
                    f"MANDATORY ({rule.name}): {rule.description}"
                )

        instructions = "\n\n".join(instructions_parts) if instructions_parts else None
        return Capabilities(instructions=instructions)

    def _compile_resources(self) -> ResourceConstraints:
        """Map resource config to ResourceConstraints."""
        res = self.schema.resources
        if res is None:
            return ResourceConstraints()

        return ResourceConstraints(
            tokens=res.token_budget,
            cost_usd=res.cost_limit_usd,
            iterations=res.max_retries,
        )

    def _compile_temporal(self) -> TemporalConstraints | None:
        """Map temporal config to TemporalConstraints."""
        if self.schema.temporal is None:
            return None

        return TemporalConstraints(
            max_duration=timedelta(seconds=self.schema.temporal.max_duration_seconds),
        )

    def _compile_success_criteria(self) -> list[SuccessCriterion]:
        """Build success criteria from explicit criteria + warn rules."""
        criteria: list[SuccessCriterion] = []

        for sc in self.schema.success_criteria:
            criteria.append(
                SuccessCriterion(name=sc.name, condition=sc.name, weight=sc.weight)
            )

        for rule in self.schema.semantic.rules:
            if rule.enforcement == EnforcementLevel.WARN:
                criteria.append(
                    SuccessCriterion(
                        name=rule.name,
                        condition=rule.description,
                        weight=0.5,
                    )
                )

        return criteria

    def _compile_termination_conditions(self) -> list[TerminationCondition]:
        """Build termination conditions from block rules."""
        conditions: list[TerminationCondition] = []

        for rule in self.schema.semantic.rules:
            if rule.enforcement == EnforcementLevel.BLOCK:
                conditions.append(
                    TerminationCondition(
                        type="data_contract_violation",
                        condition=f"{rule.name}: {rule.description}",
                    )
                )

        return conditions

    def _compile_metadata(self) -> dict[str, Any]:
        """Build metadata dict with data-specific fields."""
        metadata: dict[str, Any] = {"data_contract_name": self.schema.name}
        semantic = self.schema.semantic

        if semantic.source:
            metadata["source_of_truth"] = {
                "type": semantic.source.type,
                "path": semantic.source.path,
            }

        allowed_tables = self._get_all_allowed_tables()
        if allowed_tables:
            metadata["allowed_tables"] = allowed_tables

        if semantic.forbidden_operations:
            metadata["forbidden_operations"] = semantic.forbidden_operations

        res = self.schema.resources
        if res:
            if res.max_query_time_seconds is not None:
                metadata["max_query_time_seconds"] = res.max_query_time_seconds
            if res.max_rows_scanned is not None:
                metadata["max_rows_scanned"] = res.max_rows_scanned

        log_rules = [r for r in semantic.rules if r.enforcement == EnforcementLevel.LOG]
        if log_rules:
            metadata["log_only_rules"] = [
                {"name": r.name, "description": r.description} for r in log_rules
            ]

        return metadata

    def _get_all_allowed_tables(self) -> list[str]:
        """Collect all qualified table names from allowed_tables."""
        tables: list[str] = []
        for group in self.schema.semantic.allowed_tables:
            tables.extend(group.qualified_names())
        return tables
```

- [ ] **Step 4: Run tests to verify they pass**

```bash
cd ~/Documents/agentic-data-contracts
pytest tests/test_compiler.py -v
```

Expected: All tests PASS

- [ ] **Step 5: Commit**

```bash
git add src/agentic_data_contracts/compiler.py tests/test_compiler.py
git commit -m "feat: add DataContract compiler (YAML -> Contract)"
```

---

### Task 4: SQL Rule Checkers

**Files:**
- Create: `src/agentic_data_contracts/checkers.py`
- Create: `tests/test_checkers.py`

- [ ] **Step 1: Write failing tests for checkers**

Create `tests/test_checkers.py`:

```python
"""Tests for built-in SQL rule checkers."""

import pytest

from agentic_data_contracts.checkers import (
    CheckResult,
    NoSelectStarChecker,
    OperationBlocklistChecker,
    RequiredFilterChecker,
    TableAllowlistChecker,
)


class TestTableAllowlistChecker:
    """Test table allowlist enforcement."""

    def setup_method(self) -> None:
        self.checker = TableAllowlistChecker(
            allowed_tables=["analytics.orders", "analytics.customers"]
        )

    def test_allowed_table_passes(self) -> None:
        result = self.checker.check("SELECT id FROM analytics.orders")
        assert result.passed

    def test_forbidden_table_fails(self) -> None:
        result = self.checker.check("SELECT id FROM raw.payments")
        assert not result.passed
        assert "raw.payments" in result.message

    def test_multiple_tables_all_allowed(self) -> None:
        sql = "SELECT o.id FROM analytics.orders o JOIN analytics.customers c ON o.cid = c.id"
        result = self.checker.check(sql)
        assert result.passed

    def test_multiple_tables_one_forbidden(self) -> None:
        sql = "SELECT o.id FROM analytics.orders o JOIN raw.payments p ON o.id = p.oid"
        result = self.checker.check(sql)
        assert not result.passed

    def test_subquery_table_checked(self) -> None:
        sql = "SELECT * FROM (SELECT id FROM raw.payments) sub"
        result = self.checker.check(sql)
        assert not result.passed

    def test_cte_table_checked(self) -> None:
        sql = """
        WITH cte AS (SELECT id FROM raw.payments)
        SELECT * FROM cte
        """
        result = self.checker.check(sql)
        assert not result.passed

    def test_case_insensitive(self) -> None:
        result = self.checker.check("SELECT id FROM ANALYTICS.ORDERS")
        assert result.passed


class TestOperationBlocklistChecker:
    """Test forbidden operation enforcement."""

    def setup_method(self) -> None:
        self.checker = OperationBlocklistChecker(
            forbidden_operations=["DELETE", "DROP", "TRUNCATE", "UPDATE", "INSERT"]
        )

    def test_select_passes(self) -> None:
        result = self.checker.check("SELECT id FROM orders")
        assert result.passed

    def test_delete_blocked(self) -> None:
        result = self.checker.check("DELETE FROM orders WHERE id = 1")
        assert not result.passed
        assert "DELETE" in result.message

    def test_drop_table_blocked(self) -> None:
        result = self.checker.check("DROP TABLE orders")
        assert not result.passed

    def test_truncate_blocked(self) -> None:
        result = self.checker.check("TRUNCATE TABLE orders")
        assert not result.passed

    def test_insert_blocked(self) -> None:
        result = self.checker.check("INSERT INTO orders (id) VALUES (1)")
        assert not result.passed

    def test_update_blocked(self) -> None:
        result = self.checker.check("UPDATE orders SET status = 'done' WHERE id = 1")
        assert not result.passed


class TestNoSelectStarChecker:
    """Test SELECT * enforcement."""

    def setup_method(self) -> None:
        self.checker = NoSelectStarChecker()

    def test_explicit_columns_pass(self) -> None:
        result = self.checker.check("SELECT id, name FROM orders")
        assert result.passed

    def test_select_star_blocked(self) -> None:
        result = self.checker.check("SELECT * FROM orders")
        assert not result.passed

    def test_select_table_star_blocked(self) -> None:
        result = self.checker.check("SELECT o.* FROM orders o")
        assert not result.passed

    def test_count_star_allowed(self) -> None:
        result = self.checker.check("SELECT COUNT(*) FROM orders")
        assert result.passed


class TestRequiredFilterChecker:
    """Test required WHERE clause enforcement."""

    def setup_method(self) -> None:
        self.checker = RequiredFilterChecker(column="tenant_id")

    def test_filter_present_passes(self) -> None:
        result = self.checker.check("SELECT id FROM orders WHERE tenant_id = 'acme'")
        assert result.passed

    def test_filter_missing_fails(self) -> None:
        result = self.checker.check("SELECT id FROM orders WHERE status = 'active'")
        assert not result.passed
        assert "tenant_id" in result.message

    def test_filter_in_and_clause(self) -> None:
        result = self.checker.check(
            "SELECT id FROM orders WHERE status = 'active' AND tenant_id = 'acme'"
        )
        assert result.passed

    def test_no_where_clause_fails(self) -> None:
        result = self.checker.check("SELECT id FROM orders")
        assert not result.passed


class TestCheckResult:
    """Test CheckResult data class."""

    def test_passed_result(self) -> None:
        result = CheckResult(passed=True, rule_name="test", message="OK")
        assert result.passed

    def test_failed_result(self) -> None:
        result = CheckResult(passed=False, rule_name="test", message="violation")
        assert not result.passed
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
cd ~/Documents/agentic-data-contracts
pytest tests/test_checkers.py -v
```

Expected: FAIL — `ModuleNotFoundError: No module named 'agentic_data_contracts.checkers'`

- [ ] **Step 3: Implement checkers.py**

Create `src/agentic_data_contracts/checkers.py`:

```python
"""Built-in SQL rule checkers using sqlglot for static analysis."""

from abc import ABC, abstractmethod
from dataclasses import dataclass

import sqlglot
from sqlglot import exp


@dataclass
class CheckResult:
    """Result of a rule check against a SQL query."""

    passed: bool
    rule_name: str
    message: str


class SQLChecker(ABC):
    """Base class for SQL rule checkers."""

    @abstractmethod
    def check(self, sql: str) -> CheckResult:
        """Check a SQL query against this rule."""


class TableAllowlistChecker(SQLChecker):
    """Checks that all referenced tables are in the allowlist."""

    def __init__(self, allowed_tables: list[str]) -> None:
        self.allowed_tables = {t.lower() for t in allowed_tables}

    def check(self, sql: str) -> CheckResult:
        try:
            parsed = sqlglot.parse_one(sql)
        except sqlglot.errors.ParseError as e:
            return CheckResult(
                passed=False, rule_name="table_allowlist", message=f"SQL parse error: {e}"
            )

        referenced_tables: set[str] = set()
        for table in parsed.find_all(exp.Table):
            parts = []
            if table.db:
                parts.append(table.db)
            parts.append(table.name)
            qualified = ".".join(parts).lower()
            # Skip CTE references (they appear as tables but are defined in WITH)
            cte_names = {
                cte.alias.lower()
                for cte in parsed.find_all(exp.CTE)
                if cte.alias
            }
            if qualified not in cte_names:
                referenced_tables.add(qualified)

        forbidden = referenced_tables - self.allowed_tables
        if forbidden:
            tables_str = ", ".join(sorted(forbidden))
            return CheckResult(
                passed=False,
                rule_name="table_allowlist",
                message=f"Query references forbidden tables: {tables_str}",
            )

        return CheckResult(passed=True, rule_name="table_allowlist", message="OK")


class OperationBlocklistChecker(SQLChecker):
    """Checks that no forbidden SQL operations are used."""

    OPERATION_MAP: dict[str, type[exp.Expression]] = {
        "DELETE": exp.Delete,
        "DROP": exp.Drop,
        "INSERT": exp.Insert,
        "UPDATE": exp.Update,
    }

    def __init__(self, forbidden_operations: list[str]) -> None:
        self.forbidden_operations = {op.upper() for op in forbidden_operations}

    def check(self, sql: str) -> CheckResult:
        try:
            parsed = sqlglot.parse_one(sql)
        except sqlglot.errors.ParseError as e:
            return CheckResult(
                passed=False, rule_name="operation_blocklist", message=f"SQL parse error: {e}"
            )

        for op_name, op_type in self.OPERATION_MAP.items():
            if op_name in self.forbidden_operations and isinstance(parsed, op_type):
                return CheckResult(
                    passed=False,
                    rule_name="operation_blocklist",
                    message=f"Forbidden operation: {op_name}",
                )

        # Handle TRUNCATE — sqlglot may parse it differently
        if "TRUNCATE" in self.forbidden_operations:
            sql_upper = sql.strip().upper()
            if sql_upper.startswith("TRUNCATE"):
                return CheckResult(
                    passed=False,
                    rule_name="operation_blocklist",
                    message="Forbidden operation: TRUNCATE",
                )

        return CheckResult(passed=True, rule_name="operation_blocklist", message="OK")


class NoSelectStarChecker(SQLChecker):
    """Checks that queries don't use SELECT *."""

    def check(self, sql: str) -> CheckResult:
        try:
            parsed = sqlglot.parse_one(sql)
        except sqlglot.errors.ParseError as e:
            return CheckResult(
                passed=False, rule_name="no_select_star", message=f"SQL parse error: {e}"
            )

        for star in parsed.find_all(exp.Star):
            # Allow COUNT(*) — the star is inside an aggregate function
            parent = star.parent
            if isinstance(parent, exp.Column):
                # This is table.* (e.g., o.*)
                return CheckResult(
                    passed=False,
                    rule_name="no_select_star",
                    message="Query uses SELECT table.* — specify explicit columns",
                )
            # Check if star is a direct SELECT column (not inside COUNT/SUM/etc.)
            is_in_func = False
            node = star
            while node.parent:
                if isinstance(node.parent, (exp.Anonymous, exp.Func)):
                    is_in_func = True
                    break
                if isinstance(node.parent, exp.Select):
                    break
                node = node.parent

            if not is_in_func:
                return CheckResult(
                    passed=False,
                    rule_name="no_select_star",
                    message="Query uses SELECT * — specify explicit columns",
                )

        return CheckResult(passed=True, rule_name="no_select_star", message="OK")


class RequiredFilterChecker(SQLChecker):
    """Checks that a required column appears in the WHERE clause."""

    def __init__(self, column: str) -> None:
        self.column = column.lower()

    def check(self, sql: str) -> CheckResult:
        try:
            parsed = sqlglot.parse_one(sql)
        except sqlglot.errors.ParseError as e:
            return CheckResult(
                passed=False, rule_name="required_filter", message=f"SQL parse error: {e}"
            )

        where = parsed.find(exp.Where)
        if where is None:
            return CheckResult(
                passed=False,
                rule_name="required_filter",
                message=f"Query missing required filter on '{self.column}' — no WHERE clause",
            )

        # Check if the required column appears anywhere in the WHERE clause
        for column in where.find_all(exp.Column):
            if column.name.lower() == self.column:
                return CheckResult(
                    passed=True, rule_name="required_filter", message="OK"
                )

        return CheckResult(
            passed=False,
            rule_name="required_filter",
            message=f"WHERE clause does not filter on required column '{self.column}'",
        )
```

- [ ] **Step 4: Run tests to verify they pass**

```bash
cd ~/Documents/agentic-data-contracts
pytest tests/test_checkers.py -v
```

Expected: All tests PASS. Some tests may need adjustment based on sqlglot's exact parsing behavior — fix any failures iteratively.

- [ ] **Step 5: Commit**

```bash
git add src/agentic_data_contracts/checkers.py tests/test_checkers.py
git commit -m "feat: add SQL rule checkers (table allowlist, operation blocklist, required filter, no select star)"
```

---

### Task 5: Runtime Validator

**Files:**
- Create: `src/agentic_data_contracts/validator.py`
- Create: `tests/test_validator.py`
- Modify: `src/agentic_data_contracts/compiler.py` (add `create_validator()`)

- [ ] **Step 1: Write failing tests for validator**

Create `tests/test_validator.py`:

```python
"""Tests for DataContractValidator runtime SQL validation."""

from pathlib import Path

import pytest

from agentic_data_contracts.compiler import DataContract
from agentic_data_contracts.validator import DataContractValidator, ValidationResult

FIXTURES = Path(__file__).parent / "fixtures"


class TestValidatorFromContract:
    """Test creating validator from a DataContract."""

    def test_create_validator(self) -> None:
        dc = DataContract.from_yaml(FIXTURES / "valid_full.yml")
        validator = dc.create_validator()
        assert isinstance(validator, DataContractValidator)

    def test_minimal_contract_validator(self) -> None:
        dc = DataContract.from_yaml(FIXTURES / "valid_minimal.yml")
        validator = dc.create_validator()
        assert isinstance(validator, DataContractValidator)


class TestValidatorTableChecking:
    """Test table allowlist validation."""

    def setup_method(self) -> None:
        dc = DataContract.from_yaml(FIXTURES / "valid_full.yml")
        self.validator = dc.create_validator()

    def test_allowed_table_passes(self) -> None:
        result = self.validator.validate_sql(
            "SELECT order_id FROM analytics.orders WHERE tenant_id = 'acme'"
        )
        assert result.passed

    def test_forbidden_table_blocked(self) -> None:
        result = self.validator.validate_sql(
            "SELECT id FROM raw.payments WHERE tenant_id = 'acme'"
        )
        assert not result.passed
        assert any(r.rule_name == "table_allowlist" for r in result.violations)


class TestValidatorOperationChecking:
    """Test forbidden operation validation."""

    def setup_method(self) -> None:
        dc = DataContract.from_yaml(FIXTURES / "valid_full.yml")
        self.validator = dc.create_validator()

    def test_select_passes(self) -> None:
        result = self.validator.validate_sql(
            "SELECT id FROM analytics.orders WHERE tenant_id = 'acme'"
        )
        assert result.passed

    def test_delete_blocked(self) -> None:
        result = self.validator.validate_sql("DELETE FROM analytics.orders WHERE id = 1")
        assert not result.passed


class TestValidatorRuleChecking:
    """Test rule-based validation."""

    def setup_method(self) -> None:
        dc = DataContract.from_yaml(FIXTURES / "valid_full.yml")
        self.validator = dc.create_validator()

    def test_select_star_blocked(self) -> None:
        result = self.validator.validate_sql(
            "SELECT * FROM analytics.orders WHERE tenant_id = 'acme'"
        )
        assert not result.passed
        assert any(r.rule_name == "no_select_star" for r in result.violations)

    def test_missing_tenant_id_blocked(self) -> None:
        result = self.validator.validate_sql(
            "SELECT order_id FROM analytics.orders"
        )
        assert not result.passed
        assert any(r.rule_name == "required_filter" for r in result.violations)


class TestValidationResult:
    """Test ValidationResult aggregation."""

    def setup_method(self) -> None:
        dc = DataContract.from_yaml(FIXTURES / "valid_full.yml")
        self.validator = dc.create_validator()

    def test_multiple_violations_collected(self) -> None:
        # SELECT * + missing tenant_id = two violations
        result = self.validator.validate_sql("SELECT * FROM analytics.orders")
        assert not result.passed
        assert len(result.violations) >= 2

    def test_warnings_collected(self) -> None:
        result = self.validator.validate_sql(
            "SELECT order_id FROM analytics.orders WHERE tenant_id = 'acme'"
        )
        # use_approved_metrics is a warn rule — not a violation, but a warning
        assert len(result.warnings) >= 1

    def test_clean_query_no_violations(self) -> None:
        result = self.validator.validate_sql(
            "SELECT order_id, amount FROM analytics.orders WHERE tenant_id = 'acme'"
        )
        assert result.passed
        assert len(result.violations) == 0
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
cd ~/Documents/agentic-data-contracts
pytest tests/test_validator.py -v
```

Expected: FAIL — `ModuleNotFoundError: No module named 'agentic_data_contracts.validator'`

- [ ] **Step 3: Implement validator.py**

Create `src/agentic_data_contracts/validator.py`:

```python
"""Runtime SQL validation against a data contract."""

from dataclasses import dataclass, field

from agentic_data_contracts.checkers import (
    CheckResult,
    NoSelectStarChecker,
    OperationBlocklistChecker,
    RequiredFilterChecker,
    SQLChecker,
    TableAllowlistChecker,
)
from agentic_data_contracts.schema import (
    DataContractSchema,
    EnforcementLevel,
)


@dataclass
class ValidationResult:
    """Aggregated result of validating a SQL query against all rules."""

    passed: bool
    violations: list[CheckResult] = field(default_factory=list)
    warnings: list[CheckResult] = field(default_factory=list)
    log_entries: list[CheckResult] = field(default_factory=list)


class DataContractValidator:
    """Validates SQL queries against a data contract's rules.

    Runs all registered checkers and separates results by enforcement level:
    - block: query is rejected (violation)
    - warn: query proceeds but warning recorded
    - log: recorded for audit only
    """

    def __init__(self, schema: DataContractSchema) -> None:
        self._schema = schema
        self._block_checkers: list[SQLChecker] = []
        self._warn_checkers: list[SQLChecker] = []
        self._log_checkers: list[SQLChecker] = []
        self._build_checkers()

    def _build_checkers(self) -> None:
        """Build checkers from the schema's semantic config."""
        semantic = self._schema.semantic

        # Table allowlist — always a block rule if allowed_tables specified
        allowed_tables: list[str] = []
        for group in semantic.allowed_tables:
            allowed_tables.extend(group.qualified_names())
        if allowed_tables:
            self._block_checkers.append(TableAllowlistChecker(allowed_tables))

        # Operation blocklist — always a block rule
        if semantic.forbidden_operations:
            self._block_checkers.append(
                OperationBlocklistChecker(semantic.forbidden_operations)
            )

        # Named rules
        for rule in semantic.rules:
            checker = self._rule_to_checker(rule.name)
            if checker is None:
                continue

            if rule.enforcement == EnforcementLevel.BLOCK:
                self._block_checkers.append(checker)
            elif rule.enforcement == EnforcementLevel.WARN:
                self._warn_checkers.append(checker)
            elif rule.enforcement == EnforcementLevel.LOG:
                self._log_checkers.append(checker)

    def _rule_to_checker(self, rule_name: str) -> SQLChecker | None:
        """Map a rule name to a built-in checker, if available."""
        if rule_name == "no_select_star":
            return NoSelectStarChecker()
        if rule_name == "tenant_isolation":
            return RequiredFilterChecker(column="tenant_id")
        # Rules without built-in checkers are enforced via instructions only
        return None

    def validate_sql(self, sql: str) -> ValidationResult:
        """Validate a SQL query against all rules."""
        violations: list[CheckResult] = []
        warnings: list[CheckResult] = []
        log_entries: list[CheckResult] = []

        for checker in self._block_checkers:
            result = checker.check(sql)
            if not result.passed:
                violations.append(result)

        for checker in self._warn_checkers:
            result = checker.check(sql)
            if not result.passed:
                warnings.append(result)

        for checker in self._log_checkers:
            result = checker.check(sql)
            if not result.passed:
                log_entries.append(result)

        # Also add instruction-only rules as warnings
        for rule in self._schema.semantic.rules:
            if rule.enforcement == EnforcementLevel.WARN:
                checker = self._rule_to_checker(rule.name)
                if checker is None:
                    # No built-in checker — add as a standing warning
                    warnings.append(
                        CheckResult(
                            passed=False,
                            rule_name=rule.name,
                            message=f"Guideline: {rule.description}",
                        )
                    )

        return ValidationResult(
            passed=len(violations) == 0,
            violations=violations,
            warnings=warnings,
            log_entries=log_entries,
        )
```

- [ ] **Step 4: Add create_validator() to DataContract**

Add the following method to `DataContract` in `src/agentic_data_contracts/compiler.py`:

```python
from agentic_data_contracts.validator import DataContractValidator

# Add this method to the DataContract class:
def create_validator(self) -> DataContractValidator:
    """Create a runtime SQL validator from this contract."""
    return DataContractValidator(self.schema)
```

Add the import at the top of compiler.py and the method to the class body.

- [ ] **Step 5: Run tests to verify they pass**

```bash
cd ~/Documents/agentic-data-contracts
pytest tests/test_validator.py -v
```

Expected: All tests PASS

- [ ] **Step 6: Commit**

```bash
git add src/agentic_data_contracts/validator.py src/agentic_data_contracts/compiler.py tests/test_validator.py
git commit -m "feat: add runtime SQL validator with enforcement levels"
```

---

### Task 6: EXPLAIN Adapters (Layer 2)

**Files:**
- Create: `src/agentic_data_contracts/explain.py`
- Create: `tests/test_explain.py`

- [ ] **Step 1: Write failing tests for EXPLAIN adapters**

Create `tests/test_explain.py`:

```python
"""Tests for EXPLAIN-based validation adapters (Layer 2)."""

from unittest.mock import MagicMock, patch

import pytest

from agentic_data_contracts.explain import (
    ExplainAdapter,
    ExplainResult,
    BigQueryExplainAdapter,
)


class TestExplainResult:
    """Test ExplainResult data class."""

    def test_within_limits(self) -> None:
        result = ExplainResult(
            valid=True,
            estimated_bytes_processed=1_000_000,
            estimated_rows_scanned=10_000,
        )
        assert result.within_limits(max_rows_scanned=100_000)

    def test_exceeds_row_limit(self) -> None:
        result = ExplainResult(
            valid=True,
            estimated_bytes_processed=1_000_000,
            estimated_rows_scanned=200_000,
        )
        assert not result.within_limits(max_rows_scanned=100_000)

    def test_invalid_query(self) -> None:
        result = ExplainResult(
            valid=False,
            error_message="Table not found",
        )
        assert not result.within_limits(max_rows_scanned=100_000)


class TestBigQueryExplainAdapter:
    """Test BigQuery EXPLAIN adapter with mocked client."""

    def test_dry_run_extracts_bytes(self) -> None:
        mock_client = MagicMock()
        mock_job = MagicMock()
        mock_job.total_bytes_processed = 5_000_000
        mock_client.query.return_value = mock_job

        adapter = BigQueryExplainAdapter(client=mock_client)
        result = adapter.explain("SELECT id FROM analytics.orders")

        assert result.valid
        assert result.estimated_bytes_processed == 5_000_000
        mock_client.query.assert_called_once()

    def test_dry_run_error_returns_invalid(self) -> None:
        mock_client = MagicMock()
        mock_client.query.side_effect = Exception("Table not found: raw.payments")

        adapter = BigQueryExplainAdapter(client=mock_client)
        result = adapter.explain("SELECT id FROM raw.payments")

        assert not result.valid
        assert "Table not found" in result.error_message
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
cd ~/Documents/agentic-data-contracts
pytest tests/test_explain.py -v
```

Expected: FAIL — `ModuleNotFoundError: No module named 'agentic_data_contracts.explain'`

- [ ] **Step 3: Implement explain.py**

Create `src/agentic_data_contracts/explain.py`:

```python
"""Optional EXPLAIN-based query validation adapters (Layer 2).

These adapters dry-run queries against real databases to get cost estimates
and schema validation without executing the query.

Usage:
    pip install agentic-data-contracts[bigquery]
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any


@dataclass
class ExplainResult:
    """Result of an EXPLAIN dry-run."""

    valid: bool
    estimated_bytes_processed: int | None = None
    estimated_rows_scanned: int | None = None
    error_message: str | None = None

    def within_limits(
        self,
        max_rows_scanned: int | None = None,
        max_bytes_processed: int | None = None,
    ) -> bool:
        """Check if the estimated cost is within the given limits."""
        if not self.valid:
            return False
        if max_rows_scanned and self.estimated_rows_scanned:
            if self.estimated_rows_scanned > max_rows_scanned:
                return False
        if max_bytes_processed and self.estimated_bytes_processed:
            if self.estimated_bytes_processed > max_bytes_processed:
                return False
        return True


class ExplainAdapter(ABC):
    """Abstract base for database-specific EXPLAIN adapters."""

    @abstractmethod
    def explain(self, sql: str) -> ExplainResult:
        """Dry-run a SQL query and return cost estimates."""


class BigQueryExplainAdapter(ExplainAdapter):
    """BigQuery dry-run adapter.

    Uses BigQuery's job configuration with dry_run=True to get
    estimated bytes processed without executing the query.
    """

    def __init__(self, client: Any) -> None:
        self._client = client

    def explain(self, sql: str) -> ExplainResult:
        try:
            from google.cloud.bigquery import QueryJobConfig

            job_config = QueryJobConfig(dry_run=True, use_query_cache=False)
            query_job = self._client.query(sql, job_config=job_config)

            return ExplainResult(
                valid=True,
                estimated_bytes_processed=query_job.total_bytes_processed,
            )
        except ImportError:
            return ExplainResult(
                valid=False,
                error_message="google-cloud-bigquery not installed. "
                "Install with: pip install agentic-data-contracts[bigquery]",
            )
        except Exception as e:
            return ExplainResult(valid=False, error_message=str(e))
```

- [ ] **Step 4: Run tests to verify they pass**

```bash
cd ~/Documents/agentic-data-contracts
pytest tests/test_explain.py -v
```

Expected: All tests PASS

- [ ] **Step 5: Commit**

```bash
git add src/agentic_data_contracts/explain.py tests/test_explain.py
git commit -m "feat: add EXPLAIN adapter for Layer 2 validation (BigQuery)"
```

---

### Task 7: Public API and Package Exports

**Files:**
- Modify: `src/agentic_data_contracts/__init__.py`

- [ ] **Step 1: Write a test for the public API**

Add to a new file `tests/test_init.py`:

```python
"""Tests for public API exports."""


def test_datacontract_importable() -> None:
    from agentic_data_contracts import DataContract
    assert DataContract is not None


def test_validator_importable() -> None:
    from agentic_data_contracts import DataContractValidator
    assert DataContractValidator is not None


def test_version_defined() -> None:
    from agentic_data_contracts import __version__
    assert __version__ == "0.1.0"


def test_schema_types_importable() -> None:
    from agentic_data_contracts import DataContractSchema, EnforcementLevel
    assert DataContractSchema is not None
    assert EnforcementLevel is not None
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
cd ~/Documents/agentic-data-contracts
pytest tests/test_init.py -v
```

Expected: FAIL — `ImportError: cannot import name 'DataContract' from 'agentic_data_contracts'`

- [ ] **Step 3: Update __init__.py with exports**

Update `src/agentic_data_contracts/__init__.py`:

```python
"""Agentic Data Contracts: YAML-first governance for AI agents querying databases."""

__version__ = "0.1.0"

from agentic_data_contracts.compiler import DataContract
from agentic_data_contracts.schema import DataContractSchema, EnforcementLevel
from agentic_data_contracts.validator import DataContractValidator

__all__ = [
    "DataContract",
    "DataContractSchema",
    "DataContractValidator",
    "EnforcementLevel",
]
```

- [ ] **Step 4: Run all tests to verify everything passes**

```bash
cd ~/Documents/agentic-data-contracts
pytest -v
```

Expected: All tests PASS

- [ ] **Step 5: Commit**

```bash
git add src/agentic_data_contracts/__init__.py tests/test_init.py
git commit -m "feat: export public API (DataContract, DataContractValidator)"
```

---

### Task 8: End-to-End Example

**Files:**
- Create: `examples/revenue_agent/contract.yml`
- Create: `examples/revenue_agent/run_agent.py`

- [ ] **Step 1: Create example contract YAML**

Create `examples/revenue_agent/contract.yml`:

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
  token_budget: 50000
  max_retries: 3

temporal:
  max_duration_seconds: 300
```

- [ ] **Step 2: Create example Python script**

Create `examples/revenue_agent/run_agent.py`:

```python
"""Example: Using a data contract to govern an analytics agent.

This script demonstrates:
1. Loading a data contract from YAML
2. Compiling it to an agent_contracts.Contract
3. Validating SQL queries against the contract
"""

from pathlib import Path

from agentic_data_contracts import DataContract

# Load the contract
contract_path = Path(__file__).parent / "contract.yml"
dc = DataContract.from_yaml(contract_path)

# Compile to agent_contracts.Contract (for use with LiteLLM, LangChain, etc.)
contract = dc.compile()
print(f"Contract: {contract.contract_id}")
print(f"Instructions preview: {contract.capabilities.instructions[:200]}...")
print()

# Create a validator for runtime SQL checking
validator = dc.create_validator()

# Test queries
test_queries = [
    ("SELECT * FROM analytics.orders", "SELECT * should be blocked"),
    ("SELECT order_id FROM analytics.orders", "Missing tenant_id filter"),
    (
        "SELECT order_id, amount FROM analytics.orders WHERE tenant_id = 'acme'",
        "Valid query",
    ),
    (
        "SELECT id FROM raw.payments WHERE tenant_id = 'acme'",
        "Forbidden table",
    ),
    ("DELETE FROM analytics.orders WHERE id = 1", "Forbidden operation"),
]

for sql, description in test_queries:
    result = validator.validate_sql(sql)
    status = "PASSED" if result.passed else "BLOCKED"
    print(f"[{status}] {description}")
    print(f"  SQL: {sql}")
    if result.violations:
        for v in result.violations:
            print(f"  Violation: {v.message}")
    if result.warnings:
        for w in result.warnings:
            print(f"  Warning: {w.message}")
    print()
```

- [ ] **Step 3: Run the example**

```bash
cd ~/Documents/agentic-data-contracts
python examples/revenue_agent/run_agent.py
```

Expected output:
```
Contract: data-contract-revenue-analysis
Instructions preview: IMPORTANT: Consult the dbt source of truth at './dbt/manifest.json' ...

[BLOCKED] SELECT * should be blocked
  SQL: SELECT * FROM analytics.orders
  Violation: Query uses SELECT * — specify explicit columns
  ...

[BLOCKED] Missing tenant_id filter
  ...

[PASSED] Valid query
  ...

[BLOCKED] Forbidden table
  ...

[BLOCKED] Forbidden operation
  ...
```

- [ ] **Step 4: Commit**

```bash
git add examples/
git commit -m "docs: add revenue agent end-to-end example"
```

---

### Task 9: README

**Files:**
- Create: `README.md`

- [ ] **Step 1: Create README.md**

Create `~/Documents/agentic-data-contracts/README.md` with:
- One-paragraph description of the problem (analytics agents need governance)
- The YAML contract example from the spec
- The 5-line Python usage example
- The runtime behavior example (what gets blocked/passed)
- Installation instructions (`pip install agentic-data-contracts`)
- Link to the agent-contracts paper (https://arxiv.org/abs/2601.08815)
- Credit to Robert Yi's post for the "agentic contract layer" concept

Keep it concise — the README is the entry point. Link to docs for details.

- [ ] **Step 2: Commit**

```bash
git add README.md
git commit -m "docs: add README with examples and installation"
```

---

### Task 10: CI and Final Verification

**Files:**
- Create: `.github/workflows/ci.yml`

- [ ] **Step 1: Create CI workflow**

Create `.github/workflows/ci.yml`:

```yaml
name: CI

on:
  push:
    branches: [main]
  pull_request:
    branches: [main]

jobs:
  test:
    runs-on: ubuntu-latest
    strategy:
      matrix:
        python-version: ["3.12", "3.13"]

    steps:
      - uses: actions/checkout@v4
      - name: Install uv
        uses: astral-sh/setup-uv@v4
      - name: Set up Python ${{ matrix.python-version }}
        run: uv python install ${{ matrix.python-version }}
      - name: Install dependencies
        run: uv sync --all-extras
      - name: Lint
        run: uv run ruff check .
      - name: Format check
        run: uv run ruff format --check .
      - name: Type check
        run: uv run mypy src/
      - name: Test
        run: uv run pytest -v
```

- [ ] **Step 2: Run full verification locally**

```bash
cd ~/Documents/agentic-data-contracts
uv run ruff check .
uv run ruff format --check .
uv run mypy src/
uv run pytest -v --cov
```

Expected: All checks pass, coverage > 90%

- [ ] **Step 3: Commit**

```bash
git add .github/
git commit -m "ci: add GitHub Actions workflow"
```

- [ ] **Step 4: Create GitHub repository and push**

```bash
gh repo create flyersworder/agentic-data-contracts --public --source=. --push
```
