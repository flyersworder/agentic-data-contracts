from pathlib import Path

import pytest
import yaml

from agentic_data_contracts.core.schema import (
    AllowedTable,
    DataContractSchema,
    Enforcement,
    SemanticRule,
    SuccessCriterionConfig,
)


def test_full_contract_parses(fixtures_dir: Path) -> None:
    raw = yaml.safe_load((fixtures_dir / "valid_contract.yml").read_text())
    schema = DataContractSchema.model_validate(raw)
    assert schema.name == "revenue-analysis"
    assert schema.version == "1.0"
    assert len(schema.semantic.allowed_tables) == 2
    assert schema.semantic.allowed_tables[0].schema_ == "analytics"
    assert schema.semantic.allowed_tables[0].tables == [
        "orders",
        "customers",
        "subscriptions",
    ]
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
        SemanticRule.model_validate(
            {"name": "bad", "description": "bad rule", "enforcement": "crash"}
        )


def test_enforcement_values() -> None:
    for val in (Enforcement.BLOCK, Enforcement.WARN, Enforcement.LOG):
        rule = SemanticRule(name="test", description="test", enforcement=val)
        assert rule.enforcement == val


def test_success_criteria_weight_validation() -> None:
    with pytest.raises(Exception):
        SuccessCriterionConfig(name="bad", weight=1.5)
    with pytest.raises(Exception):
        SuccessCriterionConfig(name="bad", weight=-0.1)


def test_allowed_table_empty_tables() -> None:
    t = AllowedTable.model_validate({"schema": "raw", "tables": []})
    assert t.tables == []
