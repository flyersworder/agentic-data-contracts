"""Tests for DataContract.to_sdk_config()."""

from pathlib import Path

from agentic_data_contracts.core.contract import DataContract
from agentic_data_contracts.core.schema import (
    DataContractSchema,
    ResourceConfig,
    SemanticConfig,
)


def test_sdk_config_from_full_contract(fixtures_dir: Path) -> None:
    dc = DataContract.from_yaml(fixtures_dir / "valid_contract.yml")
    config = dc.to_sdk_config()
    assert config["task_budget"] == 50000
    assert config["max_turns"] == 3


def test_sdk_config_no_resources() -> None:
    schema = DataContractSchema(name="test", semantic=SemanticConfig())
    dc = DataContract(schema)
    config = dc.to_sdk_config()
    assert config == {}


def test_sdk_config_partial_resources() -> None:
    schema = DataContractSchema(
        name="test",
        semantic=SemanticConfig(),
        resources=ResourceConfig(token_budget=10000),
    )
    dc = DataContract(schema)
    config = dc.to_sdk_config()
    assert config["task_budget"] == 10000
    assert "max_turns" not in config
