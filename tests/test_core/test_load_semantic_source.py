"""Tests for DataContract.load_semantic_source() auto-loading."""

from pathlib import Path

import pytest

from agentic_data_contracts.core.contract import DataContract
from agentic_data_contracts.core.schema import (
    DataContractSchema,
    SemanticConfig,
)
from agentic_data_contracts.core.schema import (
    SemanticSource as SemanticSourceConfig,
)
from agentic_data_contracts.semantic.cube import CubeSource
from agentic_data_contracts.semantic.dbt import DbtSource
from agentic_data_contracts.semantic.yaml_source import YamlSource


def test_load_yaml_source(fixtures_dir: Path) -> None:
    schema = DataContractSchema(
        name="test",
        semantic=SemanticConfig(
            source=SemanticSourceConfig(
                type="yaml",
                path=str(fixtures_dir / "semantic_source.yml"),
            ),
        ),
    )
    dc = DataContract(schema)
    source = dc.load_semantic_source()
    assert isinstance(source, YamlSource)
    assert len(source.get_metrics()) == 2


def test_load_dbt_source(fixtures_dir: Path) -> None:
    schema = DataContractSchema(
        name="test",
        semantic=SemanticConfig(
            source=SemanticSourceConfig(
                type="dbt",
                path=str(fixtures_dir / "sample_dbt_manifest.json"),
            ),
        ),
    )
    dc = DataContract(schema)
    source = dc.load_semantic_source()
    assert isinstance(source, DbtSource)
    assert len(source.get_metrics()) == 1


def test_load_cube_source(fixtures_dir: Path) -> None:
    schema = DataContractSchema(
        name="test",
        semantic=SemanticConfig(
            source=SemanticSourceConfig(
                type="cube",
                path=str(fixtures_dir / "sample_cube_schema.yml"),
            ),
        ),
    )
    dc = DataContract(schema)
    source = dc.load_semantic_source()
    assert isinstance(source, CubeSource)
    assert len(source.get_metrics()) == 1


def test_load_no_source_returns_none() -> None:
    schema = DataContractSchema(
        name="test",
        semantic=SemanticConfig(),
    )
    dc = DataContract(schema)
    assert dc.load_semantic_source() is None


def test_load_unknown_type_raises() -> None:
    schema = DataContractSchema(
        name="test",
        semantic=SemanticConfig(
            source=SemanticSourceConfig(type="unknown", path="foo.json"),
        ),
    )
    dc = DataContract(schema)
    with pytest.raises(ValueError, match="Unknown semantic source type"):
        dc.load_semantic_source()


def test_load_case_insensitive(fixtures_dir: Path) -> None:
    schema = DataContractSchema(
        name="test",
        semantic=SemanticConfig(
            source=SemanticSourceConfig(
                type="YAML",
                path=str(fixtures_dir / "semantic_source.yml"),
            ),
        ),
    )
    dc = DataContract(schema)
    source = dc.load_semantic_source()
    assert isinstance(source, YamlSource)
