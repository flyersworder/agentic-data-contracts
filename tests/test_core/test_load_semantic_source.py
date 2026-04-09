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


def test_load_relative_path_resolves_from_contract_dir(
    fixtures_dir: Path, tmp_path: Path
) -> None:
    """Relative paths in source.path should resolve against the contract file's dir."""
    # Copy the semantic source into tmp_path alongside the contract
    import shutil

    shutil.copy(fixtures_dir / "semantic_source.yml", tmp_path / "semantic_source.yml")
    contract_yaml = (
        "name: test\n"
        "semantic:\n"
        "  allowed_tables:\n"
        "    - schema: analytics\n"
        "      tables: [orders]\n"
        "  source:\n"
        "    type: yaml\n"
        "    path: ./semantic_source.yml\n"
    )
    (tmp_path / "contract.yml").write_text(contract_yaml)
    dc = DataContract.from_yaml(tmp_path / "contract.yml")
    source = dc.load_semantic_source()
    assert isinstance(source, YamlSource)
    assert len(source.get_metrics()) == 2


def test_load_relative_path_from_subdirectory(
    fixtures_dir: Path, tmp_path: Path
) -> None:
    """Relative path with ../ resolves from contract dir."""
    # Put a semantic file in tmp_path
    semantic_content = (
        "metrics:\n  - name: m1\n    description: test\n    sql_expression: '1'\n"
    )
    (tmp_path / "semantic.yml").write_text(semantic_content)

    # Put a contract in a subdirectory that references ../semantic.yml
    subdir = tmp_path / "contracts"
    subdir.mkdir()
    contract_yaml = (
        "name: test\n"
        "semantic:\n"
        "  allowed_tables:\n"
        "    - schema: s\n"
        "      tables: [t]\n"
        "  source:\n"
        "    type: yaml\n"
        "    path: ../semantic.yml\n"
    )
    (subdir / "contract.yml").write_text(contract_yaml)
    dc = DataContract.from_yaml(subdir / "contract.yml")
    source = dc.load_semantic_source()
    assert isinstance(source, YamlSource)
    assert len(source.get_metrics()) == 1


def test_load_absolute_path_unaffected_by_source_dir(
    fixtures_dir: Path, tmp_path: Path
) -> None:
    """Absolute paths should work regardless of _source_dir."""
    abs_path = str(fixtures_dir / "semantic_source.yml")
    contract_yaml = (
        "name: test\n"
        "semantic:\n"
        "  allowed_tables:\n"
        "    - schema: analytics\n"
        "      tables: [orders]\n"
        "  source:\n"
        "    type: yaml\n"
        "    path: " + abs_path + "\n"
    )
    # Contract in tmp_path, but semantic source path is absolute
    (tmp_path / "contract.yml").write_text(contract_yaml)
    dc = DataContract.from_yaml(tmp_path / "contract.yml")
    source = dc.load_semantic_source()
    assert isinstance(source, YamlSource)
    assert len(source.get_metrics()) == 2


def test_from_yaml_string_has_no_source_dir() -> None:
    """Contracts loaded from strings have no _source_dir (backward compat)."""
    contract_yaml = (
        "name: test\nsemantic:\n  allowed_tables:\n    - schema: s\n      tables: [t]\n"
    )
    dc = DataContract.from_yaml_string(contract_yaml)
    assert dc._source_dir is None
