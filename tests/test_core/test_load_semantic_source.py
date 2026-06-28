"""Tests for DataContract.load_semantic_source() auto-loading."""

from pathlib import Path

import pytest

from agentic_data_contracts.core.contract import (
    DataContract,
    SemanticSourceUnavailableError,
)
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


def test_declared_but_missing_source_fails_closed(fixtures_dir: Path) -> None:
    """A declared-but-unavailable semantic source must fail closed.

    It raises a governance-specific error rather than a bare FileNotFoundError,
    so calling applications cannot silently swallow it under generic file-error
    handling and proceed with relationship/metric enforcement (and the discovery
    tools) silently degraded.
    """
    schema = DataContractSchema(
        name="test",
        semantic=SemanticConfig(
            source=SemanticSourceConfig(
                type="yaml", path=str(fixtures_dir / "does_not_exist.yml")
            ),
        ),
    )
    dc = DataContract(schema)
    with pytest.raises(SemanticSourceUnavailableError):
        dc.load_semantic_source()
    # Design intent: NOT a FileNotFoundError subclass, so an app's file-error
    # handling won't catch it and fall through to under-enforcement.
    assert not issubclass(SemanticSourceUnavailableError, FileNotFoundError)


def test_freeze_makes_source_self_contained(fixtures_dir: Path, tmp_path: Path) -> None:
    """After freezing, the semantic source loads with NO access to the original
    file — the contract carries its own semantics (portability)."""
    import shutil

    shutil.copy(fixtures_dir / "relationships_checker.yml", tmp_path / "rels.yml")
    (tmp_path / "contract.yml").write_text(
        "name: test\n"
        "semantic:\n"
        "  allowed_tables:\n"
        "    - schema: analytics\n"
        "      tables: [orders, customers]\n"
        "  source:\n"
        "    type: yaml\n"
        "    path: rels.yml\n"
    )
    dc = DataContract.from_yaml(tmp_path / "contract.yml")
    dc.freeze_semantic_source()
    assert dc.schema.semantic.source is not None
    assert dc.schema.semantic.source.inline is not None  # snapshot populated

    # Delete the original source file: a frozen contract must not need it.
    (tmp_path / "rels.yml").unlink()
    source = dc.load_semantic_source()
    assert source is not None
    # relationships_checker.yml declares 3 relationships — all survive the freeze.
    assert len(source.get_relationships()) == 3


def test_freeze_is_idempotent_and_noop_without_source() -> None:
    """Re-freezing leaves the snapshot untouched; freezing a source-less contract
    is a harmless no-op."""
    no_source = DataContract(DataContractSchema(name="test", semantic=SemanticConfig()))
    no_source.freeze_semantic_source()  # must not raise
    assert no_source.schema.semantic.source is None

    schema = DataContractSchema(
        name="test",
        semantic=SemanticConfig(
            source=SemanticSourceConfig(
                type="yaml", path=str(Path(__file__).parent.parent / "fixtures")
            ),
        ),
    )
    # Pre-populate an inline snapshot; a second freeze must leave it identical.
    dc = DataContract(schema)
    source = dc.schema.semantic.source
    assert source is not None
    source.inline = {"relationships": []}
    sentinel = source.inline
    dc.freeze_semantic_source()
    assert source.inline is sentinel
