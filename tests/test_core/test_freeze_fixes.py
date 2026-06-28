"""Code-review fixes for the freeze / portability feature.

Covers: freeze clears the machine-specific path and normalizes type (so the
content address is reproducible and leaks no paths); freeze captures table
column-schemas (describe_table keeps authored descriptions off-box);
freeze(force=True) does not crash on inline-only contracts; the fail-closed
wrapper covers all unloadable-source errors, not just FileNotFoundError; and a
semantic source must declare either a path or an inline snapshot.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest
from pydantic import ValidationError

from agentic_data_contracts.core.contract import (
    DataContract,
    SemanticSourceUnavailableError,
)
from agentic_data_contracts.core.schema import DataContractSchema, SemanticConfig
from agentic_data_contracts.core.schema import SemanticSource as SemanticSourceConfig
from agentic_data_contracts.semantic.base import dump_semantic_source
from agentic_data_contracts.semantic.yaml_source import YamlSource


def test_freeze_clears_path_and_normalizes_type(fixtures_dir: Path) -> None:
    # Fix 1 + 6: the external path is machine-specific and must NOT survive into
    # the frozen, content-addressed artifact; type must match the YamlSource the
    # inline snapshot rehydrates into.
    dc = DataContract.from_yaml(fixtures_dir / "roundtrip_contract.yml")
    src = dc.schema.semantic.source
    assert src is not None and src.path is not None  # path present before freeze
    dc.freeze_semantic_source()
    assert src.inline is not None
    assert src.path is None
    assert src.type == "yaml"


def test_dump_round_trip_preserves_table_column_descriptions(
    fixtures_dir: Path,
) -> None:
    # Fix 2: authored column descriptions must survive freeze -> rehydrate so
    # describe_table still shows business context on a frozen contract.
    src = YamlSource(fixtures_dir / "semantic_source.yml")
    rebuilt = YamlSource.from_raw(dump_semantic_source(src))
    original = src.get_table_schema("analytics", "orders")
    restored = rebuilt.get_table_schema("analytics", "orders")
    assert original is not None
    assert restored is not None
    assert {c.name: c.description for c in restored.columns} == {
        c.name: c.description for c in original.columns
    }


def test_freeze_force_inline_only_does_not_crash() -> None:
    # Fix 3: a frozen / hand-authored inline-only contract (no path) has nothing
    # to reload from; freeze(force=True) must be a no-op, not a crash.
    dc = DataContract(
        DataContractSchema(
            name="t",
            semantic=SemanticConfig(
                source=SemanticSourceConfig(type="yaml", inline={"relationships": []}),
            ),
        )
    )
    dc.freeze_semantic_source(force=True)  # must not raise
    src = dc.schema.semantic.source
    assert src is not None
    assert src.inline == {"relationships": []}


def test_directory_source_path_fails_closed(tmp_path: Path) -> None:
    # Fix 5: a source path that is a directory (IsADirectoryError) must surface
    # as the governance error, not a raw OSError that escapes the fail-closed
    # contract.
    dc = DataContract(
        DataContractSchema(
            name="t",
            semantic=SemanticConfig(
                source=SemanticSourceConfig(type="yaml", path=str(tmp_path)),
            ),
        )
    )
    with pytest.raises(SemanticSourceUnavailableError):
        dc.load_semantic_source()


def test_semantic_source_requires_path_or_inline() -> None:
    # Fix 7: a source with neither path nor inline must fail fast at load time,
    # not silently validate and explode later at agent runtime.
    with pytest.raises(ValidationError):
        SemanticSourceConfig(type="yaml")


def test_malformed_dbt_source_fails_closed(tmp_path: Path) -> None:
    # Review #1: a malformed dbt manifest (json.JSONDecodeError, a ValueError —
    # not an OSError/YAMLError) must still surface as the governance error so the
    # fail-closed contract is uniform across source types.
    bad = tmp_path / "manifest.json"
    bad.write_text("{ this is not valid json")
    dc = DataContract(
        DataContractSchema(
            name="t",
            semantic=SemanticConfig(
                source=SemanticSourceConfig(type="dbt", path=str(bad)),
            ),
        )
    )
    with pytest.raises(SemanticSourceUnavailableError):
        dc.load_semantic_source()


def test_freeze_dbt_source_normalizes_to_yaml_and_preserves_metrics(
    fixtures_dir: Path,
) -> None:
    # Review #5: the "source-type-agnostic" claim — a dbt source freezes into the
    # canonical YAML-source inline shape and rehydrates losslessly off-box.
    dc = DataContract(
        DataContractSchema(
            name="t",
            semantic=SemanticConfig(
                source=SemanticSourceConfig(
                    type="dbt", path=str(fixtures_dir / "sample_dbt_manifest.json")
                ),
            ),
        )
    )
    dc.freeze_semantic_source()
    src = dc.schema.semantic.source
    assert src is not None
    assert src.type == "yaml" and src.path is None  # normalized
    rebuilt = DataContract.from_yaml_string(
        json.dumps(dc.schema.model_dump(mode="json"))
    )
    source = rebuilt.load_semantic_source()
    assert source is not None
    assert len(source.get_metrics()) == 1  # the dbt manifest's metric survived
