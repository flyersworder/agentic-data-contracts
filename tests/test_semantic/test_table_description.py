"""A table-level description on ``TableSchema`` (#89).

``Column`` carries a description and ``AllowedTable`` carries one for a
*schema group* -- a schema name plus the tables under it, collectively. The
table itself was skipped, so the granularities on offer were "this whole
schema" and "this one column" with the middle missing: "``payments`` is one
row per authorisation attempt, not per settled transaction" had nowhere to
live.

The constraint shaping the serialization tests below: ``dump_semantic_source``
feeds ``contract_canonical_bytes``, which dumps with no ``exclude_none``, so
an *always-present* new key moves every published digest. The field is
therefore emitted only when non-empty -- the omit-when-empty rule already
recorded on ``_dump_metric``.
"""

from __future__ import annotations

from agentic_data_contracts.adapters.base import Column, TableSchema
from agentic_data_contracts.semantic.base import dump_semantic_source
from agentic_data_contracts.semantic.yaml_source import YamlSource

_DESC = "One row per authorisation attempt, not per settled transaction."


# ── The dataclass ───────────────────────────────────────────────────────────


def test_table_schema_carries_a_description() -> None:
    assert TableSchema(columns=[], description=_DESC).description == _DESC


def test_table_schema_description_defaults_to_empty() -> None:
    assert TableSchema().description == ""


def test_positional_construction_still_binds_columns() -> None:
    """``description`` is appended, not inserted.

    ``TableSchema`` is public API and not ``kw_only``, so field order is part
    of its contract -- the rule already recorded on ``Attempt.final_rows``.
    """
    ts = TableSchema([Column(name="region", type="VARCHAR")])
    assert ts.columns[0].name == "region"
    assert ts.description == ""


# ── Parsing ─────────────────────────────────────────────────────────────────


def test_yaml_source_reads_a_table_description() -> None:
    src = YamlSource.from_raw(
        {
            "metrics": [],
            "tables": [
                {"schema": "main", "table": "payments", "description": _DESC},
            ],
        },
        expected_extras=[],
    )
    assert src.get_table_schemas()["main.payments"].description == _DESC


def test_a_table_without_a_description_gets_the_empty_string() -> None:
    src = YamlSource.from_raw(
        {"metrics": [], "tables": [{"schema": "main", "table": "payments"}]}
    )
    assert src.get_table_schemas()["main.payments"].description == ""


# ── Serialization, and the digest it feeds ──────────────────────────────────


def test_table_description_round_trips_through_a_dump() -> None:
    raw = {
        "metrics": [],
        "tables": [{"schema": "main", "table": "payments", "description": _DESC}],
    }
    rebuilt = YamlSource.from_raw(dump_semantic_source(YamlSource.from_raw(raw)))
    assert rebuilt.get_table_schemas()["main.payments"].description == _DESC


def test_a_description_free_table_dumps_without_the_key() -> None:
    """The digest-stability guarantee, stated as the property it protects.

    A frozen contract carrying no table descriptions must produce
    byte-identical canonical bytes across this upgrade. An unconditional
    ``"description": ""`` in the dumped dict would move every published
    digest.
    """
    dumped = dump_semantic_source(
        YamlSource.from_raw(
            {"metrics": [], "tables": [{"schema": "main", "table": "payments"}]}
        )
    )
    assert "description" not in dumped["tables"][0]


def test_a_described_table_dumps_with_the_key() -> None:
    dumped = dump_semantic_source(
        YamlSource.from_raw(
            {
                "metrics": [],
                "tables": [
                    {"schema": "main", "table": "payments", "description": _DESC}
                ],
            }
        )
    )
    assert dumped["tables"][0]["description"] == _DESC


# ── The other semantic sources already hold one and discarded it ────────────


def test_dbt_source_carries_a_model_description(fixtures_dir) -> None:  # noqa: ANN001
    """A dbt model's ``description`` is the same fact under another name."""
    from agentic_data_contracts.semantic.dbt import DbtSource

    src = DbtSource(fixtures_dir / "sample_dbt_manifest.json")
    described = [
        ts.description for ts in src.get_table_schemas().values() if ts.description
    ]
    assert described, "no dbt model description reached TableSchema"


def test_ossie_source_carries_a_dataset_description(fixtures_dir) -> None:  # noqa: ANN001
    from agentic_data_contracts.semantic.ossie import OssieSource

    src = OssieSource(fixtures_dir / "sample_ossie_model.yml")
    described = [
        ts.description for ts in src.get_table_schemas().values() if ts.description
    ]
    assert described, "no Ossie dataset description reached TableSchema"
