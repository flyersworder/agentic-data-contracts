"""An explicit null in a source document must not reach a ``str`` field.

Found while fixing #89: `dbt.py` built a `Relationship` with
``node.get("description", "")``, whose default never fires when the key is
*present and null* -- `{"description": null}` in a manifest, or a bare
``description:`` in YAML, both load as `None`. The field is annotated `str` and
is public API, so a consumer calling `.strip()` or `.lower()` on it gets an
`AttributeError` from data that parsed without complaint.

`.get(key, "")` is the wrong tool for this whenever the key may be authored: it
defends against *absence*, and the hazard is *presence with no value*. The same
shape appeared at roughly twenty sites across the four semantic sources, so
these tests cover the class rather than the one line that surfaced it.

Fields annotated ``str | None`` (``convention``, ``required_filter``,
``indicator_kind``, the owners) are deliberately untouched -- there `None` is a
meaningful value, not a malformed one.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest
import yaml

from agentic_data_contracts.semantic.cube import CubeSource
from agentic_data_contracts.semantic.dbt import DbtSource
from agentic_data_contracts.semantic.ossie import OssieSource
from agentic_data_contracts.semantic.yaml_source import YamlSource


def _write(path: Path, payload: Any) -> Path:
    path.write_text(
        json.dumps(payload) if path.suffix == ".json" else yaml.safe_dump(payload)
    )
    return path


# ── YamlSource ──────────────────────────────────────────────────────────────


_NULL_EVERYTHING = """
metrics:
  - name: revenue
    description:
    sql_expression:
    source_model:
tables:
  - schema: main
    table: payments
    columns:
      - name: region
        type:
        description:
relationships:
  - from: main.payments.region
    to: main.regions.id
    type:
    description:
metric_impacts:
  - from: revenue
    to: revenue
    direction:
    confidence:
    evidence:
    description:
"""


@pytest.fixture
def null_source() -> YamlSource:
    return YamlSource.from_raw(yaml.safe_load(_NULL_EVERYTHING))


def test_metric_text_fields_are_strings(null_source: YamlSource) -> None:
    metric = null_source.get_metric("revenue")
    assert metric is not None
    assert metric.description == ""
    assert metric.sql_expression == ""
    assert metric.source_model == ""


def test_column_text_fields_are_strings(null_source: YamlSource) -> None:
    column = null_source.get_table_schemas()["main.payments"].columns[0]
    assert column.type == ""
    assert column.description == ""


def test_relationship_text_fields_are_strings(null_source: YamlSource) -> None:
    rel = null_source.get_relationships()[0]
    assert rel.description == ""
    # A null `type:` must fall back to the documented default, not become None --
    # `_relationship_note` and the prompt renderer both read it as a string.
    assert rel.type == "many_to_one"


def test_metric_impact_text_fields_are_strings(null_source: YamlSource) -> None:
    impact = null_source.get_metric_impacts()[0]
    assert impact.evidence == ""
    assert impact.description == ""
    assert impact.direction == "positive"
    assert impact.confidence == "hypothesized"


# ── DbtSource ───────────────────────────────────────────────────────────────


def test_dbt_relationship_description_is_a_string(tmp_path: Path) -> None:
    """The site that surfaced the class (`dbt.py`'s `_parse_relationships`)."""
    manifest = _write(
        tmp_path / "manifest.json",
        {
            "nodes": {
                "model.p.orders": {
                    "resource_type": "model",
                    "schema": "analytics",
                    "name": "orders",
                    "columns": {},
                },
                "model.p.customers": {
                    "resource_type": "model",
                    "schema": "analytics",
                    "name": "customers",
                    "columns": {},
                },
                "test.p.rel": {
                    "resource_type": "test",
                    "description": None,
                    "attached_node": "model.p.orders",
                    "depends_on": {"nodes": ["model.p.orders", "model.p.customers"]},
                    "test_metadata": {
                        "name": "relationships",
                        "kwargs": {"column_name": "customer_id", "field": "id"},
                    },
                },
            },
            "metrics": {},
        },
    )
    rel = DbtSource(manifest).get_relationships()[0]
    assert rel.description == ""
    assert rel.type == "many_to_one"


def test_dbt_metric_and_column_text_fields_are_strings(tmp_path: Path) -> None:
    manifest = _write(
        tmp_path / "manifest.json",
        {
            "nodes": {
                "model.p.orders": {
                    "resource_type": "model",
                    "schema": "analytics",
                    "name": "orders",
                    "description": None,
                    "columns": {
                        "id": {"name": "id", "data_type": None, "description": None}
                    },
                }
            },
            "metrics": {
                "metric.p.revenue": {
                    "name": "revenue",
                    "description": None,
                    "model": None,
                    "type_params": {"measure": {"expr": None}},
                }
            },
        },
    )
    src = DbtSource(manifest)
    column = src.get_table_schemas()["analytics.orders"].columns[0]
    assert column.type == ""
    assert column.description == ""
    metric = src.get_metrics()[0]
    assert metric.description == ""
    assert metric.source_model == ""


# ── CubeSource ──────────────────────────────────────────────────────────────


def test_cube_text_fields_are_strings(tmp_path: Path) -> None:
    schema = _write(
        tmp_path / "cube.yml",
        {
            "cubes": [
                {
                    "name": "Orders",
                    "sql_table": "analytics.orders",
                    "description": None,
                    "measures": [
                        {"name": "revenue", "sql": None, "description": None},
                    ],
                    "columns": [{"name": "id", "type": None, "description": None}],
                }
            ]
        },
    )
    src = CubeSource(schema)
    table = src.get_table_schemas()["analytics.orders"]
    assert table.description == ""
    assert table.columns[0].type == ""
    assert table.columns[0].description == ""
    assert src.get_metrics()[0].description == ""


# ── OssieSource ─────────────────────────────────────────────────────────────


def test_ossie_field_text_is_a_string(tmp_path: Path) -> None:
    model = _write(
        tmp_path / "ossie.yml",
        {
            "semantic_model": [
                {
                    "name": "retail",
                    "datasets": [
                        {
                            "name": "orders",
                            "source": "analytics.orders",
                            "description": None,
                            "fields": [
                                {"name": "id", "datatype": None, "description": None}
                            ],
                        }
                    ],
                }
            ]
        },
    )
    table = OssieSource(model).get_table_schemas()["analytics.orders"]
    assert table.description == ""
    assert table.columns[0].type == ""
    assert table.columns[0].description == ""


def test_ossie_metric_impact_text_fields_are_strings(tmp_path: Path) -> None:
    """The CHANGELOG claimed "all four sources"; Ossie's impacts were missed.

    A null ``direction:`` reached `_format_impact_edge` and rendered
    "None impact on X (None)" into the agent's context -- exactly what the
    YamlSource fix was added to prevent.
    """
    model = _write(
        tmp_path / "ossie.yml",
        {
            "semantic_model": [
                {
                    "name": "retail",
                    "datasets": [
                        {
                            "name": "orders",
                            "source": "analytics.orders",
                            "fields": [{"name": "id", "datatype": "INT"}],
                        }
                    ],
                    "custom_extensions": [
                        {
                            "vendor_name": "AGENTIC_DATA_CONTRACTS",
                            "data": {
                                "metric_impacts": [
                                    {
                                        "from": "a",
                                        "to": "b",
                                        "direction": None,
                                        "confidence": None,
                                        "evidence": None,
                                        "description": None,
                                    }
                                ]
                            },
                        }
                    ],
                }
            ]
        },
    )
    impacts = OssieSource(model).get_metric_impacts()
    assert impacts, "no impact parsed — fixture shape is wrong, not the code"
    assert impacts[0].direction == "positive"
    assert impacts[0].confidence == "hypothesized"
    assert impacts[0].evidence == ""
    assert impacts[0].description == ""
