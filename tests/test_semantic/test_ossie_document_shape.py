"""OssieSource against the current spec's document shape and expressions.

Three spec changes landed after OssieSource was written, and each one made a
current-spec document load as *less* than it says, without an error:

- apache/ossie#383 removed the ``semantic_model`` wrapper: a document is one
  model, its ``name``/``datasets``/``metrics`` at the root. Read through the
  wrapper, such a document had no models, so it loaded as an empty source.
- apache/ossie#617 added ``expression.sql``, the primary portable expression,
  beside ``dialects``. A metric written with only ``sql`` had no expression.
- apache/ossie#591 added dataset-scoped ``datasets[].metrics``, which were
  never read.
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

import pytest
import yaml

from agentic_data_contracts.semantic.base import MetricDefinition
from agentic_data_contracts.semantic.ossie import OssieSource

NVIDIA = (
    Path(__file__).parent.parent / "fixtures" / "ossie_flat_nvidia_auto_ontology.yml"
)


def _flat(**overrides: Any) -> dict[str, Any]:
    doc: dict[str, Any] = {
        "version": "0.2.0.dev0",
        "name": "sales",
        "datasets": [
            {
                "name": "orders",
                "source": "analytics.public.orders",
                "primary_key": ["order_id"],
                "fields": [
                    {"name": "order_id", "expression": {"sql": "order_id"}},
                    {"name": "amount", "expression": {"sql": "amount"}},
                ],
            }
        ],
    }
    doc.update(overrides)
    return doc


def _load(tmp_path: Path, doc: Any) -> OssieSource:
    path = tmp_path / "model.yml"
    path.write_text(yaml.safe_dump(doc))
    return OssieSource(path)


def _get(source: OssieSource, name: str) -> MetricDefinition:
    metric = source.get_metric(name)
    assert metric is not None, f"{name} did not load"
    return metric


def _metric(expression: dict[str, Any], name: str = "m") -> dict[str, Any]:
    return {"name": name, "expression": expression}


# --- the flat document ----------------------------------------------------


def test_a_producers_flat_export_loads_its_tables() -> None:
    source = OssieSource(NVIDIA)
    assert sorted(source.get_table_schemas()) == ["public.customers", "public.orders"]


def test_a_producers_flat_export_loads_its_metric() -> None:
    metric = OssieSource(NVIDIA).get_metric("revenue_per_customer")
    assert metric is not None
    assert metric.sql_expression == (
        "SUM(orders.subtotal) / COUNT(DISTINCT customers.customer_id)"
    )


def test_a_producers_flat_export_loads_its_relationship() -> None:
    [rel] = OssieSource(NVIDIA).get_relationships()
    assert (rel.from_, rel.to, rel.type) == (
        "public.orders.customer_id",
        "public.customers.customer_id",
        "many_to_one",
    )


def test_a_flat_documents_root_name_scopes_its_extras() -> None:
    """The root ``name`` is the model name extras are keyed by."""
    extensions = OssieSource(NVIDIA).get_extras()["ossie_custom_extensions"]
    assert "NVIDIA_AUTO_ONTOLOGY" in extensions["sales"]


def test_the_legacy_wrapper_still_loads_and_says_it_is_legacy(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    model = _flat()
    version = model.pop("version")
    with caplog.at_level(logging.WARNING):
        source = _load(tmp_path, {"version": version, "semantic_model": [model]})
    assert source.get_table_schema("public", "orders") is not None
    assert "semantic_model" in caplog.text


# --- a document that is neither shape fails instead of loading empty -------


def test_a_document_with_no_datasets_and_no_wrapper_is_rejected(
    tmp_path: Path,
) -> None:
    with pytest.raises(ValueError, match="datasets"):
        _load(tmp_path, {"version": "0.2.0.dev0", "name": "sales"})


def test_an_empty_legacy_wrapper_is_rejected(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="semantic_model"):
        _load(tmp_path, {"version": "0.2.0.dev0", "semantic_model": []})


def test_a_document_in_both_shapes_is_rejected(tmp_path: Path) -> None:
    """Choosing one would silently drop the other's models."""
    model = _flat()
    with pytest.raises(ValueError, match="both"):
        _load(tmp_path, {**model, "semantic_model": [_flat(name="other")]})


# --- expression.sql and the OSSIE_SQL_2026 spelling ------------------------


def test_a_metric_written_with_only_sql_has_that_expression(tmp_path: Path) -> None:
    source = _load(tmp_path, _flat(metrics=[_metric({"sql": "SUM(orders.amount)"})]))
    assert _get(source, "m").sql_expression == "SUM(orders.amount)"


def test_sql_is_authoritative_over_an_ossie_sql_dialect_entry(tmp_path: Path) -> None:
    expression = {
        "sql": "SUM(orders.amount)",
        "dialects": [{"dialect": "OSSIE_SQL_2026", "expression": "stale"}],
    }
    source = _load(tmp_path, _flat(metrics=[_metric(expression)]))
    assert _get(source, "m").sql_expression == "SUM(orders.amount)"


def test_ansi_sql_is_still_preferred_over_the_portable_sql_field(
    tmp_path: Path,
) -> None:
    """ANSI is what a warehouse runs; Ossie SQL reserves constructs like AGG()."""
    expression = {
        "sql": "AGG(orders.amount)",
        "dialects": [{"dialect": "ANSI_SQL", "expression": "SUM(orders.amount)"}],
    }
    source = _load(tmp_path, _flat(metrics=[_metric(expression)]))
    assert _get(source, "m").sql_expression == "SUM(orders.amount)"


def test_the_spec_spelling_of_ossie_sql_beats_a_vendor_dialect(
    tmp_path: Path,
) -> None:
    expression = {
        "dialects": [
            {"dialect": "SNOWFLAKE", "expression": "SUM(orders.amount)::NUMBER"},
            {"dialect": "OSSIE_SQL_2026", "expression": "SUM(orders.amount)"},
        ]
    }
    source = _load(tmp_path, _flat(metrics=[_metric(expression)]))
    assert _get(source, "m").sql_expression == "SUM(orders.amount)"


# --- dataset-scoped metrics -------------------------------------------------


def _with_dataset_metric(**extra: Any) -> dict[str, Any]:
    doc = _flat(**extra)
    doc["datasets"][0]["metrics"] = [
        {
            "name": "order_count",
            "description": "Number of orders",
            "expression": {"sql": "COUNT(order_id)"},
        }
    ]
    return doc


def test_a_dataset_scoped_metric_is_named_as_the_spec_references_it(
    tmp_path: Path,
) -> None:
    """Referenced as ``dataset_name.metric_name``, which also keeps it apart
    from a model-scoped metric of the same bare name."""
    source = _load(tmp_path, _with_dataset_metric())
    metric = source.get_metric("orders.order_count")
    assert metric is not None
    assert metric.sql_expression == "COUNT(order_id)"
    assert metric.description == "Number of orders"


def test_a_dataset_scoped_metric_names_its_table_as_source_model(
    tmp_path: Path,
) -> None:
    """Its expression is unqualified, so the table is what anchors it."""
    source = _load(tmp_path, _with_dataset_metric())
    assert _get(source, "orders.order_count").source_model == ("public.orders")


def test_a_dataset_scoped_metric_takes_our_vendor_overlay(tmp_path: Path) -> None:
    overlay = {
        "vendor_name": "AGENTIC_DATA_CONTRACTS",
        "data": {"metrics": {"orders.order_count": {"tier": ["gold"]}}},
    }
    source = _load(tmp_path, _with_dataset_metric(custom_extensions=[overlay]))
    assert _get(source, "orders.order_count").tier == ["gold"]
