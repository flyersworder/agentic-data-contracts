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


def test_a_dataset_scoped_metric_claims_no_source_model(tmp_path: Path) -> None:
    """A ``source_model`` with no ``from_sql`` says the expression reads that
    table alone, but this expression names the dataset's *fields*, which may
    be renamed or computed. Unset, as a model-scoped metric's is."""
    doc = _with_dataset_metric()
    doc["datasets"][0]["metrics"][0]["expression"] = {"sql": "SUM(net_amount)"}
    doc["datasets"][0]["fields"].append(
        {"name": "net_amount", "expression": {"sql": "amount - discount"}}
    )
    assert _get(_load(tmp_path, doc), "orders.order_count").source_model == ""


def test_a_dataset_scoped_metric_takes_our_vendor_overlay(tmp_path: Path) -> None:
    overlay = {
        "vendor_name": "AGENTIC_DATA_CONTRACTS",
        "data": {"metrics": {"orders.order_count": {"tier": ["gold"]}}},
    }
    source = _load(tmp_path, _with_dataset_metric(custom_extensions=[overlay]))
    assert _get(source, "orders.order_count").tier == ["gold"]


# --- no part of a model is dropped without a message -----------------------


@pytest.mark.parametrize("datasets", [[], None], ids=["empty", "null"])
def test_a_flat_document_with_no_datasets_is_rejected(
    tmp_path: Path, datasets: Any
) -> None:
    """The schema requires at least one dataset; none is an empty source."""
    with pytest.raises(ValueError, match="datasets"):
        _load(tmp_path, _flat(datasets=datasets))


def test_a_legacy_model_with_no_datasets_is_rejected(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="datasets"):
        _load(tmp_path, {"semantic_model": [{"name": "m"}]})


def test_a_nameless_dataset_is_rejected(tmp_path: Path) -> None:
    doc = _flat()
    del doc["datasets"][0]["name"]
    with pytest.raises(ValueError, match="name"):
        _load(tmp_path, doc)


def test_a_nameless_field_is_rejected(tmp_path: Path) -> None:
    doc = _flat()
    del doc["datasets"][0]["fields"][0]["name"]
    with pytest.raises(ValueError, match="name"):
        _load(tmp_path, doc)


@pytest.mark.parametrize("scope", ["model", "dataset"])
def test_a_nameless_metric_is_rejected(tmp_path: Path, scope: str) -> None:
    nameless = {"expression": {"sql": "COUNT(*)"}}
    doc = _flat(metrics=[nameless]) if scope == "model" else _flat()
    if scope == "dataset":
        doc["datasets"][0]["metrics"] = [nameless]
    with pytest.raises(ValueError, match="name"):
        _load(tmp_path, doc)


@pytest.mark.parametrize(
    "expression",
    [None, {}, {"dialects": []}, [{"dialect": "ANSI_SQL", "expression": "x"}]],
    ids=["missing", "empty", "no-dialects", "bare-list"],
)
def test_a_metric_with_no_expression_is_rejected(
    tmp_path: Path, expression: Any
) -> None:
    metric: dict[str, Any] = {"name": "m"}
    if expression is not None:
        metric["expression"] = expression
    with pytest.raises(ValueError, match="m"):
        _load(tmp_path, _flat(metrics=[metric]))


# --- fields register the physical column they read -------------------------


def _columns(source: OssieSource, table: str) -> list[str]:
    schema = source.get_table_schema(*table.split("."))
    assert schema is not None, f"{table} did not load"
    return [c.name for c in schema.columns]


def test_a_renamed_field_registers_the_column_it_reads() -> None:
    """NVIDIA's `customer_name` field is the physical column `name`."""
    assert _columns(OssieSource(NVIDIA), "public.customers") == ["customer_id", "name"]


def test_a_computed_field_is_not_registered_as_a_column() -> None:
    """`net_total` is `subtotal - discount`; no table has that column."""
    assert "net_total" not in _columns(OssieSource(NVIDIA), "public.orders")


def test_a_field_with_no_expression_keeps_its_name_as_the_column(
    tmp_path: Path,
) -> None:
    doc = _flat()
    del doc["datasets"][0]["fields"][1]["expression"]
    assert _columns(_load(tmp_path, doc), "public.orders") == ["order_id", "amount"]


def test_a_renamed_fields_description_lands_on_its_column(tmp_path: Path) -> None:
    doc = _flat()
    doc["datasets"][0]["fields"][1] = {
        "name": "order_amount",
        "description": "Gross amount",
        "expression": {"sql": "amount"},
    }
    schema = _load(tmp_path, doc).get_table_schema("public", "orders")
    assert schema is not None
    assert {c.name: c.description for c in schema.columns}["amount"] == "Gross amount"


# --- dialect names match whatever their case --------------------------------


def test_the_callers_dialect_matches_an_entry_in_another_case(
    tmp_path: Path,
) -> None:
    expression = {
        "dialects": [
            {"dialect": "ANSI_SQL", "expression": "SUM(orders.amount)"},
            {"dialect": "Ossie_SQL_2026", "expression": "SUM(orders.amount) + 0"},
        ]
    }
    path = tmp_path / "model.yml"
    path.write_text(yaml.safe_dump(_flat(metrics=[_metric(expression)])))
    source = OssieSource(path, dialect="OSSIE_SQL_2026")
    assert _get(source, "m").sql_expression == "SUM(orders.amount) + 0"


def test_sql_overrides_an_ossie_sql_entry_in_any_case(tmp_path: Path) -> None:
    expression = {
        "sql": "SUM(orders.amount)",
        "dialects": [{"dialect": "Ossie_SQL_2026", "expression": "stale"}],
    }
    path = tmp_path / "model.yml"
    path.write_text(yaml.safe_dump(_flat(metrics=[_metric(expression)])))
    source = OssieSource(path, dialect="Ossie_SQL_2026")
    assert _get(source, "m").sql_expression == "SUM(orders.amount)"


# --- extensions below the model are carried, not dropped --------------------


def test_a_foreign_extension_on_a_field_is_carried() -> None:
    entities = OssieSource(NVIDIA).get_extras()["ossie_entity_extensions"]["sales"]
    block = entities["fields"]["orders.net_total"]["NVIDIA_AUTO_ONTOLOGY"]
    assert block["entity"] == "sql_attribute"


def test_a_foreign_extension_on_a_metric_is_carried() -> None:
    entities = OssieSource(NVIDIA).get_extras()["ossie_entity_extensions"]["sales"]
    block = entities["metrics"]["revenue_per_customer"]["NVIDIA_AUTO_ONTOLOGY"]
    assert block["sql"].startswith("SELECT SUM(orders.subtotal)")


def test_no_entity_extensions_section_when_there_are_none(tmp_path: Path) -> None:
    assert "ossie_entity_extensions" not in _load(tmp_path, _flat()).get_extras()


def test_our_vendor_block_on_an_entity_is_named_as_unread(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    ours = {"vendor_name": "AGENTIC_DATA_CONTRACTS", "data": {"tier": ["gold"]}}
    metric = {**_metric({"sql": "COUNT(*)"}), "custom_extensions": [ours]}
    with caplog.at_level(logging.WARNING):
        _load(tmp_path, _flat(metrics=[metric]))
    assert "model-level" in caplog.text


def test_a_repeated_vendor_block_is_named(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    blocks = [
        {"vendor_name": "SNOWFLAKE", "data": {"warehouse": "A"}},
        {"vendor_name": "SNOWFLAKE", "data": {"warehouse": "B"}},
    ]
    with caplog.at_level(logging.WARNING):
        _load(tmp_path, _flat(custom_extensions=blocks))
    assert "SNOWFLAKE" in caplog.text


# --- overlay keys and metric names that resolve to nothing ------------------


def _overlay(metrics: dict[str, Any]) -> list[dict[str, Any]]:
    return [{"vendor_name": "AGENTIC_DATA_CONTRACTS", "data": {"metrics": metrics}}]


def test_an_overlay_key_matching_no_metric_is_named(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    doc = _flat(
        metrics=[_metric({"sql": "COUNT(*)"})],
        custom_extensions=_overlay({"typo_metric": {"tier": ["gold"]}}),
    )
    with caplog.at_level(logging.WARNING):
        _load(tmp_path, doc)
    assert "typo_metric" in caplog.text


def test_a_bare_overlay_key_for_a_dataset_scoped_metric_suggests_its_name(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    """Moving a metric onto its dataset renames it; the overlay key must follow."""
    doc = _with_dataset_metric(
        custom_extensions=_overlay({"order_count": {"tier": ["gold"]}})
    )
    with caplog.at_level(logging.WARNING):
        _load(tmp_path, doc)
    assert "orders.order_count" in caplog.text


def test_a_duplicate_metric_name_is_named(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    metrics = [_metric({"sql": "COUNT(*)"}), _metric({"sql": "SUM(orders.amount)"})]
    with caplog.at_level(logging.WARNING):
        _load(tmp_path, _flat(metrics=metrics))
    assert "duplicate" in caplog.text.lower()


def _joined(customer_key_expression: str) -> dict[str, Any]:
    doc = _flat()
    doc["datasets"][0]["fields"].append(
        {"name": "customer_id", "expression": {"sql": "customer_id"}}
    )
    doc["datasets"].append(
        {
            "name": "customers",
            "source": "analytics.public.customers",
            "primary_key": ["customer_id"],
            "fields": [
                {"name": "customer_id", "expression": {"sql": customer_key_expression}}
            ],
        }
    )
    doc["relationships"] = [
        {
            "name": "orders_to_customers",
            "from": "orders",
            "to": "customers",
            "from_columns": ["customer_id"],
            "to_columns": ["customer_id"],
        }
    ]
    return doc


def test_a_relationship_on_a_renamed_field_joins_the_physical_column(
    tmp_path: Path,
) -> None:
    [rel] = _load(tmp_path, _joined("cust_key")).get_relationships()
    assert rel.to == "public.customers.cust_key"


def test_a_relationship_on_a_computed_field_is_skipped_by_name(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    with caplog.at_level(logging.WARNING):
        source = _load(tmp_path, _joined("id + 0"))
    assert source.get_relationships() == []
    assert "orders_to_customers" in caplog.text


# --- a BI formula is never handed over as SQL -------------------------------


def _dialects(*pairs: tuple[str, str]) -> dict[str, Any]:
    return {"dialects": [{"dialect": d, "expression": e} for d, e in pairs]}


def test_the_fallback_skips_a_bi_formula_for_a_sql_dialect(tmp_path: Path) -> None:
    """`SUM([Profit])` is Tableau's calculation language, not SQL."""
    expression = _dialects(
        ("TABLEAU", "SUM([Profit])"), ("SNOWFLAKE", "SUM(orders.profit)")
    )
    source = _load(tmp_path, _flat(metrics=[_metric(expression)]))
    assert _get(source, "m").sql_expression == "SUM(orders.profit)"


def test_a_bi_dialect_is_recognised_whatever_its_case(tmp_path: Path) -> None:
    expression = _dialects(("dax", "SUM(Orders[Profit])"), ("DUCKDB", "SUM(profit)"))
    source = _load(tmp_path, _flat(metrics=[_metric(expression)]))
    assert _get(source, "m").sql_expression == "SUM(profit)"


def test_a_bi_dialect_the_caller_asks_for_is_still_returned(tmp_path: Path) -> None:
    expression = _dialects(
        ("ANSI_SQL", "SUM(orders.profit)"), ("TABLEAU", "SUM([Profit])")
    )
    path = tmp_path / "model.yml"
    path.write_text(yaml.safe_dump(_flat(metrics=[_metric(expression)])))
    source = OssieSource(path, dialect="TABLEAU")
    assert _get(source, "m").sql_expression == "SUM([Profit])"


def test_an_unregistered_dialect_is_still_a_fallback(tmp_path: Path) -> None:
    """`DENODO` may well be SQL; only dialects known not to be are skipped."""
    expression = _dialects(("DENODO", "SUM(orders.profit)"))
    source = _load(tmp_path, _flat(metrics=[_metric(expression)]))
    assert _get(source, "m").sql_expression == "SUM(orders.profit)"


def test_a_metric_with_only_bi_formulas_is_kept_as_untranslated(
    tmp_path: Path,
) -> None:
    """Valid Ossie with no SQL, as dbt and Cube report theirs (#123): the
    agent learns the metric exists and its formula, labelled as not SQL."""
    metrics = [_metric(_dialects(("TABLEAU", "SUM([Profit])")), name="profit_ratio")]
    metric = _get(_load(tmp_path, _flat(metrics=metrics)), "profit_ratio")
    assert metric.sql_expression == ""
    assert metric.untranslated is not None
    assert "TABLEAU" in metric.untranslated
    assert "SUM([Profit])" in metric.untranslated


def test_a_field_with_only_a_bi_formula_registers_no_column(tmp_path: Path) -> None:
    """Its name is what Tableau calls it, not evidence of a column."""
    doc = _flat()
    doc["datasets"][0]["fields"].append(
        {"name": "Profit", "expression": _dialects(("SIGMA", "Profit"))}
    )
    assert "Profit" not in _columns(_load(tmp_path, doc), "public.orders")
