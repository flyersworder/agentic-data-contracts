"""Tests for the Apache Ossie semantic source."""

from __future__ import annotations

import logging
from pathlib import Path

import pytest

from agentic_data_contracts.semantic.base import (
    ExtensibleSemanticSource,
    SemanticSource,
)
from agentic_data_contracts.semantic.ossie import OssieSource

FIXTURE = Path(__file__).parent.parent / "fixtures" / "sample_ossie_model.yml"


@pytest.fixture
def source() -> OssieSource:
    return OssieSource(FIXTURE)


# --- protocol conformance -------------------------------------------------


def test_satisfies_semantic_source_protocol(source: OssieSource) -> None:
    assert isinstance(source, SemanticSource)


def test_satisfies_extensible_semantic_source_protocol(source: OssieSource) -> None:
    assert isinstance(source, ExtensibleSemanticSource)


# --- datasets -> table schemas --------------------------------------------


def test_three_part_source_is_keyed_by_trailing_schema_and_table(
    source: OssieSource,
) -> None:
    """`warehouse.public.store_sales` keys as `public.store_sales`.

    Our table keys are two-part `schema.table` because `Relationship`
    endpoints are `schema.table.column` and `build_relationship_index`
    recovers the table with a single `rsplit`.
    """
    assert source.get_table_schema("public", "store_sales") is not None
    assert source.get_table_schema("warehouse.public", "store_sales") is None


def test_fields_become_columns_with_datatype_and_description(
    source: OssieSource,
) -> None:
    schema = source.get_table_schema("public", "store_sales")
    assert schema is not None
    by_name = {c.name: c for c in schema.columns}
    assert by_name["ss_ext_sales_price"].type == "Decimal"
    assert by_name["ss_ext_sales_price"].description == "Extended sales price"


def test_query_backed_dataset_is_not_registered_as_a_table(
    source: OssieSource,
) -> None:
    """A `source:` that is a query has no `schema.table` to key on."""
    assert "active_customers" not in source.get_table_schemas()
    assert not any(k.endswith("active_customers") for k in source.get_table_schemas())


# --- metrics and dialect selection ----------------------------------------


def test_metric_expression_defaults_to_ansi_sql(source: OssieSource) -> None:
    metric = source.get_metric("total_sales")
    assert metric is not None
    assert metric.sql_expression == "SUM(store_sales.ss_ext_sales_price)"


def test_requested_dialect_is_preferred() -> None:
    source = OssieSource(FIXTURE, dialect="BIGQUERY")
    metric = source.get_metric("total_sales")
    assert metric is not None
    assert metric.sql_expression == (
        "SUM(store_sales.ss_ext_sales_price) /* bigquery */"
    )


def test_falls_back_to_first_dialect_when_none_preferred_is_present(
    source: OssieSource,
) -> None:
    """A metric offered only in SNOWFLAKE still yields an expression."""
    metric = source.get_metric("snowflake_only_metric")
    assert metric is not None
    assert metric.sql_expression == "SUM(store_sales.ss_ext_sales_price)"


def test_search_metrics_matches_on_description(source: OssieSource) -> None:
    names = [m.name for m in source.search_metrics("net profit")]
    assert "total_profit" in names


# --- relationships --------------------------------------------------------


def test_relationship_endpoints_are_schema_table_column(source: OssieSource) -> None:
    rels = {r.from_: r for r in source.get_relationships()}
    rel = rels["public.store_sales.ss_customer_sk"]
    assert rel.to == "public.customer.c_customer_sk"


def test_relationship_defaults_to_many_to_one(source: OssieSource) -> None:
    rel = next(
        r
        for r in source.get_relationships()
        if r.from_ == "public.store_sales.ss_customer_sk"
    )
    assert rel.type == "many_to_one"


def test_relationship_is_one_to_one_when_from_columns_are_a_unique_key(
    source: OssieSource,
) -> None:
    """Ossie derives cardinality from keys: a unique `from` side is 1:1."""
    rel = next(
        r
        for r in source.get_relationships()
        if r.from_ == "public.customer.c_customer_sk"
    )
    assert rel.type == "one_to_one"


def test_composite_relationship_is_skipped_with_a_warning(
    caplog: pytest.LogCaptureFixture,
) -> None:
    with caplog.at_level(logging.WARNING):
        source = OssieSource(FIXTURE)
    assert not any(
        r.from_.startswith("public.store_sales.ss_item_sk")
        for r in source.get_relationships()
    )
    assert "sales_to_returns" in caplog.text


def test_relationship_description_comes_from_ai_context_instructions(
    source: OssieSource,
) -> None:
    rel = next(
        r
        for r in source.get_relationships()
        if r.from_ == "public.store_sales.ss_customer_sk"
    )
    assert rel.description == "Join sales to the buying customer."


def test_relationships_are_indexed_by_table(source: OssieSource) -> None:
    rels = source.get_relationships_for_table("public.customer")
    assert {r.from_ for r in rels} == {
        "public.store_sales.ss_customer_sk",
        "public.customer.c_customer_sk",
    }


# --- our own custom_extensions round-trip ---------------------------------


def test_vendor_extension_restores_ownership_and_review_date(
    source: OssieSource,
) -> None:
    metric = source.get_metric("total_sales")
    assert metric is not None
    assert metric.business_owner == "revenue-analytics"
    assert metric.operational_owner == "data-platform"
    assert metric.last_reviewed is not None
    assert metric.last_reviewed.isoformat() == "2026-05-01"


def test_vendor_extension_restores_tier_and_domains(source: OssieSource) -> None:
    metric = source.get_metric("total_sales")
    assert metric is not None
    assert metric.tier == ["gold"]
    assert metric.domains == ["sales"]
    assert metric.indicator_kind == "lagging"


def test_vendor_extension_restores_decompositions(source: OssieSource) -> None:
    metric = source.get_metric("total_sales")
    assert metric is not None
    assert len(metric.decompositions) == 1
    assert metric.decompositions[0].operator == "sum"
    assert metric.decompositions[0].operands == ["total_profit", "total_cost"]


def test_vendor_extension_restores_drill_by(source: OssieSource) -> None:
    metric = source.get_metric("total_sales")
    assert metric is not None
    assert [d.column for d in metric.drill_by] == ["public.customer.c_region"]


def test_vendor_extension_restores_metric_impacts(source: OssieSource) -> None:
    impacts = source.get_metric_impacts()
    assert len(impacts) == 1
    assert impacts[0].from_metric == "total_cost"
    assert impacts[0].to_metric == "total_profit"
    assert impacts[0].direction == "negative"
    assert impacts[0].confidence == "verified"


def test_invalid_decomposition_in_vendor_extension_is_rejected(
    tmp_path: Path,
) -> None:
    """Restored decompositions go through the same validator as YamlSource."""
    model = FIXTURE.read_text().replace(
        '{"operator": "sum", "operands": ["total_profit", "total_cost"]}',
        '{"operator": "sum", "operands": ["total_profit", "no_such_metric"]}',
    )
    path = tmp_path / "bad.yml"
    path.write_text(model)
    with pytest.raises(ValueError, match="no_such_metric"):
        OssieSource(path)


# --- foreign extensions and ai_context ride in extras ---------------------


def test_foreign_vendor_extension_lands_in_extras(source: OssieSource) -> None:
    extras = source.get_extras()
    assert extras["ossie_custom_extensions"]["SALESFORCE"] == {
        "tableau_workbook_id": "retail_dashboard"
    }


def test_our_own_vendor_extension_is_not_duplicated_into_extras(
    source: OssieSource,
) -> None:
    """It became real vocabulary, so it must not also ride as opaque extras."""
    assert (
        "AGENTIC_DATA_CONTRACTS" not in source.get_extras()["ossie_custom_extensions"]
    )


def test_unparseable_extension_data_is_carried_verbatim(
    source: OssieSource,
) -> None:
    """Another vendor's malformed JSON must not fail the load."""
    assert (
        source.get_extras()["ossie_custom_extensions"]["BROKEN_VENDOR"]
        == "not valid json {"
    )


def test_ai_context_is_collected_into_extras(source: OssieSource) -> None:
    ai = source.get_extras()["ossie_ai_context"]
    assert ai["metrics"]["total_sales"]["synonyms"] == ["revenue", "gross sales"]
    assert ai["datasets"]["store_sales"]["synonyms"] == [
        "sales transactions",
        "POS data",
    ]


def test_string_form_ai_context_is_normalised_to_instructions(
    source: OssieSource,
) -> None:
    """`ai_context` is a string or an object; both reach extras as an object."""
    ai = source.get_extras()["ossie_ai_context"]
    assert ai["datasets"]["customer"] == {"instructions": "One row per customer."}


def test_extras_are_json_safe(source: OssieSource) -> None:
    import json

    json.dumps(source.get_extras())
