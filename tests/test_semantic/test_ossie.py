"""Tests for the Apache Ossie semantic source."""

from __future__ import annotations

import json
import logging
from pathlib import Path

import pytest
import yaml

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
    assert ("public.store_sales.ss_customer_sk", "public.customer.c_customer_sk") in {
        (r.from_, r.to) for r in source.get_relationships()
    }


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
        and r.to == "public.customer_profile.cp_customer_sk"
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
    assert {(r.from_, r.to) for r in rels} == {
        ("public.store_sales.ss_customer_sk", "public.customer.c_customer_sk"),
        ("public.customer.c_customer_sk", "public.customer_profile.cp_customer_sk"),
        ("public.customer.c_customer_sk", "public.store_sales.ss_customer_sk"),
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
    assert extras["ossie_custom_extensions"]["retail_model"]["SALESFORCE"] == {
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
    extensions = source.get_extras()["ossie_custom_extensions"]["retail_model"]
    assert extensions["BROKEN_VENDOR"] == "not valid json {"


def test_ai_context_is_collected_into_extras(source: OssieSource) -> None:
    ai = source.get_extras()["ossie_ai_context"]["retail_model"]
    assert ai["metrics"]["total_sales"]["synonyms"] == ["revenue", "gross sales"]
    assert ai["datasets"]["store_sales"]["synonyms"] == [
        "sales transactions",
        "POS data",
    ]


def test_string_form_ai_context_is_normalised_to_instructions(
    source: OssieSource,
) -> None:
    """`ai_context` is a string or an object; both reach extras as an object."""
    ai = source.get_extras()["ossie_ai_context"]["retail_model"]
    assert ai["datasets"]["customer"] == {"instructions": "One row per customer."}


def test_extras_are_json_safe(source: OssieSource) -> None:
    import json

    json.dumps(source.get_extras())


# --- cardinality is derived from both sides -------------------------------


def test_relationship_is_one_to_many_when_to_columns_are_not_a_key(
    source: OssieSource,
) -> None:
    """A relationship declared "backwards" must not read as one-to-one.

    Ossie documents `to` as the one side, but nothing validates it. If the
    `to` columns are not a key of the `to` dataset, the join fans out — and
    `RelationshipChecker._check_fan_out` fires only on `one_to_many`, so
    trusting the declaration here silently disables the row-multiplication
    warning on an aggregate.
    """
    rel = next(
        r
        for r in source.get_relationships()
        if r.from_ == "public.customer.c_customer_sk"
        and r.to == "public.store_sales.ss_customer_sk"
    )
    assert rel.type == "one_to_many"


def test_relationship_trusts_the_declaration_when_to_declares_no_keys(
    source: OssieSource,
) -> None:
    """Keys are optional in Ossie; absence is not evidence of fan-out.

    Downgrading every keyless target to `one_to_many` would flood the
    fan-out checker with false positives on models that simply omit keys.
    """
    rel = next(
        r for r in source.get_relationships() if r.to == "public.keyless_dim.k_id"
    )
    assert rel.type == "many_to_one"


# --- scalar coercion parity with YamlSource -------------------------------


def test_scalar_tier_is_promoted_to_a_list(tmp_path: Path) -> None:
    """`"tier": "gold"` must not become `['g', 'o', 'l', 'd']`.

    `YamlSource` deliberately promotes a bare string; the same authoring
    slip in an Ossie vendor block has to behave identically, or it silently
    corrupts the values that drive tier policy and domain filtering.
    """
    path = tmp_path / "scalar.yml"
    path.write_text(
        FIXTURE.read_text()
        .replace('"tier": ["gold"]', '"tier": "gold"')
        .replace('"domains": ["sales"]', '"domains": "sales"')
    )
    metric = OssieSource(path).get_metric("total_sales")
    assert metric is not None
    assert metric.tier == ["gold"]
    assert metric.domains == ["sales"]


# --- extras hygiene -------------------------------------------------------


def test_empty_custom_extensions_key_is_omitted(tmp_path: Path) -> None:
    """An always-present empty dict breaks freeze -> rehydrate.

    `dump_semantic_source` writes extras into the contract's inline
    snapshot, which `YamlSource` then reloads under the contract's
    `expected_extras` policy. A synthesized `ossie_custom_extensions: {}`
    turns a strict contract into a load error, and adds a noise key to
    `contract_canonical_bytes` for every Ossie contract.
    """
    path = tmp_path / "no_vendors.yml"
    model = FIXTURE.read_text()
    path.write_text(model[: model.index("    custom_extensions:")])
    assert "ossie_custom_extensions" not in OssieSource(path).get_extras()


def test_extras_are_normalised_to_json_safe_values(tmp_path: Path) -> None:
    """A YAML-native date in `ai_context` must not reach canonical bytes.

    Extras ride into `contract_canonical_bytes` via `json.dumps`, so the
    coercion `YamlSource` applies has to apply here too.
    """
    path = tmp_path / "dated.yml"
    path.write_text(
        FIXTURE.read_text().replace(
            '      instructions: "Use for retail revenue analysis."',
            "      instructions: verified\n      verified_on: 2026-05-01",
        )
    )
    extras = OssieSource(path).get_extras()
    models = extras["ossie_ai_context"]["retail_model"]["models"]
    assert models["retail_model"]["verified_on"] == "2026-05-01"


def test_mapping_extension_payload_does_not_warn_about_json(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    """A YAML mapping under `data:` is usable, not "not valid JSON"."""
    path = tmp_path / "mapping.yml"
    path.write_text(
        FIXTURE.read_text().replace(
            """      - vendor_name: SALESFORCE
        data: '{"tableau_workbook_id": "retail_dashboard"}'""",
            """      - vendor_name: SALESFORCE
        data:
          tableau_workbook_id: retail_dashboard""",
        )
    )
    with caplog.at_level(logging.WARNING):
        source = OssieSource(path)
    extensions = source.get_extras()["ossie_custom_extensions"]["retail_model"]
    assert extensions["SALESFORCE"] == {"tableau_workbook_id": "retail_dashboard"}
    assert "SALESFORCE" not in caplog.text


def test_unresolvable_dataset_source_is_logged(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    """A typo'd `source:` silently turns off drill-by column validation."""
    path = tmp_path / "bad_source.yml"
    path.write_text(
        FIXTURE.read_text().replace(
            "        source: warehouse.public.customer\n",
            "        source: customer\n",
        )
    )
    with caplog.at_level(logging.WARNING):
        OssieSource(path)
    assert "no resolvable schema.table" in caplog.text


# --- multiple semantic models in one file ---------------------------------


MULTI = Path(__file__).parent.parent / "fixtures" / "sample_ossie_multi_model.yml"


def test_each_model_keeps_its_own_vendor_extensions() -> None:
    """Two models may each carry a block from the same vendor."""
    extensions = OssieSource(MULTI).get_extras()["ossie_custom_extensions"]
    assert extensions["retail_model"]["SALESFORCE"] == {"workbook": "retail"}
    assert extensions["wholesale_model"]["SALESFORCE"] == {"workbook": "wholesale"}


def test_each_model_keeps_its_own_ai_context() -> None:
    """Ossie namespaces entity names per model, so `customer` differs."""
    ai = OssieSource(MULTI).get_extras()["ossie_ai_context"]
    assert ai["retail_model"]["datasets"]["customer"]["synonyms"] == ["retail shopper"]
    assert ai["wholesale_model"]["datasets"]["customer"]["synonyms"] == [
        "wholesale account"
    ]


# --- attribution convention ------------------------------------------------


def _convention_model(extension: dict, *, name: str = "retail") -> dict:
    """One Ossie semantic_model carrying our vendor block as a JSON string."""
    return {
        "name": name,
        "metrics": [
            {"name": "activations"},
            {"name": "volume"},
            {"name": "rate"},
        ],
        "custom_extensions": [
            {
                "vendor_name": "AGENTIC_DATA_CONTRACTS",
                "data": json.dumps(extension),
            }
        ],
    }


_PRODUCT_DECOMP = {
    "metrics": {
        "activations": {
            "decompositions": [{"operator": "product", "operands": ["volume", "rate"]}]
        }
    }
}


def test_ossie_per_decomposition_convention_round_trips(tmp_path: Path) -> None:
    extension = {
        "metrics": {
            "activations": {
                "decompositions": [
                    {
                        "operator": "product",
                        "operands": ["volume", "rate"],
                        "convention": "fold_into",
                        "convention_operand": "rate",
                    }
                ]
            }
        }
    }
    path = tmp_path / "model.yml"
    path.write_text(yaml.safe_dump({"semantic_model": [_convention_model(extension)]}))
    metric = OssieSource(path).get_metric("activations")
    assert metric is not None
    assert metric.decompositions[0].convention == "fold_into"
    assert metric.decompositions[0].convention_operand == "rate"


def test_ossie_source_level_default_round_trips(tmp_path: Path) -> None:
    extension = {
        "decomposition_convention": {"convention": "split_evenly"},
        **_PRODUCT_DECOMP,
    }
    path = tmp_path / "model.yml"
    path.write_text(yaml.safe_dump({"semantic_model": [_convention_model(extension)]}))
    metric = OssieSource(path).get_metric("activations")
    assert metric is not None
    assert metric.decompositions[0].convention == "split_evenly"


def test_ossie_default_does_not_leak_across_models(tmp_path: Path) -> None:
    """A second model's house convention must not reach the first model's metrics.

    ``self._metrics`` accumulates across the ``semantic_model`` loop, so
    applying the default to the whole list would stamp model B's convention
    onto model A. Ossie namespaces entities per model; the default is scoped
    the same way.
    """
    plain = _convention_model(_PRODUCT_DECOMP, name="plain")
    defaulted = _convention_model(
        {"decomposition_convention": {"convention": "split_evenly"}, **_PRODUCT_DECOMP},
        name="defaulted",
    )
    # Rename the second model's metrics so both models can coexist in one file.
    for metric in defaulted["metrics"]:
        metric["name"] = f"b_{metric['name']}"
    defaulted["custom_extensions"][0]["data"] = json.dumps(
        {
            "decomposition_convention": {"convention": "split_evenly"},
            "metrics": {
                "b_activations": {
                    "decompositions": [
                        {"operator": "product", "operands": ["b_volume", "b_rate"]}
                    ]
                }
            },
        }
    )
    path = tmp_path / "two_models.yml"
    path.write_text(yaml.safe_dump({"semantic_model": [plain, defaulted]}))
    source = OssieSource(path)

    first = source.get_metric("activations")
    assert first is not None
    assert first.decompositions[0].convention is None  # declares no default

    second = source.get_metric("b_activations")
    assert second is not None
    assert second.decompositions[0].convention == "split_evenly"
