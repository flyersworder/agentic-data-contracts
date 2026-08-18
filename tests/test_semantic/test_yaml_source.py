import json
import logging
from datetime import date, datetime
from pathlib import Path

import pytest

from agentic_data_contracts.semantic.base import SemanticSource, parse_review_date
from agentic_data_contracts.semantic.yaml_source import (
    SEMANTIC_KEYS,
    YamlSource,
)


@pytest.fixture
def source(fixtures_dir: Path) -> YamlSource:
    return YamlSource(fixtures_dir / "semantic_source.yml")


def test_source_implements_protocol(source: YamlSource) -> None:
    assert isinstance(source, SemanticSource)


def test_get_metrics(source: YamlSource) -> None:
    metrics = source.get_metrics()
    assert len(metrics) == 2
    names = [m.name for m in metrics]
    assert "total_revenue" in names
    assert "active_customers" in names


def test_get_metric(source: YamlSource) -> None:
    metric = source.get_metric("total_revenue")
    assert metric is not None
    assert metric.name == "total_revenue"
    assert "SUM(amount)" in metric.sql_expression
    assert metric.source_model == "analytics.orders"


def test_get_metric_not_found(source: YamlSource) -> None:
    metric = source.get_metric("nonexistent")
    assert metric is None


def test_get_table_schema(source: YamlSource) -> None:
    schema = source.get_table_schema("analytics", "orders")
    assert schema is not None
    assert len(schema.columns) == 5
    col_names = [c.name for c in schema.columns]
    assert "id" in col_names
    assert "amount" in col_names


def test_get_table_schema_not_found(source: YamlSource) -> None:
    schema = source.get_table_schema("analytics", "nonexistent")
    assert schema is None


def test_metric_parses_owners_and_last_reviewed(source: YamlSource) -> None:
    """Owners and the review date are read off the metric (ISO string → date)."""
    metric = source.get_metric("total_revenue")
    assert metric is not None
    assert metric.business_owner == "revenue-platform"
    assert metric.operational_owner == "data-eng-finance"
    assert metric.last_reviewed == date(2020, 1, 1)


def test_metric_without_governance_fields_defaults_none(source: YamlSource) -> None:
    """Metrics that omit the new keys keep None defaults — no breakage."""
    metric = source.get_metric("active_customers")
    assert metric is not None
    assert metric.business_owner is None
    assert metric.operational_owner is None
    assert metric.last_reviewed is None


def test_parse_date_normalizes_datetime_to_date() -> None:
    """A YAML scalar with a time component (parsed as datetime) → date.

    datetime subclasses date, so without normalization it would slip through and
    later crash the staleness arithmetic with `date - datetime`.
    """
    assert parse_review_date(datetime(2020, 1, 1, 12, 30, 0)) == date(2020, 1, 1)


def test_parse_date_passes_through_native_date() -> None:
    assert parse_review_date(date(2020, 1, 1)) == date(2020, 1, 1)


def test_parse_date_none_is_none() -> None:
    assert parse_review_date(None) is None


def test_parse_date_malformed_string_raises_clear_error() -> None:
    """A bad ISO string fails fast with a message naming the offending value."""
    with pytest.raises(ValueError, match="last_reviewed must be an ISO date"):
        parse_review_date("2020-13-01")


def test_unknown_top_level_keys_are_kept_as_extras() -> None:
    """Issue #60's reproduction: an unsupported section must not vanish."""
    src = YamlSource.from_raw(
        {
            "metrics": [],
            "widget_hints": [{"table": "t", "prefer": "a", "over": "b"}],
        }
    )
    assert src.get_extras() == {
        "widget_hints": [{"table": "t", "prefer": "a", "over": "b"}]
    }


def test_interpreted_keys_never_appear_in_extras() -> None:
    src = YamlSource.from_raw(
        {
            "metrics": [],
            "tables": [],
            "relationships": [],
            "metric_impacts": [],
            "column_hints": [{"table": "t", "prefer": "a"}],
        }
    )
    assert set(src.get_extras()) == {"column_hints"}


def test_no_extras_is_an_empty_dict() -> None:
    assert YamlSource.from_raw({"metrics": []}).get_extras() == {}


def test_semantic_keys_matches_what_the_parser_reads() -> None:
    """Guards the constant against drift when a new section is added."""
    assert SEMANTIC_KEYS == {
        "metrics",
        "tables",
        "relationships",
        "metric_impacts",
        "decomposition_convention",
    }


def test_get_extras_returns_a_copy() -> None:
    src = YamlSource.from_raw({"metrics": [], "notes": ["a"]})
    src.get_extras()["notes"] = ["mutated"]
    assert src.get_extras() == {"notes": ["a"]}


def test_extras_warn_by_default(caplog) -> None:  # noqa: ANN001
    logger_name = "agentic_data_contracts.semantic.yaml_source"
    with caplog.at_level(logging.WARNING, logger=logger_name):
        YamlSource.from_raw({"metrics": [], "widget_hints": []})
    joined = " ".join(r.getMessage() for r in caplog.records)
    assert "widget_hints" in joined
    assert "get_extras()" in joined


def test_no_warning_when_there_are_no_extras(caplog) -> None:  # noqa: ANN001
    logger_name = "agentic_data_contracts.semantic.yaml_source"
    with caplog.at_level(logging.WARNING, logger=logger_name):
        YamlSource.from_raw({"metrics": []})
    assert caplog.records == []


def test_expected_extras_silences_declared_sections(caplog) -> None:  # noqa: ANN001
    logger_name = "agentic_data_contracts.semantic.yaml_source"
    with caplog.at_level(logging.WARNING, logger=logger_name):
        src = YamlSource.from_raw(
            {"metrics": [], "column_hints": []},
            expected_extras={"column_hints"},
        )
    assert caplog.records == []
    assert set(src.get_extras()) == {"column_hints"}


def test_expected_extras_raises_on_an_undeclared_key() -> None:
    with pytest.raises(ValueError, match="widget_hints"):
        YamlSource.from_raw(
            {"metrics": [], "widget_hints": []},
            expected_extras={"column_hints"},
        )


def test_empty_expected_extras_is_strict_mode() -> None:
    with pytest.raises(ValueError, match="anything"):
        YamlSource.from_raw({"metrics": [], "anything": 1}, expected_extras=frozenset())


def test_typo_in_a_supported_key_is_caught_by_expected_extras() -> None:
    """`relationship:` for `relationships:` silently deleted a section before."""
    with pytest.raises(ValueError, match="relationship"):
        YamlSource.from_raw(
            {"metrics": [], "relationship": [{"from": "a.b.c", "to": "d.e.f"}]},
            expected_extras=frozenset(),
        )


def test_yaml_native_date_in_extras_becomes_an_iso_string() -> None:
    src = YamlSource.from_raw(
        {
            "metrics": [],
            "column_hints": [{"table": "t", "verified": date(2026, 3, 14)}],
        }
    )
    assert src.get_extras()["column_hints"][0]["verified"] == "2026-03-14"


def test_datetime_in_extras_is_normalised_to_a_date_string() -> None:
    src = YamlSource.from_raw(
        {"metrics": [], "notes": {"at": datetime(2026, 3, 14, 12, 0, 0)}}
    )
    assert src.get_extras()["notes"]["at"] == "2026-03-14"


def test_normalised_extras_are_json_serialisable() -> None:
    src = YamlSource.from_raw(
        {"metrics": [], "column_hints": [{"verified": date(2026, 3, 14)}]}
    )
    assert json.dumps(src.get_extras())  # must not raise


def test_unserialisable_extras_value_raises_naming_the_path() -> None:
    with pytest.raises(ValueError, match=r"column_hints\[0\]\.owner"):
        YamlSource.from_raw({"metrics": [], "column_hints": [{"owner": {1, 2, 3}}]})


class TestDecompositionConventionDefault:
    _RAW = {
        "decomposition_convention": {"convention": "split_evenly"},
        "metrics": [
            {
                "name": "activations",
                "decompositions": [
                    {"operator": "product", "operands": ["volume", "rate"]}
                ],
            },
            {
                "name": "signups",
                "decompositions": [
                    {
                        "operator": "product",
                        "operands": ["volume", "rate"],
                        "convention": "fold_into",
                        "convention_operand": "rate",
                    }
                ],
            },
            {
                "name": "net",
                "decompositions": [{"operator": "sum", "operands": ["volume", "rate"]}],
            },
            {"name": "volume"},
            {"name": "rate"},
        ],
    }

    def test_default_is_stamped_onto_undeclared_cross_term_decomposition(self) -> None:
        source = YamlSource.from_raw(self._RAW)
        metric = source.get_metric("activations")
        assert metric is not None
        assert metric.decompositions[0].convention == "split_evenly"

    def test_per_decomposition_declaration_wins(self) -> None:
        source = YamlSource.from_raw(self._RAW)
        metric = source.get_metric("signups")
        assert metric is not None
        assert metric.decompositions[0].convention == "fold_into"
        assert metric.decompositions[0].convention_operand == "rate"

    def test_default_is_not_stamped_onto_a_linear_operator(self) -> None:
        # Stamping a convention onto `sum` would trip its own validation.
        source = YamlSource.from_raw(self._RAW)
        metric = source.get_metric("net")
        assert metric is not None
        assert metric.decompositions[0].convention is None

    def test_default_key_is_vocabulary_not_an_extra(self) -> None:
        # expected_extras=[] is strict mode: an uninterpreted top-level key
        # raises. The default must be recognised, not warned about as a typo.
        source = YamlSource.from_raw(self._RAW, expected_extras=[])
        assert "decomposition_convention" not in source.get_extras()

    def test_fold_into_as_a_source_default_raises(self) -> None:
        with pytest.raises(ValueError, match="cannot be a source-level default"):
            YamlSource.from_raw(
                {
                    "decomposition_convention": {
                        "convention": "fold_into",
                        "convention_operand": "rate",
                    },
                    "metrics": [{"name": "volume"}],
                }
            )

    def test_unknown_default_convention_raises(self) -> None:
        with pytest.raises(ValueError, match="unknown attribution convention"):
            YamlSource.from_raw(
                {
                    "decomposition_convention": {"convention": "shapley"},
                    "metrics": [{"name": "volume"}],
                }
            )
