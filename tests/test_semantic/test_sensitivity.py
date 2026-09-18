"""Contract-declared sensitivity properties: schema, parsing, validation."""

from __future__ import annotations

import pytest

from agentic_data_contracts.semantic.base import (
    MetricDefinition,
    SensitivityProperty,
    Shadow,
    validate_sensitivity,
)
from agentic_data_contracts.semantic.yaml_source import YamlSource

SHADOW_SQL = (
    "SELECT * FROM mkt.touchpoints UNION ALL "
    "SELECT lead_id, 'display', qualified_date + 1 FROM mkt.lead_scores"
)


def _raw(**overrides: object) -> dict:
    """A one-metric semantic document carrying one sensitivity property."""
    prop: dict = {
        "name": "attribution_is_first_touch",
        "description": (
            "A touchpoint after qualification cannot change a first-touch attribution."
        ),
        "shadow": {"table": "mkt.touchpoints", "sql": SHADOW_SQL},
        "expect": "unchanged",
    }
    prop.update(overrides)
    return {
        "metrics": [
            {
                "name": "mql_count",
                "description": "Count of leads flagged is_mql, first touch.",
                "sql_expression": "COUNT(DISTINCT lead_id)",
                "sensitivity": [prop],
            }
        ]
    }


class TestDataModel:
    def test_metric_defaults_to_no_sensitivity(self) -> None:
        m = MetricDefinition(name="signups", description="", sql_expression="COUNT(*)")
        assert m.sensitivity == []

    def test_property_holds_claim_and_shadow(self) -> None:
        p = SensitivityProperty(
            name="p",
            description="the claim",
            shadow=Shadow(table="mkt.touchpoints", sql=SHADOW_SQL),
            expect="unchanged",
        )
        assert p.shadow.table == "mkt.touchpoints"
        assert p.expect == "unchanged"


class TestParsing:
    def test_parses_a_declared_property(self) -> None:
        metric = YamlSource.from_raw(_raw()).get_metric("mql_count")
        assert metric is not None
        (prop,) = metric.sensitivity
        assert prop.name == "attribution_is_first_touch"
        assert prop.expect == "unchanged"
        assert prop.shadow.table == "mkt.touchpoints"
        assert "UNION ALL" in prop.shadow.sql
        assert prop.description.startswith("A touchpoint after qualification")

    def test_metric_without_the_key_parses_to_empty(self) -> None:
        raw = {"metrics": [{"name": "m", "description": "", "sql_expression": "1"}]}
        metric = YamlSource.from_raw(raw).get_metric("m")
        assert metric is not None
        assert metric.sensitivity == []

    def test_non_mapping_shadow_raises(self) -> None:
        with pytest.raises(ValueError, match="shadow"):
            YamlSource.from_raw(_raw(shadow="SELECT 1"))


class TestLoadTimeValidation:
    # `_check_entry_keys` raises only in strict mode, and `strict` is
    # `expected_extras is not None` -- declaring it at all is this loader's
    # documented way to say "fail my build on a key you do not read". Without
    # it an unknown nested key logs a warning and is dropped, which is the
    # behaviour every sibling key set already has.
    def test_unknown_property_key_raises(self) -> None:
        with pytest.raises(ValueError, match="sensitivity"):
            YamlSource.from_raw(_raw(expects="unchanged"), expected_extras=[])

    def test_unknown_shadow_key_raises(self) -> None:
        raw = _raw(
            shadow={"table": "mkt.touchpoints", "sql": SHADOW_SQL, "where": "1=1"}
        )
        with pytest.raises(ValueError, match="shadow"):
            YamlSource.from_raw(raw, expected_extras=[])

    def test_empty_description_raises(self) -> None:
        # This tests the load path: `require_text` rejects the blank description
        # in `_load_from_raw` before `validate_sensitivity` is called. Coverage
        # of the validator's own description check is in
        # `test_blank_description_raises_for_a_directly_built_metric`.
        with pytest.raises(ValueError, match="description"):
            YamlSource.from_raw(_raw(description="   "))

    def test_blank_description_raises_for_a_directly_built_metric(self) -> None:
        # `YamlSource` never reaches this branch -- `require_text` rejects a
        # blank description at parse time. But dbt/Cube/Ossie sources and
        # direct construction build `MetricDefinition` without it, so this is
        # the call path the check actually defends.
        metric = MetricDefinition(
            name="mql_count",
            description="",
            sql_expression="COUNT(*)",
            sensitivity=[
                SensitivityProperty(
                    name="p",
                    description="   ",
                    shadow=Shadow(table="mkt.touchpoints", sql="SELECT 1"),
                    expect="unchanged",
                )
            ],
        )
        with pytest.raises(ValueError, match="non-empty description"):
            validate_sensitivity([metric])

    def test_bad_expect_raises(self) -> None:
        with pytest.raises(ValueError, match="expect"):
            YamlSource.from_raw(_raw(expect="maybe"))

    def test_missing_expect_raises(self) -> None:
        raw = _raw()
        del raw["metrics"][0]["sensitivity"][0]["expect"]
        with pytest.raises(ValueError, match="expect"):
            YamlSource.from_raw(raw)

    def test_malformed_shadow_table_raises(self) -> None:
        with pytest.raises(ValueError, match="schema.table"):
            YamlSource.from_raw(
                _raw(shadow={"table": "touchpoints", "sql": "SELECT 1"})
            )

    def test_duplicate_property_name_raises(self) -> None:
        raw = _raw()
        raw["metrics"][0]["sensitivity"].append(
            dict(raw["metrics"][0]["sensitivity"][0])
        )
        with pytest.raises(ValueError, match="duplicate"):
            YamlSource.from_raw(raw)

    def test_two_distinct_names_are_fine(self) -> None:
        raw = _raw()
        second = dict(raw["metrics"][0]["sensitivity"][0])
        second["name"] = "another_property"
        raw["metrics"][0]["sensitivity"].append(second)
        metric = YamlSource.from_raw(raw).get_metric("mql_count")
        assert metric is not None
        assert [p.name for p in metric.sensitivity] == [
            "attribution_is_first_touch",
            "another_property",
        ]
