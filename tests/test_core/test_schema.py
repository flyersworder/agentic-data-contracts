from pathlib import Path

import pytest
import yaml

from agentic_data_contracts.core.schema import (
    AllowedTable,
    DataContractSchema,
    Enforcement,
    QueryCheck,
    ResultCheck,
    SemanticRule,
    SuccessCriterionConfig,
)


def test_full_contract_parses(fixtures_dir: Path) -> None:
    raw = yaml.safe_load((fixtures_dir / "valid_contract.yml").read_text())
    schema = DataContractSchema.model_validate(raw)
    assert schema.name == "revenue-analysis"
    assert schema.version == "1.0"
    assert len(schema.semantic.allowed_tables) == 2
    assert schema.semantic.allowed_tables[0].schema_ == "analytics"
    assert schema.semantic.allowed_tables[0].tables == [
        "orders",
        "customers",
        "subscriptions",
    ]
    assert schema.resources is not None
    assert schema.resources.cost_limit_usd == 5.00
    assert schema.resources.max_retries == 3
    assert schema.temporal is not None
    assert schema.temporal.max_duration_seconds == 300
    assert len(schema.success_criteria) == 3
    assert schema.success_criteria[0].weight == pytest.approx(0.4)


def test_minimal_contract_parses(fixtures_dir: Path) -> None:
    raw = yaml.safe_load((fixtures_dir / "minimal_contract.yml").read_text())
    schema = DataContractSchema.model_validate(raw)
    assert schema.name == "basic-query"
    assert schema.semantic.source is None
    assert schema.resources is None
    assert schema.temporal is None
    assert schema.success_criteria == []


def test_invalid_enforcement_rejected() -> None:
    with pytest.raises(Exception):
        SemanticRule.model_validate(
            {
                "name": "bad",
                "description": "bad rule",
                "enforcement": "crash",
                "query_check": {"no_select_star": True},
            }
        )


def test_enforcement_values() -> None:
    for val in (Enforcement.BLOCK, Enforcement.WARN, Enforcement.LOG):
        rule = SemanticRule(
            name="test",
            description="test",
            enforcement=val,
            query_check=QueryCheck(no_select_star=True),
        )
        assert rule.enforcement == val


def test_success_criteria_weight_validation() -> None:
    with pytest.raises(Exception):
        SuccessCriterionConfig(name="bad", weight=1.5)
    with pytest.raises(Exception):
        SuccessCriterionConfig(name="bad", weight=-0.1)


def test_allowed_table_empty_tables() -> None:
    t = AllowedTable.model_validate({"schema": "raw", "tables": []})
    assert t.tables == []


def test_query_check_rule() -> None:
    rule = SemanticRule(
        name="tenant_filter",
        description="Must filter by tenant_id",
        enforcement=Enforcement.BLOCK,
        table="analytics.orders",
        query_check=QueryCheck(required_filter="tenant_id"),
    )
    assert rule.table == "analytics.orders"
    assert rule.query_check is not None
    assert rule.query_check.required_filter == "tenant_id"
    assert rule.result_check is None


def test_result_check_rule() -> None:
    rule = SemanticRule(
        name="wau_sanity",
        description="WAU must be reasonable",
        enforcement=Enforcement.WARN,
        table="analytics.user_metrics",
        result_check=ResultCheck(column="wau", max_value=8_000_000_000),
    )
    assert rule.result_check is not None
    assert rule.result_check.column == "wau"
    assert rule.result_check.max_value == 8_000_000_000


def test_rule_rejects_both_checks() -> None:
    with pytest.raises(ValueError, match="must not have both"):
        SemanticRule(
            name="bad",
            description="bad",
            enforcement=Enforcement.BLOCK,
            query_check=QueryCheck(no_select_star=True),
            result_check=ResultCheck(min_rows=1),
        )


def test_old_filter_column_rejected() -> None:
    """Old YAML with filter_column should fail loudly, not silently lose enforcement."""
    with pytest.raises(ValueError, match="extra"):
        SemanticRule.model_validate(
            {
                "name": "tenant_filter",
                "description": "Must filter by tenant_id",
                "enforcement": "block",
                "filter_column": "tenant_id",
            }
        )


def test_advisory_rule_no_checks() -> None:
    """Rules with neither check are advisory — shown in prompt only."""
    rule = SemanticRule(
        name="advisory",
        description="Just a guideline",
        enforcement=Enforcement.WARN,
    )
    assert rule.query_check is None
    assert rule.result_check is None


def test_table_scoping_optional() -> None:
    rule = SemanticRule(
        name="global_rule",
        description="Applies everywhere",
        enforcement=Enforcement.BLOCK,
        query_check=QueryCheck(require_limit=True),
    )
    assert rule.table is None


def test_table_must_be_fully_qualified() -> None:
    with pytest.raises(ValueError, match="fully qualified"):
        SemanticRule(
            name="bad",
            description="bad",
            enforcement=Enforcement.BLOCK,
            table="orders",  # missing schema prefix
            query_check=QueryCheck(require_limit=True),
        )


def test_table_wildcard_accepted() -> None:
    rule = SemanticRule(
        name="global",
        description="all tables",
        enforcement=Enforcement.BLOCK,
        table="*",
        query_check=QueryCheck(no_select_star=True),
    )
    assert rule.table == "*"


def test_table_qualified_accepted() -> None:
    rule = SemanticRule(
        name="scoped",
        description="scoped",
        enforcement=Enforcement.BLOCK,
        table="analytics.orders",
        query_check=QueryCheck(required_filter="tenant_id"),
    )
    assert rule.table == "analytics.orders"


def test_query_check_multiple_fields() -> None:
    qc = QueryCheck(
        required_filter="tenant_id",
        no_select_star=True,
        max_joins=3,
    )
    assert qc.required_filter == "tenant_id"
    assert qc.no_select_star is True
    assert qc.max_joins == 3


def test_result_check_row_bounds() -> None:
    rc = ResultCheck(min_rows=1, max_rows=10000)
    assert rc.min_rows == 1
    assert rc.max_rows == 10000
    assert rc.column is None


def test_result_check_column_bounds() -> None:
    rc = ResultCheck(column="revenue", min_value=0, not_null=True)
    assert rc.column == "revenue"
    assert rc.min_value == 0
    assert rc.not_null is True
