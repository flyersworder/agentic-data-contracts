from pathlib import Path

import pytest

from agentic_data_contracts.core.contract import DataContract
from agentic_data_contracts.validation.examples import (
    ExampleResult,
    ExampleValidationReport,
    VerifiedExample,
    validate_examples,
)
from agentic_data_contracts.validation.explain import ExplainResult


def test_from_dict_maps_known_fields() -> None:
    ex = VerifiedExample.from_dict(
        {"id": "wau", "question": "weekly active users", "sql": "SELECT 1"}
    )
    assert ex.id == "wau"
    assert ex.question == "weekly active users"
    assert ex.sql == "SELECT 1"


def test_from_dict_stashes_unknown_keys_in_metadata() -> None:
    ex = VerifiedExample.from_dict(
        {"sql": "SELECT 1", "verified_by": "jsmith", "type": "sql"}
    )
    assert ex.metadata["verified_by"] == "jsmith"
    assert ex.metadata["type"] == "sql"


def test_from_dict_merges_explicit_metadata() -> None:
    ex = VerifiedExample.from_dict({"sql": "SELECT 1", "metadata": {"a": 1}, "b": 2})
    assert ex.metadata == {"a": 1, "b": 2}


def test_from_dict_requires_sql() -> None:
    import pytest

    with pytest.raises(ValueError, match="sql"):
        VerifiedExample.from_dict({"question": "no sql here"})


def _res(status: str, *, contract_checked: bool = True) -> ExampleResult:
    return ExampleResult(
        example=VerifiedExample(sql="SELECT 1", id=status),
        status=status,
        reasons=[],
        warnings=[],
        contract_checked=contract_checked,
        engine_checked=False,
    )


def test_report_partitions_by_status() -> None:
    report = ExampleValidationReport(
        results=[_res("valid"), _res("violation"), _res("unchecked")]
    )
    assert [r.status for r in report.valid] == ["valid"]
    assert [r.status for r in report.violations] == ["violation"]
    assert [r.status for r in report.unchecked] == ["unchecked"]


def test_ok_is_false_only_when_violations_present() -> None:
    assert ExampleValidationReport(results=[_res("valid")]).ok
    assert ExampleValidationReport(results=[_res("unchecked")]).ok
    assert not ExampleValidationReport(results=[_res("violation")]).ok


def test_unverified_compliance_flags_engine_only_passes() -> None:
    report = ExampleValidationReport(
        results=[_res("valid"), _res("valid", contract_checked=False)]
    )
    assert len(report.unverified_compliance) == 1
    assert not report.unverified_compliance[0].contract_checked


def test_summary_mentions_counts_and_offenders() -> None:
    report = ExampleValidationReport(results=[_res("valid"), _res("violation")])
    text = report.summary()
    assert "violation" in text
    assert "1" in text  # a count appears


_OK_SQL = "SELECT id, amount FROM analytics.orders WHERE tenant_id = 'acme'"


class FakeExplainAdapter:
    def __init__(self, result: ExplainResult) -> None:
        self._result = result

    def explain(self, sql: str) -> ExplainResult:
        return self._result


@pytest.fixture
def contract(fixtures_dir: Path) -> DataContract:
    return DataContract.from_yaml(fixtures_dir / "valid_contract.yml")


def test_valid_example_passes(contract: DataContract) -> None:
    report = validate_examples([VerifiedExample(sql=_OK_SQL)], contract)
    assert report.ok
    r = report.results[0]
    assert r.status == "valid"
    assert r.contract_checked
    assert not r.engine_checked  # no adapter passed


def test_forbidden_table_is_violation(contract: DataContract) -> None:
    ex = VerifiedExample(sql="SELECT id FROM raw.payments WHERE tenant_id = 'x'")
    report = validate_examples([ex], contract)
    assert not report.ok
    assert report.results[0].status == "violation"
    assert any("raw.payments" in r for r in report.results[0].reasons)


def test_missing_filter_is_violation(contract: DataContract) -> None:
    report = validate_examples(
        [VerifiedExample(sql="SELECT id FROM analytics.orders")], contract
    )
    assert report.results[0].status == "violation"


def test_schema_drift_is_violation_via_explain(contract: DataContract) -> None:
    adapter = FakeExplainAdapter(
        ExplainResult(
            estimated_cost_usd=None,
            estimated_rows=None,
            schema_valid=False,
            errors=["Column 'amount' not found"],
        )
    )
    report = validate_examples(
        [VerifiedExample(sql=_OK_SQL)], contract, explain_adapter=adapter
    )
    r = report.results[0]
    assert r.status == "violation"
    assert r.contract_checked
    assert r.engine_checked


def test_valid_with_adapter_marks_engine_checked(contract: DataContract) -> None:
    adapter = FakeExplainAdapter(
        ExplainResult(estimated_cost_usd=None, estimated_rows=10, schema_valid=True)
    )
    report = validate_examples(
        [VerifiedExample(sql=_OK_SQL)], contract, explain_adapter=adapter
    )
    r = report.results[0]
    assert r.status == "valid"
    assert r.engine_checked


def test_parse_error_without_adapter_is_unchecked(contract: DataContract) -> None:
    report = validate_examples([VerifiedExample(sql="SELECT * FROM (")], contract)
    r = report.results[0]
    assert r.status == "unchecked"
    assert not r.contract_checked
    assert not r.engine_checked


def test_empty_input(contract: DataContract) -> None:
    report = validate_examples([], contract)
    assert report.results == []
    assert report.ok


def test_results_preserve_input_order(contract: DataContract) -> None:
    exs = [
        VerifiedExample(sql=_OK_SQL, id="a"),
        VerifiedExample(sql="SELECT id FROM analytics.orders", id="b"),
    ]
    report = validate_examples(exs, contract)
    assert [r.example.id for r in report.results] == ["a", "b"]


def test_forbidden_op_is_violation(contract: DataContract) -> None:
    report = validate_examples(
        [VerifiedExample(sql="DELETE FROM analytics.orders WHERE tenant_id = 'x'")],
        contract,
    )
    assert report.results[0].status == "violation"


def test_cost_block_marks_engine_checked(contract: DataContract) -> None:
    # valid_contract.yml sets cost_limit_usd = 5.00; 10.0 exceeds it. The block
    # comes from EXPLAIN (schema_valid stays True), so engine_checked must be
    # True even though it is not a schema rejection.
    adapter = FakeExplainAdapter(
        ExplainResult(estimated_cost_usd=10.0, estimated_rows=None, schema_valid=True)
    )
    report = validate_examples(
        [VerifiedExample(sql=_OK_SQL)], contract, explain_adapter=adapter
    )
    r = report.results[0]
    assert r.status == "violation"
    assert r.engine_checked


def test_contract_policy_drift_valid_then_violation(contract: DataContract) -> None:
    # Same SQL: valid under contract A (analytics.orders allowed), a violation
    # under contract B (only analytics.customers allowed). Proves the verdict is
    # contract-relative — the drift sweep's core promise.
    from agentic_data_contracts.core.schema import DataContractSchema

    example = [VerifiedExample(sql=_OK_SQL)]
    assert validate_examples(example, contract).results[0].status == "valid"

    schema_b = DataContractSchema.model_validate(
        {
            "version": "1.0",
            "name": "drift-b",
            "semantic": {
                "allowed_tables": [{"schema": "analytics", "tables": ["customers"]}],
                "forbidden_operations": [],
                "rules": [],
            },
        }
    )
    contract_b = DataContract(schema=schema_b)
    assert validate_examples(example, contract_b).results[0].status == "violation"


def test_warn_rule_surfaces_warning_without_failing() -> None:
    # A warn-enforcement rule must land in ExampleResult.warnings and keep the
    # example valid (ok stays True). valid_contract's warn rule has no
    # query_check and never fires, so build a contract whose warn rule can.
    from agentic_data_contracts.core.schema import DataContractSchema

    schema = DataContractSchema.model_validate(
        {
            "version": "1.0",
            "name": "warn-test",
            "semantic": {
                "allowed_tables": [{"schema": "analytics", "tables": ["orders"]}],
                "forbidden_operations": [],
                "rules": [
                    {
                        "name": "prefer_explicit_columns",
                        "description": "avoid SELECT *",
                        "enforcement": "warn",
                        "query_check": {"no_select_star": True},
                    }
                ],
            },
        }
    )
    warn_contract = DataContract(schema=schema)
    report = validate_examples(
        [VerifiedExample(sql="SELECT * FROM analytics.orders")], warn_contract
    )
    r = report.results[0]
    assert r.status == "valid"
    assert report.ok
    assert any("SELECT *" in w for w in r.warnings)
