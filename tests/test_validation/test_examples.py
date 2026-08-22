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
    with pytest.raises(ValueError, match="sql"):
        VerifiedExample.from_dict({"question": "no sql here"})


def test_from_dict_rejects_non_mapping_row() -> None:
    # A mis-indented YAML entry can parse to a bare string; from_dict must give a
    # clear error, not do substring matching on "sql" and crash with a TypeError.
    with pytest.raises(ValueError, match="expects a mapping"):
        VerifiedExample.from_dict("type: sql")


def test_from_dict_rejects_non_mapping_metadata() -> None:
    # A scalar `metadata:` value must raise a clear error, not crash in dict().
    with pytest.raises(ValueError, match="metadata"):
        VerifiedExample.from_dict({"sql": "SELECT 1", "metadata": "reviewed"})


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


def test_ok_requires_every_example_verified_and_passing() -> None:
    # A safe CI gate: only an all-valid corpus is ok. Any violation, unchecked,
    # or unverified (engine-planned but policy-unchecked) row makes it unsafe.
    assert ExampleValidationReport(results=[_res("valid")]).ok
    assert not ExampleValidationReport(results=[_res("violation")]).ok
    assert not ExampleValidationReport(results=[_res("unchecked")]).ok
    assert not ExampleValidationReport(
        results=[_res("unverified", contract_checked=False)]
    ).ok
    assert not ExampleValidationReport(results=[_res("valid"), _res("unchecked")]).ok


def test_unverified_compliance_flags_engine_only_passes() -> None:
    report = ExampleValidationReport(
        results=[_res("valid"), _res("unverified", contract_checked=False)]
    )
    assert len(report.unverified_compliance) == 1
    assert report.unverified_compliance[0].status == "unverified"
    assert not report.ok  # an unverified row makes the gate unsafe


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


def test_empty_input_is_not_ok(contract: DataContract) -> None:
    # An empty corpus validated nothing, so the gate must NOT pass — otherwise a
    # bad load path or emptied file silently green-lights the MR.
    report = validate_examples([], contract)
    assert report.results == []
    assert not report.ok


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


_UNPARSEABLE_SQL = "SELECT * FROM ("  # confirmed to raise ParseError in Task 1


def test_parse_fallback_engine_plans_is_unverified(
    contract: DataContract,
) -> None:
    adapter = FakeExplainAdapter(
        ExplainResult(estimated_cost_usd=None, estimated_rows=1, schema_valid=True)
    )
    report = validate_examples(
        [VerifiedExample(sql=_UNPARSEABLE_SQL, id="vdp")],
        contract,
        explain_adapter=adapter,
    )
    r = report.results[0]
    assert r.status == "unverified"  # engine planned it, policy NOT checked
    assert not r.contract_checked
    assert r.engine_checked
    assert any("not statically verified" in w for w in r.warnings)
    assert report.unverified_compliance == [r]
    assert not report.ok  # unverified — the gate must NOT green-light it


def test_parse_fallback_engine_rejects_is_violation(contract: DataContract) -> None:
    adapter = FakeExplainAdapter(
        ExplainResult(
            estimated_cost_usd=None,
            estimated_rows=None,
            schema_valid=False,
            errors=["syntax error near '('"],
        )
    )
    report = validate_examples(
        [VerifiedExample(sql=_UNPARSEABLE_SQL)], contract, explain_adapter=adapter
    )
    r = report.results[0]
    assert r.status == "violation"
    assert not r.contract_checked
    assert r.engine_checked
    assert any("syntax error" in reason for reason in r.reasons)


def test_public_exports() -> None:
    from agentic_data_contracts.validation import (
        ExampleResult,
        ExampleValidationReport,
        VerifiedExample,
        validate_examples,
    )

    assert VerifiedExample and ExampleResult
    assert ExampleValidationReport and validate_examples


def test_principal_scoped_validation(fixtures_dir: Path) -> None:
    contract = DataContract.from_yaml(fixtures_dir / "filter_values_contract.yml")
    sql = "SELECT id FROM sales.opps WHERE account_id = 123"
    report = validate_examples(
        [
            VerifiedExample(sql=sql, principal="partner@co.com", id="partner"),
            VerifiedExample(sql=sql, principal="vip@co.com", id="vip"),
        ],
        contract,
    )
    by_id = {r.example.id: r.status for r in report.results}
    assert by_id["partner"] == "valid"  # 123 in partner's allowlist
    assert by_id["vip"] == "violation"  # 123 not in vip's [999]


def test_summary_renders_offender_line_with_reason() -> None:
    # Strengthen the earlier weak substring check: the per-row offender line must
    # actually render, with the violation's reason text — not just the header.
    report = ExampleValidationReport(
        results=[
            ExampleResult(
                example=VerifiedExample(sql="SELECT 1", id="bad-query"),
                status="violation",
                reasons=["forbidden table raw.payments"],
                warnings=[],
                contract_checked=True,
                engine_checked=False,
            )
        ]
    )
    text = report.summary()
    assert "- violation `bad-query`: forbidden table raw.payments" in text


def test_summary_distinguishes_two_unnamed_rows() -> None:
    # Two rows with no id and no question must not collapse to one label — the
    # positional #index fallback keeps them distinct in the MR comment.
    unnamed = [
        ExampleResult(
            example=VerifiedExample(sql="SELECT 1"),
            status="violation",
            reasons=["reason A"],
            warnings=[],
            contract_checked=True,
            engine_checked=False,
        ),
        ExampleResult(
            example=VerifiedExample(sql="SELECT 2"),
            status="violation",
            reasons=["reason B"],
            warnings=[],
            contract_checked=True,
            engine_checked=False,
        ),
    ]
    text = ExampleValidationReport(results=unnamed).summary()
    assert "`#0`: reason A" in text
    assert "`#1`: reason B" in text


def test_all_unparseable_corpus_is_not_ok(contract: DataContract) -> None:
    # No adapter + unparseable SQL -> every result is "unchecked". The CI gate
    # (`if not report.ok`) must FAIL, not green-light an unvalidated corpus.
    report = validate_examples(
        [
            VerifiedExample(sql=_UNPARSEABLE_SQL, id="a"),
            VerifiedExample(sql=_UNPARSEABLE_SQL, id="b"),
        ],
        contract,
    )
    assert all(r.status == "unchecked" for r in report.results)
    assert not report.violations
    assert not report.ok


class _RaisingExplainAdapter:
    def explain(self, sql: str) -> ExplainResult:
        raise RuntimeError("driver could not parse")


def test_raising_adapter_degrades_to_unchecked_without_aborting(
    contract: DataContract,
) -> None:
    # A thin adapter that raises (Layer 2 or decision-B) must not crash the batch;
    # the offending example degrades to "unchecked" and the rest still validate.
    report = validate_examples(
        [
            VerifiedExample(sql=_UNPARSEABLE_SQL, id="x"),  # decision-B path
            VerifiedExample(sql=_OK_SQL, id="y"),  # Layer-2 path
        ],
        contract,
        explain_adapter=_RaisingExplainAdapter(),
    )
    by_id = {r.example.id: r for r in report.results}
    assert by_id["x"].status == "unchecked"
    assert by_id["y"].status == "unchecked"
    assert any("validation error" in reason for reason in by_id["y"].reasons)
    assert not report.ok


def test_row_limit_block_marks_engine_checked(contract: DataContract) -> None:
    # valid_contract.yml sets max_rows_scanned = 1_000_000. A block from the
    # row-limit check (schema_valid stays True, estimated_rows non-None) must
    # still report engine_checked True — the sibling path to the cost-limit case.
    adapter = FakeExplainAdapter(
        ExplainResult(
            estimated_cost_usd=None, estimated_rows=1_000_001, schema_valid=True
        )
    )
    report = validate_examples(
        [VerifiedExample(sql=_OK_SQL)], contract, explain_adapter=adapter
    )
    r = report.results[0]
    assert r.status == "violation"
    assert r.engine_checked


def test_from_dict_maps_assertion_fields() -> None:
    ex = VerifiedExample.from_dict(
        {
            "sql": "SELECT 1",
            "expected": 1204338.55,
            "rel_tol": 1e-6,
            "abs_tol": 0.5,
            "time_scoped": True,
        }
    )
    assert ex.expected == 1204338.55
    assert ex.rel_tol == 1e-6
    assert ex.abs_tol == 0.5
    assert ex.time_scoped is True
    # They are first-class fields now, not unknown keys.
    assert ex.metadata == {}


def test_from_dict_defaults_assertion_fields_to_absent() -> None:
    ex = VerifiedExample.from_dict({"sql": "SELECT 1"})
    assert ex.expected is None
    assert ex.rel_tol is None
    assert ex.abs_tol is None
    assert ex.time_scoped is False


def test_from_dict_coerces_integer_expected_to_float() -> None:
    ex = VerifiedExample.from_dict({"sql": "SELECT 1", "expected": 940})
    assert ex.expected == 940.0
    assert isinstance(ex.expected, float)


def test_from_dict_rejects_boolean_expected() -> None:
    # bool is an int subclass in Python, so a naive isinstance check would let
    # `expected: true` through and assert against 1.0.
    with pytest.raises(ValueError, match="expected"):
        VerifiedExample.from_dict({"sql": "SELECT 1", "expected": True})


def test_from_dict_rejects_non_numeric_expected() -> None:
    with pytest.raises(ValueError, match="expected"):
        VerifiedExample.from_dict({"sql": "SELECT 1", "expected": "1.2M"})


def test_from_dict_rejects_non_finite_expected() -> None:
    # A YAML `.nan` / `.inf` is a malformed answer, not an assertion that can
    # ever match.
    with pytest.raises(ValueError, match="finite"):
        VerifiedExample.from_dict({"sql": "SELECT 1", "expected": float("nan")})
    with pytest.raises(ValueError, match="finite"):
        VerifiedExample.from_dict({"sql": "SELECT 1", "expected": float("inf")})


def test_from_dict_rejects_negative_tolerance() -> None:
    with pytest.raises(ValueError, match="rel_tol"):
        VerifiedExample.from_dict({"sql": "SELECT 1", "rel_tol": -1e-6})
    with pytest.raises(ValueError, match="abs_tol"):
        VerifiedExample.from_dict({"sql": "SELECT 1", "abs_tol": -0.5})


def test_from_dict_rejects_non_boolean_time_scoped() -> None:
    with pytest.raises(ValueError, match="time_scoped"):
        VerifiedExample.from_dict({"sql": "SELECT 1", "time_scoped": "yes"})
