import math
from decimal import Decimal
from pathlib import Path

import pytest

from agentic_data_contracts.adapters.base import QueryResult, TableSchema
from agentic_data_contracts.core.contract import DataContract
from agentic_data_contracts.validation.examples import (
    ExampleAnswerReport,
    ExampleAnswerResult,
    ExampleResult,
    ExampleValidationReport,
    VerifiedExample,
    _compare,
    _label,
    check_example_answers,
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


def _ans(status: str, **kw: object) -> ExampleAnswerResult:
    return ExampleAnswerResult(
        example=VerifiedExample(sql="SELECT 1", id=status, expected=1.0),
        status=status,
        **kw,  # type: ignore
    )


def test_answer_report_partitions_by_status() -> None:
    report = ExampleAnswerReport(
        results=[
            _ans("match"),
            _ans("mismatch"),
            _ans("unassertable"),
            _ans("error"),
        ]
    )
    assert [r.status for r in report.matches] == ["match"]
    assert [r.status for r in report.mismatches] == ["mismatch"]
    assert [r.status for r in report.unassertable] == ["unassertable"]
    assert [r.status for r in report.errors] == ["error"]


def test_answer_report_ok_requires_every_assertion_to_match() -> None:
    assert ExampleAnswerReport(results=[_ans("match")]).ok
    assert not ExampleAnswerReport(results=[_ans("mismatch")]).ok
    assert not ExampleAnswerReport(results=[_ans("unassertable")]).ok
    assert not ExampleAnswerReport(results=[_ans("error")]).ok
    assert not ExampleAnswerReport(results=[_ans("match"), _ans("error")]).ok


def test_empty_answer_report_is_not_ok() -> None:
    # Nothing declared an `expected`: a filter, a schema change, or an emptied
    # file dropped every assertion. That must fail rather than pass a no-op gate.
    assert not ExampleAnswerReport(results=[]).ok


def test_answer_summary_mentions_counts_and_offenders() -> None:
    report = ExampleAnswerReport(
        results=[
            _ans("match"),
            _ans("mismatch", expected=100.0, actual=98.0, abs_diff=2.0, rel_diff=0.02),
        ]
    )
    text = report.summary()
    assert "mismatch" in text
    assert "100" in text and "98" in text


def test_label_falls_back_from_id_to_question_to_index() -> None:
    assert _label(VerifiedExample(sql="s", id="ident", question="q"), 3) == "ident"
    assert _label(VerifiedExample(sql="s", question="q"), 3) == "q"
    assert _label(VerifiedExample(sql="s"), 3) == "#3"


def test_summary_tolerates_a_mismatch_with_unset_diffs() -> None:
    # summary() is read when something already went wrong; it must never be
    # the thing that raises.
    report = ExampleAnswerReport(
        results=[_ans("mismatch", expected=100.0, actual=98.0)]
    )
    text = report.summary()
    assert "mismatch" in text
    assert "?" in text


class TestCompare:
    def test_exact_match(self) -> None:
        abs_diff, rel_diff, matched = _compare(100.0, 100.0, 1e-9, 0.0)
        assert (abs_diff, rel_diff, matched) == (0.0, 0.0, True)

    def test_outside_tolerance_is_mismatch(self) -> None:
        abs_diff, rel_diff, matched = _compare(98.0, 100.0, 1e-9, 0.0)
        assert abs_diff == 2.0
        assert rel_diff == pytest.approx(0.02)
        assert not matched

    def test_within_relative_tolerance(self) -> None:
        _, _, matched = _compare(100.05, 100.0, 1e-3, 0.0)
        assert matched

    def test_within_absolute_tolerance(self) -> None:
        _, _, matched = _compare(100.5, 100.0, 0.0, 1.0)
        assert matched

    def test_zero_expected_exact_match_does_not_divide_by_zero(self) -> None:
        # A certified answer of zero is legitimate ("how many failed orders in
        # Q1? None") and must not raise or report a meaningless inf.
        abs_diff, rel_diff, matched = _compare(0.0, 0.0, 1e-9, 0.0)
        assert (abs_diff, rel_diff, matched) == (0.0, 0.0, True)

    def test_zero_expected_near_miss_reports_infinite_rel_diff(self) -> None:
        abs_diff, rel_diff, matched = _compare(0.5, 0.0, 1e-9, 0.0)
        assert abs_diff == 0.5
        assert rel_diff == math.inf
        assert not matched

    def test_zero_expected_rescued_by_abs_tol(self) -> None:
        # The relative term vanishes at expected == 0, so abs_tol is the only
        # way to allow any slack there.
        _, _, matched = _compare(0.5, 0.0, 1e-9, 1.0)
        assert matched

    def test_relative_term_is_anchored_on_expected_not_actual(self) -> None:
        # expected=100, actual=200, rel_tol=0.75. Anchored on expected the
        # threshold is 75 and the diff of 100 is a mismatch; anchored on actual
        # (or on max(|a|,|b|), as math.isclose does) it would be 150 and pass.
        _, _, matched = _compare(200.0, 100.0, 0.75, 0.0)
        assert not matched


_NO_ROWS = object()
_TWO_COLS = object()


class SpyAdapter:
    """A DatabaseAdapter that returns canned scalars and records every call."""

    def __init__(self, values: dict[str, object] | None = None) -> None:
        self.values = values or {}
        self.calls: list[str] = []

    def execute(self, sql: str) -> QueryResult:
        self.calls.append(sql)
        if sql not in self.values:
            raise AssertionError(f"SpyAdapter has no canned value for: {sql}")
        value = self.values[sql]
        if isinstance(value, Exception):
            raise value
        if value is _NO_ROWS:
            return QueryResult(columns=["v"], rows=[])
        if value is _TWO_COLS:
            return QueryResult(columns=["a", "b"], rows=[(1, 2)])
        return QueryResult(columns=["v"], rows=[(value,)])

    def explain(self, sql: str) -> ExplainResult:
        return ExplainResult(
            estimated_cost_usd=None, estimated_rows=1, schema_valid=True
        )

    def describe_table(self, schema: str, table: str) -> TableSchema:
        return TableSchema(columns=[])

    def list_tables(self, schema: str) -> list[str]:
        return []

    @property
    def dialect(self) -> str:
        return "duckdb"


_SUM_SQL = "SELECT SUM(amount) FROM analytics.orders WHERE tenant_id = 'acme'"


def _asserted(
    sql: str,
    expected: float,
    *,
    id: str | None = None,
    rel_tol: float | None = None,
    abs_tol: float | None = None,
    time_scoped: bool = False,
) -> VerifiedExample:
    return VerifiedExample(
        sql=sql,
        expected=expected,
        id=id,
        rel_tol=rel_tol,
        abs_tol=abs_tol,
        time_scoped=time_scoped,
    )


def test_matching_assertion_is_a_match(contract: DataContract) -> None:
    adapter = SpyAdapter({_SUM_SQL: 1204338.55})
    report = validate_examples([_asserted(_SUM_SQL, 1204338.55)], contract)
    answers = check_example_answers(report, adapter=adapter)
    assert answers.ok
    r = answers.results[0]
    assert r.status == "match"
    assert r.expected == 1204338.55
    assert r.actual == 1204338.55
    assert adapter.calls == [_SUM_SQL]


def test_wrong_number_is_a_mismatch(contract: DataContract) -> None:
    adapter = SpyAdapter({_SUM_SQL: 1198000.0})
    report = validate_examples([_asserted(_SUM_SQL, 1204338.55)], contract)
    answers = check_example_answers(report, adapter=adapter)
    assert not answers.ok
    r = answers.results[0]
    assert r.status == "mismatch"
    assert r.expected == 1204338.55
    assert r.actual == 1198000.0
    assert r.abs_diff is not None and r.abs_diff > 0
    assert r.rel_diff is not None and r.rel_diff > 0


def test_violation_row_is_never_executed(contract: DataContract) -> None:
    # The security-relevant guarantee: a query that failed the contract must
    # not be run against the warehouse to see what it returns.
    bad = _asserted("SELECT SUM(amount) FROM raw.payments WHERE tenant_id = 'x'", 1.0)
    adapter = SpyAdapter()
    report = validate_examples([bad], contract)
    assert report.results[0].status == "violation"
    answers = check_example_answers(report, adapter=adapter)
    assert answers.results == []
    assert adapter.calls == []


def test_unchecked_row_is_never_executed(contract: DataContract) -> None:
    adapter = SpyAdapter()
    report = validate_examples([_asserted("SELECT * FROM (", 1.0)], contract)
    assert report.results[0].status == "unchecked"
    answers = check_example_answers(report, adapter=adapter)
    assert answers.results == []
    assert adapter.calls == []


def test_unverified_row_is_never_executed(contract: DataContract) -> None:
    # Decision-B: the engine planned it but contract policy was never statically
    # checked, so it is not vouched-valid and must not be executed either.
    explain = FakeExplainAdapter(
        ExplainResult(estimated_cost_usd=None, estimated_rows=1, schema_valid=True)
    )
    adapter = SpyAdapter()
    report = validate_examples(
        [_asserted("SELECT * FROM (", 1.0)], contract, explain_adapter=explain
    )
    assert report.results[0].status == "unverified"
    answers = check_example_answers(report, adapter=adapter)
    assert answers.results == []
    assert adapter.calls == []


def test_a_decimal_result_compares_as_a_number(contract: DataContract) -> None:
    # A money column comes back as DECIMAL from DuckDB/Postgres, not float.
    # _scalar's float() coercion handles it; this pins that it keeps doing so.
    adapter = SpyAdapter({_SUM_SQL: Decimal("10700.00")})
    answers = check_example_answers(
        validate_examples([_asserted(_SUM_SQL, 10700.0)], contract), adapter=adapter
    )
    assert answers.results[0].status == "match"
    assert answers.results[0].actual == 10700.0


def test_row_without_expected_produces_no_result(contract: DataContract) -> None:
    adapter = SpyAdapter()
    report = validate_examples([VerifiedExample(sql=_SUM_SQL)], contract)
    answers = check_example_answers(report, adapter=adapter)
    assert answers.results == []
    assert adapter.calls == []


def test_per_example_tolerance_beats_the_call_level_default(
    contract: DataContract,
) -> None:
    # An answer certified from a dashboard rounded to cents against a
    # full-precision SUM: rescued by the row's own rel_tol.
    adapter = SpyAdapter({_SUM_SQL: 1204338.5612})
    ex = _asserted(_SUM_SQL, 1204338.55, rel_tol=1e-6)
    answers = check_example_answers(validate_examples([ex], contract), adapter=adapter)
    assert answers.results[0].status == "match"
    assert answers.results[0].rel_tol == 1e-6


def test_call_level_tolerance_applies_when_the_row_sets_none(
    contract: DataContract,
) -> None:
    adapter = SpyAdapter({_SUM_SQL: 1204338.5612})
    report = validate_examples([_asserted(_SUM_SQL, 1204338.55)], contract)
    assert check_example_answers(report, adapter=adapter).results[0].status == (
        "mismatch"
    )
    report2 = validate_examples([_asserted(_SUM_SQL, 1204338.55)], contract)
    loose = check_example_answers(report2, adapter=adapter, rel_tol=1e-6)
    assert loose.results[0].status == "match"


def test_a_row_may_override_one_tolerance_without_the_other(
    contract: DataContract,
) -> None:
    adapter = SpyAdapter({_SUM_SQL: 100.5})
    ex = _asserted(_SUM_SQL, 100.0, abs_tol=1.0)
    answers = check_example_answers(
        validate_examples([ex], contract), adapter=adapter, rel_tol=1e-3
    )
    r = answers.results[0]
    assert r.status == "match"
    assert r.abs_tol == 1.0  # from the row
    assert r.rel_tol == 1e-3  # from the call


_ROLLING_SQL = (
    "SELECT SUM(amount) FROM analytics.orders "
    "WHERE tenant_id = 'acme' AND created_at >= CURRENT_DATE - 30"
)


def test_relative_time_window_is_unassertable_and_never_executed(
    contract: DataContract,
) -> None:
    adapter = SpyAdapter()
    report = validate_examples([_asserted(_ROLLING_SQL, 88120.0)], contract)
    assert report.results[0].status == "valid"  # it is a fine query, just not pinnable
    answers = check_example_answers(report, adapter=adapter)
    r = answers.results[0]
    assert r.status == "unassertable"
    assert r.reason is not None and "CurrentDate" in r.reason
    assert adapter.calls == []  # the guarantee, asserted directly
    assert not answers.ok


def test_time_scoped_flag_permits_a_relative_window(contract: DataContract) -> None:
    adapter = SpyAdapter({_ROLLING_SQL: 88120.0})
    ex = _asserted(_ROLLING_SQL, 88120.0, time_scoped=True)
    answers = check_example_answers(validate_examples([ex], contract), adapter=adapter)
    assert answers.results[0].status == "match"
    assert adapter.calls == [_ROLLING_SQL]


@pytest.mark.parametrize(
    ("fragment", "marker"),
    [
        # Typed arm — the only two spellings sqlglot types in every dialect.
        ("CURRENT_DATE", "CurrentDate"),
        ("CURRENT_TIMESTAMP", "CurrentTimestamp"),
        # Anonymous arm — under duckdb (what SpyAdapter reports) these stay
        # exp.Anonymous, so a typed-node-only scan would miss them. NOW() is
        # the common case and the reason the second arm exists.
        ("NOW()", "NOW()"),
        ("GETDATE()", "GETDATE()"),
    ],
)
def test_each_relative_time_spelling_is_detected(
    contract: DataContract, fragment: str, marker: str
) -> None:
    sql = (
        "SELECT SUM(amount) FROM analytics.orders "
        f"WHERE tenant_id = 'acme' AND created_at >= {fragment}"
    )
    adapter = SpyAdapter()
    answers = check_example_answers(
        validate_examples([_asserted(sql, 1.0)], contract), adapter=adapter
    )
    assert answers.results[0].status == "unassertable"
    assert marker in (answers.results[0].reason or "")
    assert adapter.calls == []


@pytest.mark.parametrize(
    "predicate",
    [
        "now_flag = 1",  # a COLUMN whose name looks like a time function
        "created_at >= DATE '2026-01-01'",  # a pinned literal
        "created_at >= make_date(2026, 1, 1)",  # an unrelated function call
    ],
)
def test_pinned_and_lookalike_predicates_are_not_flagged(
    contract: DataContract, predicate: str
) -> None:
    # The anonymous arm matches function CALLS by name; it must not fire on a
    # column that merely reads like one, or the checker refuses valid work.
    sql = (
        "SELECT SUM(amount) FROM analytics.orders "
        f"WHERE tenant_id = 'acme' AND {predicate}"
    )
    adapter = SpyAdapter({sql: 1.0})
    answers = check_example_answers(
        validate_examples([_asserted(sql, 1.0)], contract), adapter=adapter
    )
    assert answers.results[0].status == "match"
    assert adapter.calls == [sql]


def test_dialect_defaults_to_the_adapters(contract: DataContract) -> None:
    adapter = SpyAdapter({_SUM_SQL: 1.0})
    answers = check_example_answers(
        validate_examples([_asserted(_SUM_SQL, 1.0)], contract), adapter=adapter
    )
    assert answers.results[0].status == "match"  # parsed under duckdb, no crash


def test_explicit_dialect_wins_over_the_adapters(contract: DataContract) -> None:
    adapter = SpyAdapter({_SUM_SQL: 1.0})
    answers = check_example_answers(
        validate_examples([_asserted(_SUM_SQL, 1.0)], contract),
        adapter=adapter,
        dialect="postgres",
    )
    assert answers.results[0].status == "match"


def test_non_scalar_sql_is_an_error(contract: DataContract) -> None:
    sql = (
        "SELECT SUM(amount) AS a, COUNT(id) AS b "
        "FROM analytics.orders WHERE tenant_id = 'acme'"
    )
    adapter = SpyAdapter({sql: _TWO_COLS})
    answers = check_example_answers(
        validate_examples([_asserted(sql, 1.0)], contract), adapter=adapter
    )
    r = answers.results[0]
    assert r.status == "error"
    assert r.reason is not None and "exactly one column" in r.reason


@pytest.mark.parametrize(
    ("value", "fragment"),
    [(_NO_ROWS, "no rows"), (None, "NULL"), (float("nan"), "non-finite")],
)
def test_unusable_scalar_is_an_error(
    contract: DataContract, value: object, fragment: str
) -> None:
    adapter = SpyAdapter({_SUM_SQL: value})
    answers = check_example_answers(
        validate_examples([_asserted(_SUM_SQL, 1.0)], contract), adapter=adapter
    )
    r = answers.results[0]
    assert r.status == "error"
    assert r.reason is not None and fragment in r.reason


def test_a_raising_adapter_degrades_only_its_own_row(contract: DataContract) -> None:
    other = "SELECT COUNT(id) FROM analytics.orders WHERE tenant_id = 'acme'"
    adapter = SpyAdapter({_SUM_SQL: RuntimeError("connection reset"), other: 42.0})
    report = validate_examples(
        [_asserted(_SUM_SQL, 1.0, id="boom"), _asserted(other, 42.0, id="fine")],
        contract,
    )
    answers = check_example_answers(report, adapter=adapter)
    assert [r.status for r in answers.results] == ["error", "match"]
    assert "connection reset" in (answers.results[0].reason or "")


def test_unparseable_sql_is_an_error_and_is_never_executed(
    contract: DataContract,
) -> None:
    # A normalizer or dialect mismatch between the two passes. An unparseable
    # statement cannot be cleared of a relative time window, so it must not run.
    class BrokenNormalizer:
        def normalize_sql(self, sql: str) -> str:
            return "SELECT * FROM ("

    adapter = SpyAdapter()
    answers = check_example_answers(
        validate_examples([_asserted(_SUM_SQL, 1.0)], contract),
        adapter=adapter,
        sql_normalizer=BrokenNormalizer(),
    )
    assert answers.results[0].status == "error"
    assert adapter.calls == []


def test_results_preserve_report_order(contract: DataContract) -> None:
    other = "SELECT COUNT(id) FROM analytics.orders WHERE tenant_id = 'acme'"
    adapter = SpyAdapter({_SUM_SQL: 1.0, other: 2.0})
    report = validate_examples(
        [
            _asserted(_SUM_SQL, 1.0, id="a"),
            VerifiedExample(sql=other, id="skipped"),  # no expected
            _asserted(other, 2.0, id="c"),
        ],
        contract,
    )
    answers = check_example_answers(report, adapter=adapter)
    assert [r.example.id for r in answers.results] == ["a", "c"]
