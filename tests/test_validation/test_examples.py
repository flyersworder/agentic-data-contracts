import math
from decimal import Decimal
from pathlib import Path
from typing import Any

import pytest
from sqlglot import exp

from agentic_data_contracts.adapters.base import QueryResult, TableSchema
from agentic_data_contracts.core.contract import DataContract
from agentic_data_contracts.validation._timewindow import _TIME_FUNC_NAMES, _TIME_FUNCS
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


def test_from_dict_rejects_non_numeric_tolerance() -> None:
    # Only the negative-value case existed; a wrong-typed tolerance (e.g. a
    # YAML string) must raise the same clear error _numeric gives 'expected'.
    with pytest.raises(ValueError, match="rel_tol"):
        VerifiedExample.from_dict({"sql": "SELECT 1", "rel_tol": "tight"})
    with pytest.raises(ValueError, match="abs_tol"):
        VerifiedExample.from_dict({"sql": "SELECT 1", "abs_tol": "loose"})


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


def test_empty_answer_summary_explains_why_nothing_was_checked() -> None:
    # `ok` is False for an empty report by design, so this text is what a
    # first-time user sees when CI goes red. Four zeroes do not explain it.
    text = ExampleAnswerReport(results=[]).summary()
    assert "no assertions found" in text
    assert "expected" in text
    assert "0 match" not in text  # the bare-counts line must not be used here


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


def test_answer_summary_reuses_the_validation_report_label(
    contract: DataContract,
) -> None:
    # Minor-B regression: an unnamed row's label must be computed once, against
    # its index in the FULL validation report, and reused verbatim in
    # summary() — not recomputed against the filtered, asserted-only results
    # (where the same row would land at a different index and render a
    # different #N).
    other = "SELECT COUNT(id) FROM analytics.orders WHERE tenant_id = 'acme'"
    adapter = SpyAdapter({_SUM_SQL: 1.0, other: 999.0})
    report = validate_examples(
        [
            VerifiedExample(sql=other, id="skipped"),  # no expected; #0 in report
            _asserted(_SUM_SQL, 2.0),  # unnamed; #1 in report, #0 once filtered
        ],
        contract,
    )
    answers = check_example_answers(report, adapter=adapter)
    r = answers.results[0]
    assert r.label == "#1"  # its position in report.results, not answers.results
    assert "`#1`" in answers.summary()
    assert "`#0`" not in answers.summary()


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
        if isinstance(value, QueryResult):
            return value
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


# Each case is a fragment plus the marker(s) its refusal reason may carry.
# Several entries accept more than one: *which* arm catches a spelling is
# sqlglot's choice, not ours, and it moves within the supported floor range —
# `TODAY()` is exp.Anonymous on the 28.6 floor and exp.CurrentDate on current
# releases. Pinning one name would assert upstream's taxonomy rather than the
# property that matters here, which is that the row is refused unexecuted.
@pytest.mark.parametrize(
    ("fragment", "markers"),
    [
        # Typed arm — the only two spellings sqlglot types in every dialect.
        ("CURRENT_DATE", ("CurrentDate",)),
        ("CURRENT_TIMESTAMP", ("CurrentTimestamp",)),
        # Anonymous arm — under duckdb (what SpyAdapter reports) these stay
        # exp.Anonymous, so a typed-node-only scan would miss them. NOW() is
        # the common case and the reason the second arm exists.
        ("NOW()", ("NOW()",)),
        ("GETDATE()", ("GETDATE()",)),
        ("TODAY()", ("CurrentDate", "TODAY()")),  # arm depends on the version
        # Important-1 regression: LOCALTIMESTAMP / LOCALTIME parse to typed
        # nodes (exp.Localtimestamp / exp.Localtime) that used to be missing
        # from _TIME_FUNCS entirely, so they slipped past both arms.
        ("LOCALTIMESTAMP", ("Localtimestamp",)),
        ("LOCALTIME", ("Localtime",)),
    ],
)
def test_each_relative_time_spelling_is_detected(
    contract: DataContract, fragment: str, markers: tuple[str, ...]
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
    assert any(m in (answers.results[0].reason or "") for m in markers)
    assert adapter.calls == []


def test_current_time_is_detected_under_snowflake(contract: DataContract) -> None:
    # Important-1 regression: CURRENT_TIME parses to exp.CurrentTime under
    # duckdb (and most dialects) but to exp.Localtime under snowflake on
    # current sqlglot — the exp.CurrentTime entry alone misses Snowflake.
    # Both names are accepted because snowflake typed it CurrentTime on the
    # 28.6 floor; the refusal is the invariant, the node name is not.
    sql = (
        "SELECT SUM(amount) FROM analytics.orders "
        "WHERE tenant_id = 'acme' AND created_at >= CURRENT_TIME"
    )
    adapter = SpyAdapter()
    answers = check_example_answers(
        validate_examples([_asserted(sql, 1.0)], contract),
        adapter=adapter,
        dialect="snowflake",
    )
    assert answers.results[0].status == "unassertable"
    reason = answers.results[0].reason or ""
    assert "Localtime" in reason or "CurrentTime" in reason
    assert adapter.calls == []


# fragment + dialect (None -> the adapter's own duckdb) that produces each
# _TIME_FUNCS node type. Systimestamp is typed only under oracle; every other
# entry is reachable, and stays reachable, under plain duckdb.
_TIME_FUNCS_FRAGMENTS: dict[type[exp.Expression], tuple[str, str | None]] = {
    exp.CurrentDate: ("CURRENT_DATE", None),
    exp.CurrentTimestamp: ("CURRENT_TIMESTAMP", None),
    exp.CurrentTime: ("CURRENT_TIME", None),
    exp.CurrentDatetime: ("CURRENT_DATETIME()", None),
    exp.Localtime: ("LOCALTIME", None),
    exp.Localtimestamp: ("LOCALTIMESTAMP", None),
    exp.Systimestamp: ("SYSTIMESTAMP", "oracle"),
    exp.UtcTimestamp: ("UTC_TIMESTAMP()", None),
}


@pytest.mark.parametrize("node_type", _TIME_FUNCS)
def test_every_time_funcs_entry_is_detected(
    contract: DataContract, node_type: type[exp.Expression]
) -> None:
    # Guards Important 1 going forward: a node type added to _TIME_FUNCS
    # without a matching fixture here fails loudly (KeyError) rather than a
    # node type quietly missing from _TIME_FUNCS hiding behind untested code.
    fragment, dialect = _TIME_FUNCS_FRAGMENTS[node_type]
    sql = (
        "SELECT SUM(amount) FROM analytics.orders "
        f"WHERE tenant_id = 'acme' AND created_at >= {fragment}"
    )
    adapter = SpyAdapter()
    answers = check_example_answers(
        validate_examples([_asserted(sql, 1.0)], contract),
        adapter=adapter,
        dialect=dialect,
    )
    assert answers.results[0].status == "unassertable"
    assert node_type.__name__ in (answers.results[0].reason or "")
    assert adapter.calls == []


@pytest.mark.parametrize("name", sorted(_TIME_FUNC_NAMES))
def test_every_time_func_name_is_detected(contract: DataContract, name: str) -> None:
    # Every entry in _TIME_FUNC_NAMES, called as NAME() under plain duckdb,
    # must be flagged — whether it lands via the typed arm (a name that
    # became a typed node, e.g. "localtime") or the Anonymous arm. An entry
    # that stops being detected (typo, renamed, or a dialect change that
    # neither arm covers) fails here instead of silently passing rows through.
    sql = (
        "SELECT SUM(amount) FROM analytics.orders "
        f"WHERE tenant_id = 'acme' AND created_at >= {name.upper()}()"
    )
    adapter = SpyAdapter()
    answers = check_example_answers(
        validate_examples([_asserted(sql, 1.0)], contract), adapter=adapter
    )
    assert answers.results[0].status == "unassertable"
    assert adapter.calls == []


@pytest.mark.parametrize("name", sorted(_TIME_FUNC_NAMES))
def test_every_time_func_name_is_detected_with_a_precision_argument(
    contract: DataContract, name: str
) -> None:
    # The sibling test above only ever renders NAME(). MySQL's fractional-
    # seconds forms — NOW(3), SYSDATE(6) — are the same clock read with a
    # precision spec, and NOW/SYSDATE are exactly the names that reach only the
    # Anonymous arm, so a zero-arg-only guard silently let them through.
    sql = (
        "SELECT SUM(amount) FROM analytics.orders "
        f"WHERE tenant_id = 'acme' AND created_at >= {name.upper()}(3)"
    )
    adapter = SpyAdapter()
    answers = check_example_answers(
        validate_examples([_asserted(sql, 1.0)], contract), adapter=adapter
    )
    assert answers.results[0].status == "unassertable"
    assert adapter.calls == []


@pytest.mark.parametrize("dialect", ["mysql", "duckdb"])
@pytest.mark.parametrize("fragment", ["NOW(3)", "SYSDATE(6)"])
def test_fractional_second_clock_reads_are_detected(
    contract: DataContract, dialect: str, fragment: str
) -> None:
    # Regression: the arity guard added for UNIX_TIMESTAMP(created_at)
    # originally disqualified ANY argument, which let these through.
    sql = (
        "SELECT SUM(amount) FROM analytics.orders "
        f"WHERE tenant_id = 'acme' AND created_at >= {fragment}"
    )
    adapter = SpyAdapter()
    answers = check_example_answers(
        validate_examples([_asserted(sql, 1.0)], contract),
        adapter=adapter,
        dialect=dialect,
    )
    assert answers.results[0].status == "unassertable"
    assert adapter.calls == []


@pytest.mark.parametrize(
    "row",
    [
        {"sql": "SELECT 1", "rel_tol": 0.01},
        {"sql": "SELECT 1", "abs_tol": 1.0},
        {"sql": "SELECT 1", "time_scoped": True},
    ],
)
def test_from_dict_rejects_tolerances_without_an_expected(row: dict) -> None:
    # A typo'd `expcted:` lands in metadata by design, so the row silently
    # stops being an assertion and no gate can see the loss. The orphaned
    # tolerance is the evidence of intent — fail on it.
    with pytest.raises(ValueError, match="without 'expected'"):
        VerifiedExample.from_dict(row)


def test_from_dict_allows_tolerances_alongside_an_expected() -> None:
    ex = VerifiedExample.from_dict(
        {"sql": "SELECT 1", "expected": 10.0, "rel_tol": 0.01, "time_scoped": True}
    )
    assert (ex.expected, ex.rel_tol, ex.time_scoped) == (10.0, 0.01, True)


@pytest.mark.parametrize(
    "kwargs",
    [
        {"rel_tol": 0.01},
        {"abs_tol": 1.0},
        {"time_scoped": True},
    ],
)
def test_direct_construction_rejects_tolerances_without_an_expected(
    kwargs: dict,
) -> None:
    # from_dict is not the only door: a loader that builds records directly
    # must hit the same orphan check, or the invariant belongs to one
    # constructor rather than to the record.
    with pytest.raises(ValueError, match="without 'expected'"):
        VerifiedExample(sql="SELECT 1", **kwargs)


@pytest.mark.parametrize("bad", [float("nan"), float("inf"), True, "10"])
def test_direct_construction_rejects_a_non_numeric_expected(bad: object) -> None:
    # A nan `expected` is the one that fails silently if unchecked: nan <=
    # threshold is False, so the row reports a permanent mismatch with nan
    # diffs instead of naming itself at load time.
    with pytest.raises(ValueError, match="'expected' must be"):
        VerifiedExample(sql="SELECT 1", expected=bad)  # ty: ignore[invalid-argument-type]


def test_direct_construction_coerces_an_integer_expected() -> None:
    assert VerifiedExample(sql="SELECT 1", expected=10).expected == 10.0


def test_unix_timestamp_with_an_argument_is_not_flagged(
    contract: DataContract,
) -> None:
    # Important 2 regression: UNIX_TIMESTAMP(created_at) is MySQL/Spark's
    # deterministic datetime-to-epoch conversion, not a clock read. Its
    # argument is a COLUMN, which is what distinguishes it from NOW(3)'s
    # integer precision literal — the arity alone would not.
    sql = (
        "SELECT SUM(amount) FROM analytics.orders "
        "WHERE tenant_id = 'acme' AND UNIX_TIMESTAMP(created_at) > 100"
    )
    adapter = SpyAdapter({sql: 1.0})
    answers = check_example_answers(
        validate_examples([_asserted(sql, 1.0)], contract), adapter=adapter
    )
    assert answers.results[0].status == "match"
    assert adapter.calls == [sql]


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


def test_expects_metrics_defaults_to_empty() -> None:
    assert VerifiedExample(sql="SELECT 1").expects_metrics == []


def test_expects_metrics_round_trips_through_from_dict() -> None:
    ex = VerifiedExample.from_dict({"sql": "SELECT 1", "expects_metrics": ["CAC"]})
    assert ex.expects_metrics == ["CAC"]


def test_expects_metrics_is_not_duplicated_into_metadata() -> None:
    """_KNOWN_KEYS must list it, or from_dict copies it into metadata too."""
    ex = VerifiedExample.from_dict({"sql": "SELECT 1", "expects_metrics": ["CAC"]})
    assert "expects_metrics" not in ex.metadata


def test_expects_metrics_needs_no_expected() -> None:
    """A protocol-only row may declare it; it is not an orphaned assertion key."""
    ex = VerifiedExample(sql="SELECT 1", expects_metrics=["CAC"])
    assert ex.expected is None


def test_malformed_expects_metrics_is_rejected() -> None:
    with pytest.raises(ValueError, match="expects_metrics"):
        VerifiedExample(sql="SELECT 1", expects_metrics="CAC")  # ty: ignore[invalid-argument-type]
    with pytest.raises(ValueError, match="expects_metrics"):
        VerifiedExample(sql="SELECT 1", expects_metrics=[""])


class TestExpectedRowsLoading:
    def test_a_breakdown_row_loads(self) -> None:
        example = VerifiedExample(
            sql="SELECT region, SUM(amount) FROM analytics.orders GROUP BY region",
            expected_rows=[["EMEA", 5000.0], ["APAC", 3000.0]],
        )
        assert example.expected_rows == [["EMEA", 5000.0], ["APAC", 3000.0]]
        assert example.ordered is False

    def test_from_dict_reads_both_new_keys(self) -> None:
        example = VerifiedExample.from_dict(
            {"sql": "SELECT 1", "expected_rows": [["EMEA", 1.0]], "ordered": True}
        )
        assert example.ordered is True
        assert "expected_rows" not in example.metadata  # not swallowed as metadata

    def test_declaring_both_assertions_raises(self) -> None:
        with pytest.raises(ValueError, match="expected.*expected_rows"):
            VerifiedExample(sql="SELECT 1", expected=1.0, expected_rows=[["EMEA", 1.0]])

    def test_an_empty_list_raises(self) -> None:
        # "Returns nothing" is certifiable as `SELECT COUNT(*) ... expected: 0`,
        # so the empty list carries no capability and stays evidence of a typo.
        with pytest.raises(ValueError, match="non-empty"):
            VerifiedExample(sql="SELECT 1", expected_rows=[])

    def test_ragged_rows_raise(self) -> None:
        with pytest.raises(ValueError, match="same number of cells"):
            VerifiedExample(sql="SELECT 1", expected_rows=[["EMEA", 1.0], ["APAC"]])

    def test_rows_disagreeing_on_the_key_partition_raise(self) -> None:
        with pytest.raises(ValueError, match="same columns"):
            VerifiedExample(sql="SELECT 1", expected_rows=[["EMEA", 1.0], [2.0, 3.0]])

    def test_an_all_numeric_row_raises_and_names_ordered(self) -> None:
        with pytest.raises(ValueError, match="ordered: true"):
            VerifiedExample(sql="SELECT 1", expected_rows=[[2025, 5000.0]])

    def test_an_all_numeric_row_is_fine_when_ordered(self) -> None:
        example = VerifiedExample(
            sql="SELECT 1", expected_rows=[[2025, 5000.0]], ordered=True
        )
        assert example.ordered is True

    def test_duplicate_keys_raise_when_unordered(self) -> None:
        with pytest.raises(ValueError, match="twice"):
            VerifiedExample(
                sql="SELECT 1", expected_rows=[["EMEA", 1.0], ["EMEA", 2.0]]
            )

    def test_duplicate_keys_are_legitimate_when_ordered(self) -> None:
        # Position is identity under `ordered`; a ranking may name one
        # category twice.
        example = VerifiedExample(
            sql="SELECT 1", expected_rows=[["EMEA", 1.0], ["EMEA", 2.0]], ordered=True
        )
        assert len(example.expected_rows or []) == 2

    def test_ordered_without_an_assertion_is_an_orphan(self) -> None:
        with pytest.raises(ValueError, match="ordered"):
            VerifiedExample(sql="SELECT 1", ordered=True)

    def test_a_tolerance_beside_expected_rows_is_not_an_orphan(self) -> None:
        # The orphan guard keyed on `expected is None`; with a second
        # assertion field that would fire on every valid breakdown row.
        example = VerifiedExample(
            sql="SELECT 1", expected_rows=[["EMEA", 1.0]], abs_tol=0.5
        )
        assert example.abs_tol == 0.5


class TestBreakdownRendering:
    def _result(self, differences: list[str]) -> ExampleAnswerResult:
        return ExampleAnswerResult(
            example=VerifiedExample(sql="SELECT 1", expected_rows=[["EMEA", 1.0]]),
            status="mismatch",
            expected_rows=[["EMEA", 1.0]],
            actual_row_count=3,
            row_differences=differences,
            label="revenue-by-region",
        )

    def test_a_breakdown_mismatch_names_its_differences(self) -> None:
        report = ExampleAnswerReport(results=[self._result(["missing group APAC"])])
        assert "missing group APAC" in report.summary()
        assert "revenue-by-region" in report.summary()

    def test_only_the_first_three_differences_are_named(self) -> None:
        report = ExampleAnswerReport(
            results=[self._result([f"missing group G{i}" for i in range(6)])]
        )
        summary = report.summary()
        assert "G0" in summary and "G2" in summary
        assert "G3" not in summary
        assert "and 3 more" in summary

    def test_the_empty_report_message_mentions_both_assertion_fields(self) -> None:
        assert "expected_rows" in ExampleAnswerReport(results=[]).summary()


_BREAKDOWN_SQL = (
    "SELECT region, SUM(amount) FROM analytics.orders "
    "WHERE tenant_id = 'acme' GROUP BY region"
)


def _breakdown(rows: list[tuple]) -> QueryResult:
    return QueryResult(columns=["region", "revenue"], rows=rows)


def _asserted_rows(sql: str, expected_rows: list[list], **kw: Any) -> VerifiedExample:
    return VerifiedExample(sql=sql, expected_rows=expected_rows, **kw)


_CERTIFIED = [["EMEA", 5000.0], ["APAC", 3000.0]]


class TestBreakdownAnswerChecks:
    def test_a_correct_breakdown_is_a_match(self, contract: DataContract) -> None:
        adapter = SpyAdapter(
            {_BREAKDOWN_SQL: _breakdown([("APAC", 3000.0), ("EMEA", 5000.0)])}
        )
        report = validate_examples(
            [_asserted_rows(_BREAKDOWN_SQL, _CERTIFIED)], contract
        )
        answers = check_example_answers(report, adapter=adapter)
        assert [r.status for r in answers.results] == ["match"]
        assert answers.ok

    def test_the_right_total_with_the_wrong_split_is_a_mismatch(
        self, contract: DataContract
    ) -> None:
        adapter = SpyAdapter(
            {_BREAKDOWN_SQL: _breakdown([("EMEA", 5200.0), ("APAC", 2800.0)])}
        )
        report = validate_examples(
            [_asserted_rows(_BREAKDOWN_SQL, _CERTIFIED)], contract
        )
        answers = check_example_answers(report, adapter=adapter)
        assert [r.status for r in answers.results] == ["mismatch"]
        assert answers.results[0].row_differences

    def test_a_dropped_group_is_a_mismatch(self, contract: DataContract) -> None:
        adapter = SpyAdapter({_BREAKDOWN_SQL: _breakdown([("EMEA", 5000.0)])})
        report = validate_examples(
            [_asserted_rows(_BREAKDOWN_SQL, _CERTIFIED)], contract
        )
        answers = check_example_answers(report, adapter=adapter)
        assert answers.results[0].status == "mismatch"
        assert any("APAC" in d for d in answers.results[0].row_differences)

    def test_a_structural_fault_is_an_error_not_a_mismatch(
        self, contract: DataContract
    ) -> None:
        adapter = SpyAdapter(
            {_BREAKDOWN_SQL: QueryResult(columns=["region"], rows=[("EMEA",)])}
        )
        report = validate_examples(
            [_asserted_rows(_BREAKDOWN_SQL, _CERTIFIED)], contract
        )
        answers = check_example_answers(report, adapter=adapter)
        assert answers.results[0].status == "error"

    def test_a_rolling_window_is_unassertable_and_never_executed(
        self, contract: DataContract
    ) -> None:
        sql = (
            "SELECT region, SUM(amount) FROM analytics.orders "
            "WHERE tenant_id = 'acme' AND created_at >= CURRENT_DATE - 30 "
            "GROUP BY region"
        )
        adapter = SpyAdapter({})
        report = validate_examples([_asserted_rows(sql, _CERTIFIED)], contract)
        answers = check_example_answers(report, adapter=adapter)
        assert answers.results[0].status == "unassertable"
        assert adapter.calls == []
