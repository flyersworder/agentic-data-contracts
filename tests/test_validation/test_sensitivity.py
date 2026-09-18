"""Sensitivity checks: rewrite mechanics, result types, check_sensitivity."""

from __future__ import annotations

import pytest
import sqlglot

from agentic_data_contracts.adapters.duckdb import DuckDBAdapter
from agentic_data_contracts.core.contract import DataContract
from agentic_data_contracts.semantic.base import (
    MetricDefinition,
    SensitivityProperty,
    Shadow,
)
from agentic_data_contracts.validation.sensitivity import (
    SensitivityReport,
    SensitivityResult,
    _free_alias,
    _norm,
    _Refused,
    _rewrite,
    _shadow_tables,
    _table_refs,
    check_sensitivity,
    validate_sensitivity_tables,
)

SHADOW = Shadow(
    table="main.payments",
    sql="SELECT * REPLACE (eur_amount * 10 AS eur_amount) FROM main.payments",
)


def _rw(sql: str) -> str:
    out = _rewrite(sql, SHADOW, dialect="duckdb")
    # Every rewrite must still be SQL. The assertions below only look for
    # substrings, which a splice that broke an identifier or dropped a comma
    # would still satisfy -- this is what would notice.
    sqlglot.parse_one(out, dialect="duckdb")
    return out


class TestLocate:
    def test_counts_a_qualified_reference(self) -> None:
        assert (
            _table_refs(
                "SELECT 1 FROM main.payments", "main.payments", dialect="duckdb"
            )
            == 1
        )

    def test_counts_a_bare_reference(self) -> None:
        assert (
            _table_refs("SELECT 1 FROM payments", "main.payments", dialect="duckdb")
            == 1
        )

    def test_ignores_another_schema(self) -> None:
        assert (
            _table_refs(
                "SELECT 1 FROM other.payments", "main.payments", dialect="duckdb"
            )
            == 0
        )

    def test_ignores_a_cte_of_the_same_name(self) -> None:
        sql = "WITH payments AS (SELECT 1 x) SELECT * FROM payments"
        with pytest.raises(_Refused, match="shadowed by a CTE"):
            _table_refs(sql, "main.payments", dialect="duckdb")

    def test_unparseable_sql_is_refused(self) -> None:
        with pytest.raises(_Refused, match="unparseable"):
            _table_refs("SELECT FROM WHERE )(", "main.payments", dialect="duckdb")


class TestRewrite:
    def test_prepends_a_with_clause(self) -> None:
        out = _rw("SELECT count(*) FROM main.payments")
        assert out.startswith("WITH __sens_0 AS (")
        assert "FROM __sens_0" in out
        assert "FROM main.payments)" in out  # inside the shadow, untouched

    def test_rewrites_a_bare_reference_and_keeps_its_alias(self) -> None:
        out = _rw("SELECT count(*) FROM payments p WHERE p.eur_amount > 10")
        assert "FROM __sens_0 p" in out

    def test_splices_into_an_existing_with(self) -> None:
        sql = "WITH v AS (SELECT 1 FROM payments) SELECT * FROM v"
        out = _rw(sql)
        assert out.startswith("WITH __sens_0 AS (")
        assert "v AS (SELECT 1 FROM __sens_0)" in out

    def test_splices_after_with_recursive(self) -> None:
        sql = (
            "WITH RECURSIVE r(n) AS (SELECT 1) "
            "SELECT (SELECT count(*) FROM payments) FROM r"
        )
        out = _rw(sql)
        assert out.startswith("WITH RECURSIVE __sens_0 AS (")
        assert "FROM __sens_0) FROM r" in out

    def test_rewrites_a_reference_inside_a_correlated_subquery(self) -> None:
        sql = (
            "SELECT (SELECT count(*) FROM payments x WHERE x.merchant=f.merchant) "
            "FROM main.fees f"
        )
        out = _rw(sql)
        assert "FROM __sens_0 x" in out

    def test_leaves_a_string_literal_alone(self) -> None:
        out = _rw("SELECT 'payments' AS label, count(*) FROM main.payments")
        assert "'payments' AS label" in out
        assert "FROM __sens_0" in out

    def test_leaves_a_column_alias_alone(self) -> None:
        # The count guard's false-positive class, measured on the DABStep
        # corpus: four stored queries write this shape and were refused.
        sql = "SELECT count(DISTINCT psp_reference) AS payments FROM main.payments"
        out = _rw(sql)
        assert "AS payments" in out
        assert "FROM __sens_0" in out

    def test_not_applicable_when_the_target_is_absent(self) -> None:
        with pytest.raises(_Refused, match="not_applicable"):
            _rw("SELECT * FROM main.fees")

    def test_alias_collision_picks_the_next_free_name(self) -> None:
        assert _free_alias("SELECT 1") == "__sens_0"
        assert _free_alias("SELECT __sens_0 FROM t") == "__sens_1"
        assert _free_alias("SELECT __sens_0, __sens_1 FROM t") == "__sens_2"


class TestNormalise:
    def test_orders_and_rounds(self) -> None:
        assert _norm([(2, 1.000000001), (1, 2.0)]) == _norm([(1, 2.0), (2, 1.0)])

    def test_distinguishes_different_values(self) -> None:
        assert _norm([(1,)]) != _norm([(2,)])


def _res(status: str, name: str = "p") -> SensitivityResult:
    return SensitivityResult(name=name, metric="m", status=status, expected="unchanged")


class TestReport:
    def test_empty_report_is_ok(self) -> None:
        # Silence is the honest answer: nothing was claimed, nothing checked.
        assert SensitivityReport(results=()).ok is True

    def test_all_pass_is_ok(self) -> None:
        assert SensitivityReport(results=(_res("pass"),)).ok is True

    def test_not_applicable_does_not_block(self) -> None:
        report = SensitivityReport(results=(_res("pass"), _res("not_applicable", "q")))
        assert report.ok is True
        assert len(report.not_applicable) == 1

    def test_violation_blocks(self) -> None:
        report = SensitivityReport(results=(_res("pass"), _res("violation", "q")))
        assert report.ok is False
        assert [r.name for r in report.violations] == ["q"]

    def test_unchecked_blocks(self) -> None:
        # "no verdict was possible" must not read as "passed".
        report = SensitivityReport(results=(_res("unchecked"),))
        assert report.ok is False
        assert len(report.unchecked) == 1

    def test_summary_names_every_status_present(self) -> None:
        report = SensitivityReport(
            results=(_res("pass"), _res("violation", "q"), _res("unchecked", "r"))
        )
        text = report.summary()
        assert "violation" in text and "unchecked" in text and "q" in text


MKT_SHADOW = Shadow(
    table="mkt.touchpoints",
    sql=(
        "SELECT * FROM mkt.touchpoints UNION ALL "
        "SELECT lead_id, 'display', qualified_date + 1 FROM mkt.lead_scores"
    ),
)

# A dangling QUALIFY with no predicate is not valid SQL in any dialect sqlglot
# knows; `sqlglot.parse_one` raises `ParseError` on it, which is exactly the
# `_shadow_tables` -> None path this file's governance-hole tests exercise.
UNPARSEABLE_SHADOW = Shadow(
    table="mkt.touchpoints",
    sql="SELECT * FROM secret.t QUALIFY",
)

FAN_OUT = """
SELECT t.channel, COUNT(DISTINCT l.lead_id) AS mqls
FROM mkt.lead_scores l JOIN mkt.touchpoints t USING (lead_id)
WHERE l.is_mql GROUP BY 1 ORDER BY 1
"""

FIRST_TOUCH = """
WITH first_touch AS (
  SELECT lead_id, channel FROM (
    SELECT lead_id, channel,
           row_number() OVER (PARTITION BY lead_id ORDER BY touch_date) rn
    FROM mkt.touchpoints) WHERE rn = 1)
SELECT f.channel, COUNT(DISTINCT l.lead_id) AS mqls
FROM mkt.lead_scores l JOIN first_touch f USING (lead_id)
WHERE l.is_mql GROUP BY 1 ORDER BY 1
"""


@pytest.fixture
def mkt_adapter() -> DuckDBAdapter:
    adapter = DuckDBAdapter(":memory:")
    adapter.execute("CREATE SCHEMA mkt")
    adapter.execute(
        "CREATE TABLE mkt.lead_scores"
        "(lead_id INT, region TEXT, is_mql BOOL, qualified_date INT)"
    )
    adapter.execute(
        "CREATE TABLE mkt.touchpoints(lead_id INT, channel TEXT, touch_date INT)"
    )
    adapter.execute(
        "INSERT INTO mkt.lead_scores VALUES "
        "(1,'DACH',true,100),(2,'DACH',true,120),(3,'APAC',true,130),"
        "(4,'DACH',false,140)"
    )
    adapter.execute(
        "INSERT INTO mkt.touchpoints VALUES "
        "(1,'search',10),(1,'email',50),(2,'social',20),(2,'search',60),"
        "(3,'search',30),(4,'email',40)"
    )
    return adapter


def _contract(*tables: str) -> DataContract:
    """A minimal contract governing exactly *tables* in schema `mkt`."""
    listed = ", ".join(tables)
    return DataContract.from_yaml_string(
        f"""
version: "1.0"
name: sensitivity-test
semantic:
  allowed_tables:
    - schema: mkt
      tables: [{listed}]
  forbidden_operations: [DELETE, DROP]
  rules: []
"""
    )


@pytest.fixture
def mkt_contract() -> DataContract:
    return _contract("lead_scores", "touchpoints")


def _metric(*props: SensitivityProperty) -> MetricDefinition:
    return MetricDefinition(
        name="mql_count",
        description="MQLs, first-touch attributed",
        sql_expression="COUNT(DISTINCT lead_id)",
        sensitivity=list(props),
    )


FIRST_TOUCH_PROP = SensitivityProperty(
    name="attribution_is_first_touch",
    description="A touchpoint after qualification cannot change a first touch.",
    shadow=MKT_SHADOW,
    expect="unchanged",
)


class TestCheckSensitivity:
    def test_first_touch_query_passes(self, mkt_adapter, mkt_contract) -> None:
        report = check_sensitivity(
            _metric(FIRST_TOUCH_PROP),
            FIRST_TOUCH,
            contract=mkt_contract,
            adapter=mkt_adapter,
        )
        (result,) = report.results
        assert result.status == "pass"
        assert result.moved is False
        assert report.ok is True

    def test_fan_out_query_violates(self, mkt_adapter, mkt_contract) -> None:
        report = check_sensitivity(
            _metric(FIRST_TOUCH_PROP),
            FAN_OUT,
            contract=mkt_contract,
            adapter=mkt_adapter,
        )
        (result,) = report.results
        assert result.status == "violation"
        assert result.moved is True
        assert report.ok is False

    def test_query_not_touching_the_table_is_not_applicable(
        self, mkt_adapter, mkt_contract
    ) -> None:
        report = check_sensitivity(
            _metric(FIRST_TOUCH_PROP),
            "SELECT count(*) FROM mkt.lead_scores",
            contract=mkt_contract,
            adapter=mkt_adapter,
        )
        (result,) = report.results
        assert result.status == "not_applicable"
        assert result.moved is None
        assert report.ok is True

    def test_metric_with_no_properties_yields_an_empty_ok_report(
        self, mkt_adapter, mkt_contract
    ) -> None:
        report = check_sensitivity(
            _metric(),
            FIRST_TOUCH,
            contract=mkt_contract,
            adapter=mkt_adapter,
        )
        assert report.results == ()
        assert report.ok is True

    def test_unparseable_sql_is_unchecked(
        self, mkt_adapter, mkt_contract, monkeypatch
    ) -> None:
        # Security property, not just an outcome: nothing may be executed
        # against the adapter when Layer 1 could not render a verdict at all.
        # Skipping the policy raise on a parse error is only safe because
        # `check_sensitivity` falls through to a report instead of running
        # anything -- this is what proves it.
        seen: list[str] = []
        original = mkt_adapter.execute

        def spy(sql: str):
            seen.append(sql)
            return original(sql)

        monkeypatch.setattr(mkt_adapter, "execute", spy)
        report = check_sensitivity(
            _metric(FIRST_TOUCH_PROP),
            "SELECT FROM mkt.touchpoints )(",
            contract=mkt_contract,
            adapter=mkt_adapter,
        )
        (result,) = report.results
        assert result.status == "unchecked"
        assert report.ok is False
        assert seen == []

    def test_engine_error_is_unchecked(self, mkt_adapter, mkt_contract) -> None:
        report = check_sensitivity(
            _metric(FIRST_TOUCH_PROP),
            "SELECT no_such_column FROM mkt.touchpoints",
            contract=mkt_contract,
            adapter=mkt_adapter,
        )
        (result,) = report.results
        assert result.status == "unchecked"

    def test_vacuous_changes_test_is_unchecked(self, mkt_adapter, mkt_contract) -> None:
        # An empty answer cannot move, so `expect: changes` asserts nothing.
        prop = SensitivityProperty(
            name="must_move",
            description="the answer must respond",
            shadow=MKT_SHADOW,
            expect="changes",
        )
        report = check_sensitivity(
            _metric(prop),
            "SELECT channel FROM mkt.touchpoints WHERE false",
            contract=mkt_contract,
            adapter=mkt_adapter,
        )
        (result,) = report.results
        assert result.status == "unchecked"
        assert "vacuous" in result.reason

    def test_properties_selects_a_subset(self, mkt_adapter, mkt_contract) -> None:
        second = SensitivityProperty(
            name="other",
            description="another claim",
            shadow=MKT_SHADOW,
            expect="changes",
        )
        report = check_sensitivity(
            _metric(FIRST_TOUCH_PROP, second),
            FIRST_TOUCH,
            contract=mkt_contract,
            adapter=mkt_adapter,
            properties=["other"],
        )
        assert [r.name for r in report.results] == ["other"]

    def test_unknown_property_name_raises(self, mkt_adapter, mkt_contract) -> None:
        with pytest.raises(ValueError, match="nope"):
            check_sensitivity(
                _metric(FIRST_TOUCH_PROP),
                FIRST_TOUCH,
                contract=mkt_contract,
                adapter=mkt_adapter,
                properties=["nope"],
            )

    def test_base_is_executed_once_and_reused(
        self, mkt_adapter, mkt_contract, monkeypatch
    ) -> None:
        # Two base executions per call plus one per property -- not three per
        # property. With two properties that is 2 + 2 = 4, not 6.
        seen: list[str] = []
        original = mkt_adapter.execute

        def spy(sql: str):
            seen.append(sql)
            return original(sql)

        monkeypatch.setattr(mkt_adapter, "execute", spy)
        second = SensitivityProperty(
            name="other",
            description="another claim",
            shadow=MKT_SHADOW,
            expect="unchanged",
        )
        check_sensitivity(
            _metric(FIRST_TOUCH_PROP, second),
            FIRST_TOUCH,
            contract=mkt_contract,
            adapter=mkt_adapter,
        )
        assert len(seen) == 4
        assert sum(1 for s in seen if "__sens_" not in s) == 2

    def test_unparseable_shadow_is_unchecked(
        self, mkt_adapter, mkt_contract, monkeypatch
    ) -> None:
        # A shadow sqlglot cannot parse must be refused a verdict, not treated
        # as reading no tables: `_shadow_tables` returns None here (see
        # test_shadow_tables_returns_none_for_an_unparseable_shadow, which
        # probes the same string), and that must never be silently trusted as
        # "reads nothing" -- it has to fail closed, executing nothing.
        seen: list[str] = []
        original = mkt_adapter.execute

        def spy(sql: str):
            seen.append(sql)
            return original(sql)

        monkeypatch.setattr(mkt_adapter, "execute", spy)
        prop = SensitivityProperty(
            name="broken_shadow",
            description="a shadow sqlglot cannot parse",
            shadow=UNPARSEABLE_SHADOW,
            expect="unchanged",
        )
        report = check_sensitivity(
            _metric(prop),
            FIRST_TOUCH,
            contract=mkt_contract,
            adapter=mkt_adapter,
        )
        (result,) = report.results
        assert result.status == "unchecked"
        assert result.reason.startswith("unparseable shadow")
        assert seen == []

    def test_shadow_tables_returns_none_for_an_unparseable_shadow(self) -> None:
        # The probe behind the test above: confirms UNPARSEABLE_SHADOW really
        # exercises the None-from-_shadow_tables path and isn't accidentally
        # valid SQL that happens to read no governed table.
        assert _shadow_tables(UNPARSEABLE_SHADOW, dialect="duckdb") is None


class TestPolicy:
    def test_query_blocked_by_layer_one_raises(self, mkt_adapter) -> None:
        # Without this, check_sensitivity is an entry point that executes
        # arbitrary SQL against the adapter with none of the checks every other
        # path applies.
        contract = _contract("other")
        with pytest.raises(ValueError, match="blocked"):
            check_sensitivity(
                _metric(FIRST_TOUCH_PROP),
                FIRST_TOUCH,
                contract=contract,
                adapter=mkt_adapter,
            )

    def test_multiple_statements_raise_and_execute_nothing(
        self, mkt_adapter, mkt_contract, monkeypatch
    ) -> None:
        # A forbidden DELETE smuggled behind a SELECT is a policy block, not an
        # unparseable query: it must raise, and nothing may reach the adapter.
        seen: list[str] = []
        original = mkt_adapter.execute

        def spy(sql: str):
            seen.append(sql)
            return original(sql)

        monkeypatch.setattr(mkt_adapter, "execute", spy)
        with pytest.raises(ValueError, match="multiple statements"):
            check_sensitivity(
                _metric(FIRST_TOUCH_PROP),
                "SELECT count(*) FROM mkt.touchpoints; DELETE FROM mkt.touchpoints",
                contract=mkt_contract,
                adapter=mkt_adapter,
            )
        assert seen == []

    def test_shadow_reading_an_ungoverned_table_raises(self, mkt_adapter) -> None:
        # The shadow reads mkt.lead_scores, which this contract does not govern.
        contract = _contract("touchpoints")
        with pytest.raises(ValueError, match="lead_scores"):
            check_sensitivity(
                _metric(FIRST_TOUCH_PROP),
                "SELECT count(*) FROM mkt.touchpoints",
                contract=contract,
                adapter=mkt_adapter,
            )

    def test_validate_sensitivity_tables_reports_the_same_problem(self) -> None:
        contract = _contract("touchpoints")
        problems = validate_sensitivity_tables(contract, [_metric(FIRST_TOUCH_PROP)])
        assert len(problems) == 1
        assert "lead_scores" in problems[0]

    def test_validate_sensitivity_tables_is_silent_when_governed(self) -> None:
        contract = _contract("touchpoints", "lead_scores")
        assert validate_sensitivity_tables(contract, [_metric(FIRST_TOUCH_PROP)]) == []

    def test_validate_sensitivity_tables_reports_an_unparseable_shadow(self) -> None:
        # Governed or not is undecidable when the shadow doesn't parse -- the
        # CI gate must flag that as a problem too, not stay silent about it.
        contract = _contract("touchpoints", "lead_scores")
        prop = SensitivityProperty(
            name="broken_shadow",
            description="a shadow sqlglot cannot parse",
            shadow=UNPARSEABLE_SHADOW,
            expect="unchanged",
        )
        problems = validate_sensitivity_tables(contract, [_metric(prop)])
        assert len(problems) == 1
        assert "broken_shadow" in problems[0]
