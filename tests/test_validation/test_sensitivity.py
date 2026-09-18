"""Sensitivity checks: rewrite mechanics, result types, check_sensitivity."""

from __future__ import annotations

import re

import pytest
import sqlglot

from agentic_data_contracts.adapters.base import QueryResult
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
    _identity,
    _norm,
    _prove_edit,
    _prove_parses,
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

#: VQL's CONTEXT clause: sqlglot cannot parse it, but it tokenizes, and Denodo
#: accepts it. Stripping it is the smallest faithful stand-in for a Denodo
#: normalizer -- syntax only, table names untouched.
_CONTEXT = re.compile(r"\s*\bCONTEXT\s*\([^)]*\)", re.IGNORECASE)
_CTX = " CONTEXT ('i18n' = 'us_est')"


def _strip_context(sql: str) -> str:
    return _CONTEXT.sub("", sql)


class _VqlNormalizer:
    """A standalone SqlNormalizer -- the CI gate takes no adapter."""

    def normalize_sql(self, sql: str) -> str:
        return _strip_context(sql)


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
        assert out.startswith("WITH sens_shadow_0 AS (")
        assert "FROM sens_shadow_0" in out
        assert "FROM main.payments\n)" in out  # inside the shadow, untouched

    def test_rewrites_a_bare_reference_and_keeps_its_alias(self) -> None:
        out = _rw("SELECT count(*) FROM payments p WHERE p.eur_amount > 10")
        assert "FROM sens_shadow_0 p" in out

    def test_splices_into_an_existing_with(self) -> None:
        sql = "WITH v AS (SELECT 1 FROM payments) SELECT * FROM v"
        out = _rw(sql)
        assert out.startswith("WITH sens_shadow_0 AS (")
        assert "v AS (SELECT 1 FROM sens_shadow_0)" in out

    def test_splices_after_with_recursive(self) -> None:
        sql = (
            "WITH RECURSIVE r(n) AS (SELECT 1) "
            "SELECT (SELECT count(*) FROM payments) FROM r"
        )
        out = _rw(sql)
        assert out.startswith("WITH RECURSIVE sens_shadow_0 AS (")
        assert "FROM sens_shadow_0) FROM r" in out

    def test_rewrites_a_reference_inside_a_correlated_subquery(self) -> None:
        sql = (
            "SELECT (SELECT count(*) FROM payments x WHERE x.merchant=f.merchant) "
            "FROM main.fees f"
        )
        out = _rw(sql)
        assert "FROM sens_shadow_0 x" in out

    def test_leaves_a_string_literal_alone(self) -> None:
        out = _rw("SELECT 'payments' AS label, count(*) FROM main.payments")
        assert "'payments' AS label" in out
        assert "FROM sens_shadow_0" in out

    def test_leaves_a_column_alias_alone(self) -> None:
        # The count guard's false-positive class, measured on the DABStep
        # corpus: four stored queries write this shape and were refused.
        sql = "SELECT count(DISTINCT psp_reference) AS payments FROM main.payments"
        out = _rw(sql)
        assert "AS payments" in out
        assert "FROM sens_shadow_0" in out

    def test_not_applicable_when_the_target_is_absent(self) -> None:
        with pytest.raises(_Refused, match="not_applicable"):
            _rw("SELECT * FROM main.fees")

    def test_a_trailing_line_comment_in_the_shadow_keeps_the_cte_closed(
        self,
    ) -> None:
        # Unguarded, the comment runs to the end of the line and swallows the
        # CTE's closing paren, so the rewrite no longer parses.
        shadow = Shadow(
            table="main.payments",
            sql="SELECT * FROM main.payments -- the shadow's own note",
        )
        out = _rewrite("SELECT count(*) FROM main.payments", shadow, dialect="duckdb")
        sqlglot.parse_one(out, dialect="duckdb")
        assert "FROM sens_shadow_0" in out

    def test_alias_collision_picks_the_next_free_name(self) -> None:
        assert _free_alias("SELECT 1") == "sens_shadow_0"
        assert _free_alias("SELECT sens_shadow_0 FROM t") == "sens_shadow_1"
        assert (
            _free_alias("SELECT sens_shadow_0, sens_shadow_1 FROM t") == "sens_shadow_2"
        )


class TestNormalizedRewrite:
    """References are COUNTED in the normalized text and LOCATED in the original."""

    def test_edits_the_original_when_only_the_normalized_text_parses(self) -> None:
        sql = f"SELECT count(*) FROM mkt.touchpoints{_CTX}"
        with pytest.raises(sqlglot.errors.ParseError):
            sqlglot.parse_one(sql, dialect="duckdb")
        out = _rewrite(sql, MKT_SHADOW, dialect="duckdb", normalize=_strip_context)
        assert out.startswith("WITH sens_shadow_0 AS (")
        assert "FROM sens_shadow_0 CONTEXT ('i18n' = 'us_est')" in out  # original kept
        sqlglot.parse_one(_strip_context(out), dialect="duckdb")

    def test_absent_from_both_texts_is_not_applicable(self) -> None:
        with pytest.raises(_Refused, match="not_applicable"):
            _rewrite(
                f"SELECT count(*) FROM mkt.lead_scores{_CTX}",
                MKT_SHADOW,
                dialect="duckdb",
                normalize=_strip_context,
            )

    def test_a_renaming_normalizer_trips_the_count_guard(self) -> None:
        # The normalized text holds zero references and the original holds one.
        # That is a disagreement, not an absence.
        with pytest.raises(_Refused, match="count guard"):
            _rewrite(
                "SELECT count(*) FROM mkt.touchpoints",
                MKT_SHADOW,
                dialect="duckdb",
                normalize=lambda s: s.replace("mkt.touchpoints", "mkt.tp"),
            )

    def test_absent_from_the_original_is_not_applicable_despite_a_lookalike(
        self,
    ) -> None:
        # Regression: `touchpoints` here is a plain column, not a table
        # reference, so the ORIGINAL text holds no span at all -- there is
        # nothing to edit. A normalizer that happens to put a look-alike span
        # in table position in the NORMALIZED text (wrapping it in parens
        # puts it right after an L_PAREN) must not turn that into a rewrite:
        # `refs` from the normalized AST is still 0, since it is a column
        # expression, not an `exp.Table` node, and the original itself is
        # absent, so this must refuse as not_applicable -- never proceed with
        # zero edits.
        with pytest.raises(_Refused, match="not_applicable"):
            _rewrite(
                "SELECT touchpoints FROM mkt.lead_scores",
                MKT_SHADOW,
                dialect="duckdb",
                normalize=lambda s: s.replace(
                    "SELECT touchpoints", "SELECT (touchpoints)"
                ),
            )

    def test_an_untokenizable_original_is_refused(self) -> None:
        # Normalizes to parseable SQL, but the original's unterminated literal
        # cannot be tokenized -- so its spans cannot be found.
        with pytest.raises(_Refused, match="could not be tokenized"):
            _rewrite(
                "SELECT 'x FROM mkt.touchpoints",
                MKT_SHADOW,
                dialect="duckdb",
                normalize=lambda s: "SELECT 1 FROM mkt.touchpoints",
            )

    def test_a_normalizer_that_raises_is_refused(self) -> None:
        def boom(sql: str) -> str:
            raise RuntimeError("no VQL today")

        with pytest.raises(_Refused, match="normalizer failed"):
            _rewrite(
                "SELECT count(*) FROM mkt.touchpoints",
                MKT_SHADOW,
                dialect="duckdb",
                normalize=boom,
            )

    def test_a_bare_name_column_is_not_applicable_with_no_normalizer(self) -> None:
        # Regression: `_spans` cannot tell a column that shares the target's
        # bare name from an actual table reference -- it puts a span in table
        # position for both -- and with no normalizer a renamed target is
        # impossible, so this must read as not_applicable, not the count
        # guard that a real disagreement trips.
        for sql in (
            "SELECT lead_id, touchpoints FROM mkt.lead_scores",
            "SELECT count(touchpoints) FROM mkt.lead_scores",
            "SELECT lead_id FROM mkt.lead_scores ORDER BY 1, touchpoints",
        ):
            with pytest.raises(_Refused, match="not_applicable"):
                _rewrite(sql, MKT_SHADOW, dialect="duckdb")

    def test_a_normalizer_returning_none_is_refused(self) -> None:
        # `_normalized` must refuse a non-str result the same way it refuses
        # a raising one, so the rewrite and proof paths fail closed
        # identically instead of letting a TypeError escape.
        with pytest.raises(_Refused, match="normalizer failed.*not str"):
            _rewrite(
                "SELECT count(*) FROM mkt.touchpoints",
                MKT_SHADOW,
                dialect="duckdb",
                normalize=lambda s: None,  # ty: ignore[invalid-argument-type]
            )


class TestProof:
    """The edit is proved, not trusted. Tested directly, since a count that
    agrees while the wrong span was edited is hard to reach naturally."""

    def test_refuses_a_body_that_still_references_the_target(self) -> None:
        with pytest.raises(_Refused, match="remain after the edit"):
            _prove_edit(
                "SELECT 1 FROM main.payments",
                "main.payments",
                dialect="duckdb",
                normalize=_identity,
            )

    def test_accepts_a_body_with_no_reference_left(self) -> None:
        _prove_edit(
            "SELECT 1 FROM sens_shadow_0",
            "main.payments",
            dialect="duckdb",
            normalize=_identity,
        )

    def test_refuses_when_the_normalizer_fails_on_the_body(self) -> None:
        def boom(sql: str) -> str:
            raise RuntimeError("no")

        with pytest.raises(_Refused, match="could not be proved"):
            _prove_edit(
                "SELECT 1 FROM sens_shadow_0",
                "main.payments",
                dialect="duckdb",
                normalize=boom,
            )

    def test_refuses_a_final_text_of_two_statements(self) -> None:
        with pytest.raises(_Refused, match="could not be proved"):
            _prove_parses("SELECT 1; SELECT 2", dialect="duckdb", normalize=_identity)

    def test_refuses_a_final_text_that_does_not_parse(self) -> None:
        with pytest.raises(_Refused, match="could not be proved"):
            _prove_parses("SELECT FROM )(", dialect="duckdb", normalize=_identity)


class TestNormalise:
    def test_orders_and_rounds(self) -> None:
        # 1e-13 of noise on a value near 1.0 is well past the 12 significant
        # digits `_norm` keeps (relative precision, not the fixed 6 decimal
        # places absolute rounding used to apply -- see TestRelativeFloatPrecision
        # for the magnitude this changed the tolerance for).
        assert _norm([(2, 1.0 + 1e-13), (1, 2.0)]) == _norm([(1, 2.0), (2, 1.0)])

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

    def test_all_not_applicable_is_not_ok(self) -> None:
        # Properties were claimed and none could render a verdict: the query
        # read none of the shadowed tables. "A run that checked nothing must
        # not read like a run that found nothing" -- the rule check_schema_drift
        # already follows.
        report = SensitivityReport(
            results=(_res("not_applicable"), _res("not_applicable", "q"))
        )
        assert report.ok is False

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


def _load_mkt[A: DuckDBAdapter](adapter: A) -> A:
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


class VqlDuckDBAdapter(DuckDBAdapter):
    """DuckDB that accepts VQL's CONTEXT clause, standing in for Denodo.

    sqlglot cannot parse ``CONTEXT (...)``, so the library must normalize before
    it parses; the engine accepts it, so the ORIGINAL text must be what runs.
    ``normalize_sql`` makes this a ``SqlNormalizer``; ``execute`` strips the
    clause the way Denodo would simply honour it.
    """

    def normalize_sql(self, sql: str) -> str:
        return _strip_context(sql)

    def execute(self, sql: str) -> QueryResult:
        return super().execute(_strip_context(sql))


@pytest.fixture
def mkt_adapter() -> DuckDBAdapter:
    return _load_mkt(DuckDBAdapter(":memory:"))


@pytest.fixture
def vql_adapter() -> VqlDuckDBAdapter:
    return _load_mkt(VqlDuckDBAdapter(":memory:"))


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

VQL_SHADOW = Shadow(table="mkt.touchpoints", sql=MKT_SHADOW.sql + _CTX)

VQL_PROP = SensitivityProperty(
    name="attribution_is_first_touch",
    description="A touchpoint after qualification cannot change a first touch.",
    shadow=VQL_SHADOW,
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
        assert report.ok is False

    def test_a_bare_name_column_is_not_applicable(
        self, mkt_adapter, mkt_contract
    ) -> None:
        # End-to-end regression for the same bug: a column named like the
        # target must read not_applicable, not fail report.ok via the count
        # guard. The column does not exist on `lead_scores`, but it is never
        # executed -- not_applicable short-circuits before the base query.
        report = check_sensitivity(
            _metric(FIRST_TOUCH_PROP),
            "SELECT lead_id, touchpoints FROM mkt.lead_scores",
            contract=mkt_contract,
            adapter=mkt_adapter,
        )
        (result,) = report.results
        assert result.status == "not_applicable"

    def test_a_hardcoded_answer_is_not_ok(self, mkt_adapter, mkt_contract) -> None:
        # The purest form of premature materialization: the answer pasted in
        # as a literal. It reads no table, so every property is not_applicable
        # -- and before coverage was required, `ok` passed it.
        report = check_sensitivity(
            _metric(FIRST_TOUCH_PROP),
            "SELECT 'search' AS channel, 2 AS mqls",
            contract=mkt_contract,
            adapter=mkt_adapter,
        )
        assert [r.status for r in report.results] == ["not_applicable"]
        assert report.ok is False

    def test_an_explicit_empty_selection_opts_out(
        self, mkt_adapter, mkt_contract
    ) -> None:
        # A caller who knows no declared property applies to this query says so
        # with properties=[] -- a decision, not a silent pass.
        report = check_sensitivity(
            _metric(FIRST_TOUCH_PROP),
            "SELECT count(*) FROM mkt.lead_scores",
            contract=mkt_contract,
            adapter=mkt_adapter,
            properties=[],
        )
        assert report.results == ()
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
        assert result.reason.startswith("unparseable")
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
        assert sum(1 for s in seen if "sens_shadow_" not in s) == 2

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

    def test_tokenizer_error_is_unchecked_and_executes_nothing(
        self, mkt_adapter, mkt_contract, monkeypatch
    ) -> None:
        # An unterminated literal fails in sqlglot's TOKENIZER, whose error is
        # not a ParseError. It is still "Layer 1 could not read this", so it
        # takes the unparseable path: no verdict, nothing executed.
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
            expect="changes",
        )
        report = check_sensitivity(
            _metric(FIRST_TOUCH_PROP, second),
            "SELECT 'abc",
            contract=mkt_contract,
            adapter=mkt_adapter,
        )
        assert [r.status for r in report.results] == ["unchecked", "unchecked"]
        assert all(r.reason.startswith("unparseable") for r in report.results)
        assert seen == []

    def test_nondeterministic_base_is_unchecked_and_refused_once(
        self, mkt_adapter, mkt_contract, monkeypatch
    ) -> None:
        # random() differs on every execution, so the repeat filter trips. The
        # refusal is cached: the base query runs `repeats` times for the whole
        # call, not per property, and no rewrite is ever executed.
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
            expect="changes",
        )
        sql = "SELECT lead_id, random() AS r FROM mkt.touchpoints"
        report = check_sensitivity(
            _metric(FIRST_TOUCH_PROP, second),
            sql,
            contract=mkt_contract,
            adapter=mkt_adapter,
        )
        assert [r.status for r in report.results] == ["unchecked", "unchecked"]
        assert all("not deterministic" in r.reason for r in report.results)
        assert seen == [sql, sql]

    def test_engine_error_on_the_base_is_cached(
        self, mkt_adapter, mkt_contract, monkeypatch
    ) -> None:
        # A base query the engine rejects is refused once for the whole call.
        # Re-running it for every property would break the stated cost model
        # and only repeat the same error.
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
            expect="changes",
        )
        sql = "SELECT no_such_column FROM mkt.touchpoints"
        report = check_sensitivity(
            _metric(FIRST_TOUCH_PROP, second),
            sql,
            contract=mkt_contract,
            adapter=mkt_adapter,
        )
        assert [r.status for r in report.results] == ["unchecked", "unchecked"]
        assert all(r.reason.startswith("engine error") for r in report.results)
        assert seen == [sql]

    @pytest.mark.parametrize("repeats", [0, -1])
    def test_repeats_below_one_raises(
        self, mkt_adapter, mkt_contract, repeats: int
    ) -> None:
        # Fewer than one base execution would disable the determinism filter
        # silently; malformed input raises instead.
        with pytest.raises(ValueError, match="repeats"):
            check_sensitivity(
                _metric(FIRST_TOUCH_PROP),
                FIRST_TOUCH,
                contract=mkt_contract,
                adapter=mkt_adapter,
                repeats=repeats,
            )

    @pytest.mark.parametrize("broken_first", [True, False])
    def test_unparseable_shadow_never_executes_beside_a_good_one(
        self, mkt_adapter, mkt_contract, monkeypatch, broken_first: bool
    ) -> None:
        # Security property: in ONE call, the good property still earns its
        # verdict while the unparseable shadow -- whose tables were never
        # checked against the contract -- is never sent to the engine, in
        # either order.
        seen: list[str] = []
        original = mkt_adapter.execute

        def spy(sql: str):
            seen.append(sql)
            return original(sql)

        monkeypatch.setattr(mkt_adapter, "execute", spy)
        broken = SensitivityProperty(
            name="broken_shadow",
            description="a shadow sqlglot cannot parse",
            shadow=UNPARSEABLE_SHADOW,
            expect="unchanged",
        )
        props = (
            (broken, FIRST_TOUCH_PROP) if broken_first else (FIRST_TOUCH_PROP, broken)
        )
        report = check_sensitivity(
            _metric(*props),
            FIRST_TOUCH,
            contract=mkt_contract,
            adapter=mkt_adapter,
        )
        by_name = {r.name: r for r in report.results}
        assert by_name["attribution_is_first_touch"].status == "pass"
        assert by_name["broken_shadow"].status == "unchecked"
        assert seen, "the good property must have executed"
        assert not any(UNPARSEABLE_SHADOW.sql in stmt for stmt in seen)

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


class TestNormalizedShadowGovernance:
    def test_a_shadow_is_read_after_normalization(self) -> None:
        assert _shadow_tables(VQL_SHADOW, dialect="duckdb") is None  # raw: unreadable
        assert _shadow_tables(
            VQL_SHADOW, dialect="duckdb", normalize=_strip_context
        ) == {"mkt.touchpoints", "mkt.lead_scores"}

    def test_a_normalizer_failure_leaves_the_tables_unknown(self) -> None:
        def boom(sql: str) -> str:
            raise RuntimeError("no")

        assert _shadow_tables(MKT_SHADOW, dialect="duckdb", normalize=boom) is None

    def test_a_multi_statement_shadow_leaves_the_tables_unknown(self) -> None:
        # Not exploitable -- the CTE's parentheses make the `;` a syntax error --
        # but governance saw only governed tables and waved it through.
        smuggle = Shadow(
            table="mkt.touchpoints",
            sql="SELECT * FROM mkt.touchpoints; DELETE FROM mkt.touchpoints",
        )
        assert _shadow_tables(smuggle, dialect="duckdb") is None

    def test_the_ci_gate_governs_a_normalized_shadow(self) -> None:
        governed = _contract("touchpoints", "lead_scores")
        assert (
            validate_sensitivity_tables(
                governed,
                [_metric(VQL_PROP)],
                dialect="duckdb",
                sql_normalizer=_VqlNormalizer(),
            )
            == []
        )

    def test_the_ci_gate_without_a_normalizer_cannot_read_it(self) -> None:
        governed = _contract("touchpoints", "lead_scores")
        (problem,) = validate_sensitivity_tables(
            governed, [_metric(VQL_PROP)], dialect="duckdb"
        )
        assert "cannot be parsed" in problem

    def test_the_ci_gate_still_refuses_an_ungoverned_normalized_shadow(self) -> None:
        (problem,) = validate_sensitivity_tables(
            _contract("touchpoints"),
            [_metric(VQL_PROP)],
            dialect="duckdb",
            sql_normalizer=_VqlNormalizer(),
        )
        assert "lead_scores" in problem


def _spy(adapter, monkeypatch) -> list[str]:
    seen: list[str] = []
    original = adapter.execute

    def spy(sql: str):
        seen.append(sql)
        return original(sql)

    monkeypatch.setattr(adapter, "execute", spy)
    return seen


class _Renaming:
    """Syntax-only it is not: renames the target to another GOVERNED table, so
    Layer 1 and shadow governance both pass and only the count guard sees it."""

    def normalize_sql(self, sql: str) -> str:
        return sql.replace("mkt.touchpoints", "mkt.lead_scores")


class _Raising:
    def normalize_sql(self, sql: str) -> str:
        raise RuntimeError("the normalizer is down")


class _ReturnsNone:
    """A normalizer that breaks its own contract by returning a non-str."""

    def normalize_sql(self, sql: str) -> str:
        return None  # ty: ignore[invalid-return-type]


class TestNormalizedCheckSensitivity:
    def test_fan_out_violates_on_a_dialect_that_parses_only_after_normalization(
        self, vql_adapter, mkt_contract
    ) -> None:
        report = check_sensitivity(
            _metric(FIRST_TOUCH_PROP),
            FAN_OUT + _CTX,
            contract=mkt_contract,
            adapter=vql_adapter,  # a SqlNormalizer: used with no keyword
        )
        (result,) = report.results
        assert result.status == "violation"
        assert result.moved is True

    def test_first_touch_passes_on_a_dialect_that_parses_only_after_normalization(
        self, vql_adapter, mkt_contract
    ) -> None:
        report = check_sensitivity(
            _metric(FIRST_TOUCH_PROP),
            FIRST_TOUCH + _CTX,
            contract=mkt_contract,
            adapter=vql_adapter,
        )
        (result,) = report.results
        assert result.status == "pass"
        assert report.ok is True

    def test_the_original_text_executes_never_the_normalized_one(
        self, vql_adapter, mkt_contract, monkeypatch
    ) -> None:
        seen = _spy(vql_adapter, monkeypatch)
        check_sensitivity(
            _metric(FIRST_TOUCH_PROP),
            FAN_OUT + _CTX,
            contract=mkt_contract,
            adapter=vql_adapter,
        )
        assert seen, "something must have executed"
        assert all("CONTEXT" in s for s in seen)
        assert any("sens_shadow_0" in s for s in seen)

    def test_without_a_normalizer_the_same_query_is_unchecked(
        self, mkt_adapter, mkt_contract, monkeypatch
    ) -> None:
        # Before and after: this is the capability the task adds.
        seen = _spy(mkt_adapter, monkeypatch)
        report = check_sensitivity(
            _metric(FIRST_TOUCH_PROP),
            FAN_OUT + _CTX,
            contract=mkt_contract,
            adapter=mkt_adapter,
        )
        (result,) = report.results
        assert result.status == "unchecked"
        assert result.reason.startswith("unparseable")
        assert seen == []

    def test_a_renaming_normalizer_is_unchecked_and_runs_nothing(
        self, mkt_adapter, mkt_contract, monkeypatch
    ) -> None:
        seen = _spy(mkt_adapter, monkeypatch)
        report = check_sensitivity(
            _metric(FIRST_TOUCH_PROP),
            FAN_OUT,
            contract=mkt_contract,
            adapter=mkt_adapter,
            sql_normalizer=_Renaming(),
        )
        (result,) = report.results
        assert result.status == "unchecked"
        assert result.reason.startswith("count guard")
        assert seen == []

    def test_a_raising_normalizer_leaves_every_property_unchecked(
        self, mkt_adapter, mkt_contract, monkeypatch
    ) -> None:
        seen = _spy(mkt_adapter, monkeypatch)
        report = check_sensitivity(
            _metric(FIRST_TOUCH_PROP),
            FAN_OUT,
            contract=mkt_contract,
            adapter=mkt_adapter,
            sql_normalizer=_Raising(),
        )
        (result,) = report.results
        assert result.status == "unchecked"
        assert result.reason.startswith("normalizer failed")
        assert report.ok is False
        assert seen == []

    def test_a_normalizer_returning_none_does_not_escape_as_typeerror(
        self, mkt_adapter, mkt_contract, monkeypatch
    ) -> None:
        # A normalizer breaking its own str-in-str-out contract must fail
        # closed as one more "normalizer failed" outcome, not raise TypeError
        # out of check_sensitivity.
        seen = _spy(mkt_adapter, monkeypatch)
        report = check_sensitivity(
            _metric(FIRST_TOUCH_PROP),
            FAN_OUT,
            contract=mkt_contract,
            adapter=mkt_adapter,
            sql_normalizer=_ReturnsNone(),
        )
        (result,) = report.results
        assert result.status == "unchecked"
        assert result.reason.startswith("normalizer failed")
        assert seen == []

    def test_an_explicit_normalizer_beats_the_adapters_own(
        self, vql_adapter, mkt_contract, monkeypatch
    ) -> None:
        # vql_adapter is itself a working SqlNormalizer, so an explicit
        # `sql_normalizer` keyword must be used instead of it -- not merged,
        # not ignored.
        seen = _spy(vql_adapter, monkeypatch)
        report = check_sensitivity(
            _metric(FIRST_TOUCH_PROP),
            FAN_OUT + _CTX,
            contract=mkt_contract,
            adapter=vql_adapter,
            sql_normalizer=_Raising(),
        )
        (result,) = report.results
        assert result.status == "unchecked"
        assert result.reason.startswith("normalizer failed")
        assert seen == []

    def test_a_governed_normalized_shadow_is_checked(
        self, vql_adapter, mkt_contract
    ) -> None:
        report = check_sensitivity(
            _metric(VQL_PROP),
            FIRST_TOUCH + _CTX,
            contract=mkt_contract,
            adapter=vql_adapter,
        )
        (result,) = report.results
        assert result.status == "pass"

    def test_an_ungoverned_normalized_shadow_raises(self, vql_adapter) -> None:
        # VQL_SHADOW also reads mkt.lead_scores, which this contract does not
        # govern -- visible only after normalization.
        with pytest.raises(ValueError, match="lead_scores"):
            check_sensitivity(
                _metric(VQL_PROP),
                f"SELECT count(*) FROM mkt.touchpoints{_CTX}",
                contract=_contract("touchpoints"),
                adapter=vql_adapter,
            )


class TestTrailingSemicolonShadow:
    """A shadow ending in ``;`` used to make `_inject` paste an empty
    statement inside `alias AS (...;\n)`, which fails to parse -- so every
    check on an otherwise-fine shadow came back `unchecked`."""

    @pytest.mark.parametrize(
        "suffix",
        [";", ";  \n", "; -- note"],
        ids=["semicolon", "semicolon-trailing-ws", "semicolon-comment"],
    )
    def test_first_touch_still_passes(
        self, mkt_adapter, mkt_contract, suffix: str
    ) -> None:
        shadow = Shadow(table="mkt.touchpoints", sql=MKT_SHADOW.sql + suffix)
        prop = SensitivityProperty(
            name="attribution_is_first_touch",
            description="A touchpoint after qualification cannot change a first touch.",
            shadow=shadow,
            expect="unchanged",
        )
        report = check_sensitivity(
            _metric(prop), FIRST_TOUCH, contract=mkt_contract, adapter=mkt_adapter
        )
        (result,) = report.results
        assert result.status == "pass"
        assert result.moved is False
        assert report.ok is True
        assert validate_sensitivity_tables(mkt_contract, [_metric(prop)]) == []

    @pytest.mark.parametrize(
        "suffix",
        [";", ";  \n", "; -- note"],
        ids=["semicolon", "semicolon-trailing-ws", "semicolon-comment"],
    )
    def test_fan_out_still_violates(
        self, mkt_adapter, mkt_contract, suffix: str
    ) -> None:
        shadow = Shadow(table="mkt.touchpoints", sql=MKT_SHADOW.sql + suffix)
        prop = SensitivityProperty(
            name="attribution_is_first_touch",
            description="A touchpoint after qualification cannot change a first touch.",
            shadow=shadow,
            expect="unchanged",
        )
        report = check_sensitivity(
            _metric(prop), FAN_OUT, contract=mkt_contract, adapter=mkt_adapter
        )
        (result,) = report.results
        assert result.status == "violation"
        assert result.moved is True
        assert report.ok is False
        assert validate_sensitivity_tables(mkt_contract, [_metric(prop)]) == []


class TestAliasCollidesWithShadow:
    """`_free_alias` used to search only the caller's own text, so a shadow
    that itself mentions `sens_shadow_0` collided with the injected CTE."""

    def test_alias_avoids_a_name_the_shadow_itself_contains(self) -> None:
        shadow = Shadow(
            table="main.payments",
            sql="SELECT * FROM main.payments -- sens_shadow_0 is taken",
        )
        out = _rewrite("SELECT count(*) FROM main.payments", shadow, dialect="duckdb")
        sqlglot.parse_one(out, dialect="duckdb")
        assert out.startswith("WITH sens_shadow_1 AS (")
        assert "FROM sens_shadow_1" in out

    def test_check_sensitivity_still_yields_a_real_verdict(
        self, mkt_adapter, mkt_contract
    ) -> None:
        shadow = Shadow(
            table="mkt.touchpoints", sql=MKT_SHADOW.sql + " -- sens_shadow_0"
        )
        prop = SensitivityProperty(
            name="attribution_is_first_touch",
            description="A touchpoint after qualification cannot change a first touch.",
            shadow=shadow,
            expect="unchanged",
        )
        report = check_sensitivity(
            _metric(prop), FIRST_TOUCH, contract=mkt_contract, adapter=mkt_adapter
        )
        (result,) = report.results
        assert result.status == "pass"
        assert report.ok is True


class TestVacuousUnchanged:
    """`expect: unchanged` against an empty base and an empty shadowed result
    tests nothing -- it must not silently report `pass`."""

    _EMPTY_FIRST_TOUCH = """
    WITH first_touch AS (
      SELECT lead_id, channel FROM (
        SELECT lead_id, channel,
               row_number() OVER (PARTITION BY lead_id ORDER BY touch_date) rn
        FROM mkt.touchpoints) WHERE rn = 1)
    SELECT f.channel, COUNT(DISTINCT l.lead_id) AS mqls
    FROM mkt.lead_scores l JOIN first_touch f USING (lead_id)
    WHERE l.is_mql AND f.channel = 'zzz_no_such_channel' GROUP BY 1 ORDER BY 1
    """

    def test_empty_base_and_empty_shadow_is_unchecked(
        self, mkt_adapter, mkt_contract
    ) -> None:
        # MKT_SHADOW's extra rows carry channel 'display', which never
        # matches 'zzz_no_such_channel' either, so the shadowed result is
        # empty too -- a genuinely vacuous test, not a real pass.
        report = check_sensitivity(
            _metric(FIRST_TOUCH_PROP),
            self._EMPTY_FIRST_TOUCH,
            contract=mkt_contract,
            adapter=mkt_adapter,
        )
        (result,) = report.results
        assert result.status == "unchecked"
        assert result.reason.startswith("vacuous")
        assert report.ok is False

    def test_empty_base_but_the_shadow_makes_it_non_empty_is_a_violation(
        self, mkt_adapter, mkt_contract
    ) -> None:
        # The shadow inserts a touchpoint, earlier than any real one, whose
        # channel matches the filter -- so the shadowed result is non-empty
        # while the base is empty. That is a real move, not a vacuous test.
        shadow = Shadow(
            table="mkt.touchpoints",
            sql=(
                "SELECT * FROM mkt.touchpoints UNION ALL "
                "SELECT 1, 'zzz_no_such_channel', 1"
            ),
        )
        prop = SensitivityProperty(
            name="attribution_is_first_touch",
            description="A touchpoint after qualification cannot change a first touch.",
            shadow=shadow,
            expect="unchanged",
        )
        report = check_sensitivity(
            _metric(prop),
            self._EMPTY_FIRST_TOUCH,
            contract=mkt_contract,
            adapter=mkt_adapter,
        )
        (result,) = report.results
        assert result.status == "violation"
        assert result.moved is True
        assert report.ok is False


class TestRelativeFloatPrecision:
    """`_norm` rounds floats to a fixed number of significant digits, not a
    fixed decimal place -- an absolute round carries run-to-run noise in its
    least-significant surviving digit once a value's magnitude passes it."""

    def test_noise_past_the_twelfth_significant_digit_normalizes_equal(self) -> None:
        base = 1_234_567_890.123456
        noisy = base + 1e-6  # perturbs well past the 12th significant digit
        assert _norm([(base,)]) == _norm([(noisy,)])

    def test_a_change_in_the_tenth_significant_digit_is_not_hidden(self) -> None:
        a = 1_234_567_890.123456
        b = 1_234_567_891.123456  # differs in the 10th significant digit
        assert _norm([(a,)]) != _norm([(b,)])

    @pytest.mark.parametrize("value", [0.0, -1.5, float("nan"), float("inf")])
    def test_edge_values_do_not_raise(self, value: float) -> None:
        _norm([(value,)])  # must not raise
