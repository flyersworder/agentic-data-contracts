"""``{{ metric:NAME }}`` in a certified example executes the metric's own SQL.

Without it, nothing in the library runs a metric's ``sql_expression``: a
certified example repeats the logic in hand-written SQL, so an edit to the
metric moves no certified answer. These tests pin the headline property on a
real engine (an edited predicate turns a match into a mismatch) and the
load-time refusals that keep an expansion from certifying the wrong quantity.
"""

from pathlib import Path
from typing import Any

import pytest

from agentic_data_contracts.adapters.duckdb import DuckDBAdapter
from agentic_data_contracts.core.contract import DataContract
from agentic_data_contracts.semantic.cube import CubeSource
from agentic_data_contracts.semantic.dbt import DbtSource
from agentic_data_contracts.semantic.yaml_source import YamlSource
from agentic_data_contracts.validation.examples import (
    ExampleAnswerReport,
    ExampleAnswerResult,
    VerifiedExample,
    check_example_answers,
    validate_examples,
)
from agentic_data_contracts.validation.explain import ExplainResult

_CONTRACT = """
version: "1.0"
name: fees
semantic:
  allowed_tables:
    - schema: analytics
      tables: [payments, fees]
  forbidden_operations: [DELETE, DROP]
  rules: []
"""

# The eval's defect in miniature: `aci` encodes "applies to every value" as an
# empty list, never as NULL. The correct predicate accepts the empty list; the
# literal reading of "a null field applies to all values" does not.
_CORRECT = (
    "(f.card_scheme IS NULL OR f.card_scheme = p.card_scheme)"
    " AND (f.aci IS NULL OR len(f.aci) = 0 OR list_contains(f.aci, p.aci))"
)
_LITERAL = (
    "(f.card_scheme IS NULL OR f.card_scheme = p.card_scheme)"
    " AND (f.aci IS NULL OR list_contains(f.aci, p.aci))"
)

_COUNT_SQL = (
    "SELECT count(*) FROM analytics.payments p CROSS JOIN analytics.fees f"
    " WHERE p.id = 1 AND {{ metric:rule_matches }}"
)


def _source(**metrics: Any) -> YamlSource:
    rows = []
    for name, spec in metrics.items():
        row = {"name": name, "description": name}
        row.update(spec if isinstance(spec, dict) else {"sql_expression": spec})
        rows.append(row)
    return YamlSource.from_raw({"metrics": rows})


@pytest.fixture
def contract() -> DataContract:
    return DataContract.from_yaml_string(_CONTRACT)


@pytest.fixture
def adapter() -> DuckDBAdapter:
    db = DuckDBAdapter(":memory:")
    db.connection.execute(
        """
        CREATE SCHEMA analytics;
        CREATE TABLE analytics.payments (id INTEGER, card_scheme VARCHAR, aci VARCHAR);
        INSERT INTO analytics.payments VALUES (1, 'Visa', 'A');
        CREATE TABLE analytics.fees (id INTEGER, card_scheme VARCHAR, aci VARCHAR[]);
        INSERT INTO analytics.fees VALUES
            (1, 'Visa', ['A']),
            (2, NULL, []),
            (3, 'Visa', []),
            (4, 'Mastercard', ['A']),
            (5, 'Visa', ['B']);
        """
    )
    return db


def _answers(
    contract: DataContract,
    adapter: DuckDBAdapter,
    source: YamlSource,
    *examples: VerifiedExample,
) -> ExampleAnswerReport:
    report = validate_examples(examples, contract, semantic_source=source)
    assert report.ok, report.summary()
    return check_example_answers(report, adapter=adapter)


class TestExecutesTheMetricsOwnSql:
    def test_the_correct_definition_matches_its_certified_answer(
        self, contract: DataContract, adapter: DuckDBAdapter
    ) -> None:
        ex = VerifiedExample(sql=_COUNT_SQL, id="rules-for-1", expected=3)
        answers = _answers(contract, adapter, _source(rule_matches=_CORRECT), ex)
        assert answers.ok, answers.summary()

    def test_an_edited_definition_moves_the_certified_answer(
        self, contract: DataContract, adapter: DuckDBAdapter
    ) -> None:
        # Same example, same certified answer: only the metric changed. Every
        # structural check passes the literal predicate; this is what fails.
        ex = VerifiedExample(sql=_COUNT_SQL, id="rules-for-1", expected=3)
        answers = _answers(contract, adapter, _source(rule_matches=_LITERAL), ex)
        assert [r.status for r in answers.results] == ["mismatch"]
        assert answers.results[0].actual == 1

    def test_the_expansion_is_parenthesized(
        self, contract: DataContract, adapter: DuckDBAdapter
    ) -> None:
        # Unparenthesized, `p.id = 2 AND f.aci IS NULL OR len(f.aci) = 0` binds
        # as `(p.id = 2 AND ...) OR len(f.aci) = 0` and counts fees 2 and 3.
        ex = VerifiedExample(
            sql=(
                "SELECT count(*) FROM analytics.payments p CROSS JOIN analytics.fees f"
                " WHERE p.id = 2 AND {{ metric:wildcard }}"
            ),
            expected=0,
        )
        source = _source(wildcard="f.aci IS NULL OR len(f.aci) = 0")
        answers = _answers(contract, adapter, source, ex)
        assert answers.ok, answers.summary()

    def test_an_aggregate_metric_expands_in_the_select_list(
        self, contract: DataContract, adapter: DuckDBAdapter
    ) -> None:
        ex = VerifiedExample(
            sql="SELECT {{metric:rules}} FROM analytics.fees f WHERE f.id < 4",
            expected=3,
        )
        answers = _answers(contract, adapter, _source(rules="count(*)"), ex)
        assert answers.ok, answers.summary()

    def test_a_trailing_line_comment_in_the_metric_keeps_its_paren(
        self, contract: DataContract, adapter: DuckDBAdapter
    ) -> None:
        # A `|` block scalar keeps newlines; its last line may be a comment,
        # which would swallow a closing paren placed on the same line.
        ex = VerifiedExample(
            sql="SELECT {{ metric:rules }} FROM analytics.fees f", expected=5
        )
        source = _source(rules="count(*)  -- one per fee rule\n")
        answers = _answers(contract, adapter, source, ex)
        assert answers.ok, answers.summary()

    def test_a_breakdown_row_executes_the_expansion(
        self, contract: DataContract, adapter: DuckDBAdapter
    ) -> None:
        ex = VerifiedExample(
            sql=(
                "SELECT f.card_scheme, {{ metric:rules }} FROM analytics.fees f"
                " WHERE f.card_scheme IS NOT NULL GROUP BY f.card_scheme"
            ),
            expected_rows=[["Mastercard", 1], ["Visa", 3]],
        )
        answers = _answers(contract, adapter, _source(rules="count(*)"), ex)
        assert answers.ok, answers.summary()


class TestTheReportCarriesWhatWasValidated:
    def test_the_result_records_the_expanded_sql_and_the_metrics(
        self, contract: DataContract
    ) -> None:
        ex = VerifiedExample(sql="SELECT {{ metric:rules }} FROM analytics.fees f")
        report = validate_examples(
            [ex], contract, semantic_source=_source(rules="count(*)")
        )
        (row,) = report.results
        assert row.sql == "SELECT (count(*)\n) FROM analytics.fees f"
        assert row.metrics == ["rules"]
        assert (
            ex.sql == "SELECT {{ metric:rules }} FROM analytics.fees f"
        )  # input untouched

    def test_a_row_without_placeholders_records_its_own_sql(
        self, contract: DataContract
    ) -> None:
        ex = VerifiedExample(sql="SELECT count(*) FROM analytics.fees")
        (row,) = validate_examples([ex], contract).results
        assert row.sql == ex.sql
        assert row.metrics == []

    def test_the_expanded_sql_is_what_contract_validation_sees(
        self, contract: DataContract
    ) -> None:
        # The metric reads a table the contract does not allow: the example's
        # own text is clean, so only a validator that saw the expansion blocks.
        ex = VerifiedExample(sql="SELECT {{ metric:leak }} FROM analytics.fees f")
        source = _source(leak="(SELECT count(*) FROM secret.salaries)")
        (row,) = validate_examples([ex], contract, semantic_source=source).results
        assert row.status == "violation"
        assert "secret.salaries" in "; ".join(row.reasons)

    def test_the_parse_fallback_plans_the_expanded_sql(
        self, contract: DataContract
    ) -> None:
        # Decision B asks the engine directly when sqlglot cannot parse; the
        # engine must be asked about the expansion, not the placeholder.
        planned: list[str] = []

        class RecordingExplain:
            def explain(self, sql: str) -> ExplainResult:
                planned.append(sql)
                return ExplainResult(
                    estimated_cost_usd=None, estimated_rows=1, schema_valid=True
                )

        ex = VerifiedExample(sql="SELECT {{ metric:rules }} FROM analytics.fees f ((")
        source = _source(rules="count(*)")
        (row,) = validate_examples(
            [ex], contract, semantic_source=source, explain_adapter=RecordingExplain()
        ).results
        assert row.status == "unverified"
        assert planned == ["SELECT (count(*)\n) FROM analytics.fees f (("]

    def test_a_metric_referenced_twice_is_recorded_once(
        self, contract: DataContract
    ) -> None:
        ex = VerifiedExample(
            sql=(
                "SELECT {{ metric:rules }}, {{ metric:rules }} + 1"
                " FROM analytics.fees f"
            )
        )
        report = validate_examples(
            [ex], contract, semantic_source=_source(rules="count(*)")
        )
        assert report.results[0].metrics == ["rules"]


class TestRefusedAtLoadTime:
    """A placeholder that cannot expand faithfully raises, naming the row.

    Never a silent pass, and never a per-row ``unchecked`` either: nothing is
    validated until every row expands, as with any other malformed corpus row.
    """

    def _refuse(self, contract: DataContract, ex: VerifiedExample, source: Any) -> str:
        with pytest.raises(ValueError) as info:
            validate_examples([ex], contract, semantic_source=source)
        return str(info.value)

    def test_an_unknown_metric(self, contract: DataContract) -> None:
        ex = VerifiedExample(
            sql="SELECT {{ metric:nope }} FROM analytics.fees", id="r1"
        )
        msg = self._refuse(contract, ex, _source(rules="count(*)"))
        assert "r1" in msg and "nope" in msg

    def test_an_empty_expression(self, contract: DataContract) -> None:
        ex = VerifiedExample(
            sql="SELECT {{ metric:blank }} FROM analytics.fees", id="r1"
        )
        msg = self._refuse(contract, ex, _source(blank="  "))
        assert "r1" in msg and "blank" in msg

    def test_a_placeholder_with_no_semantic_source(
        self, contract: DataContract
    ) -> None:
        ex = VerifiedExample(
            sql="SELECT {{ metric:rules }} FROM analytics.fees", id="r1"
        )
        msg = self._refuse(contract, ex, None)
        assert "r1" in msg and "semantic_source" in msg

    @pytest.mark.parametrize(
        "sql",
        [
            "SELECT {{ metrc:rules }} FROM analytics.fees",
            "SELECT {{ metric: }} FROM analytics.fees",
            "SELECT {{ metric:rules FROM analytics.fees",
            # A third brace would survive expansion as `{(...)}`.
            "SELECT {{{ metric:rules }}} FROM analytics.fees",
            "SELECT {{{ metric:rules }} FROM analytics.fees",
        ],
    )
    def test_an_unrecognized_placeholder(
        self, contract: DataContract, sql: str
    ) -> None:
        ex = VerifiedExample(sql=sql, id="r1")
        msg = self._refuse(contract, ex, _source(rules="count(*)"))
        assert "r1" in msg and "{{" in msg

    def test_a_placeholder_inside_a_metrics_expression(
        self, contract: DataContract
    ) -> None:
        # Single pass: nesting is refused rather than expanded recursively.
        ex = VerifiedExample(
            sql="SELECT {{ metric:outer }} FROM analytics.fees f", id="r1"
        )
        source = _source(inner="count(*)", outer="{{ metric:inner }} + 1")
        msg = self._refuse(contract, ex, source)
        assert "outer" in msg

    def test_a_metric_that_declares_filters(self, contract: DataContract) -> None:
        # Expanding sql_expression alone would drop the filter and certify a
        # different quantity than the one the metric defines.
        ex = VerifiedExample(
            sql="SELECT {{ metric:recent }} FROM analytics.fees f", id="r1"
        )
        source = _source(recent={"sql_expression": "count(*)", "filters": ["f.id > 2"]})
        msg = self._refuse(contract, ex, source)
        assert "recent" in msg and "filters" in msg

    @pytest.mark.parametrize(
        ("source_cls", "fixture"),
        [
            (DbtSource, "sample_dbt_manifest.json"),
            (CubeSource, "sample_cube_schema.yml"),
        ],
    )
    def test_a_source_whose_expression_is_not_the_metric(
        self,
        contract: DataContract,
        fixtures_dir: Path,
        source_cls: type,
        fixture: str,
    ) -> None:
        # dbt and Cube keep a metric's aggregation and filters beside its
        # expression (MetricFlow's filters are Jinja, not SQL), so no metric
        # from either can be expanded faithfully, filters declared or not.
        source = source_cls(fixtures_dir / fixture)
        ex = VerifiedExample(
            sql="SELECT {{ metric:total_revenue }} FROM analytics.fees", id="r1"
        )
        msg = self._refuse(contract, ex, source)
        assert "r1" in msg and source_cls.__name__ in msg

    @pytest.mark.parametrize(
        ("source_cls", "fixture"),
        [
            (DbtSource, "sample_dbt_manifest.json"),
            (CubeSource, "sample_cube_schema.yml"),
        ],
    )
    def test_a_placeholder_free_corpus_still_validates_on_those_sources(
        self,
        contract: DataContract,
        fixtures_dir: Path,
        source_cls: type,
        fixture: str,
    ) -> None:
        # `semantic_source` predates placeholders (it feeds the Validator's
        # relationship checks), so a dbt or Cube user with no placeholder
        # must see exactly the behaviour they had before.
        source = source_cls(fixtures_dir / fixture)
        ex = VerifiedExample(sql="SELECT count(*) FROM analytics.fees")
        report = validate_examples([ex], contract, semantic_source=source)
        assert report.ok, report.summary()

    def test_a_bad_row_stops_the_corpus_before_anything_is_validated(
        self, contract: DataContract
    ) -> None:
        good = VerifiedExample(sql="SELECT {{ metric:rules }} FROM analytics.fees f")
        bad = VerifiedExample(sql="SELECT {{ metric:nope }} FROM analytics.fees f")
        with pytest.raises(ValueError, match="nope"):
            validate_examples(
                [good, bad], contract, semantic_source=_source(rules="count(*)")
            )


class TestCoverage:
    def test_a_matched_metric_is_covered(
        self, contract: DataContract, adapter: DuckDBAdapter
    ) -> None:
        source = _source(rule_matches=_CORRECT, rules="count(*)", unused="1")
        ex = VerifiedExample(sql=_COUNT_SQL, expected=3)
        answers = _answers(contract, adapter, source, ex)
        assert answers.covered_metrics == ["rule_matches"]
        assert answers.uncovered_metrics(source) == ["rules", "unused"]

    def test_a_mismatched_metric_is_not_covered(
        self, contract: DataContract, adapter: DuckDBAdapter
    ) -> None:
        source = _source(rule_matches=_LITERAL)
        ex = VerifiedExample(sql=_COUNT_SQL, expected=3)
        answers = _answers(contract, adapter, source, ex)
        assert answers.covered_metrics == []
        assert answers.uncovered_metrics(source) == ["rule_matches"]

    def test_an_unasserted_row_covers_nothing(
        self, contract: DataContract, adapter: DuckDBAdapter
    ) -> None:
        # Validated, never executed: it certifies nothing about the metric.
        source = _source(rules="count(*)")
        asserted = VerifiedExample(
            sql="SELECT count(*) FROM analytics.fees", expected=5
        )
        protocol_only = VerifiedExample(
            sql="SELECT {{ metric:rules }} FROM analytics.fees f"
        )
        answers = _answers(contract, adapter, source, asserted, protocol_only)
        assert answers.covered_metrics == []

    def test_the_answer_result_carries_the_metrics(self) -> None:
        r = ExampleAnswerResult(example=VerifiedExample(sql="SELECT 1"), status="match")
        assert r.metrics == []
