from agentic_data_contracts.validation.checkers import ResultCheckRunner


class TestResultCheckRunnerRowBounds:
    def test_min_rows_passes(self) -> None:
        runner = ResultCheckRunner(
            column=None,
            min_value=None,
            max_value=None,
            not_null=None,
            min_rows=1,
            max_rows=None,
            rule_name="test",
        )
        result = runner.check_results(["id"], [(1,), (2,)])
        assert result.passed

    def test_min_rows_fails(self) -> None:
        runner = ResultCheckRunner(
            column=None,
            min_value=None,
            max_value=None,
            not_null=None,
            min_rows=1,
            max_rows=None,
            rule_name="not_empty",
        )
        result = runner.check_results(["id"], [])
        assert not result.passed
        assert "0 rows" in result.message
        assert "not_empty" in result.message

    def test_max_rows_passes(self) -> None:
        runner = ResultCheckRunner(
            column=None,
            min_value=None,
            max_value=None,
            not_null=None,
            min_rows=None,
            max_rows=100,
            rule_name="test",
        )
        result = runner.check_results(["id"], [(1,), (2,)])
        assert result.passed

    def test_max_rows_fails(self) -> None:
        runner = ResultCheckRunner(
            column=None,
            min_value=None,
            max_value=None,
            not_null=None,
            min_rows=None,
            max_rows=2,
            rule_name="size_limit",
        )
        rows = [(i,) for i in range(5)]
        result = runner.check_results(["id"], rows)
        assert not result.passed
        assert "5 rows" in result.message


class TestResultCheckRunnerColumnBounds:
    def test_max_value_passes(self) -> None:
        runner = ResultCheckRunner(
            column="wau",
            min_value=None,
            max_value=8_000_000_000,
            not_null=None,
            min_rows=None,
            max_rows=None,
            rule_name="wau_check",
        )
        result = runner.check_results(["wau"], [(1_000_000,), (2_000_000,)])
        assert result.passed

    def test_max_value_fails(self) -> None:
        runner = ResultCheckRunner(
            column="wau",
            min_value=None,
            max_value=8_000_000_000,
            not_null=None,
            min_rows=None,
            max_rows=None,
            rule_name="wau_sanity",
        )
        result = runner.check_results(["wau"], [(12_000_000_000,)])
        assert not result.passed
        assert "12000000000" in result.message
        assert "wau_sanity" in result.message

    def test_min_value_passes(self) -> None:
        runner = ResultCheckRunner(
            column="revenue",
            min_value=0,
            max_value=None,
            not_null=None,
            min_rows=None,
            max_rows=None,
            rule_name="test",
        )
        result = runner.check_results(["revenue"], [(100,), (200,)])
        assert result.passed

    def test_min_value_fails(self) -> None:
        runner = ResultCheckRunner(
            column="revenue",
            min_value=0,
            max_value=None,
            not_null=None,
            min_rows=None,
            max_rows=None,
            rule_name="no_neg",
        )
        result = runner.check_results(["revenue"], [(100,), (-50,)])
        assert not result.passed
        assert "-50" in result.message

    def test_column_not_in_results_skips(self) -> None:
        runner = ResultCheckRunner(
            column="missing_col",
            min_value=0,
            max_value=None,
            not_null=None,
            min_rows=None,
            max_rows=None,
            rule_name="test",
        )
        result = runner.check_results(["id", "name"], [(1, "a")])
        assert result.passed

    def test_column_case_insensitive(self) -> None:
        runner = ResultCheckRunner(
            column="WAU",
            min_value=None,
            max_value=100,
            not_null=None,
            min_rows=None,
            max_rows=None,
            rule_name="test",
        )
        result = runner.check_results(["wau"], [(999,)])
        assert not result.passed


class TestResultCheckRunnerNotNull:
    def test_not_null_passes(self) -> None:
        runner = ResultCheckRunner(
            column="name",
            min_value=None,
            max_value=None,
            not_null=True,
            min_rows=None,
            max_rows=None,
            rule_name="test",
        )
        result = runner.check_results(["name"], [("alice",), ("bob",)])
        assert result.passed

    def test_not_null_fails(self) -> None:
        runner = ResultCheckRunner(
            column="name",
            min_value=None,
            max_value=None,
            not_null=True,
            min_rows=None,
            max_rows=None,
            rule_name="no_nulls",
        )
        result = runner.check_results(["name"], [("alice",), (None,)])
        assert not result.passed
        assert "1 null" in result.message
