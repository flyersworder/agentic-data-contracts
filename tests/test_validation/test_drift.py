"""A declaration that contradicts the live schema must say so (#90).

The defect: a semantic source could declare a column that does not exist, and
nothing reported it -- not at load, not at `describe_table`, not anywhere. The
declaration sits inside `contract_digest`, so the contract stayed "frozen"
around a column the warehouse had renamed, and every gate stayed green because
the declarations had not changed. The *world* had.

`check_schema_drift` is the preflight half of the fix: it belongs in CI, where
a schema migration trips it, rather than in an agent's turn.

Two properties this file holds beyond the obvious ones, both learned from #89:

- **A check that could not run is not a check that passed.** An unresolved
  wildcard, or an adapter that raised, lands in `unchecked` -- and `ok` is
  False, so a CI gate written the obvious way fails rather than reporting a
  clean bill of health it never earned.
- **The check must not move the digest.** `resolve_tables()` rewrites
  `allowed_tables[].tables` in place, which changes the canonical bytes. A
  preflight that silently re-pinned the artifact it was auditing would be worse
  than the defect.
"""

from __future__ import annotations

import hashlib
from typing import Any

import pytest

from agentic_data_contracts.adapters.base import Column, TableSchema
from agentic_data_contracts.adapters.duckdb import DuckDBAdapter
from agentic_data_contracts.ard import contract_canonical_bytes
from agentic_data_contracts.core.contract import DataContract
from agentic_data_contracts.semantic.yaml_source import YamlSource
from agentic_data_contracts.validation.drift import (
    SchemaDrift,
    SchemaDriftReport,
    UncheckedTable,
    check_schema_drift,
)


def _contract(
    tables: str = "[orders, customers]", schema: str = "main"
) -> DataContract:
    return DataContract.from_yaml_string(
        f"""
version: "1.0"
name: drift-test
semantic:
  allowed_tables:
    - schema: {schema}
      tables: {tables}
"""
    )


@pytest.fixture
def adapter() -> DuckDBAdapter:
    db = DuckDBAdapter(":memory:")
    db.connection.execute(
        """
        CREATE SCHEMA IF NOT EXISTS main;
        CREATE TABLE main.orders (id INTEGER, amount DECIMAL(10,2), mcc VARCHAR);
        CREATE TABLE main.customers (id INTEGER, region VARCHAR);
        """
    )
    return db


def _source(tables: list[dict[str, Any]]) -> YamlSource:
    return YamlSource.from_raw({"metrics": [], "tables": tables}, expected_extras=[])


def _kinds(report: SchemaDriftReport) -> list[str]:
    return [d.kind for d in report.drifts]


class _StubSource:
    """The rest of `SemanticSource`, so a test double can be a real one.

    `check_schema_drift` reads exactly one method off the protocol. Subclassing
    this rather than passing a one-method object with a type-ignore keeps the
    doubles *structurally* valid, which is the point being tested -- a
    third-party source satisfies the whole protocol and returns its own
    schema-shaped objects, and it is only those objects the check must not make
    assumptions about.
    """

    def get_table_schemas(self) -> dict[str, Any]:
        return {}

    def get_table_schema(self, schema: str, table: str) -> Any:
        return self.get_table_schemas().get(f"{schema}.{table}")

    def get_metrics(self) -> list[Any]:
        return []

    def get_metric(self, name: str) -> Any:
        return None

    def search_metrics(self, query: str) -> list[Any]:
        return []

    def get_relationships(self) -> list[Any]:
        return []

    def get_relationships_for_table(self, table: str) -> list[Any]:
        return []

    def get_metric_impacts(self) -> list[Any]:
        return []


class TestAgreement:
    def test_a_contract_that_matches_the_database_is_ok(
        self, adapter: DuckDBAdapter
    ) -> None:
        source = _source(
            [
                {
                    "schema": "main",
                    "table": "orders",
                    "columns": [{"name": "id"}, {"name": "amount"}],
                },
                {"schema": "main", "table": "customers", "columns": [{"name": "id"}]},
            ]
        )
        report = check_schema_drift(_contract(), adapter, source)
        assert report.ok
        assert report.drifts == []
        assert report.unchecked == []
        assert report.tables_checked == 2
        assert report.columns_checked == 3

    def test_a_live_column_nobody_declared_is_not_drift(
        self, adapter: DuckDBAdapter
    ) -> None:
        """The overlay is a left join by design: an undeclared column is fine.

        Only the other direction -- declared, absent -- is the defect. Reporting
        every undocumented column would make the check unusable on any real
        warehouse and would drown the finding that matters.
        """
        source = _source(
            [{"schema": "main", "table": "orders", "columns": [{"name": "id"}]}]
        )
        report = check_schema_drift(_contract("[orders]"), adapter, source)
        assert report.ok

    def test_no_semantic_source_still_checks_tables(
        self, adapter: DuckDBAdapter
    ) -> None:
        report = check_schema_drift(_contract("[orders, ghosts]"), adapter)
        assert _kinds(report) == ["missing_table"]
        assert report.columns_checked == 0


class TestMissingColumn:
    def test_a_declared_column_that_does_not_exist_is_reported(
        self, adapter: DuckDBAdapter
    ) -> None:
        """The reproduction from #90, as a gate."""
        source = _source(
            [
                {
                    "schema": "main",
                    "table": "orders",
                    "columns": [
                        {"name": "mcc", "description": "real column"},
                        {"name": "column1", "description": "PHANTOM"},
                    ],
                }
            ]
        )
        report = check_schema_drift(_contract("[orders]"), adapter, source)
        assert not report.ok
        assert report.has_drift
        assert report.drifts == [
            SchemaDrift(
                kind="missing_column", schema="main", table="orders", column="column1"
            )
        ]

    def test_an_undescribed_declaration_is_checked_too(
        self, adapter: DuckDBAdapter
    ) -> None:
        """The issue's sketch diffs `sem_descs`, which holds only *described*
        columns. A declared column with no description is just as absent, and
        just as much a stale claim about the schema."""
        source = _source(
            [{"schema": "main", "table": "orders", "columns": [{"name": "column1"}]}]
        )
        report = check_schema_drift(_contract("[orders]"), adapter, source)
        assert _kinds(report) == ["missing_column"]

    def test_every_missing_column_is_reported_not_just_the_first(
        self, adapter: DuckDBAdapter
    ) -> None:
        source = _source(
            [
                {
                    "schema": "main",
                    "table": "orders",
                    "columns": [
                        {"name": "column1"},
                        {"name": "column2"},
                        {"name": "id"},
                    ],
                }
            ]
        )
        report = check_schema_drift(_contract("[orders]"), adapter, source)
        assert [d.column for d in report.drifts] == ["column1", "column2"]


class TestCaseOnlyDifference:
    def test_a_case_mismatch_is_its_own_kind_and_names_the_live_column(
        self, adapter: DuckDBAdapter
    ) -> None:
        """Snowflake upper-cases; DuckDB does not. A declaration that differs
        only in case is a real defect -- the overlay is an exact-match dict
        lookup, so the authored description never reaches the agent -- but
        calling it "missing" sends the reader hunting for a column that is
        right there under another spelling.
        """
        source = _source(
            [{"schema": "main", "table": "orders", "columns": [{"name": "MCC"}]}]
        )
        report = check_schema_drift(_contract("[orders]"), adapter, source)
        assert report.drifts == [
            SchemaDrift(
                kind="case_mismatch",
                schema="main",
                table="orders",
                column="MCC",
                live_name="mcc",
            )
        ]

    def test_a_case_mismatch_is_drift_not_a_pass(self, adapter: DuckDBAdapter) -> None:
        source = _source(
            [{"schema": "main", "table": "orders", "columns": [{"name": "MCC"}]}]
        )
        assert not check_schema_drift(_contract("[orders]"), adapter, source).ok


class TestMissingTable:
    def test_a_declared_table_that_does_not_exist_is_reported(
        self, adapter: DuckDBAdapter
    ) -> None:
        report = check_schema_drift(_contract("[orders, ghosts]"), adapter)
        assert report.drifts == [
            SchemaDrift(kind="missing_table", schema="main", table="ghosts")
        ]

    def test_a_missing_table_does_not_also_report_each_of_its_columns(
        self, adapter: DuckDBAdapter
    ) -> None:
        """`DuckDBAdapter.describe_table` returns an empty `TableSchema` for a
        table that does not exist rather than raising, so the naive walk turns
        one missing table into one finding per declared column -- burying the
        single fact the reader needs under N restatements of it.
        """
        source = _source(
            [
                {
                    "schema": "main",
                    "table": "ghosts",
                    "columns": [{"name": "a"}, {"name": "b"}, {"name": "c"}],
                }
            ]
        )
        report = check_schema_drift(_contract("[ghosts]"), adapter, source)
        assert _kinds(report) == ["missing_table"]

    def test_a_schema_with_nothing_live_is_reported_once(
        self, adapter: DuckDBAdapter
    ) -> None:
        """A typo'd schema name makes `list_tables` return `[]`, which reads as
        every declared table being missing. One `missing_schema` is the finding
        a human can act on; N `missing_table`s is noise about a single typo."""
        report = check_schema_drift(
            _contract("[orders, customers]", schema="analytcs"), adapter
        )
        assert report.drifts == [
            SchemaDrift(kind="missing_schema", schema="analytcs", table="")
        ]

    def test_a_schema_declaring_no_tables_is_not_a_missing_schema(
        self, adapter: DuckDBAdapter
    ) -> None:
        """`allowed_tables: [{schema: raw, tables: []}]` is legal and common --
        a schema listed with nothing allowed in it yet. There is no declaration
        to contradict, so there is nothing to report."""
        contract = DataContract.from_yaml_string(
            """
version: "1.0"
name: drift-test
semantic:
  allowed_tables:
    - schema: main
      tables: [orders]
    - schema: nowhere
      tables: []
"""
        )
        report = check_schema_drift(contract, adapter)
        assert report.ok


class TestUnchecked:
    def test_an_unresolved_wildcard_is_unchecked_not_clean(
        self, adapter: DuckDBAdapter
    ) -> None:
        """`allowed_table_names()` skips an unresolved `*`, so a wildcard
        contract would otherwise check nothing and report a clean bill of
        health -- the exact shape #89 and #90 both exist to eliminate."""
        report = check_schema_drift(_contract('["*"]'), adapter)
        assert not report.ok
        assert not report.has_drift
        assert report.unchecked == [
            UncheckedTable(
                schema="main", table="*", reason=UncheckedTable.UNRESOLVED_WILDCARD
            )
        ]

    def test_the_check_never_resolves_the_wildcard_itself(
        self, adapter: DuckDBAdapter
    ) -> None:
        """`resolve_tables()` rewrites `allowed_tables[].tables` in place, and
        those bytes are inside `contract_digest`. A preflight that re-pinned the
        artifact it was auditing would break the provenance claim the digest
        exists to make."""
        contract = _contract('["*"]')
        before = hashlib.sha256(contract_canonical_bytes(contract)).hexdigest()
        check_schema_drift(contract, adapter)
        after = hashlib.sha256(contract_canonical_bytes(contract)).hexdigest()
        assert before == after
        assert contract.has_wildcard_tables()

    def test_an_adapter_that_raises_is_unchecked_not_drift(self) -> None:
        """A connection failure is not evidence that a table is missing.
        Reporting it as drift would send someone to fix a contract that is
        correct; swallowing it would pass a check that never ran."""

        class _BrokenAdapter:
            dialect = "duckdb"

            def list_tables(self, schema: str) -> list[str]:
                raise RuntimeError("connection refused")

            def describe_table(self, schema: str, table: str) -> TableSchema:
                raise AssertionError("should not be reached")

            def execute(self, sql: str) -> Any:
                raise AssertionError("should not be reached")

            def explain(self, sql: str) -> Any:
                raise AssertionError("should not be reached")

        report = check_schema_drift(
            _contract("[orders]"),
            _BrokenAdapter(),  # type: ignore[arg-type]
        )
        assert not report.ok
        assert not report.has_drift
        assert len(report.unchecked) == 1
        assert "connection refused" in report.unchecked[0].reason

    def test_a_table_whose_description_raises_is_unchecked_not_drift(
        self, adapter: DuckDBAdapter
    ) -> None:
        class _FlakyAdapter:
            dialect = "duckdb"

            def list_tables(self, schema: str) -> list[str]:
                return adapter.list_tables(schema)

            def describe_table(self, schema: str, table: str) -> TableSchema:
                raise RuntimeError("permission denied")

            def execute(self, sql: str) -> Any:
                raise AssertionError("should not be reached")

            def explain(self, sql: str) -> Any:
                raise AssertionError("should not be reached")

        source = _source(
            [{"schema": "main", "table": "orders", "columns": [{"name": "column1"}]}]
        )
        report = check_schema_drift(
            _contract("[orders]"),
            _FlakyAdapter(),  # type: ignore[arg-type]
            source,
        )
        assert not report.has_drift
        assert [u.table for u in report.unchecked] == ["orders"]


class TestReport:
    def test_findings_are_ordered_deterministically(
        self, adapter: DuckDBAdapter
    ) -> None:
        """CI diffs the output. Set iteration order would make an unchanged
        contract produce a changed report."""
        source = _source(
            [
                {
                    "schema": "main",
                    "table": "orders",
                    "columns": [{"name": "zeta"}, {"name": "alpha"}],
                },
                {"schema": "main", "table": "customers", "columns": [{"name": "beta"}]},
            ]
        )
        args = (_contract(), adapter, source)
        first = check_schema_drift(*args)
        assert [(d.table, d.column) for d in first.drifts] == [
            ("customers", "beta"),
            ("orders", "alpha"),
            ("orders", "zeta"),
        ]
        assert check_schema_drift(*args).drifts == first.drifts

    def test_a_drift_renders_as_one_actionable_line(
        self, adapter: DuckDBAdapter
    ) -> None:
        source = _source(
            [{"schema": "main", "table": "orders", "columns": [{"name": "column1"}]}]
        )
        line = str(check_schema_drift(_contract("[orders]"), adapter, source).drifts[0])
        assert "main.orders" in line
        assert "column1" in line

    def test_the_summary_says_what_was_checked_not_only_what_failed(
        self, adapter: DuckDBAdapter
    ) -> None:
        """A report that checked nothing must not read like a report that
        passed. The counts are the only thing separating the two."""
        empty = DataContract.from_yaml_string(
            """
version: "1.0"
name: drift-test
semantic:
  allowed_tables: []
"""
        )
        report = check_schema_drift(empty, adapter)
        assert report.ok
        assert report.tables_checked == 0
        assert "0 table" in report.summary()

    def test_the_summary_names_every_finding(self, adapter: DuckDBAdapter) -> None:
        source = _source(
            [{"schema": "main", "table": "orders", "columns": [{"name": "column1"}]}]
        )
        summary = check_schema_drift(
            _contract("[orders, ghosts]"), adapter, source
        ).summary()
        assert "column1" in summary
        assert "ghosts" in summary


class TestProtocolTolerance:
    def test_a_duck_typed_semantic_source_is_tolerated(
        self, adapter: DuckDBAdapter
    ) -> None:
        """`SemanticSource` is a structural protocol, so a third-party source
        returns its own schema-shaped objects. Reading `.columns` off one is
        part of the protocol; assuming our `Column` dataclass is not."""

        class _ForeignSchema:
            def __init__(self, names: list[str]) -> None:
                self.columns = [Column(name=n, type="") for n in names]

        class _ForeignSource(_StubSource):
            def get_table_schemas(self) -> dict[str, Any]:
                return {"main.orders": _ForeignSchema(["id", "column1"])}

        report = check_schema_drift(_contract("[orders]"), adapter, _ForeignSource())
        assert [d.column for d in report.drifts] == ["column1"]

    def test_a_table_key_without_a_schema_is_skipped_not_crashed(
        self, adapter: DuckDBAdapter
    ) -> None:
        """Every bundled source keys `get_table_schemas()` as `schema.table`,
        but nothing in the protocol says so. An unqualified key matches no
        allowed table; it must fall out quietly, not raise."""

        class _OddSource(_StubSource):
            def get_table_schemas(self) -> dict[str, Any]:
                return {"orders": TableSchema(columns=[Column("column1", "")])}

        report = check_schema_drift(_contract("[orders]"), adapter, _OddSource())
        assert report.ok
