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

    def test_a_table_key_without_a_schema_is_reported_not_crashed(
        self, adapter: DuckDBAdapter
    ) -> None:
        """Every bundled source keys `get_table_schemas()` as `schema.table`,
        but nothing in the protocol says so. An unqualified key must not raise
        -- and must not pass silently either.

        The first cut of this test asserted `report.ok` here, which blessed the
        defect: the check compared no columns and said everything was fine. The
        key convention gap is now the finding.
        """

        class _OddSource(_StubSource):
            def get_table_schemas(self) -> dict[str, Any]:
                return {"orders": TableSchema(columns=[Column("column1", "")])}

        report = check_schema_drift(_contract("[orders]"), adapter, _OddSource())
        assert not report.has_drift
        assert [u.qualified for u in report.unchecked] == ["the contract"]


class TestContractDeclaredSource:
    """Review finding: the check ignored the source the contract itself names.

    Every other entry point falls back to `contract.load_semantic_source()` when
    the caller passes none -- `create_tools` does exactly that. This one did
    not, so a contract carrying its own source (including the inline frozen
    snapshot this module's docstring is built around), checked the obvious CI
    way, compared zero columns and reported a clean bill of health. That is the
    silent pass the module exists to eliminate, arrived at by a different road.
    """

    @staticmethod
    def _inline_contract(columns: list[str]) -> DataContract:
        cols = "\n".join(f"            - name: {c}" for c in columns)
        return DataContract.from_yaml_string(
            f"""
version: "1.0"
name: inline-source
semantic:
  source:
    type: yaml
    inline:
      metrics: []
      tables:
        - schema: main
          table: orders
          columns:
{cols}
  allowed_tables:
    - schema: main
      tables: [orders]
"""
        )

    def test_the_contracts_own_source_is_loaded_when_none_is_passed(
        self, adapter: DuckDBAdapter
    ) -> None:
        report = check_schema_drift(self._inline_contract(["phantom"]), adapter)
        assert report.columns_checked == 1
        assert [d.column for d in report.drifts] == ["phantom"]

    def test_an_explicit_source_still_wins(self, adapter: DuckDBAdapter) -> None:
        """The argument is an override, not a supplement -- same precedence as
        `create_tools`, so the two cannot disagree about what was enforced."""
        report = check_schema_drift(
            self._inline_contract(["phantom"]),
            adapter,
            _source(
                [{"schema": "main", "table": "orders", "columns": [{"name": "id"}]}]
            ),
        )
        assert report.ok

    def test_a_source_that_cannot_be_loaded_is_unchecked_not_clean(
        self, adapter: DuckDBAdapter
    ) -> None:
        """A contract naming a file that is not there checked nothing. Passing
        would be the same lie as before, one layer along."""
        contract = DataContract.from_yaml_string(
            """
version: "1.0"
name: absent-source
semantic:
  source:
    type: dbt
    path: "./nowhere/manifest.json"
  allowed_tables:
    - schema: main
      tables: [orders]
"""
        )
        report = check_schema_drift(contract, adapter)
        assert not report.ok
        assert not report.has_drift
        assert len(report.unchecked) == 1
        assert "manifest.json" in report.unchecked[0].reason


class TestIdentifierCase:
    """Review finding: columns were folded for case, tables and schemas were not.

    A warehouse whose catalog returns unquoted identifiers upper-cased
    (Snowflake) would make a wholly correct lower-case contract report every
    table missing -- a CI failure with a wrong diagnosis, and no columns checked
    behind it. Unlike a column, a table-name case difference breaks nothing:
    the adapter resolves it at the database. So it is matched loosely and not
    reported.
    """

    @staticmethod
    def _shouting_adapter(adapter: DuckDBAdapter) -> Any:
        class _Shouting:
            dialect = "duckdb"

            def list_tables(self, schema: str) -> list[str]:
                # Only answers to the upper-cased schema, and shouts back.
                if schema != schema.upper():
                    return []
                return [t.upper() for t in adapter.list_tables(schema.lower())]

            def describe_table(self, schema: str, table: str) -> TableSchema:
                return adapter.describe_table(schema.lower(), table.lower())

            def execute(self, sql: str) -> Any:
                return adapter.execute(sql)

            def explain(self, sql: str) -> Any:
                return adapter.explain(sql)

        return _Shouting()

    def test_a_table_differing_only_in_case_is_not_missing(
        self, adapter: DuckDBAdapter
    ) -> None:
        report = check_schema_drift(
            _contract("[orders]"), self._shouting_adapter(adapter)
        )
        assert report.ok
        assert report.tables_checked == 1

    def test_columns_are_still_reached_through_a_case_folded_table(
        self, adapter: DuckDBAdapter
    ) -> None:
        """The table match must yield the *live* spelling, or the follow-up
        `describe_table` asks for a name the warehouse does not know and the
        column check silently compares against nothing."""
        source = _source(
            [
                {
                    "schema": "main",
                    "table": "orders",
                    "columns": [{"name": "id"}, {"name": "column1"}],
                }
            ]
        )
        report = check_schema_drift(
            _contract("[orders]"), self._shouting_adapter(adapter), source
        )
        assert report.columns_checked == 2
        assert [d.column for d in report.drifts] == ["column1"]


class TestKeyConventionMismatch:
    def test_a_source_describing_nothing_the_contract_allows_is_flagged_once(
        self, adapter: DuckDBAdapter
    ) -> None:
        """Review finding: a table with no declarations found is indistinguishable
        from one that declares none, and both read as `ok`.

        Per-table this cannot be told apart -- a source legitimately describing
        3 of 10 allowed tables is the normal case, and flagging the other 7
        would make the gate useless. But a source whose keys overlap the
        allow-list *not at all* is a systematic mismatch, not seven absences:
        a third-party source keying `project.dataset.table`, or a schema name
        spelled differently on the two sides. One O(1) check over the whole
        output, so it cannot fire per-table.
        """

        class _OtherConvention(_StubSource):
            def get_table_schemas(self) -> dict[str, Any]:
                return {"proj:main.orders": TableSchema(columns=[Column("id", "")])}

        report = check_schema_drift(_contract("[orders]"), adapter, _OtherConvention())
        assert not report.ok
        assert not report.has_drift
        assert len(report.unchecked) == 1

    def test_a_partial_overlap_is_the_normal_case_and_stays_ok(
        self, adapter: DuckDBAdapter
    ) -> None:
        """A source describing some allowed tables and not others is what every
        real contract looks like."""
        source = _source(
            [{"schema": "main", "table": "orders", "columns": [{"name": "id"}]}]
        )
        report = check_schema_drift(_contract(), adapter, source)
        assert report.ok

    def test_a_source_describing_no_tables_at_all_is_not_flagged(
        self, adapter: DuckDBAdapter
    ) -> None:
        """Nothing declared is nothing to mismatch. Only a source that describes
        tables, none of them the contract's, is evidence of a convention gap."""
        report = check_schema_drift(_contract("[orders]"), adapter, _StubSource())
        assert report.ok


class TestFailureFanOut:
    def test_one_unreachable_schema_is_one_finding_not_one_per_table(
        self, adapter: DuckDBAdapter
    ) -> None:
        """Review finding: the same fan-out `missing_schema` deliberately
        collapses. A schema with 200 allowed tables produced 200 identical
        "connection refused" lines, burying everything else in the report."""

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
            _contract("[a, b, c, d, e]"),
            _BrokenAdapter(),  # type: ignore[arg-type]
        )
        assert len(report.unchecked) == 1
        assert "connection refused" in report.unchecked[0].reason
        # The count is not lost by collapsing it.
        assert "5" in report.unchecked[0].reason


class TestDeclarationLookupCase:
    def test_a_source_keyed_in_another_case_still_has_its_columns_checked(
        self, adapter: DuckDBAdapter
    ) -> None:
        """Review finding: the per-table lookup was exact while the guard meant
        to catch a key mismatch casefolded — so the guard silenced itself.

        A source keyed `MAIN.ORDERS` against a contract allowing `main.orders`
        satisfied `_key_convention_mismatch` *and* missed every per-table
        lookup: zero columns compared, nothing unchecked, `ok` True. The two
        halves of the same question have to fold the same way.
        """

        class _Shouting(_StubSource):
            def get_table_schemas(self) -> dict[str, Any]:
                return {
                    "MAIN.ORDERS": TableSchema(
                        columns=[Column("id", ""), Column("column1", "")]
                    )
                }

        report = check_schema_drift(_contract("[orders]"), adapter, _Shouting())
        assert report.columns_checked == 2
        assert [d.column for d in report.drifts] == ["column1"]

    def test_one_matching_key_does_not_silence_the_guard_for_the_rest(
        self, adapter: DuckDBAdapter
    ) -> None:
        """The partial-overlap variant, which is worse than the total one: a
        single matching key satisfied `any(...)` and let every mismatched key
        through unexamined."""

        class _Mixed(_StubSource):
            def get_table_schemas(self) -> dict[str, Any]:
                return {
                    "main.orders": TableSchema(columns=[Column("id", "")]),
                    "MAIN.CUSTOMERS": TableSchema(columns=[Column("column9", "")]),
                }

        report = check_schema_drift(_contract(), adapter, _Mixed())
        assert [d.column for d in report.drifts] == ["column9"]


class TestUnintrospectableTable:
    def test_a_live_table_reporting_no_columns_is_unchecked_not_all_drift(
        self, adapter: DuckDBAdapter
    ) -> None:
        """Review finding: the same fan-out collapsed everywhere else.

        `describe_table` returning nothing for a table `list_tables` just named
        — a view the connection cannot introspect, a permissions-restricted
        table, an adapter quirk — turned every declaration into a bogus
        `missing_column` telling the author to delete correct declarations.
        `_stale_declaration_note` already guards exactly this; the preflight did
        not. It is `unchecked` rather than one drift: nothing was compared, so
        there is no evidence the declarations are wrong.
        """

        class _Opaque:
            dialect = "duckdb"

            def list_tables(self, schema: str) -> list[str]:
                return adapter.list_tables(schema)

            def describe_table(self, schema: str, table: str) -> TableSchema:
                return TableSchema(columns=[])

            def execute(self, sql: str) -> Any:
                return adapter.execute(sql)

            def explain(self, sql: str) -> Any:
                return adapter.explain(sql)

        source = _source(
            [
                {
                    "schema": "main",
                    "table": "orders",
                    "columns": [{"name": "id"}, {"name": "amount"}],
                }
            ]
        )
        report = check_schema_drift(
            _contract("[orders]"),
            _Opaque(),  # type: ignore[arg-type]
            source,
        )
        assert not report.has_drift
        assert [u.qualified for u in report.unchecked] == ["main.orders"]
        assert report.columns_checked == 0


class TestSchemaRetryDiagnosis:
    def test_an_empty_schema_on_a_raising_adapter_is_still_missing_schema(
        self, adapter: DuckDBAdapter
    ) -> None:
        """Review finding: the case-retry could convert a correct diagnosis into
        a wrong one.

        BigQuery- and Snowflake-style adapters raise for an absent dataset
        rather than returning `[]`. An *existing but empty* schema answered the
        first spelling with `[]`, and the retry's exception was then reported as
        a connection failure — sending the reader after a problem that is not
        there. Only the first spelling's failure is a real one.
        """

        class _Raising:
            dialect = "duckdb"

            def list_tables(self, schema: str) -> list[str]:
                if schema != "main":
                    raise RuntimeError(f"NotFound: schema {schema}")
                return []  # exists, but empty

            def describe_table(self, schema: str, table: str) -> TableSchema:
                raise AssertionError("should not be reached")

            def execute(self, sql: str) -> Any:
                raise AssertionError("should not be reached")

            def explain(self, sql: str) -> Any:
                raise AssertionError("should not be reached")

        report = check_schema_drift(
            _contract("[orders]"),
            _Raising(),  # type: ignore[arg-type]
        )
        assert report.unchecked == []
        assert _kinds(report) == ["missing_schema"]

    def test_the_first_spellings_failure_is_still_reported(
        self, adapter: DuckDBAdapter
    ) -> None:
        """A genuine connection failure must not be swallowed by the retry."""

        class _Broken:
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
            _Broken(),  # type: ignore[arg-type]
        )
        assert not report.has_drift
        assert "connection refused" in report.unchecked[0].reason


class TestCollidingSourceKeys:
    def test_a_key_lost_to_a_case_collision_is_unchecked_not_silent(
        self, adapter: DuckDBAdapter
    ) -> None:
        """Review finding: folding the lookup introduced a way to drop a key.

        `setdefault(key.casefold(), ...)` makes the first spelling win and the
        loser vanish — one whole table's declarations never compared, and the
        report clean. Reachable: `OssieSource` drops the database qualifier when
        building keys and warns only on an *exact* collision, so
        `db1.main.orders` and `db2.MAIN.ORDERS` both register.

        The fix for round 2's finding created this one, which is the same
        pattern a third time: a guard that introduces the gap it closes.
        """

        class _Colliding(_StubSource):
            def get_table_schemas(self) -> dict[str, Any]:
                return {
                    "main.orders": TableSchema(columns=[Column("id", "")]),
                    "MAIN.ORDERS": TableSchema(columns=[Column("amount", "")]),
                }

        report = check_schema_drift(_contract("[orders]"), adapter, _Colliding())
        assert not report.ok
        assert not report.has_drift
        assert any("MAIN.ORDERS" in u.reason for u in report.unchecked)


class TestSourceThatRaises:
    def test_a_source_raising_on_read_is_unchecked_not_a_crash(
        self, adapter: DuckDBAdapter
    ) -> None:
        """Review finding: the try wrapped the *load*, not the *read*.

        Every other failure mode here becomes an `UncheckedTable` — the adapter
        raising, the contract's source failing to load, a table that cannot be
        introspected. A source whose `get_table_schemas()` raises escaped and
        crashed the preflight instead. `SemanticSource` is a structural protocol
        by design, so a third-party source reading lazily is a supported shape.
        """

        class _Corrupt(_StubSource):
            def get_table_schemas(self) -> dict[str, Any]:
                raise RuntimeError("manifest corrupt")

        report = check_schema_drift(_contract("[orders]"), adapter, _Corrupt())
        assert not report.ok
        assert any("manifest corrupt" in u.reason for u in report.unchecked)


class TestDisjointAllowList:
    def test_an_empty_allow_list_with_declarations_is_not_a_pass(
        self, adapter: DuckDBAdapter
    ) -> None:
        """Review finding: the two directions of the same situation disagreed.

        A source describing tables none of which are allowed was a hard fail —
        unless the allow-list was *empty*, where `if not allowed` short-circuited
        the guard and the same zero comparisons became a clean pass. Nothing
        distinguishes those cases for a reader, so they must not differ.
        """
        empty = DataContract.from_yaml_string(
            """
version: "1.0"
name: no-tables
semantic:
  allowed_tables: []
"""
        )
        source = _source(
            [{"schema": "main", "table": "orders", "columns": [{"name": "phantom"}]}]
        )
        report = check_schema_drift(empty, adapter, source)
        assert not report.ok
        assert report.columns_checked == 0

    def test_the_reason_does_not_assert_a_cause_it_cannot_know(
        self, adapter: DuckDBAdapter
    ) -> None:
        """A contract allowing tables documented by warehouse comments, whose
        source documents a different schema entirely, is a legitimate config
        with no defect. The check cannot tell it apart from a key-convention
        mismatch, so the message must name both readings rather than send the
        reader after the wrong one."""
        contract = _contract("[events]", schema="raw")
        adapter.connection.execute(
            "CREATE SCHEMA IF NOT EXISTS raw; CREATE TABLE raw.events (id INTEGER)"
        )
        source = _source(
            [{"schema": "main", "table": "orders", "columns": [{"name": "id"}]}]
        )
        reason = check_schema_drift(contract, adapter, source).unchecked[0].reason
        assert "outside the contract's allow-list" in reason
        assert "key tables differently" in reason


class TestRaisingCatalogCaseRetry:
    """Review finding: the case-retry was dead for the adapters it names.

    Round 2 asked that a *retry's* exception not be reported, since an
    existing-but-empty schema answers `[]` and the retry then raises. The fix --
    re-raise at index 0 -- made the retry unreachable for the adapters whose
    behaviour motivated it: BigQuery and Snowflake raise for an unknown schema,
    so a lower-case contract against an upper-case catalog never reached the
    second spelling at all.

    Both are satisfied by trying every spelling and reporting the declared
    one's failure only when *none* succeeded. A raise no longer means "give up";
    it means "not under that name".
    """

    @staticmethod
    def _snowflake(adapter: DuckDBAdapter, *, empty: bool = False) -> Any:
        class _Snowflake:
            dialect = "duckdb"

            def list_tables(self, schema: str) -> list[str]:
                if schema != schema.upper():
                    raise RuntimeError(f"Schema '{schema}' does not exist")
                if empty:
                    return []
                return [t.upper() for t in adapter.list_tables(schema.lower())]

            def describe_table(self, schema: str, table: str) -> TableSchema:
                return adapter.describe_table(schema.lower(), table.lower())

            def execute(self, sql: str) -> Any:
                return adapter.execute(sql)

            def explain(self, sql: str) -> Any:
                return adapter.explain(sql)

        return _Snowflake()

    def test_a_raising_catalog_is_still_retried_in_the_other_case(
        self, adapter: DuckDBAdapter
    ) -> None:
        source = _source(
            [{"schema": "main", "table": "orders", "columns": [{"name": "id"}]}]
        )
        report = check_schema_drift(
            _contract("[orders]"), self._snowflake(adapter), source
        )
        assert report.ok, report.summary()
        assert report.columns_checked == 1

    def test_an_existing_but_empty_schema_is_still_missing_schema(
        self, adapter: DuckDBAdapter
    ) -> None:
        """Round 2's finding must keep holding: reaching the schema and finding
        it empty is a drift, not a connection failure."""
        report = check_schema_drift(
            _contract("[orders]"), self._snowflake(adapter, empty=True)
        )
        assert report.unchecked == []
        assert _kinds(report) == ["missing_schema"]

    def test_a_failure_under_every_spelling_is_still_reported(
        self, adapter: DuckDBAdapter
    ) -> None:
        class _Broken:
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
            _Broken(),  # type: ignore[arg-type]
        )
        assert not report.has_drift
        assert "connection refused" in report.unchecked[0].reason


class TestWildcardAndKeyGuard:
    def test_an_unresolved_wildcard_does_not_fake_a_key_mismatch(
        self, adapter: DuckDBAdapter
    ) -> None:
        """Review finding: the zero-overlap guard reads *resolved* table names,
        so an unresolved `*` contributes nothing and every declaration reads as
        outside the allow-list — a key-convention mismatch that does not exist.

        The wildcard already makes `ok` False and says why; a second, wrong
        explanation beside it sends the reader after the wrong problem.
        """
        source = _source(
            [{"schema": "main", "table": "orders", "columns": [{"name": "id"}]}]
        )
        report = check_schema_drift(_contract('["*"]'), adapter, source)
        assert not report.ok
        assert [u.reason for u in report.unchecked] == [
            UncheckedTable.UNRESOLVED_WILDCARD
        ]


class TestVerdictAccounting:
    def test_a_table_found_absent_still_counts_as_checked(
        self, adapter: DuckDBAdapter
    ) -> None:
        """Review finding: `Checked 0 tables (0 columns): 1 drifted` read like a
        run that checked nothing, which is the one thing the counter exists to
        rule out. Existence *was* verified — the verdict was "absent"."""
        report = check_schema_drift(_contract("[ghosts]"), adapter)
        assert _kinds(report) == ["missing_table"]
        assert report.tables_checked == 1

    def test_every_declared_table_is_either_counted_or_unchecked(
        self, adapter: DuckDBAdapter
    ) -> None:
        """The conservation law for tables, next to the one for columns: a
        declared table either got a verdict or is named as unreachable."""
        report = check_schema_drift(_contract("[orders, customers, ghosts]"), adapter)
        assert report.tables_checked + len(report.unchecked) == 3
