"""Every declaration is compared, or it is accounted for. Never neither.

Two review rounds on #90 found the same class three times, each in freshly
written code and twice inside the previous round's fix:

- the contract's own semantic source was never loaded, so a digest-pinned
  contract compared zero columns and reported clean;
- the per-table lookup matched exactly while the guard added to catch that
  matched case-insensitively, so the guard silenced itself;
- a live table reporting no columns turned every declaration into a false
  finding rather than admitting nothing was compared.

Three instances of one defect means the instrument is wrong, not the attention
-- the same conclusion #89 reached after three rounds of per-site patching. So
this file stops enumerating ways a declaration can go missing and asserts the
conservation law they all violate:

    A report is `ok` only if it compared **every** column the semantic source
    declares for a table the contract allows. Anything it could not compare
    lands in `drifts` or `unchecked`, and `ok` is False.

The count is known by construction rather than recomputed from the contract, so
the test cannot inherit the very case-folding bug it exists to catch. Each
variant declares `_DECLARED` columns for one allowed table; how the two sides
*spell* that table is what varies, and the answer is `_DECLARED` every time.

`ok` is the CI predicate the module docstring tells callers to gate on. If it
can be True while declarations went uncompared, the gate is decorative — which
is precisely the defect this module was written to eliminate, reappearing one
level up in the thing that reports it.

**Verified by mutation, not asserted.** A green gate proves nothing until you
have watched it go red. Every fix from all four review rounds was reverted in
turn and this file re-run:

===================================  ===========================
reverted fix                         variants failing
===================================  ===========================
casefolded declaration lookup        160
contract's own source loaded         160
table name matched case-folded       80
schema case-retry reached            40
source read wrapped                  4
empty allow-list not short-circuit   2
colliding source keys recorded       1
wildcard exempt from key guard       1
absent table counted as checked      1
empty-live-columns guard             1
===================================  ===========================

Every axis in this file exists because that exercise scored **0** for something:

- `_PROVENANCE` -- with a source passed explicitly every time, reverting the
  contract-source fallback left all 100 variants green.
- `_SHAPES` and `_CONTRACT_SHAPES` -- with only spelling axes, all three of
  round 3's findings scored 0.
- the raising catalogs in `_CATALOGS`, and the `absent_table` contract shape --
  round 4's, likewise.

Two laws are stated here, not one, and the second exists because the first
cannot see the second class of defect. *Never report clean while having
compared nothing* catches a silent pass; it says nothing about a report that
invents a problem. The dead case-retry produced an honest-looking `unchecked`
entry for a wholly correct contract, satisfied the first law, and was caught
only once the second law -- *a correct contract against a reachable warehouse
comes back `ok`* -- stopped allowing `unchecked` as an outcome there.

Scope: this file gates `check_schema_drift`. The `describe_table` note is the
other half of the same fix and is held by `tests/test_tools/
test_describe_table_drift.py`.
"""

from __future__ import annotations

import itertools
from typing import Any

import pytest

from agentic_data_contracts.adapters.base import Column, TableSchema
from agentic_data_contracts.adapters.duckdb import DuckDBAdapter
from agentic_data_contracts.core.contract import DataContract
from agentic_data_contracts.validation.drift import UncheckedTable, check_schema_drift

#: One real column and one phantom, so a variant that compares nothing and one
#: that compares everything give visibly different reports.
_COLUMNS = ["id", "column1"]
_DECLARED = len(_COLUMNS)

#: How the two sides may spell the same table. Warehouses disagree about the
#: case of unquoted identifiers, and a contract and a semantic source are
#: authored by different people at different times.
_SPELLINGS = {
    "asis": lambda s: s,
    "upper": lambda s: s.upper(),
    "lower": lambda s: s.lower(),
    "title": lambda s: s.title(),
}


def _contract(
    schema: str, table: str, inline: tuple[str, str, list[str]] | None = None
) -> DataContract:
    """The contract, optionally carrying its own inline semantic source.

    The inline form is the shape a digest-pinned contract takes -- declarations
    frozen into the contract itself rather than read from a file beside it --
    and it is the path where the source is loaded rather than passed. Round 1's
    finding lived exactly there, so the matrix has to reach it.
    """
    source_block = ""
    if inline is not None:
        src_schema, src_table, columns = inline
        cols = "\n".join(f"            - name: {c}" for c in columns)
        source_block = f"""  source:
    type: yaml
    inline:
      metrics: []
      tables:
        - schema: {src_schema}
          table: {src_table}
          columns:
{cols}
"""
    return DataContract.from_yaml_string(
        f"""
version: "1.0"
name: accounting
semantic:
{source_block}  allowed_tables:
    - schema: {schema}
      tables: [{table}]
"""
    )


class _Source:
    """A `SemanticSource` declaring the given columns under one chosen key."""

    def __init__(self, key: str, columns: list[str]) -> None:
        self._key = key
        self._columns = columns

    def get_table_schemas(self) -> dict[str, Any]:
        return {self._key: TableSchema(columns=[Column(c, "") for c in self._columns])}

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


class _Catalog:
    """An adapter whose catalog answers in a chosen case.

    Real warehouses differ: Snowflake upper-cases unquoted identifiers, DuckDB
    preserves them. `list_tables` takes the schema as an *argument*, so a case
    difference there cannot be folded client-side -- it just returns nothing,
    which is the shape that made a wholly correct contract report every table
    missing.
    """

    dialect = "duckdb"

    def __init__(
        self, inner: DuckDBAdapter, spelling: str, *, raises: bool = False
    ) -> None:
        self._inner = inner
        self._case = _SPELLINGS[spelling]
        self._raises = raises

    def list_tables(self, schema: str) -> list[str]:
        if schema != self._case(schema):
            # BigQuery and Snowflake raise for an unknown dataset rather than
            # answering `[]`. Round 4 found the case-retry dead for exactly
            # those adapters, and this double could not see it because it only
            # ever returned `[]`.
            if self._raises:
                raise RuntimeError(f"Schema '{schema}' does not exist")
            return []
        return [self._case(t) for t in self._inner.list_tables(schema.lower())]

    def describe_table(self, schema: str, table: str) -> TableSchema:
        return self._inner.describe_table(schema.lower(), table.lower())

    def execute(self, sql: str) -> Any:
        return self._inner.execute(sql)

    def explain(self, sql: str) -> Any:
        return self._inner.explain(sql)


@pytest.fixture
def duckdb() -> DuckDBAdapter:
    db = DuckDBAdapter(":memory:")
    db.connection.execute(
        """
        CREATE SCHEMA IF NOT EXISTS main;
        CREATE TABLE main.orders (id INTEGER, amount DECIMAL(10,2));
        """
    )
    return db


#: A *catalog* may answer in only three ways, because SQL identifier folding
#: has only three behaviours in the wild: fold to upper (Snowflake, Oracle,
#: standard SQL), fold to lower (Postgres), or match case-insensitively
#: (DuckDB). `title` is deliberately absent here while present on the other two
#: axes: a catalog answering only to `Main` is case-*sensitive*, and a contract
#: saying `main` is then pointing at a schema that genuinely does not exist --
#: `missing_schema` is the correct answer there, not a false negative. Guessing
#: spellings can never be complete, so the code guesses only the two foldings
#: that exist and this axis says so.
#: Paired with whether the catalog *raises* for a name it does not know, which
#: is the other half of how a warehouse can answer.
_CATALOGS: dict[str, tuple[str, bool]] = {
    "asis": ("asis", False),
    "upper": ("upper", False),
    "lower": ("lower", False),
    "upper_raises": ("upper", True),
    "lower_raises": ("lower", True),
}

#: Where the declarations come from. `create_tools` loads the contract's own
#: source when the caller passes none, and this check must agree -- round 1's
#: finding was that it did not, and a matrix that always passes a source
#: explicitly cannot see that. Verified by mutation: reverting the fallback
#: leaves every variant of the three-axis matrix passing.
_PROVENANCE = ["argument", "contract"]

_VARIANTS = list(itertools.product(_SPELLINGS, _SPELLINGS, _CATALOGS, _PROVENANCE))


def _check(
    duckdb: DuckDBAdapter,
    contract_case: str,
    source_case: str,
    catalog_case: str,
    provenance: str,
    columns: list[str],
) -> Any:
    """One variant, with the declarations reaching the check either way."""
    spell = _SPELLINGS[source_case]
    schema, table = spell("main"), spell("orders")
    inline = (schema, table, columns) if provenance == "contract" else None
    contract = _contract(
        _SPELLINGS[contract_case]("main"),
        _SPELLINGS[contract_case]("orders"),
        inline,
    )
    source = None if provenance == "contract" else _Source(f"{schema}.{table}", columns)
    spelling, raises = _CATALOGS[catalog_case]
    return check_schema_drift(
        contract, _Catalog(duckdb, spelling, raises=raises), source
    )


def test_the_matrix_is_large_enough_to_be_a_real_gate() -> None:
    """Guards the generator: an empty variant list would pass vacuously."""
    assert len(_VARIANTS) == (len(_SPELLINGS) ** 2 * len(_CATALOGS) * len(_PROVENANCE))
    assert len(_VARIANTS) > 50
    # Both provenances must actually appear, or the axis is decorative.
    assert {v[3] for v in _VARIANTS} == set(_PROVENANCE)
    # A catalog that raises must be exercised, or the case-retry is untested
    # against the adapters whose behaviour motivated it.
    assert any(_CATALOGS[v[2]][1] for v in _VARIANTS)


@pytest.mark.parametrize(
    ("contract_case", "source_case", "catalog_case", "provenance"),
    _VARIANTS,
    ids=lambda c: c,
)
def test_a_clean_report_compared_every_declaration(
    duckdb: DuckDBAdapter,
    contract_case: str,
    source_case: str,
    catalog_case: str,
    provenance: str,
) -> None:
    """The conservation law, over every way the three sides may disagree on case.

    A variant may legitimately fail to find the table -- that is a `missing_table`
    or `missing_schema` drift, and `ok` is False, which is an honest answer. What
    it may never do is report `ok` while having compared fewer than `_DECLARED`
    columns. Silence and success must not be the same output.
    """
    report = _check(
        duckdb, contract_case, source_case, catalog_case, provenance, _COLUMNS
    )
    if report.ok:
        pytest.fail(
            f"{contract_case}/{source_case}/{catalog_case}/{provenance}: reported"
            f" ok, but 'column1' does not exist — {report.summary()}"
        )
    assert report.columns_checked in (0, _DECLARED), report.summary()
    if report.columns_checked == 0:
        # Nothing compared is allowed only when the report says why.
        assert report.drifts or report.unchecked, report.summary()
    # The same law for tables: the one declared table either reached a verdict
    # or is named as unreachable. "Checked 0 tables: 1 drifted" reads like a run
    # that checked nothing, which is the one thing these counters rule out.
    per_table_unchecked = [u for u in report.unchecked if u.schema]
    assert report.tables_checked + len(per_table_unchecked) == 1, report.summary()


@pytest.mark.parametrize(
    ("contract_case", "source_case", "catalog_case", "provenance"),
    _VARIANTS,
    ids=lambda c: c,
)
def test_a_declaration_is_never_dropped_in_silence(
    duckdb: DuckDBAdapter,
    contract_case: str,
    source_case: str,
    catalog_case: str,
    provenance: str,
) -> None:
    """The same law stated the other way: an agreeing contract must come back
    clean, not merely non-failing.

    Here every declared column really does exist, so any variant that reports a
    drift is inventing one -- the false-finding direction of the same defect,
    which sends an author to delete correct declarations.
    """
    report = _check(
        duckdb,
        contract_case,
        source_case,
        catalog_case,
        provenance,
        ["id", "amount"],  # both real
    )
    # Every catalog in this matrix answers under *some* spelling, and every
    # declared column exists, so there is no honest way for this to be anything
    # but clean. Allowing an `unchecked` escape here is what let the dead
    # case-retry through: a wrong diagnosis is not a silent pass, so the
    # conservation law above cannot see it -- only this one can.
    assert report.ok, report.summary()
    assert report.columns_checked == _DECLARED, report.summary()


class _Colliding(_Source):
    """Two keys differing only in case. Folding the lookup makes one win."""

    def get_table_schemas(self) -> dict[str, Any]:
        return {
            self._key.lower(): TableSchema(columns=[Column(self._columns[0], "")]),
            self._key.upper(): TableSchema(columns=[Column(self._columns[-1], "")]),
        }


class _Raising(_Source):
    """A source that parses lazily and fails on read."""

    def get_table_schemas(self) -> dict[str, Any]:
        raise RuntimeError("manifest corrupt")


class _Disjoint(_Source):
    """Keys shaped so that nothing the contract allows can ever match."""

    def get_table_schemas(self) -> dict[str, Any]:
        return {
            f"proj:{self._key}": TableSchema(
                columns=[Column(c, "") for c in self._columns]
            )
        }


#: How the *source* may be shaped, independent of how anything is spelled. Only
#: `plain` can legitimately produce a clean report; each of the others leaves
#: declarations uncompared, and the law says that must never read as `ok`.
#:
#: This axis exists because the mutation exercise scored 0 for all three on the
#: four-axis matrix above -- they were caught only by targeted tests, which is
#: how the previous two rounds' defects were caught too, and is exactly the
#: coverage that did not converge.
_SHAPES: dict[str, tuple[type[_Source], bool]] = {
    "plain": (_Source, True),
    "colliding": (_Colliding, False),
    "raising": (_Raising, False),
    "disjoint": (_Disjoint, False),
}


def _empty_allow_list() -> DataContract:
    """A contract allowing nothing, with a source that declares plenty.

    Its own axis because the zero-overlap guard reads the allow-list, and an
    empty one used to short-circuit it -- so the same zero comparisons that
    hard-failed with a populated allow-list came back clean with an empty one.
    Nothing distinguishes those two for a reader.
    """
    return DataContract.from_yaml_string(
        """
version: "1.0"
name: no-tables
semantic:
  allowed_tables: []
"""
    )


#: How the *contract* may be shaped. Only a contract that names tables can
#: produce a clean report against a source that describes them.
def _wildcard() -> DataContract:
    """A contract whose tables are an unresolved `*`.

    Its own axis because the zero-overlap guard reads *resolved* names: a
    wildcard contributes none, so every declaration read as outside the
    allow-list and the report grew a fabricated key-convention mismatch beside
    the true wildcard finding.
    """
    return DataContract.from_yaml_string(
        """
version: "1.0"
name: wildcard
semantic:
  allowed_tables:
    - schema: main
      tables: ["*"]
"""
    )


_CONTRACT_SHAPES: dict[str, tuple[Any, bool]] = {
    "normal": (lambda: _contract("main", "orders"), True),
    "empty_allow_list": (_empty_allow_list, False),
    "wildcard": (_wildcard, False),
    # A table that is simply not there. Its own axis because nothing else in
    # this file produces a `missing_table`, so the verdict counter went
    # untested on the one path where it reads most wrongly.
    "absent_table": (lambda: _contract("main", "ghosts"), False),
}


def test_a_table_found_absent_is_counted_as_checked(duckdb: DuckDBAdapter) -> None:
    """`Checked 0 tables (0 columns): 1 drifted` reads like a run that checked
    nothing, which is the single thing these counters exist to rule out. The
    verdict was "absent", and reaching it is a check."""
    report = check_schema_drift(
        _contract("main", "ghosts"),
        _Catalog(duckdb, "asis"),
        _Source("main.ghosts", ["id"]),
    )
    assert [d.kind for d in report.drifts] == ["missing_table"]
    assert report.tables_checked == 1
    assert "Checked 1 table" in report.summary()


def test_a_wildcard_does_not_also_fabricate_a_key_mismatch(
    duckdb: DuckDBAdapter,
) -> None:
    """Not `ok` is right; two reasons, one of them invented, is not.

    A report that names a problem the reader does not have costs more than one
    that names nothing -- they will go looking for it.
    """
    report = check_schema_drift(
        _wildcard(), _Catalog(duckdb, "asis"), _Source("main.orders", ["id"])
    )
    assert not report.ok
    assert [u.reason for u in report.unchecked] == [UncheckedTable.UNRESOLVED_WILDCARD]


@pytest.mark.parametrize("contract_shape", list(_CONTRACT_SHAPES), ids=lambda s: s)
@pytest.mark.parametrize("shape", list(_SHAPES), ids=lambda s: s)
def test_only_a_fully_compared_source_can_report_clean(
    duckdb: DuckDBAdapter, shape: str, contract_shape: str
) -> None:
    """The conservation law over source and contract *shape*, not spelling.

    Every column declared here really exists, so there is no drift to find. The
    question is whether a run that could not compare everything is allowed to
    come back `ok` — and it never is. When it is not `ok`, the report must say
    why rather than merely failing.
    """
    source_factory, source_ok = _SHAPES[shape]
    contract_factory, contract_ok = _CONTRACT_SHAPES[contract_shape]
    report = check_schema_drift(
        contract_factory(),
        _Catalog(duckdb, "asis"),
        source_factory("main.orders", ["id", "amount"]),
    )
    assert report.ok is (source_ok and contract_ok), report.summary()
    if not report.ok:
        assert report.unchecked, report.summary()
        assert all(u.reason for u in report.unchecked), report.summary()


def test_every_shape_is_exercised() -> None:
    """Guards the axes: a dict trimmed to the passing case is vacuous."""
    assert len(_SHAPES) >= 4
    assert sum(1 for _, ok in _SHAPES.values() if not ok) >= 3
    assert sum(1 for _, ok in _CONTRACT_SHAPES.values() if not ok) >= 1


class TestAdapterDegradation:
    """The same law where the failure is the adapter's, not a spelling's."""

    @staticmethod
    def _report(inner: Any) -> Any:
        return check_schema_drift(
            _contract("main", "orders"), inner, _Source("main.orders", _COLUMNS)
        )

    def test_a_table_that_cannot_be_introspected_is_never_ok(
        self, duckdb: DuckDBAdapter
    ) -> None:
        class _Opaque(_Catalog):
            def describe_table(self, schema: str, table: str) -> TableSchema:
                return TableSchema(columns=[])

        report = self._report(_Opaque(duckdb, "asis"))
        assert not report.ok
        assert report.columns_checked == 0

    def test_a_table_that_raises_on_introspection_is_never_ok(
        self, duckdb: DuckDBAdapter
    ) -> None:
        class _Refusing(_Catalog):
            def describe_table(self, schema: str, table: str) -> TableSchema:
                raise RuntimeError("permission denied")

        report = self._report(_Refusing(duckdb, "asis"))
        assert not report.ok
        assert report.columns_checked == 0

    def test_a_catalog_that_raises_is_never_ok(self, duckdb: DuckDBAdapter) -> None:
        class _Broken(_Catalog):
            def list_tables(self, schema: str) -> list[str]:
                raise RuntimeError("connection refused")

        report = self._report(_Broken(duckdb, "asis"))
        assert not report.ok
        assert report.columns_checked == 0
