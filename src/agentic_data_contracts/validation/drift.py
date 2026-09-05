"""Reconcile what a contract declares against what the warehouse actually has.

A semantic source can declare a column that does not exist, and until now
nothing reported it -- not at load, not at ``describe_table``, not anywhere
(#90). The declaration lives inside ``contract_digest``, so a contract stayed
"frozen" around a column the warehouse had renamed and every gate stayed green:
the declarations had not changed, the *world* had. That is the failure a data
contract exists to prevent.

This module is the preflight half of the fix. It belongs in CI, where a schema
migration trips it, rather than in an agent's turn; ``describe_table`` carries
the other half as a ``note`` on its response, so an agent handed stale
documentation at least learns that it is stale.

Three decisions worth knowing before reading the code:

**A check that could not run is not a check that passed.** An unresolved
wildcard and an adapter that raised both land in :attr:`SchemaDriftReport.
unchecked`, and :attr:`~SchemaDriftReport.ok` is False whenever anything is
there. Gate on ``ok``, not on ``has_drift`` -- otherwise a connection failure
reads as a clean bill of health, which is the silent-success shape this whole
family of defects is about.

**This never resolves a wildcard for you.** ``DataContract.resolve_tables()``
rewrites ``allowed_tables[].tables`` in place, and those bytes are inside the
canonical form that ``contract_digest`` hashes. A preflight that quietly
re-pinned the artifact it was auditing would be worse than the defect. Resolve
first if you want wildcards checked, and accept the digest movement knowingly.

**The contract's allow-list is the scope.** Columns are checked for tables the
contract allows, not for every table the semantic source happens to describe. A
dbt manifest carries every model in the project; walking all of them would mean
hundreds of warehouse round-trips to report drift in tables the agent may not
query anyway.

Type mismatches (declared ``BIGINT``, live ``VARCHAR``) are the same class of
drift and are deliberately not covered here -- names first.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    # Deferred for the same reason as validation/reconciliation.py: adapters.base
    # imports validation.explain, which initializes this package before
    # adapters.base finishes defining DatabaseAdapter. Safe at runtime because
    # `from __future__ import annotations` keeps annotations unevaluated.
    from agentic_data_contracts.adapters.base import DatabaseAdapter
    from agentic_data_contracts.core.contract import DataContract
    from agentic_data_contracts.semantic.base import SemanticSource


@dataclass(frozen=True)
class SchemaDrift:
    """One declaration contradicted by the live schema.

    ``kind`` is one of:

    ``missing_schema``
        The schema reported no tables at all while the contract declared some.
        Emitted *instead of* one ``missing_table`` per declared table, because a
        misspelt schema name produces exactly that fan-out and the reader needs
        the one fact, not N restatements of it.
    ``missing_table``
        A declared table absent from ``adapter.list_tables(schema)``.
    ``missing_column``
        A declared column absent from the live table.
    ``case_mismatch``
        A declared column that matches a live column only case-insensitively.
        Its own kind rather than a ``missing_column``: the description overlay
        in ``describe_table`` is an exact-match dict lookup, so the authored
        text really does fail to reach the agent -- but the column is right
        there under another spelling, and calling it missing sends the reader
        hunting for something that exists.
    """

    kind: str
    schema: str
    table: str
    column: str = ""
    #: For ``case_mismatch`` only: what the warehouse actually calls the column.
    live_name: str = ""

    @property
    def qualified(self) -> str:
        return f"{self.schema}.{self.table}" if self.table else self.schema

    def __str__(self) -> str:
        if self.kind == "missing_schema":
            return (
                f"{self.schema}: declared tables, but the schema reports none"
                " (misspelt schema, or nothing created yet)"
            )
        if self.kind == "missing_table":
            return f"{self.qualified}: declared table does not exist"
        if self.kind == "case_mismatch":
            return (
                f"{self.qualified}: declared column {self.column!r} exists as"
                f" {self.live_name!r} — the overlay matches exactly, so the"
                " authored description never reaches the agent"
            )
        return f"{self.qualified}: declared column {self.column!r} does not exist"


@dataclass(frozen=True)
class UncheckedTable:
    """A declaration the check could not reach a verdict on.

    Not drift, and emphatically not a pass. A connection failure is no evidence
    that a table is missing, and reporting it as drift would send someone to
    "fix" a contract that is correct.
    """

    #: Reason constant for the wildcard case, so callers can branch on identity
    #: rather than matching prose that may be reworded.
    UNRESOLVED_WILDCARD = (
        "unresolved wildcard — call contract.resolve_tables(adapter) on a copy"
        " first (it rewrites the contract and moves its digest)"
    )

    schema: str
    table: str
    reason: str


@dataclass(frozen=True)
class SchemaDriftReport:
    drifts: list[SchemaDrift] = field(default_factory=list)
    unchecked: list[UncheckedTable] = field(default_factory=list)
    #: Tables that existed and were compared. Reported so a run that checked
    #: nothing cannot be mistaken for a run that found nothing.
    tables_checked: int = 0
    columns_checked: int = 0

    @property
    def has_drift(self) -> bool:
        return bool(self.drifts)

    @property
    def ok(self) -> bool:
        """True only when every declaration was checked and none contradicted.

        This, not :attr:`has_drift`, is the CI predicate.
        """
        return not self.drifts and not self.unchecked

    def summary(self) -> str:
        head = (
            f"Checked {_plural(self.tables_checked, 'table')}"
            f" ({_plural(self.columns_checked, 'column')}):"
            f" {len(self.drifts)} drifted, {len(self.unchecked)} unchecked."
        )
        lines = [head]
        lines += [f"  drift: {d}" for d in self.drifts]
        lines += [
            f"  unchecked: {u.schema}.{u.table} — {u.reason}" for u in self.unchecked
        ]
        return "\n".join(lines)


def _plural(count: int, noun: str) -> str:
    return f"{count} {noun}" if count == 1 else f"{count} {noun}s"


def _declared_tables(
    contract: DataContract,
) -> tuple[dict[str, list[str]], list[UncheckedTable]]:
    """Declared table names grouped by schema, plus the wildcards we cannot check."""
    by_schema: dict[str, list[str]] = {}
    unchecked: list[UncheckedTable] = []
    for entry in contract.schema.semantic.allowed_tables:
        names = by_schema.setdefault(entry.schema_, [])
        for table in entry.tables:
            if table == "*":
                unchecked.append(
                    UncheckedTable(
                        schema=entry.schema_,
                        table="*",
                        reason=UncheckedTable.UNRESOLVED_WILDCARD,
                    )
                )
            elif table not in names:
                names.append(table)
    return by_schema, unchecked


def _declared_columns(source: SemanticSource | None) -> dict[str, list[str]]:
    """Declared column names per ``schema.table``, in declaration order.

    Reads through ``getattr`` because ``SemanticSource`` is a structural
    protocol: a third-party source returns its own schema-shaped objects, and
    only ``.columns`` and ``.name`` are things the protocol actually promises.
    """
    if source is None:
        return {}
    declared: dict[str, list[str]] = {}
    for key, table_schema in source.get_table_schemas().items():
        names = [
            name
            for column in (getattr(table_schema, "columns", None) or [])
            if (name := getattr(column, "name", "") or "")
        ]
        if names:
            declared[key] = names
    return declared


def check_schema_drift(
    contract: DataContract,
    adapter: DatabaseAdapter,
    semantic_source: SemanticSource | None = None,
) -> SchemaDriftReport:
    """Compare a contract's declarations against the live schema.

    Walks every table the contract allows, and — where ``semantic_source``
    declares them — every column of those tables, reporting declarations the
    warehouse contradicts. Undeclared live columns are not reported: the
    description overlay is a left join by design, and flagging every
    undocumented column would bury the finding that matters.

    The contract is never mutated. See the module docstring for why that
    matters and for how to read :attr:`SchemaDriftReport.ok`.
    """
    by_schema, unchecked = _declared_tables(contract)
    declared_columns = _declared_columns(semantic_source)
    drifts: list[SchemaDrift] = []
    tables_checked = 0
    columns_checked = 0

    for schema_name, tables in by_schema.items():
        if not tables:
            # `allowed_tables: [{schema: raw, tables: []}]` is legal and common:
            # a schema listed with nothing allowed in it yet. No declaration, so
            # nothing to contradict -- and no round-trip worth spending.
            continue
        try:
            live_tables = set(adapter.list_tables(schema_name))
        except Exception as exc:  # noqa: BLE001 - any adapter failure, same verdict
            unchecked += [
                UncheckedTable(
                    schema=schema_name,
                    table=table,
                    reason=f"{type(exc).__name__}: {exc}",
                )
                for table in tables
            ]
            continue
        if not live_tables:
            drifts.append(
                SchemaDrift(kind="missing_schema", schema=schema_name, table="")
            )
            continue
        for table in tables:
            if table not in live_tables:
                # Deliberately no per-column findings for a table that is not
                # there: `describe_table` returns an empty schema rather than
                # raising for most adapters, so the naive walk would turn one
                # missing table into one finding per declared column.
                drifts.append(
                    SchemaDrift(kind="missing_table", schema=schema_name, table=table)
                )
                continue
            names = declared_columns.get(f"{schema_name}.{table}")
            if not names:
                tables_checked += 1
                continue
            try:
                live_schema = adapter.describe_table(schema_name, table)
                live_names = [
                    name
                    for column in (getattr(live_schema, "columns", None) or [])
                    if (name := getattr(column, "name", "") or "")
                ]
            except Exception as exc:  # noqa: BLE001 - same verdict as above
                unchecked.append(
                    UncheckedTable(
                        schema=schema_name,
                        table=table,
                        reason=f"{type(exc).__name__}: {exc}",
                    )
                )
                continue
            tables_checked += 1
            columns_checked += len(names)
            drifts += _column_drifts(schema_name, table, names, live_names)

    # Sorted so CI diffs a stable report: dict and set iteration would let an
    # unchanged contract produce a changed file.
    drifts.sort(key=lambda d: (d.schema, d.table, d.column, d.kind))
    unchecked.sort(key=lambda u: (u.schema, u.table))
    return SchemaDriftReport(
        drifts=drifts,
        unchecked=unchecked,
        tables_checked=tables_checked,
        columns_checked=columns_checked,
    )


def _column_drifts(
    schema_name: str, table: str, declared: list[str], live: list[str]
) -> list[SchemaDrift]:
    live_exact = set(live)
    # First spelling wins if a warehouse somehow returns two columns differing
    # only in case; either is equally useful for telling the author what to
    # write, and the alternative is an arbitrary sort.
    live_folded: dict[str, str] = {}
    for name in live:
        live_folded.setdefault(name.casefold(), name)
    found: list[SchemaDrift] = []
    for name in declared:
        if name in live_exact:
            continue
        actual = live_folded.get(name.casefold())
        if actual is not None:
            found.append(
                SchemaDrift(
                    kind="case_mismatch",
                    schema=schema_name,
                    table=table,
                    column=name,
                    live_name=actual,
                )
            )
        else:
            found.append(
                SchemaDrift(
                    kind="missing_column",
                    schema=schema_name,
                    table=table,
                    column=name,
                )
            )
    return found


__all__ = [
    "SchemaDrift",
    "SchemaDriftReport",
    "UncheckedTable",
    "check_schema_drift",
]
