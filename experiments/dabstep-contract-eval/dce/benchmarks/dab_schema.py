"""What a rater or an author sees of a built DataAgentBench file: per
database (a schema of the DuckDB file), each table's columns and first rows.

A database with more than `many` tables of one shape (stockmarket's 2,753
tickers) is summarised by the count and `show` of the tables, chosen with a
fixed seed (spec, "One database session per dataset"); the rest are a query
on `information_schema.tables` away. With `rows=0` there are no sample rows
(the author's no-data control).
"""

from __future__ import annotations

import csv
import io
import random
import re
from collections import Counter
from pathlib import Path

import duckdb

from dce.benchmarks.dab_data import quote

_BARE = re.compile(r"[A-Za-z_][A-Za-z0-9_]*")


def _ident(name: str) -> str:
    return name if _BARE.fullmatch(name) else quote(name)


def _name(schema: str, table: str) -> str:
    return f"{_ident(schema)}.{_ident(table)}"


def _cut(value, width: int):
    if isinstance(value, str) and len(value) > width:
        return value[:width] + "..."
    return value


def _type_split(shapes: list[list[tuple[str, str]]]) -> str:
    """One sentence per column whose type differs between same-named
    tables, most common type first."""
    out = ""
    for i, (column, _) in enumerate(shapes[0]):
        counts = Counter(s[i][1] for s in shapes).most_common()
        if len(counts) == 1:
            continue
        (first, n), *rest = counts
        tail = [f"{t} in {k}" for t, k in rest]
        others = (
            tail[0] if len(tail) == 1 else ", ".join(tail[:-1]) + " and " + tail[-1]
        )
        out += f" Column {column} is {first} in {n} of them and {others}."
    return out


def _table(con, schema: str, table: str, columns, rows: int, width: int) -> str:
    name = _name(schema, table)
    ddl = (
        f"CREATE TABLE {name} (\n"
        + ",\n".join(f"  {_ident(c)} {t}" for c, t in columns)
        + "\n);"
    )
    if rows == 0:
        return ddl
    cur = con.execute(f"SELECT * FROM {quote(schema)}.{quote(table)} LIMIT {rows}")
    buf = io.StringIO()
    writer = csv.writer(buf, lineterminator="\n")
    writer.writerow([c for c, _ in columns])
    writer.writerows([_cut(v, width) for v in r] for r in cur.fetchall())
    return f"{ddl}\n-- first {rows} rows:\n{buf.getvalue()}"


def schemas(path: Path) -> tuple[str, ...]:
    """The file's databases: every schema with a table, `main` aside."""
    con = duckdb.connect(str(path), read_only=True)
    try:
        found = con.execute(
            "SELECT DISTINCT table_schema FROM information_schema.tables "
            "WHERE table_schema <> 'main' ORDER BY table_schema"
        ).fetchall()
    finally:
        con.close()
    return tuple(s for (s,) in found)


def schema_text(
    path: Path,
    *,
    many: int = 50,
    show: int = 3,
    rows: int = 3,
    seed: int = 7,
    width: int = 200,
) -> str:
    con = duckdb.connect(str(path), read_only=True)
    try:
        found = con.execute(
            "SELECT table_schema, table_name, column_name, data_type "
            "FROM information_schema.columns WHERE table_schema <> 'main' "
            "ORDER BY table_schema, table_name, ordinal_position"
        ).fetchall()
        tables: dict[str, dict[str, list[tuple[str, str]]]] = {}
        for schema, table, column, kind in found:
            tables.setdefault(schema, {}).setdefault(table, []).append((column, kind))
        out: list[str] = []
        for schema, by_table in tables.items():
            shapes: dict[tuple, list[str]] = {}
            for table, columns in by_table.items():
                shapes.setdefault(tuple(c for c, _ in columns), []).append(table)
            parts = [f"## Database `{schema}`"]
            for names in shapes.values():
                if len(names) > many:
                    picked = sorted(random.Random(seed).sample(names, show))
                    parts.append(
                        f"This database holds {len(names)} tables with the same "
                        f"columns; {show} of them follow, chosen at random. Query "
                        "information_schema.tables for the others."
                        + _type_split([by_table[t] for t in names])
                    )
                else:
                    picked = names
                parts += [
                    _table(con, schema, t, by_table[t], rows, width) for t in picked
                ]
            out.append("\n\n".join(parts))
        return "\n\n".join(out) + "\n"
    finally:
        con.close()
