"""Experiment 7's data build: one DuckDB file per DataAgentBench dataset.

docs/superpowers/specs/2026-11-01-experiment-7-dataagentbench-design.md,
"One database session per dataset". DataAgentBench spreads a dataset over
SQLite, DuckDB and PostgreSQL databases; the harness's tools take one DuckDB
file. Per dataset, each logical database in `db_config.yaml` becomes a
schema of its name (the name `db_description.txt` uses), and:

- a SQLite file is read with Python's `sqlite3` and loaded column by column,
  each column typed by its declared type (INTEGER -> BIGINT, REAL -> DOUBLE,
  TEXT -> VARCHAR, SQLite's affinity rules); a value whose storage class
  does not fit its column's type stops the build;
- a DuckDB file is attached read-only and each table copied with its types;
- a PostgreSQL SQL file (`dce.benchmarks.pgsql`) is parsed and loaded, its
  types mapped by `pgsql.duckdb_type`.

Values are loaded byte for byte, corruptions included, in source order.
Every table is then checked against its source: its row count, and per
column its DuckDB type and a sha256 of its values in order, computed on the
source side from the source and on the built side from the built file.
Tables, schemas and names must match exactly. The build writes the file
under a temporary name and renames it only once the checks pass.

    UV_FROZEN=1 uv run python -m dce.benchmarks.dab_data --out DIR

fetches the pinned files (`dce.benchmarks.dab_fetch`) into DIR/src, builds
DIR/duckdb/<dataset>.duckdb, writes DIR/build-manifest.json (every table's
rows and column digests), and checks each dataset's content digest against
the committed `prep/dataagentbench/build-manifest.json`. The cluster job
runs the same command into its own directory.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import sqlite3
from collections.abc import Iterable, Iterator
from dataclasses import dataclass
from pathlib import Path

import duckdb
import numpy as np
import yaml

from dce.benchmarks import dab_fetch, pgsql

DEFAULT_DATA = Path(os.environ.get("DAB_DATA", Path.home() / "data" / "dataagentbench"))
#: The committed summary: each dataset's content digest, file sha256 and
#: counts, from the materials commit's build.
COMMITTED_MANIFEST = (
    Path(__file__).resolve().parents[2]
    / "prep"
    / "dataagentbench"
    / "build-manifest.json"
)
BUILD_MANIFEST = "build-manifest.json"

#: Rows per load and per digest chunk.
CHUNK = 100_000

#: SQLite storage classes each loaded type accepts (NULL always).
_SQLITE_ACCEPTS: dict[str, frozenset[type]] = {
    "BIGINT": frozenset({int}),
    "DOUBLE": frozenset({float}),
    "VARCHAR": frozenset({str}),
}


class BuildError(RuntimeError):
    """A source the build cannot load faithfully, or a check that failed."""


@dataclass(frozen=True)
class Source:
    schema: str
    kind: str  # "sqlite" | "duckdb" | "postgres"
    path: Path


def quote(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def sources(src: Path, dataset: str) -> list[Source]:
    """The dataset's logical databases, in `db_config.yaml`'s order. Each
    name must appear in `db_description.txt`, which is how the agent will
    address it."""
    folder = src / dab_fetch.dataset_dir(dataset)
    config = yaml.safe_load((folder / "db_config.yaml").read_text(encoding="utf-8"))
    description = (folder / "db_description.txt").read_text(encoding="utf-8")
    out = []
    for name, client in config["db_clients"].items():
        if name not in description:
            raise BuildError(f"{dataset}: {name} is not named in db_description.txt")
        kind = client["db_type"]
        if kind not in ("sqlite", "duckdb", "postgres"):
            raise BuildError(f"{dataset}: {name} has unsupported db_type {kind!r}")
        rel = client["sql_file"] if kind == "postgres" else client["db_path"]
        out.append(Source(name, kind, folder / rel))
    return out


# -- digests ------------------------------------------------------------------


def _canon(value) -> bytes:
    """A value's bytes for the digest, tagged by its Python type and
    length-prefixed, so 1, 1.0, "1", "" and NULL all differ."""
    if value is None:
        blob = b"n"
    elif isinstance(value, bool):
        blob = b"b1" if value else b"b0"
    elif isinstance(value, int):
        blob = b"i" + str(value).encode("ascii")
    elif isinstance(value, float):
        blob = b"f" + repr(value).encode("ascii")
    elif isinstance(value, str):
        blob = b"s" + value.encode("utf-8", "surrogatepass")
    elif isinstance(value, bytes | bytearray | memoryview):
        blob = b"x" + bytes(value)
    else:
        blob = b"o" + type(value).__name__.encode("ascii") + b":" + str(value).encode()
    return len(blob).to_bytes(8, "little") + blob


class _Digests:
    def __init__(self, width: int) -> None:
        self.hashes = [hashlib.sha256() for _ in range(width)]
        self.rows = 0

    def update(self, rows: list) -> None:
        if not rows:
            return
        self.rows += len(rows)
        for i, h in enumerate(self.hashes):
            h.update(b"".join(_canon(row[i]) for row in rows))

    def hexdigests(self) -> list[str]:
        return [h.hexdigest() for h in self.hashes]


def column_digests(rows: Iterable, width: int) -> list[str]:
    """Each column's sha256 over its values in row order."""
    digests = _Digests(width)
    digests.update(list(rows))
    return digests.hexdigests()


def _chunks(cursor, size: int = CHUNK) -> Iterator[list]:
    while rows := cursor.fetchmany(size):
        yield rows


# -- loading ------------------------------------------------------------------


def _sqlite_type(declared: str) -> str:
    """SQLite's affinity rules (datatype3.html, 3.1), for the three
    affinities phase 1's files use. Others stop the build with the type."""
    upper = declared.upper()
    if "INT" in upper:
        return "BIGINT"
    if any(k in upper for k in ("CHAR", "CLOB", "TEXT")):
        return "VARCHAR"
    if any(k in upper for k in ("REAL", "FLOA", "DOUB")):
        return "DOUBLE"
    raise BuildError(f"SQLite declared type {declared!r} has no checked DuckDB type")


def _stage_text(value) -> str | None:
    """A source value as text DuckDB casts back to the same value: shortest
    round-trip `repr` for floats."""
    if value is None or isinstance(value, str):
        return value
    if isinstance(value, float):
        return repr(value)
    return str(value)


def _insert_chunk(con, target: str, columns: list[tuple[str, str]], rows: list):
    """Insert rows of staged text into `target`, casting each column."""
    if not rows:
        return
    # Text arrays never hold None: DuckDB's scan of a large object array
    # whose first values are None fails, so a NULL travels as a mask beside
    # a placeholder its column's type casts.
    chunk = {}
    for i, (_, duck) in enumerate(columns):
        placeholder = "" if duck == "VARCHAR" else "0"
        texts = [_stage_text(row[i]) for row in rows]
        chunk[f"c{i}"] = np.array(
            [placeholder if t is None else t for t in texts], dtype=object
        )
        chunk[f"n{i}"] = np.array([t is None for t in texts], dtype=bool)
    con.register("dab_chunk", chunk)
    try:
        casts = ", ".join(
            f"CASE WHEN n{i} THEN NULL ELSE CAST(c{i} AS {duck}) END"
            for i, (_, duck) in enumerate(columns)
        )
        con.execute(f"INSERT INTO {target} SELECT {casts} FROM dab_chunk")
    finally:
        con.unregister("dab_chunk")


def _create(con, schema: str, table: str, columns: list[tuple[str, str]]) -> str:
    target = f"{quote(schema)}.{quote(table)}"
    cols = ", ".join(f"{quote(c)} {t}" for c, t in columns)
    con.execute(f"CREATE TABLE {target} ({cols})")
    return target


def _expected(columns: list[tuple[str, str]], digests: _Digests) -> dict:
    return {
        "rows": digests.rows,
        "columns": [
            [name, duck, digest]
            for (name, duck), digest in zip(columns, digests.hexdigests(), strict=True)
        ],
    }


def _load_sqlite(con, source: Source) -> dict[str, dict]:
    db = sqlite3.connect(f"file:{source.path}?mode=ro", uri=True)
    try:
        names = [
            r[0]
            for r in db.execute(
                "SELECT name FROM sqlite_master WHERE type = 'table' "
                "AND name NOT LIKE 'sqlite_%' ORDER BY name"
            )
        ]
        out = {}
        for table in names:
            info = db.execute(f"PRAGMA table_info({quote(table)})").fetchall()
            columns = [(row[1], _sqlite_type(row[2])) for row in info]
            target = _create(con, source.schema, table, columns)
            digests = _Digests(len(columns))
            # rowid order: the order SQLite returns a plain scan in.
            cursor = db.execute(f"SELECT * FROM {quote(table)} ORDER BY rowid")
            for rows in _chunks(cursor):
                for i, (name, duck) in enumerate(columns):
                    allowed = _SQLITE_ACCEPTS[duck]
                    bad = next(
                        (
                            r[i]
                            for r in rows
                            if r[i] is not None and type(r[i]) not in allowed
                        ),
                        None,
                    )
                    if bad is not None:
                        raise BuildError(
                            f"{source.schema}.{table}.{name}: value {bad!r} "
                            f"({type(bad).__name__}) in a {duck} column"
                        )
                digests.update(rows)
                _insert_chunk(con, target, columns, rows)
            out[table] = _expected(columns, digests)
        return out
    finally:
        db.close()


def _load_duckdb(con, source: Source) -> dict[str, dict]:
    alias = "dab_source"
    path = source.path.as_posix().replace("'", "''")
    con.execute(f"ATTACH '{path}' AS {alias} (READ_ONLY)")
    try:
        found = con.execute(
            "SELECT schema_name, table_name FROM duckdb_tables() "
            "WHERE database_name = ? ORDER BY table_name",
            [alias],
        ).fetchall()
        views = con.execute(
            "SELECT count(*) FROM duckdb_views() WHERE database_name = ? "
            "AND NOT internal",
            [alias],
        ).fetchone()
        if views and views[0]:
            raise BuildError(f"{source.path.name}: views are not copied")
        if {s for s, _ in found} - {"main"}:
            raise BuildError(f"{source.path.name}: tables outside schema main")
        out = {}
        for _, table in found:
            src = f"{alias}.main.{quote(table)}"
            columns = [
                (name, dtype)
                for name, dtype in con.execute(
                    "SELECT column_name, data_type FROM duckdb_columns() "
                    "WHERE database_name = ? AND table_name = ? ORDER BY column_index",
                    [alias, table],
                ).fetchall()
            ]
            con.execute(
                f"CREATE TABLE {quote(source.schema)}.{quote(table)} AS "
                f"SELECT * FROM {src}"
            )
            digests = _Digests(len(columns))
            for rows in _chunks(con.execute(f"SELECT * FROM {src}")):
                digests.update(rows)
            out[table] = _expected(columns, digests)
        return out
    finally:
        con.execute(f"DETACH {alias}")


def _load_postgres(con, source: Source) -> dict[str, dict]:
    text = source.path.read_text(encoding="utf-8")
    tables = pgsql.read_sql(text)
    # The row count against the source, counted without the reader.
    raw = pgsql.count_rows(text)
    parsed = {name: len(table.rows) for name, table in tables.items()}
    if raw != parsed:
        raise BuildError(f"{source.path.name}: rows {parsed}, raw count {raw}")
    out = {}
    for table in tables.values():
        try:
            columns = [(c, pgsql.duckdb_type(t)) for c, t in table.columns]
        except ValueError as exc:
            raise BuildError(f"{source.schema}.{table.name}: {exc}") from exc
        target = _create(con, source.schema, table.name, columns)
        digests = _Digests(len(columns))
        for start in range(0, len(table.rows), CHUNK):
            rows = table.rows[start : start + CHUNK]
            digests.update(
                [
                    [
                        pgsql.python_value(t, v)
                        for (_, t), v in zip(table.columns, row, strict=True)
                    ]
                    for row in rows
                ]
            )
            _insert_chunk(con, target, columns, rows)
        out[table.name] = _expected(columns, digests)
    return out


_LOADERS = {"sqlite": _load_sqlite, "duckdb": _load_duckdb, "postgres": _load_postgres}


# -- checks -------------------------------------------------------------------


def table_entry(con, schema: str, table: str) -> dict:
    """What the built file holds for one table, in `_expected`'s shape."""
    columns = con.execute(
        "SELECT column_name, data_type FROM duckdb_columns() "
        "WHERE database_name = current_database() AND schema_name = ? "
        "AND table_name = ? ORDER BY column_index",
        [schema, table],
    ).fetchall()
    digests = _Digests(len(columns))
    for rows in _chunks(con.execute(f"SELECT * FROM {quote(schema)}.{quote(table)}")):
        digests.update(rows)
    return _expected([tuple(c) for c in columns], digests)


def check_built(path: Path, entry: dict) -> None:
    """The built file against what its sources held: the same schemas and
    tables, and per table the same rows, column names, types and values."""
    con = duckdb.connect(str(path), read_only=True)
    try:
        found: dict[str, set[str]] = {}
        for schema, table in con.execute(
            "SELECT schema_name, table_name FROM duckdb_tables() "
            "WHERE database_name = current_database()"
        ).fetchall():
            found.setdefault(schema, set()).add(table)
        expected = {s: set(v["tables"]) for s, v in entry["schemas"].items()}
        if found != expected:
            raise BuildError(f"tables differ: built {found}, sources {expected}")
        problems = []
        for schema, spec in entry["schemas"].items():
            for table, want in spec["tables"].items():
                got = table_entry(con, schema, table)
                if got["rows"] != want["rows"]:
                    problems.append(
                        f"{schema}.{table}: {got['rows']} rows, source {want['rows']}"
                    )
                for g, w in zip(got["columns"], want["columns"], strict=False):
                    if g != w:
                        problems.append(
                            f"{schema}.{table}.{w[0]}: built {g}, source {w}"
                        )
                if len(got["columns"]) != len(want["columns"]):
                    problems.append(f"{schema}.{table}: column count differs")
        if problems:
            raise BuildError("checks failed:\n" + "\n".join(problems))
    finally:
        con.close()


def _content_sha256(schemas: dict) -> str:
    text = json.dumps(schemas, sort_keys=True, ensure_ascii=True)
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def build_dataset(src: Path, dataset: str, dest: Path) -> dict:
    """Build `dest` from the dataset's sources under `src`, check it, and
    return its manifest entry."""
    srcs = sources(src, dataset)
    dest.parent.mkdir(parents=True, exist_ok=True)
    tmp = dest.with_name(dest.name + ".building")
    tmp.unlink(missing_ok=True)
    tmp.with_name(tmp.name + ".wal").unlink(missing_ok=True)
    con = duckdb.connect(str(tmp))
    schemas: dict[str, dict] = {}
    try:
        for source in srcs:
            con.execute(f"CREATE SCHEMA {quote(source.schema)}")
            tables = _LOADERS[source.kind](con, source)
            folded = [t.lower() for t in tables]
            if len(set(folded)) != len(folded):
                raise BuildError(f"{source.schema}: table names differ only in case")
            schemas[source.schema] = {
                "source": source.path.relative_to(src).as_posix(),
                "kind": source.kind,
                "tables": tables,
            }
        con.execute("CHECKPOINT")
    finally:
        con.close()
    entry = {"schemas": schemas}
    check_built(tmp, entry)
    tmp.replace(dest)
    entry.update(
        {
            "file": dest.name,
            "sha256": dab_fetch.sha256_file(dest),
            "bytes": dest.stat().st_size,
            "tables": sum(len(s["tables"]) for s in schemas.values()),
            "rows": sum(
                t["rows"] for s in schemas.values() for t in s["tables"].values()
            ),
            "content_sha256": _content_sha256(schemas),
        }
    )
    return entry


def summary(entries: dict[str, dict]) -> dict:
    """The committed manifest: per dataset, the built file's sha256 and
    size, its counts, and the content digest a rebuild must reproduce.

    DuckDB does not write the same bytes twice for the same content (two
    builds of one dataset differ in sha256), so the file sha256 pins the one
    build it was taken from, and a rebuild is checked by its content digest:
    every table's row count and every column's type and value digest."""
    return {
        "dab_commit": dab_fetch.COMMIT,
        "duckdb_version": duckdb.__version__,
        "datasets": {
            name: {
                k: entry[k]
                for k in ("sha256", "bytes", "tables", "rows", "content_sha256")
            }
            for name, entry in entries.items()
        },
    }


def check_expected(entries: dict[str, dict], path: Path) -> None:
    """Each built dataset's content digest against the committed manifest."""
    expected = json.loads(path.read_text(encoding="utf-8"))["datasets"]
    bad = []
    for name, entry in entries.items():
        want = expected.get(name)
        if want is None:
            bad.append(f"{name}: not in {path}")
        elif entry["content_sha256"] != want["content_sha256"]:
            bad.append(f"{name}: content digest differs from {path}")
    if bad:
        raise BuildError("\n".join(bad))


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--out", type=Path, default=DEFAULT_DATA)
    parser.add_argument("--datasets", nargs="+", default=list(dab_fetch.DATASETS))
    parser.add_argument(
        "--no-fetch",
        action="store_true",
        help="build from the files already in OUT/src",
    )
    parser.add_argument(
        "--expect",
        type=Path,
        default=COMMITTED_MANIFEST,
        help="the manifest whose content digests the build must reproduce",
    )
    parser.add_argument(
        "--record",
        type=Path,
        default=None,
        help="write the summary manifest here instead of checking against --expect",
    )
    args = parser.parse_args(argv)
    if not args.no_fetch:
        dab_fetch.fetch(args.out, tuple(args.datasets))
    entries = {}
    for dataset in args.datasets:
        dest = args.out / "duckdb" / f"{dataset}.duckdb"
        entry = build_dataset(args.out / "src", dataset, dest)
        entries[dataset] = entry
        print(
            f"{dataset}: {entry['tables']} tables, {entry['rows']} rows, "
            f"{entry['bytes'] / 1e6:.1f} MB, checks passed",
            flush=True,
        )
    manifest_path = args.out / BUILD_MANIFEST
    previous = (
        json.loads(manifest_path.read_text(encoding="utf-8"))
        if manifest_path.is_file()
        else {}
    )
    previous.update(entries)
    manifest_path.write_text(json.dumps(previous, indent=1) + "\n", encoding="utf-8")
    if args.record is not None:
        args.record.parent.mkdir(parents=True, exist_ok=True)
        args.record.write_text(
            json.dumps(summary(entries), indent=1) + "\n", encoding="utf-8"
        )
        print(f"recorded {args.record}")
        return
    check_expected(entries, args.expect)
    print(f"content digests match {args.expect}")


if __name__ == "__main__":
    main()
