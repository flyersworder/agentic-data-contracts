"""Experiment 6: one database's contract diluted with other databases' contracts.

docs/superpowers/specs/2026-10-20-experiment-6-disclosure-scale-design.md.
A merged contract is the base contract, unchanged, with padding contracts
around it. Each padding database lives in a schema of its own name:
`namespace` qualifies its tables (`<db>.<table>`) in SQL, relationships and
prose, prefixes its domain, metric and rule names with `<db>.`, and drops its
enforcing rules -- a library rule without a table applies to every query, so a
padding rule would change what is enforced on base queries. Prose rules stay.

`merge` places the base's entries at the size midpoint of the padding's, list
by list, so the base sits in the middle of every section a reader meets;
`first=True` puts them first (the position control). `working_db` copies the
padding databases' tables into schemas of a copy of the base database, so
every table the merged contract allows exists and `main` stays the base's
alone (an ATTACHed database is a catalog whose tables sit in `main`, which
the library's `describe_table` would merge into the base's).
"""

from __future__ import annotations

import copy
import random
import re
import shutil
from pathlib import Path

import duckdb
import sqlglot
import yaml
from sqlglot import exp
from sqlglot.errors import ParseError

#: Lists merged entry by entry, under `contract["semantic"]` and in `semantic`.
CONTRACT_LISTS = ("allowed_tables", "domains", "rules")
SEMANTIC_LISTS = ("metrics", "tables", "relationships")
#: Prose fields, rewritten for the padding's namespace.
PROSE = ("description", "summary", "grain")


def _qualify_tree(tree: exp.Expression, db: str, tables: set[str]) -> str:
    ctes = {c.alias_or_name for c in tree.find_all(exp.CTE)}
    for table in tree.find_all(exp.Table):
        if (
            table.name in tables
            and table.name not in ctes
            and table.db in ("", "main")
            and not table.catalog
        ):
            table.set("db", exp.to_identifier(db))
    return tree.sql(dialect="duckdb")


def qualify(sql: str, db: str, tables: set[str]) -> str:
    """`sql` with every `main.<t>` or bare `<t>` of `tables` as `<db>.<t>`."""
    try:
        return _qualify_tree(sqlglot.parse_one(sql, read="duckdb"), db, tables)
    except ParseError:
        return _qualify_text(sql, db, tables)


def qualify_from(fragment: str, db: str, tables: set[str]) -> str:
    """A FROM-clause fragment (a metric's `from_sql`), qualified."""
    head = "SELECT 1 FROM "
    out = qualify(head + fragment.strip(), db, tables)
    return out[len(head) :] if out.startswith(head) else out


def qualify_expression(expression: str, db: str, tables: set[str]) -> str:
    """A select-list expression (a metric's `sql_expression`), qualified."""
    head = "SELECT "
    out = qualify(head + expression.strip(), db, tables)
    return out[len(head) :] if out.startswith(head) else out


def _qualify_text(text: str, db: str, tables: set[str]) -> str:
    for t in sorted(tables, key=len, reverse=True):
        text = re.sub(rf"(?<![\w.])main\.{re.escape(t)}\b", f"{db}.{t}", text)
        text = re.sub(
            rf"(\b(?:FROM|JOIN)\s+){re.escape(t)}\b",
            rf"\g<1>{db}.{t}",
            text,
            flags=re.I,
        )
    return text


def _qualified_name(name: str, db: str, tables: set[str]) -> str:
    """`main.t[.col]` or `t[.col]` as `<db>.t[.col]`."""
    parts = name.split(".")
    if parts[0] == "main":
        parts = parts[1:]
    if parts and parts[0] in tables:
        return ".".join([db, *parts])
    return name


def _prose(text: str, db: str, tables: set[str], names: list[str]) -> str:
    for t in sorted(tables, key=len, reverse=True):
        text = re.sub(rf"(?<![\w.])main\.{re.escape(t)}\b", f"{db}.{t}", text)
    for n in sorted(names, key=len, reverse=True):
        text = re.sub(rf"(?<![\w.]){re.escape(n)}(?!\w)", f"{db}.{n}", text)
    return text


def _rewrite_prose(entry: dict, db: str, tables: set[str], names: list[str]) -> None:
    for key in PROSE:
        if isinstance(entry.get(key), str):
            entry[key] = _prose(entry[key], db, tables, names)


def namespace(contract: dict, semantic: dict, db: str) -> tuple[dict, dict]:
    """A padding contract moved into schema `db` (see the module docstring)."""
    contract, semantic = copy.deepcopy(contract), copy.deepcopy(semantic)
    sem = contract["semantic"]
    tables = {t for entry in sem["allowed_tables"] for t in entry.get("tables") or []}
    tables |= {t["table"] for t in semantic.get("tables") or []}
    names = [m["name"] for m in semantic.get("metrics") or []]

    for entry in sem["allowed_tables"]:
        entry["schema"] = db
        _rewrite_prose(entry, db, tables, names)
    for domain in sem.get("domains") or []:
        domain["name"] = f"{db}.{domain['name']}"
        domain["tables"] = [
            _qualified_name(t, db, tables) for t in domain.get("tables") or []
        ]
        _rewrite_prose(domain, db, tables, names)
    rules = []
    for rule in sem.get("rules") or []:
        if rule.get("query_check") is not None or rule.get("result_check") is not None:
            continue
        rule["name"] = f"{db}.{rule['name']}"
        if rule.get("table"):
            rule["table"] = _qualified_name(rule["table"], db, tables)
        _rewrite_prose(rule, db, tables, names)
        rules.append(rule)
    sem["rules"] = rules

    for metric in semantic.get("metrics") or []:
        metric["name"] = f"{db}.{metric['name']}"
        metric["domains"] = [f"{db}.{d}" for d in metric.get("domains") or []]
        if metric.get("sql_expression"):
            metric["sql_expression"] = qualify_expression(
                metric["sql_expression"], db, tables
            )
        if metric.get("from_sql"):
            metric["from_sql"] = qualify_from(metric["from_sql"], db, tables)
        if metric.get("source_model"):
            metric["source_model"] = _qualified_name(metric["source_model"], db, tables)
        _rewrite_prose(metric, db, tables, names)
    for table in semantic.get("tables") or []:
        table["schema"] = db
        _rewrite_prose(table, db, tables, names)
        for column in table.get("columns") or []:
            _rewrite_prose(column, db, tables, names)
    for rel in semantic.get("relationships") or []:
        for key in ("from", "to"):
            rel[key] = _qualified_name(rel[key], db, tables)
    return contract, semantic


def padding_order(
    base: str, near: list[str], pool: list[str], *, seed: int
) -> list[str]:
    """The near databases first, then the pool in a permutation seeded per base."""
    rest = sorted(pool)
    random.Random(f"{seed}:{base}").shuffle(rest)
    return sorted(near) + rest


def level_padding(
    order: list[str], sizes: dict[str, int], base: str, multiple: float | None
) -> list[str]:
    """The shortest prefix of `order` that brings the merged size to
    `multiple` times the base's; all of it when `multiple` is None."""
    if multiple is None:
        return list(order)
    total, out = sizes[base], []
    for db in order:
        if total >= multiple * sizes[base]:
            break
        out.append(db)
        total += sizes[db]
    return out


def _size(entry) -> int:
    return len(yaml_dump(entry))


def insert_mid(padding: list, base: list, *, first: bool = False) -> list:
    """`base` inserted where the padding before it reaches half the
    padding's size, or first."""
    if first or not padding:
        return [*base, *padding]
    half, seen = sum(_size(e) for e in padding) / 2, 0
    for i, entry in enumerate(padding):
        if seen >= half:
            return [*padding[:i], *base, *padding[i:]]
        seen += _size(entry)
    return [*padding, *base]


def merge(
    base: tuple[dict, dict], padding: list[tuple[dict, dict]], *, first: bool = False
) -> tuple[dict, dict]:
    """The base contract with the (namespaced) padding contracts merged in."""
    contract, semantic = copy.deepcopy(base[0]), copy.deepcopy(base[1])
    if not padding:
        return contract, semantic
    for key in CONTRACT_LISTS:
        pad = [e for c, _ in padding for e in c["semantic"].get(key) or []]
        contract["semantic"][key] = insert_mid(
            pad, contract["semantic"].get(key) or [], first=first
        )
    for key in SEMANTIC_LISTS:
        pad = [e for _, s in padding for e in s.get(key) or []]
        merged = insert_mid(pad, semantic.get(key) or [], first=first)
        if merged:
            semantic[key] = merged
    return contract, semantic


class _Dumper(yaml.SafeDumper):
    pass


def _str(dumper: yaml.SafeDumper, value: str):
    style = "|" if "\n" in value else None
    return dumper.represent_scalar("tag:yaml.org,2002:str", value, style=style)


_Dumper.add_representer(str, _str)


def yaml_dump(data) -> str:
    return yaml.dump(
        data, Dumper=_Dumper, sort_keys=False, allow_unicode=True, width=100
    )


def write(out: Path, contract: dict, semantic: dict) -> Path:
    out.mkdir(parents=True, exist_ok=True)
    (out / "contract.yml").write_text(yaml_dump(contract), encoding="utf-8")
    (out / "semantic.yml").write_text(yaml_dump(semantic), encoding="utf-8")
    return out


def working_db(base: Path, others: dict[str, Path], out: Path) -> Path:
    """A copy of `base` with each other database's tables in a schema of its
    name."""
    out.parent.mkdir(parents=True, exist_ok=True)
    shutil.copyfile(base, out)
    con = duckdb.connect(str(out))
    try:
        for name, path in others.items():
            con.execute(f"ATTACH '{path}' AS __src (READ_ONLY)")
            con.execute(f'CREATE SCHEMA "{name}"')
            tables = con.execute(
                "SELECT table_name FROM duckdb_tables() "
                "WHERE database_name = '__src' AND schema_name = 'main'"
            ).fetchall()
            for (t,) in tables:
                con.execute(
                    f'CREATE TABLE "{name}"."{t}" AS SELECT * FROM __src.main."{t}"'
                )
            con.execute("DETACH __src")
    finally:
        con.close()
    return out
