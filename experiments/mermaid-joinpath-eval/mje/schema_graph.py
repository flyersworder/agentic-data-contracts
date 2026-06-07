from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

import sqlglot
from sqlglot import exp
from sqlglot.optimizer.qualify import qualify


@dataclass(frozen=True)
class Column:
    table: str
    name: str
    type: str


@dataclass
class FKEdge:
    a: tuple[str, str]  # (table, column)
    b: tuple[str, str]


@dataclass
class SchemaGraph:
    db_id: str
    tables: dict[str, list[Column]] = field(default_factory=dict)
    fk_edges: list[FKEdge] = field(default_factory=list)


def edge_key(a: tuple[str, str], b: tuple[str, str]) -> frozenset:
    """Undirected key for a join edge, lowercased for stable comparison."""
    return frozenset({(a[0].lower(), a[1].lower()), (b[0].lower(), b[1].lower())})


def parse_tables_json(entry: dict) -> SchemaGraph:
    table_names = entry["table_names_original"]
    col_defs = entry[
        "column_names_original"
    ]  # [[t_idx, name], ...] ; index 0 is [-1,"*"]
    col_types = entry["column_types"]

    tables: dict[str, list[Column]] = {t: [] for t in table_names}
    col_ref: dict[int, tuple[str, str]] = {}
    for idx, (t_idx, cname) in enumerate(col_defs):
        if t_idx < 0:
            continue
        tname = table_names[t_idx]
        ctype = col_types[idx] if idx < len(col_types) else "text"
        tables[tname].append(Column(table=tname, name=cname, type=ctype))
        col_ref[idx] = (tname, cname)

    fk_edges: list[FKEdge] = []
    for c1, c2 in entry.get("foreign_keys", []):
        if c1 in col_ref and c2 in col_ref:
            fk_edges.append(FKEdge(a=col_ref[c1], b=col_ref[c2]))

    return SchemaGraph(db_id=entry["db_id"], tables=tables, fk_edges=fk_edges)


def _schema_dict(graph: SchemaGraph) -> dict[str, Any]:
    """Schema mapping sqlglot's qualifier understands: {table: {col: type}}."""
    return {
        t: {c.name: (c.type or "text").upper() for c in cols}
        for t, cols in graph.tables.items()
    }


def _qualified_ast(query: str, graph: SchemaGraph) -> Any:
    ast = sqlglot.parse_one(query, read="sqlite")
    return qualify(
        ast,
        schema=_schema_dict(graph),
        qualify_columns=True,
        validate_qualify_columns=False,
    )


def _alias_map(ast: exp.Expression) -> dict[str, str]:
    """Build a mapping from alias_or_name (lowercased) -> actual table name."""
    result: dict[str, str] = {}
    for source in ast.find_all(exp.Table):
        alias = (source.alias or source.name or "").lower()
        if alias:
            result[alias] = source.name
    return result


def extract_gold_edges(query: str, graph: SchemaGraph) -> set[frozenset]:
    """Return the set of undirected join edges (column == column across tables)."""
    ast = _qualified_ast(query, graph)
    alias_to_table = _alias_map(ast)
    edges: set[frozenset] = set()
    for eq in ast.find_all(exp.EQ):
        left, right = eq.left, eq.right
        if isinstance(left, exp.Column) and isinstance(right, exp.Column):
            lt_alias = left.table.lower() if left.table else None
            rt_alias = right.table.lower() if right.table else None
            lt = alias_to_table.get(lt_alias) if lt_alias else None
            rt = alias_to_table.get(rt_alias) if rt_alias else None
            if lt and rt and lt.lower() != rt.lower():
                edges.add(edge_key((lt, left.name), (rt, right.name)))
    return edges


def gold_tables(query: str, graph: SchemaGraph) -> set[str]:
    ast = _qualified_ast(query, graph)
    valid = {k.lower(): k for k in graph.tables}
    return {
        valid[t.name.lower()]
        for t in ast.find_all(exp.Table)
        if t.name.lower() in valid
    }
