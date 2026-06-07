from __future__ import annotations

from dataclasses import dataclass, field

from mje.schema_graph import Column, FKEdge, SchemaGraph, edge_key


@dataclass
class Mapping:
    table: dict[str, str] = field(default_factory=dict)  # orig table -> t{i}
    column: dict[tuple[str, str], str] = field(
        default_factory=dict
    )  # (orig table, orig col) -> c{j}


def anonymize(graph: SchemaGraph) -> tuple[SchemaGraph, Mapping]:
    m = Mapping()
    new_tables: dict[str, list[Column]] = {}
    for ti, (tname, cols) in enumerate(graph.tables.items()):
        new_t = f"t{ti}"
        m.table[tname] = new_t
        new_cols: list[Column] = []
        for cj, col in enumerate(cols):
            new_c = f"c{cj}"
            m.column[(tname, col.name)] = new_c
            new_cols.append(Column(table=new_t, name=new_c, type=col.type))
        new_tables[new_t] = new_cols

    new_edges: list[FKEdge] = []
    for e in graph.fk_edges:
        a = (m.table[e.a[0]], m.column[(e.a[0], e.a[1])])
        b = (m.table[e.b[0]], m.column[(e.b[0], e.b[1])])
        new_edges.append(FKEdge(a=a, b=b))

    return SchemaGraph(db_id=graph.db_id, tables=new_tables, fk_edges=new_edges), m


def _map_pair(pair: tuple[str, str], m: Mapping) -> tuple[str, str]:
    tname, cname = pair
    return m.table[tname], m.column[(tname, cname)]


def map_edges(edges: set[frozenset], m: Mapping) -> set[frozenset]:
    out: set[frozenset] = set()
    for e in edges:
        (p1, p2) = tuple(e)
        out.add(edge_key(_map_pair(p1, m), _map_pair(p2, m)))
    return out


def map_tables(tables: set[str], m: Mapping) -> set[str]:
    return {m.table[t] for t in tables}
