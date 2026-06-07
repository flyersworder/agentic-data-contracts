from __future__ import annotations

from mje.schema_graph import FKEdge, SchemaGraph


def _sorted_tables(graph: SchemaGraph) -> list[str]:
    return sorted(graph.tables)


def _sorted_edges(graph: SchemaGraph) -> list[FKEdge]:
    def key(e: FKEdge):
        return tuple(sorted([f"{e.a[0]}.{e.a[1]}", f"{e.b[0]}.{e.b[1]}"]))

    return sorted(graph.fk_edges, key=key)


def render_xml(graph: SchemaGraph) -> str:
    lines = ["<schema>"]
    for t in _sorted_tables(graph):
        cols = "".join(
            f'<column name="{c.name}" type="{c.type}"/>' for c in graph.tables[t]
        )
        lines.append(f'  <table name="{t}">{cols}</table>')
    lines.append("  <relationships>")
    for e in _sorted_edges(graph):
        from_col = f"{e.a[0]}.{e.a[1]}"
        to_col = f"{e.b[0]}.{e.b[1]}"
        lines.append(f'    <rel from="{from_col}" to="{to_col}" type="many_to_one"/>')
    lines.append("  </relationships>")
    lines.append("</schema>")
    return "\n".join(lines)


def render_mermaid(graph: SchemaGraph) -> str:
    lines = ["erDiagram"]
    for t in _sorted_tables(graph):
        body = "  ".join(f"{c.type} {c.name}" for c in graph.tables[t])
        lines.append(f"  {t} {{ {body} }}")
    for e in _sorted_edges(graph):
        lines.append(f'  {e.a[0]} ||--o{{ {e.b[0]} : "{e.a[1]} = {e.b[1]}"')
    return "\n".join(lines)


def render_nl_adjacency(graph: SchemaGraph) -> str:
    lines = []
    for t in _sorted_tables(graph):
        cols = ", ".join(f"{c.name} ({c.type})" for c in graph.tables[t])
        lines.append(f"{t} has columns {cols}.")
    for e in _sorted_edges(graph):
        join_expr = f"{e.a[0]}.{e.a[1]} = {e.b[0]}.{e.b[1]}"
        lines.append(f"{e.a[0]} joins to {e.b[0]} via {join_expr} (many_to_one).")
    return "\n".join(lines)


RENDERERS = {
    "xml": render_xml,
    "mermaid": render_mermaid,
    "nl_adjacency": render_nl_adjacency,
}
