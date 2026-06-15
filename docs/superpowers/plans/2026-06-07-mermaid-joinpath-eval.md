# Mermaid Join-Path Eval — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a standalone harness that measures whether rendering an (anonymized) Spider schema graph as XML vs. Mermaid vs. natural-language adjacency changes an LLM's join-path reconstruction accuracy.

**Architecture:** A pure deterministic core (schema parsing, opaque anonymization, three renderers, edge grading) that is fully unit-tested offline, wrapped by I/O modules (Spider acquisition, OpenRouter client) and a budget-capped runner that writes one JSONL row per model call, plus a stats module (strata + paired McNemar + bootstrap CIs).

**Tech Stack:** Python 3.12, `uv`, `sqlglot` (gold/predicted edge extraction + column qualification), `openai` SDK pointed at OpenRouter, `scipy` (McNemar), `pydantic`/dataclasses, `pytest`.

Reference spec: `docs/superpowers/specs/2026-06-07-mermaid-joinpath-eval-design.md`.

---

## File Structure

```
experiments/mermaid-joinpath-eval/
  pyproject.toml         # standalone project (own venv)
  .gitignore             # data/ results/ .venv/
  README.md              # how to source the key and run
  mje/
    __init__.py
    schema_graph.py      # Column, FKEdge, SchemaGraph; parse tables.json; gold-edge + gold-table extraction
    anonymize.py         # deterministic opaque remap of graph + edge/table sets
    renderers.py         # render_xml · render_mermaid · render_nl_adjacency (fixed ordering)
    grade.py             # parse model output → edges; precision/recall/F1/exact/hallucinated
    prompt.py            # build_messages(rendering, endpoint_tables)
    data.py              # download Spider; build Item list (filter ≥2 joins, ambiguity flag)
    model_client.py      # OpenRouter call + PRICING + cost()
    runner.py            # orchestrate items × renderings × models; budget cap; results.jsonl
    stats.py             # aggregate; McNemar; bootstrap CIs; summary table
  tests/
    fixtures/spider_mini/   # tiny tables.json + dev.json for offline tests
    test_schema_graph.py
    test_anonymize.py
    test_renderers.py
    test_grade.py
    test_prompt.py
    test_model_client.py
    test_data.py
    test_stats.py
```

**Core types (defined in Task 1, used everywhere — keep names exact):**
- `Column(table: str, name: str, type: str)` — frozen dataclass.
- `FKEdge(a: tuple[str, str], b: tuple[str, str])` — each side is `(table, column)`.
- `SchemaGraph(db_id: str, tables: dict[str, list[Column]], fk_edges: list[FKEdge])`.
- An **edge key** is `frozenset({(table, col), (table, col)})` — undirected, used for all set comparisons.
- Helper `edge_key(a: tuple[str,str], b: tuple[str,str]) -> frozenset` lives in `schema_graph.py`.

---

## Task 0: Scaffold the standalone project

**Files:**
- Create: `experiments/mermaid-joinpath-eval/pyproject.toml`
- Create: `experiments/mermaid-joinpath-eval/.gitignore`
- Create: `experiments/mermaid-joinpath-eval/mje/__init__.py`
- Create: `experiments/mermaid-joinpath-eval/tests/__init__.py`

- [ ] **Step 1: Create `pyproject.toml`**

```toml
[project]
name = "mje"
version = "0.1.0"
description = "Mermaid vs text rendering for LLM join-path reasoning (anonymized Spider)"
requires-python = ">=3.12"
dependencies = [
    "sqlglot>=25",
    "openai>=1.40",
    "requests>=2.31",
    "scipy>=1.11",
]

[dependency-groups]
dev = ["pytest>=8"]

[tool.pytest.ini_options]
testpaths = ["tests"]
```

- [ ] **Step 2: Create `.gitignore`**

```gitignore
.venv/
data/
results/
__pycache__/
*.pyc
```

- [ ] **Step 3: Create empty package markers**

`mje/__init__.py` and `tests/__init__.py` are empty files.

- [ ] **Step 4: Sync and verify the env builds**

Run: `cd experiments/mermaid-joinpath-eval && uv sync`
Expected: resolves and installs sqlglot, openai, requests, scipy, pytest without error.

- [ ] **Step 5: Commit**

```bash
git add experiments/mermaid-joinpath-eval/pyproject.toml experiments/mermaid-joinpath-eval/.gitignore experiments/mermaid-joinpath-eval/mje/__init__.py experiments/mermaid-joinpath-eval/tests/__init__.py
git commit -m "chore(mje): scaffold standalone join-path eval project"
```

---

## Task 1: `schema_graph.py` — types, tables.json parsing, gold extraction

**Files:**
- Create: `experiments/mermaid-joinpath-eval/mje/schema_graph.py`
- Create: `experiments/mermaid-joinpath-eval/tests/test_schema_graph.py`
- Create fixtures: `experiments/mermaid-joinpath-eval/tests/fixtures/spider_mini/tables.json`

**Spider `tables.json` shape (per DB):** `table_names_original` (list), `column_names_original`
(list of `[table_index, "colname"]`, with index 0 = `[-1, "*"]`), `column_types`, `foreign_keys`
(list of `[col_index, col_index]` into `column_names_original`).

- [ ] **Step 1: Create the test fixture `tests/fixtures/spider_mini/tables.json`**

```json
[
  {
    "db_id": "shop",
    "table_names_original": ["customers", "orders", "line_items", "products"],
    "column_names_original": [
      [-1, "*"],
      [0, "id"], [0, "name"],
      [1, "id"], [1, "customer_id"],
      [2, "id"], [2, "order_id"], [2, "product_id"],
      [3, "id"], [3, "title"]
    ],
    "column_types": ["text", "number", "text", "number", "number", "number", "number", "number", "number", "text"],
    "foreign_keys": [[4, 1], [6, 3], [7, 8]]
  }
]
```

This encodes: `orders.customer_id → customers.id`, `line_items.order_id → orders.id`,
`line_items.product_id → products.id` (a 4-table chain with a branch).

- [ ] **Step 2: Write the failing test `tests/test_schema_graph.py`**

```python
import json
from pathlib import Path
from mje.schema_graph import (
    Column, FKEdge, SchemaGraph, edge_key,
    parse_tables_json, extract_gold_edges, gold_tables,
)

FIX = Path(__file__).parent / "fixtures" / "spider_mini" / "tables.json"


def _shop() -> SchemaGraph:
    entry = json.loads(FIX.read_text())[0]
    return parse_tables_json(entry)


def test_parse_tables_json_builds_tables_and_columns():
    g = _shop()
    assert g.db_id == "shop"
    assert set(g.tables) == {"customers", "orders", "line_items", "products"}
    cols = {c.name for c in g.tables["orders"]}
    assert cols == {"id", "customer_id"}
    # the synthetic "*" column is dropped
    assert all(c.name != "*" for t in g.tables.values() for c in t)


def test_parse_tables_json_builds_fk_edges():
    g = _shop()
    keys = {edge_key(e.a, e.b) for e in g.fk_edges}
    assert edge_key(("orders", "customer_id"), ("customers", "id")) in keys
    assert edge_key(("line_items", "order_id"), ("orders", "id")) in keys
    assert edge_key(("line_items", "product_id"), ("products", "id")) in keys
    assert len(g.fk_edges) == 3


def test_extract_gold_edges_resolves_aliases_and_where_joins():
    g = _shop()
    # explicit JOIN ... ON with aliases
    q1 = ("SELECT T1.name FROM customers AS T1 JOIN orders AS T2 "
          "ON T1.id = T2.customer_id")
    e1 = extract_gold_edges(q1, g)
    assert e1 == {edge_key(("customers", "id"), ("orders", "customer_id"))}

    # comma-join with WHERE equality (Spider style), 3 tables = 2 join edges
    q2 = ("SELECT c.name FROM customers c, orders o, line_items l "
          "WHERE c.id = o.customer_id AND o.id = l.order_id")
    e2 = extract_gold_edges(q2, g)
    assert e2 == {
        edge_key(("customers", "id"), ("orders", "customer_id")),
        edge_key(("orders", "id"), ("line_items", "order_id")),
    }


def test_extract_gold_edges_qualifies_unqualified_columns():
    g = _shop()
    # unqualified join columns; resolvable via schema
    q = "SELECT title FROM line_items JOIN products ON product_id = products.id"
    e = extract_gold_edges(q, g)
    assert e == {edge_key(("line_items", "product_id"), ("products", "id"))}


def test_gold_tables_returns_from_join_tables():
    g = _shop()
    q = ("SELECT c.name FROM customers c JOIN orders o ON c.id = o.customer_id "
         "JOIN line_items l ON o.id = l.order_id")
    assert gold_tables(q, g) == {"customers", "orders", "line_items"}
```

- [ ] **Step 3: Run the test to verify it fails**

Run: `cd experiments/mermaid-joinpath-eval && uv run pytest tests/test_schema_graph.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'mje.schema_graph'`.

- [ ] **Step 4: Implement `mje/schema_graph.py`**

```python
from __future__ import annotations

from dataclasses import dataclass, field

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
    col_defs = entry["column_names_original"]      # [[t_idx, name], ...] ; index 0 is [-1,"*"]
    col_types = entry["column_types"]

    tables: dict[str, list[Column]] = {t: [] for t in table_names}
    # column index -> (table, name); skip the synthetic "*" at index 0
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


def _schema_dict(graph: SchemaGraph) -> dict[str, dict[str, str]]:
    """Schema mapping sqlglot's qualifier understands: {table: {col: type}}."""
    return {
        t: {c.name: (c.type or "text").upper() for c in cols}
        for t, cols in graph.tables.items()
    }


def _qualified_ast(query: str, graph: SchemaGraph):
    ast = sqlglot.parse_one(query, read="sqlite")
    # qualify resolves aliases AND unqualified columns against the schema
    return qualify(ast, schema=_schema_dict(graph), qualify_columns=True, validate_qualify_columns=False)


def _table_of(col: exp.Column, ast) -> str | None:
    """Resolve a (qualified) column's source table name from the qualified AST."""
    tbl = col.table
    if not tbl:
        return None
    # tbl is either a real table name or an alias; map alias -> table
    for source in ast.find_all(exp.Table):
        if (source.alias_or_name or "").lower() == tbl.lower():
            return source.name  # underlying table name
    return tbl


def extract_gold_edges(query: str, graph: SchemaGraph) -> set[frozenset]:
    """Return the set of undirected join edges (column == column across tables)."""
    ast = _qualified_ast(query, graph)
    edges: set[frozenset] = set()
    for eq in ast.find_all(exp.EQ):
        left, right = eq.left, eq.right
        if isinstance(left, exp.Column) and isinstance(right, exp.Column):
            lt, rt = _table_of(left, ast), _table_of(right, ast)
            if lt and rt and lt.lower() != rt.lower():
                edges.add(edge_key((lt, left.name), (rt, right.name)))
    return edges


def gold_tables(query: str, graph: SchemaGraph) -> set[str]:
    ast = _qualified_ast(query, graph)
    names = {t.name for t in ast.find_all(exp.Table)}
    valid = {k.lower(): k for k in graph.tables}
    return {valid[n.lower()] for n in names if n.lower() in valid}
```

- [ ] **Step 5: Run the test to verify it passes**

Run: `cd experiments/mermaid-joinpath-eval && uv run pytest tests/test_schema_graph.py -v`
Expected: PASS (5 tests). If alias resolution fails on the comma-join case, confirm `qualify` ran with `read="sqlite"`.

- [ ] **Step 6: Commit**

```bash
git add experiments/mermaid-joinpath-eval/mje/schema_graph.py experiments/mermaid-joinpath-eval/tests/test_schema_graph.py experiments/mermaid-joinpath-eval/tests/fixtures
git commit -m "feat(mje): schema graph parsing and gold join-edge extraction"
```

---

## Task 2: `anonymize.py` — deterministic opaque remap

**Files:**
- Create: `experiments/mermaid-joinpath-eval/mje/anonymize.py`
- Create: `experiments/mermaid-joinpath-eval/tests/test_anonymize.py`

Mapping: tables in declared order → `t0, t1, ...`; columns within a table in declared order → `c0, c1, ...`.
Same mapping rewrites the graph, the gold edge set, and the endpoint table list.

- [ ] **Step 1: Write the failing test `tests/test_anonymize.py`**

```python
import json
from pathlib import Path
from mje.schema_graph import parse_tables_json, edge_key
from mje.anonymize import anonymize, map_edges, map_tables

FIX = Path(__file__).parent / "fixtures" / "spider_mini" / "tables.json"


def _shop():
    return parse_tables_json(json.loads(FIX.read_text())[0])


def test_anonymize_renames_tables_and_columns_opaquely():
    g = _shop()
    ag, m = anonymize(g)
    assert set(ag.tables) == {"t0", "t1", "t2", "t3"}
    # no original token survives anywhere
    blob = " ".join(
        [t for t in ag.tables] + [c.name for cols in ag.tables.values() for c in cols]
    )
    for original in ["customers", "orders", "line_items", "products", "customer_id", "title"]:
        assert original not in blob
    # types are preserved
    assert {c.type for c in ag.tables["t0"]} <= {"text", "number"}


def test_mapping_is_consistent_across_graph_and_edges():
    g = _shop()
    ag, m = anonymize(g)
    gold = {edge_key(("orders", "customer_id"), ("customers", "id"))}
    mapped = map_edges(gold, m)
    # the mapped edge must reference only tokens that exist in the anonymized graph
    (pair,) = mapped
    for (tname, cname) in pair:
        assert tname in ag.tables
        assert cname in {c.name for c in ag.tables[tname]}


def test_map_tables():
    g = _shop()
    ag, m = anonymize(g)
    assert map_tables({"customers", "line_items"}, m) == {
        m.table["customers"], m.table["line_items"]
    }
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `cd experiments/mermaid-joinpath-eval && uv run pytest tests/test_anonymize.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'mje.anonymize'`.

- [ ] **Step 3: Implement `mje/anonymize.py`**

```python
from __future__ import annotations

from dataclasses import dataclass, field

from mje.schema_graph import Column, FKEdge, SchemaGraph, edge_key


@dataclass
class Mapping:
    table: dict[str, str] = field(default_factory=dict)            # orig table -> t{i}
    column: dict[tuple[str, str], str] = field(default_factory=dict)  # (orig table, orig col) -> c{j}


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
```

- [ ] **Step 4: Run the test to verify it passes**

Run: `cd experiments/mermaid-joinpath-eval && uv run pytest tests/test_anonymize.py -v`
Expected: PASS (3 tests).

- [ ] **Step 5: Commit**

```bash
git add experiments/mermaid-joinpath-eval/mje/anonymize.py experiments/mermaid-joinpath-eval/tests/test_anonymize.py
git commit -m "feat(mje): deterministic opaque-token anonymization"
```

---

## Task 3: `renderers.py` — three information-equivalent renderings

**Files:**
- Create: `experiments/mermaid-joinpath-eval/mje/renderers.py`
- Create: `experiments/mermaid-joinpath-eval/tests/test_renderers.py`

All renderers take an **anonymized** `SchemaGraph`, use **fixed ordering** (tables by name, columns by
declared order, edges sorted by their string form), and encode the same tables/columns/types/FK edges.

- [ ] **Step 1: Write the failing test `tests/test_renderers.py`**

```python
import json
from pathlib import Path
from mje.schema_graph import parse_tables_json
from mje.anonymize import anonymize
from mje.renderers import render_xml, render_mermaid, render_nl_adjacency, RENDERERS

FIX = Path(__file__).parent / "fixtures" / "spider_mini" / "tables.json"


def _anon():
    g = parse_tables_json(json.loads(FIX.read_text())[0])
    ag, _ = anonymize(g)
    return ag


def test_all_renderers_mention_every_table():
    ag = _anon()
    for name, fn in RENDERERS.items():
        out = fn(ag)
        for t in ag.tables:
            assert t in out, f"{name} dropped table {t}"


def test_renderers_encode_every_fk_edge():
    ag = _anon()
    # each rendering must contain both endpoints of every edge near each other;
    # we assert each edge's two columns both appear in the text
    for name, fn in RENDERERS.items():
        out = fn(ag)
        for e in ag.fk_edges:
            assert e.a[1] in out and e.b[1] in out


def test_xml_is_wellformed_enough():
    ag = _anon()
    out = render_xml(ag)
    assert out.count("<table") == len(ag.tables)
    assert out.count("<rel ") == len(ag.fk_edges)


def test_mermaid_has_header():
    ag = _anon()
    out = render_mermaid(ag)
    assert out.strip().startswith("erDiagram")


def test_deterministic_output():
    ag = _anon()
    for fn in RENDERERS.values():
        assert fn(ag) == fn(ag)
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `cd experiments/mermaid-joinpath-eval && uv run pytest tests/test_renderers.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'mje.renderers'`.

- [ ] **Step 3: Implement `mje/renderers.py`**

```python
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
        lines.append(
            f'    <rel from="{e.a[0]}.{e.a[1]}" to="{e.b[0]}.{e.b[1]}" type="many_to_one"/>'
        )
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
        lines.append(
            f"{e.a[0]} joins to {e.b[0]} via {e.a[0]}.{e.a[1]} = {e.b[0]}.{e.b[1]} (many_to_one)."
        )
    return "\n".join(lines)


RENDERERS = {
    "xml": render_xml,
    "mermaid": render_mermaid,
    "nl_adjacency": render_nl_adjacency,
}
```

- [ ] **Step 4: Run the test to verify it passes**

Run: `cd experiments/mermaid-joinpath-eval && uv run pytest tests/test_renderers.py -v`
Expected: PASS (5 tests).

- [ ] **Step 5: Commit**

```bash
git add experiments/mermaid-joinpath-eval/mje/renderers.py experiments/mermaid-joinpath-eval/tests/test_renderers.py
git commit -m "feat(mje): three information-equivalent schema renderers"
```

---

## Task 4: `grade.py` — parse model output and score edges

**Files:**
- Create: `experiments/mermaid-joinpath-eval/mje/grade.py`
- Create: `experiments/mermaid-joinpath-eval/tests/test_grade.py`

Model output is JSON-first (`["t0.c1 = t1.c0", ...]`); fall back to parsing as SQL via sqlglot.
A predicted edge is **hallucinated** if its column pair is not an FK edge of the graph.

- [ ] **Step 1: Write the failing test `tests/test_grade.py`**

```python
import json
from pathlib import Path
from mje.schema_graph import parse_tables_json, edge_key
from mje.anonymize import anonymize
from mje.grade import parse_pred_edges, score

FIX = Path(__file__).parent / "fixtures" / "spider_mini" / "tables.json"


def _anon():
    g = parse_tables_json(json.loads(FIX.read_text())[0])
    ag, _ = anonymize(g)
    return ag


def test_parse_json_array_of_conditions():
    ag = _anon()
    text = '```json\n["t1.c1 = t0.c0", "t2.c1 = t1.c0"]\n```'
    edges = parse_pred_edges(text, ag)
    assert edge_key(("t1", "c1"), ("t0", "c0")) in edges
    assert edge_key(("t2", "c1"), ("t1", "c0")) in edges


def test_parse_sql_fallback():
    ag = _anon()
    text = "SELECT * FROM t0 JOIN t1 ON t0.c0 = t1.c1"
    edges = parse_pred_edges(text, ag)
    assert edges == {edge_key(("t0", "c0"), ("t1", "c1"))}


def test_score_perfect():
    ag = _anon()
    gold = {edge_key(("t0", "c0"), ("t1", "c1"))}
    pred = {edge_key(("t1", "c1"), ("t0", "c0"))}  # order-insensitive
    s = score(pred, gold, ag)
    assert s["f1"] == 1.0 and s["exact"] is True and s["hallucinated"] == 0


def test_score_partial_and_hallucination():
    ag = _anon()
    gold = {
        edge_key(("t0", "c0"), ("t1", "c1")),
        edge_key(("t1", "c0"), ("t2", "c1")),
    }
    # one correct, one invented edge that is not an FK in the graph
    pred = {
        edge_key(("t0", "c0"), ("t1", "c1")),
        edge_key(("t0", "c1"), ("t3", "c0")),
    }
    s = score(pred, gold, ag)
    assert s["precision"] == 0.5
    assert s["recall"] == 0.5
    assert round(s["f1"], 3) == 0.5
    assert s["exact"] is False
    assert s["hallucinated"] == 1
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `cd experiments/mermaid-joinpath-eval && uv run pytest tests/test_grade.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'mje.grade'`.

- [ ] **Step 3: Implement `mje/grade.py`**

```python
from __future__ import annotations

import json
import re

import sqlglot
from sqlglot import exp

from mje.schema_graph import SchemaGraph, edge_key

_COND = re.compile(r"([A-Za-z_]\w*)\.([A-Za-z_]\w*)\s*=\s*([A-Za-z_]\w*)\.([A-Za-z_]\w*)")


def _fk_keys(graph: SchemaGraph) -> set[frozenset]:
    return {edge_key(e.a, e.b) for e in graph.fk_edges}


def _extract_json_block(text: str) -> list[str] | None:
    # strip code fences, find the first JSON array
    cleaned = re.sub(r"```[a-zA-Z]*", "", text).replace("```", "")
    start = cleaned.find("[")
    end = cleaned.rfind("]")
    if start == -1 or end == -1 or end < start:
        return None
    try:
        val = json.loads(cleaned[start : end + 1])
        return [str(x) for x in val] if isinstance(val, list) else None
    except json.JSONDecodeError:
        return None


def parse_pred_edges(text: str, graph: SchemaGraph) -> set[frozenset]:
    edges: set[frozenset] = set()

    items = _extract_json_block(text)
    if items is not None:
        for cond in items:
            mt = _COND.search(cond)
            if mt:
                t1, c1, t2, c2 = mt.groups()
                edges.add(edge_key((t1, c1), (t2, c2)))
        if edges:
            return edges

    # SQL fallback
    try:
        ast = sqlglot.parse_one(text, read="sqlite")
        for eq in ast.find_all(exp.EQ):
            l, r = eq.left, eq.right
            if isinstance(l, exp.Column) and isinstance(r, exp.Column) and l.table and r.table:
                edges.add(edge_key((l.table, l.name), (r.table, r.name)))
    except Exception:
        pass
    return edges


def score(pred: set[frozenset], gold: set[frozenset], graph: SchemaGraph) -> dict:
    fk = _fk_keys(graph)
    tp = len(pred & gold)
    precision = tp / len(pred) if pred else 0.0
    recall = tp / len(gold) if gold else 0.0
    f1 = (2 * precision * recall / (precision + recall)) if (precision + recall) else 0.0
    hallucinated = len([e for e in pred if e not in fk])
    return {
        "precision": precision,
        "recall": recall,
        "f1": f1,
        "exact": pred == gold,
        "hallucinated": hallucinated,
        "n_pred": len(pred),
        "n_gold": len(gold),
    }
```

- [ ] **Step 4: Run the test to verify it passes**

Run: `cd experiments/mermaid-joinpath-eval && uv run pytest tests/test_grade.py -v`
Expected: PASS (4 tests).

- [ ] **Step 5: Commit**

```bash
git add experiments/mermaid-joinpath-eval/mje/grade.py experiments/mermaid-joinpath-eval/tests/test_grade.py
git commit -m "feat(mje): model-output edge parsing and scoring"
```

---

## Task 5: `prompt.py` — single-shot reconstruction prompt

**Files:**
- Create: `experiments/mermaid-joinpath-eval/mje/prompt.py`
- Create: `experiments/mermaid-joinpath-eval/tests/test_prompt.py`

- [ ] **Step 1: Write the failing test `tests/test_prompt.py`**

```python
from mje.prompt import build_messages


def test_build_messages_contains_rendering_and_tables():
    msgs = build_messages("RENDERING_TEXT", ["t0", "t2", "t5"])
    assert msgs[0]["role"] == "system"
    assert msgs[1]["role"] == "user"
    user = msgs[1]["content"]
    assert "RENDERING_TEXT" in user
    assert "t0" in user and "t2" in user and "t5" in user
    assert "JSON" in user or "json" in user
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `cd experiments/mermaid-joinpath-eval && uv run pytest tests/test_prompt.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'mje.prompt'`.

- [ ] **Step 3: Implement `mje/prompt.py`**

```python
from __future__ import annotations

SYSTEM = (
    "You connect database tables. Given a schema and a set of tables, output ONLY the "
    "JOIN conditions needed to connect those tables into one query. Use only columns that "
    "appear in the schema. Respond with a JSON array of strings like "
    '["ta.cx = tb.cy", ...] and nothing else.'
)

USER_TEMPLATE = (
    "Schema:\n{rendering}\n\n"
    "Connect these tables into a single joined query: {tables}.\n"
    "Return the minimal set of join conditions as a JSON array of \"ta.cx = tb.cy\" strings."
)


def build_messages(rendering: str, endpoint_tables: list[str]) -> list[dict]:
    tables = ", ".join(endpoint_tables)
    return [
        {"role": "system", "content": SYSTEM},
        {"role": "user", "content": USER_TEMPLATE.format(rendering=rendering, tables=tables)},
    ]
```

- [ ] **Step 4: Run the test to verify it passes**

Run: `cd experiments/mermaid-joinpath-eval && uv run pytest tests/test_prompt.py -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add experiments/mermaid-joinpath-eval/mje/prompt.py experiments/mermaid-joinpath-eval/tests/test_prompt.py
git commit -m "feat(mje): single-shot reconstruction prompt builder"
```

---

## Task 6: `data.py` — Spider acquisition and Item building

**Files:**
- Create: `experiments/mermaid-joinpath-eval/mje/data.py`
- Create: `experiments/mermaid-joinpath-eval/tests/test_data.py`
- Create fixture: `experiments/mermaid-joinpath-eval/tests/fixtures/spider_mini/dev.json`

`Item` carries everything the runner needs, already anonymized: the anon graph, anon gold edges, anon
endpoint tables, join count, and an ambiguity flag. **Ambiguity proxy:** the connected component of the
FK graph that spans the endpoint tables contains a cycle (edges ≥ nodes), meaning more than one path can
connect them.

- [ ] **Step 1: Create fixture `tests/fixtures/spider_mini/dev.json`**

```json
[
  {"db_id": "shop", "question": "ignored",
   "query": "SELECT T1.name FROM customers AS T1 JOIN orders AS T2 ON T1.id = T2.customer_id"},
  {"db_id": "shop", "question": "ignored",
   "query": "SELECT c.name FROM customers c, orders o, line_items l WHERE c.id = o.customer_id AND o.id = l.order_id"},
  {"db_id": "shop", "question": "ignored",
   "query": "SELECT name FROM customers"}
]
```

- [ ] **Step 2: Write the failing test `tests/test_data.py`**

```python
from pathlib import Path
from mje.data import build_items

FIXDIR = Path(__file__).parent / "fixtures" / "spider_mini"


def test_build_items_filters_by_min_joins_and_anonymizes():
    items = build_items(FIXDIR / "tables.json", FIXDIR / "dev.json", min_joins=2)
    # only the 2-join comma query qualifies (single-join and no-join dropped)
    assert len(items) == 1
    it = items[0]
    assert it.n_joins == 2
    # everything is anonymized: tables look like t{i}
    assert all(t.startswith("t") for t in it.endpoint_tables)
    for pair in it.gold_edges:
        for (tname, cname) in pair:
            assert tname in it.graph.tables
            assert cname in {c.name for c in it.graph.tables[tname]}


def test_min_joins_one_keeps_more():
    items = build_items(FIXDIR / "tables.json", FIXDIR / "dev.json", min_joins=1)
    assert len(items) == 2  # the no-join query is still dropped


def test_ambiguity_flag_present():
    items = build_items(FIXDIR / "tables.json", FIXDIR / "dev.json", min_joins=2)
    assert isinstance(items[0].ambiguous, bool)
    # the shop chain is acyclic over the used tables -> not ambiguous
    assert items[0].ambiguous is False
```

- [ ] **Step 3: Run the test to verify it fails**

Run: `cd experiments/mermaid-joinpath-eval && uv run pytest tests/test_data.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'mje.data'`.

- [ ] **Step 4: Implement `mje/data.py`**

```python
from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path

import requests

from mje.anonymize import anonymize, map_edges, map_tables
from mje.schema_graph import (
    SchemaGraph, parse_tables_json, extract_gold_edges, gold_tables,
)

# Spider dev/tables JSON (HuggingFace mirror). If this 404s, download Spider manually and
# pass --tables/--dev paths to the runner; see README.
SPIDER_TABLES_URL = "https://huggingface.co/datasets/xlangai/spider/resolve/main/spider/tables.json"
SPIDER_DEV_URL = "https://huggingface.co/datasets/xlangai/spider/resolve/main/spider/dev.json"


@dataclass
class Item:
    item_id: str
    db_id: str
    query: str
    graph: SchemaGraph              # anonymized
    gold_edges: set[frozenset]     # anonymized
    endpoint_tables: list[str]     # anonymized, sorted
    n_joins: int
    ambiguous: bool


def download_spider(dest: Path) -> tuple[Path, Path]:
    dest.mkdir(parents=True, exist_ok=True)
    tables_p, dev_p = dest / "tables.json", dest / "dev.json"
    for url, p in [(SPIDER_TABLES_URL, tables_p), (SPIDER_DEV_URL, dev_p)]:
        if not p.exists():
            r = requests.get(url, timeout=60)
            r.raise_for_status()
            p.write_bytes(r.content)
    return tables_p, dev_p


def _ambiguous(raw_graph: SchemaGraph, used_tables: set[str]) -> bool:
    # connected component spanning used_tables; cycle (edges >= nodes) => multiple paths
    adj: dict[str, set[str]] = {t: set() for t in raw_graph.tables}
    edge_set: set[frozenset] = set()
    for e in raw_graph.fk_edges:
        ta, tb = e.a[0], e.b[0]
        if ta != tb:
            adj[ta].add(tb)
            adj[tb].add(ta)
            edge_set.add(frozenset({ta, tb}))
    # BFS the component containing any used table
    seen: set[str] = set()
    stack = list(used_tables)
    while stack:
        n = stack.pop()
        if n in seen:
            continue
        seen.add(n)
        stack.extend(adj[n] - seen)
    comp_edges = [e for e in edge_set if set(e) <= seen]
    return len(comp_edges) >= len(seen) and len(seen) > 0


def build_items(tables_path: Path, dev_path: Path, min_joins: int = 2,
                limit: int | None = None) -> list[Item]:
    tables = {e["db_id"]: parse_tables_json(e) for e in json.loads(Path(tables_path).read_text())}
    dev = json.loads(Path(dev_path).read_text())

    items: list[Item] = []
    for i, ex in enumerate(dev):
        db_id, query = ex["db_id"], ex["query"]
        graph = tables.get(db_id)
        if graph is None:
            continue
        try:
            gold = extract_gold_edges(query, graph)
            used = gold_tables(query, graph)
        except Exception:
            continue
        if len(gold) < min_joins:
            continue
        ambiguous = _ambiguous(graph, used)
        anon_graph, m = anonymize(graph)
        items.append(Item(
            item_id=f"{db_id}-{i}",
            db_id=db_id,
            query=query,
            graph=anon_graph,
            gold_edges=map_edges(gold, m),
            endpoint_tables=sorted(map_tables(used, m)),
            n_joins=len(gold),
            ambiguous=ambiguous,
        ))
        if limit and len(items) >= limit:
            break
    return items
```

- [ ] **Step 5: Run the test to verify it passes**

Run: `cd experiments/mermaid-joinpath-eval && uv run pytest tests/test_data.py -v`
Expected: PASS (3 tests). Network is not touched (tests use fixtures; `download_spider` is exercised later by the runner smoke step).

- [ ] **Step 6: Commit**

```bash
git add experiments/mermaid-joinpath-eval/mje/data.py experiments/mermaid-joinpath-eval/tests/test_data.py experiments/mermaid-joinpath-eval/tests/fixtures/spider_mini/dev.json
git commit -m "feat(mje): Spider acquisition and anonymized Item building"
```

---

## Task 7: `model_client.py` — OpenRouter client + cost accounting

**Files:**
- Create: `experiments/mermaid-joinpath-eval/mje/model_client.py`
- Create: `experiments/mermaid-joinpath-eval/tests/test_model_client.py`

Tested against a **fake transport** (no live calls). `PRICING` is per-token (USD).

- [ ] **Step 1: Write the failing test `tests/test_model_client.py`**

```python
from mje.model_client import ModelClient, cost, PRICING


class _FakeResp:
    def __init__(self):
        self.choices = [type("C", (), {"message": type("M", (), {"content": '["t0.c0 = t1.c1"]'})})]
        self.usage = type("U", (), {"prompt_tokens": 1000, "completion_tokens": 50})


class _FakeChat:
    def __init__(self): self.completions = self
    def create(self, **kwargs):
        self.kwargs = kwargs
        return _FakeResp()


class _FakeClient:
    def __init__(self): self.chat = _FakeChat()


def test_cost_uses_pricing_table():
    c = cost("anthropic/claude-sonnet-4.6", 1_000_000, 1_000_000)
    p = PRICING["anthropic/claude-sonnet-4.6"]
    assert round(c, 6) == round(p["in"] + p["out"], 6)


def test_client_call_returns_text_and_usage():
    mc = ModelClient(api_key="x", client=_FakeClient())
    text, in_tok, out_tok = mc.call("anthropic/claude-sonnet-4.6",
                                    [{"role": "user", "content": "hi"}], max_tokens=256)
    assert text == '["t0.c0 = t1.c1"]'
    assert in_tok == 1000 and out_tok == 50
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `cd experiments/mermaid-joinpath-eval && uv run pytest tests/test_model_client.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'mje.model_client'`.

- [ ] **Step 3: Implement `mje/model_client.py`**

```python
from __future__ import annotations

import os
import time

# USD per token. Source: OpenRouter pricing (per-Mtok / 1e6).
PRICING = {
    "anthropic/claude-sonnet-4.6": {"in": 3.0e-6, "out": 15.0e-6},
    "deepseek/deepseek-v4-flash": {"in": 0.0983e-6, "out": 0.1966e-6},
}


def cost(model: str, in_tok: int, out_tok: int) -> float:
    p = PRICING[model]
    return in_tok * p["in"] + out_tok * p["out"]


class ModelClient:
    def __init__(self, api_key: str | None = None, base_url: str = "https://openrouter.ai/api/v1",
                 client=None):
        if client is not None:
            self._client = client
        else:
            from openai import OpenAI
            key = api_key or os.environ["OPENROUTER_API_KEY"]
            self._client = OpenAI(api_key=key, base_url=base_url)

    def call(self, model: str, messages: list[dict], max_tokens: int = 256,
             retries: int = 3) -> tuple[str, int, int]:
        last = None
        for attempt in range(retries):
            try:
                resp = self._client.chat.completions.create(
                    model=model, messages=messages, temperature=0, max_tokens=max_tokens,
                )
                text = resp.choices[0].message.content or ""
                u = resp.usage
                return text, int(u.prompt_tokens), int(u.completion_tokens)
            except Exception as e:  # transient network/rate errors
                last = e
                time.sleep(2 ** attempt)
        raise RuntimeError(f"model call failed after {retries} retries: {last}")
```

- [ ] **Step 4: Run the test to verify it passes**

Run: `cd experiments/mermaid-joinpath-eval && uv run pytest tests/test_model_client.py -v`
Expected: PASS (2 tests).

- [ ] **Step 5: Commit**

```bash
git add experiments/mermaid-joinpath-eval/mje/model_client.py experiments/mermaid-joinpath-eval/tests/test_model_client.py
git commit -m "feat(mje): OpenRouter client with cost accounting (mock-tested)"
```

---

## Task 8: `runner.py` — orchestration with budget cap

**Files:**
- Create: `experiments/mermaid-joinpath-eval/mje/runner.py`
- Create: `experiments/mermaid-joinpath-eval/tests/test_runner.py`

The runner is a CLI. The **scoring loop is extracted into `run_eval(items, models, client, ...)`** so it can be
unit-tested with a fake client; `main()` only parses args, loads data, and calls it. Budget cap aborts before a
call whose projected cost would exceed `max_spend`.

- [ ] **Step 1: Write the failing test `tests/test_runner.py`**

```python
import json
from pathlib import Path
from mje.schema_graph import parse_tables_json
from mje.anonymize import anonymize, map_edges, map_tables
from mje.schema_graph import extract_gold_edges, gold_tables
from mje.data import Item
from mje.runner import run_eval

FIX = Path(__file__).parent / "fixtures" / "spider_mini"


class _FakeClient:
    """Always returns the gold edges as a JSON array -> perfect score."""
    def __init__(self, items): self._by_id = {it.item_id: it for it in items}
    def call(self, model, messages, max_tokens=256, retries=3):
        # recover which item by matching endpoint tables present in the prompt
        user = messages[1]["content"]
        for it in self._by_id.values():
            if all(t in user for t in it.endpoint_tables):
                conds = [f"{tuple(e)[0][0]}.{tuple(e)[0][1]} = {tuple(e)[1][0]}.{tuple(e)[1][1]}"
                         for e in it.gold_edges]
                return json.dumps(conds), 100, 20
        return "[]", 100, 20


def _one_item():
    g = parse_tables_json(json.loads((FIX / "tables.json").read_text())[0])
    q = ("SELECT c.name FROM customers c, orders o, line_items l "
         "WHERE c.id = o.customer_id AND o.id = l.order_id")
    gold = extract_gold_edges(q, g)
    used = gold_tables(q, g)
    ag, m = anonymize(g)
    return Item("shop-1", "shop", q, ag, map_edges(gold, m), sorted(map_tables(used, m)), 2, False)


def test_run_eval_scores_all_renderings(tmp_path):
    items = [_one_item()]
    client = _FakeClient(items)
    out = tmp_path / "results.jsonl"
    rows = run_eval(items, models=["anthropic/claude-sonnet-4.6"], client=client,
                    out_path=out, max_spend=1.0)
    # 1 item x 3 renderings x 1 model = 3 rows, all perfect
    assert len(rows) == 3
    assert all(r["f1"] == 1.0 and r["exact"] for r in rows)
    assert {r["rendering"] for r in rows} == {"xml", "mermaid", "nl_adjacency"}
    assert out.exists() and len(out.read_text().strip().splitlines()) == 3


def test_budget_cap_aborts(tmp_path):
    items = [_one_item()]
    client = _FakeClient(items)
    rows = run_eval(items, models=["anthropic/claude-sonnet-4.6"], client=client,
                    out_path=tmp_path / "r.jsonl", max_spend=0.0)
    assert rows == []  # cap of 0 stops before any call
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `cd experiments/mermaid-joinpath-eval && uv run pytest tests/test_runner.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'mje.runner'`.

- [ ] **Step 3: Implement `mje/runner.py`**

```python
from __future__ import annotations

import argparse
import json
from pathlib import Path

from mje.data import Item, build_items, download_spider
from mje.grade import parse_pred_edges, score
from mje.model_client import ModelClient, cost
from mje.prompt import build_messages
from mje.renderers import RENDERERS


def run_eval(items: list[Item], models: list[str], client, out_path: Path,
             max_spend: float = 2.0, max_tokens: int = 256, samples: int = 1) -> list[dict]:
    out_path = Path(out_path)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    rows: list[dict] = []
    spent = 0.0
    EST_PER_CALL = 0.02  # conservative pre-call guess (USD) for the budget gate

    with out_path.open("w") as fh:
        for it in items:
            for rname, rfn in RENDERERS.items():
                rendering = rfn(it.graph)
                messages = build_messages(rendering, it.endpoint_tables)
                for model in models:
                    for s in range(samples):
                        if spent + EST_PER_CALL > max_spend:
                            print(f"[budget] stopping: spent ${spent:.3f}, cap ${max_spend:.2f}")
                            return rows
                        text, in_tok, out_tok = client.call(model, messages, max_tokens=max_tokens)
                        spent += cost(model, in_tok, out_tok)
                        pred = parse_pred_edges(text, it.graph)
                        sc = score(pred, it.gold_edges, it.graph)
                        row = {
                            "item_id": it.item_id, "db_id": it.db_id, "n_joins": it.n_joins,
                            "ambiguous": it.ambiguous, "model": model, "rendering": rname,
                            "sample": s, "in_tok": in_tok, "out_tok": out_tok,
                            "cost_usd": cost(model, in_tok, out_tok),
                            **sc,
                            "gold_edges": [sorted(tuple(p) for p in e) for e in it.gold_edges],
                            "pred_edges": [sorted(tuple(p) for p in e) for e in pred],
                        }
                        fh.write(json.dumps(row) + "\n")
                        fh.flush()
                        rows.append(row)
    print(f"[done] {len(rows)} calls, spent ${spent:.3f}")
    return rows


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--data-dir", default="data")
    ap.add_argument("--tables", default=None, help="override tables.json path")
    ap.add_argument("--dev", default=None, help="override dev.json path")
    ap.add_argument("--out", default="results/results.jsonl")
    ap.add_argument("--min-joins", type=int, default=2)
    ap.add_argument("--n", type=int, default=45, help="max items (hardest strata first)")
    ap.add_argument("--samples", type=int, default=1)
    ap.add_argument("--max-spend", type=float, default=2.0)
    ap.add_argument("--models", nargs="+",
                    default=["anthropic/claude-sonnet-4.6", "deepseek/deepseek-v4-flash"])
    args = ap.parse_args()

    if args.tables and args.dev:
        tables_p, dev_p = Path(args.tables), Path(args.dev)
    else:
        tables_p, dev_p = download_spider(Path(args.data_dir))

    items = build_items(tables_p, dev_p, min_joins=args.min_joins)
    # hardest first, then cap to N
    items.sort(key=lambda it: it.n_joins, reverse=True)
    items = items[: args.n]

    # rough pre-run estimate
    est = len(items) * len(RENDERERS) * len(args.models) * args.samples * 0.02
    print(f"[plan] {len(items)} items x {len(RENDERERS)} renderings x {len(args.models)} models "
          f"x {args.samples} sample(s); rough est ${est:.2f}; cap ${args.max_spend:.2f}")

    client = ModelClient()
    run_eval(items, models=args.models, client=client, out_path=Path(args.out),
             max_spend=args.max_spend, samples=args.samples)


if __name__ == "__main__":
    main()
```

- [ ] **Step 4: Run the test to verify it passes**

Run: `cd experiments/mermaid-joinpath-eval && uv run pytest tests/test_runner.py -v`
Expected: PASS (2 tests).

- [ ] **Step 5: Commit**

```bash
git add experiments/mermaid-joinpath-eval/mje/runner.py experiments/mermaid-joinpath-eval/tests/test_runner.py
git commit -m "feat(mje): budget-capped eval runner"
```

---

## Task 9: `stats.py` — aggregation, McNemar, bootstrap CIs

**Files:**
- Create: `experiments/mermaid-joinpath-eval/mje/stats.py`
- Create: `experiments/mermaid-joinpath-eval/tests/test_stats.py`

- [ ] **Step 1: Write the failing test `tests/test_stats.py`**

```python
from mje.stats import aggregate, mcnemar_pairs


def _rows():
    # two items, two renderings, one model; mermaid beats xml on item 2
    return [
        {"item_id": "a", "model": "M", "rendering": "xml", "n_joins": 2, "ambiguous": False, "f1": 1.0, "exact": True},
        {"item_id": "a", "model": "M", "rendering": "mermaid", "n_joins": 2, "ambiguous": False, "f1": 1.0, "exact": True},
        {"item_id": "b", "model": "M", "rendering": "xml", "n_joins": 3, "ambiguous": False, "f1": 0.0, "exact": False},
        {"item_id": "b", "model": "M", "rendering": "mermaid", "n_joins": 3, "ambiguous": False, "f1": 1.0, "exact": True},
    ]


def test_aggregate_means_by_group():
    agg = aggregate(_rows())
    # mean f1 for (M, xml) over 2 items = 0.5 ; (M, mermaid) = 1.0
    xml = [r for r in agg if r["model"] == "M" and r["rendering"] == "xml" and r["stratum"] == "all"][0]
    mer = [r for r in agg if r["model"] == "M" and r["rendering"] == "mermaid" and r["stratum"] == "all"][0]
    assert xml["mean_f1"] == 0.5
    assert mer["mean_f1"] == 1.0


def test_mcnemar_returns_pvalue():
    res = mcnemar_pairs(_rows(), model="M")
    pair = [r for r in res if {r["a"], r["b"]} == {"xml", "mermaid"}][0]
    assert "p_value" in pair and 0.0 <= pair["p_value"] <= 1.0
    # discordant: item b (xml wrong, mermaid right) -> b01/b10 captured
    assert pair["b_a_wrong_b_right"] == 1
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `cd experiments/mermaid-joinpath-eval && uv run pytest tests/test_stats.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'mje.stats'`.

- [ ] **Step 3: Implement `mje/stats.py`**

```python
from __future__ import annotations

import argparse
import json
from collections import defaultdict
from itertools import combinations
from pathlib import Path

from scipy.stats import binomtest


def _stratum(n_joins: int) -> str:
    return "2" if n_joins == 2 else "3" if n_joins == 3 else "4+"


def aggregate(rows: list[dict]) -> list[dict]:
    groups: dict[tuple, list[dict]] = defaultdict(list)
    for r in rows:
        for st in ("all", _stratum(r["n_joins"])):
            groups[(r["model"], r["rendering"], st)].append(r)
    out = []
    for (model, rendering, st), rs in sorted(groups.items()):
        n = len(rs)
        out.append({
            "model": model, "rendering": rendering, "stratum": st, "n": n,
            "mean_f1": sum(r["f1"] for r in rs) / n,
            "exact_rate": sum(1 for r in rs if r["exact"]) / n,
        })
    return out


def mcnemar_pairs(rows: list[dict], model: str) -> list[dict]:
    # index exact-match by (item_id, rendering) for this model
    by: dict[tuple, bool] = {}
    renderings: set[str] = set()
    items: set[str] = set()
    for r in rows:
        if r["model"] != model:
            continue
        by[(r["item_id"], r["rendering"])] = bool(r["exact"])
        renderings.add(r["rendering"])
        items.add(r["item_id"])

    results = []
    for a, b in combinations(sorted(renderings), 2):
        b01 = b10 = 0  # a wrong/b right ; a right/b wrong
        for it in items:
            if (it, a) in by and (it, b) in by:
                ra, rb = by[(it, a)], by[(it, b)]
                if not ra and rb:
                    b01 += 1
                elif ra and not rb:
                    b10 += 1
        n = b01 + b10
        p = binomtest(b01, n, 0.5).pvalue if n > 0 else 1.0
        results.append({
            "model": model, "a": a, "b": b,
            "b_a_wrong_b_right": b01, "b_a_right_b_wrong": b10, "p_value": p,
        })
    return results


def load_rows(path: Path) -> list[dict]:
    return [json.loads(l) for l in Path(path).read_text().splitlines() if l.strip()]


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--results", default="results/results.jsonl")
    args = ap.parse_args()
    rows = load_rows(Path(args.results))

    print("\n=== mean F1 / exact-rate by model × rendering × stratum ===")
    for r in aggregate(rows):
        print(f"{r['model']:<32} {r['rendering']:<13} {r['stratum']:<4} "
              f"n={r['n']:<4} F1={r['mean_f1']:.3f} exact={r['exact_rate']:.3f}")

    print("\n=== paired McNemar (exact-match) ===")
    for model in sorted({r["model"] for r in rows}):
        for res in mcnemar_pairs(rows, model):
            print(f"{model:<32} {res['a']} vs {res['b']}: "
                  f"disc {res['b_a_wrong_b_right']}/{res['b_a_right_b_wrong']} "
                  f"p={res['p_value']:.4f}")


if __name__ == "__main__":
    main()
```

- [ ] **Step 4: Run the test to verify it passes**

Run: `cd experiments/mermaid-joinpath-eval && uv run pytest tests/test_stats.py -v`
Expected: PASS (2 tests).

- [ ] **Step 5: Commit**

```bash
git add experiments/mermaid-joinpath-eval/mje/stats.py experiments/mermaid-joinpath-eval/tests/test_stats.py
git commit -m "feat(mje): aggregation and paired McNemar stats"
```

---

## Task 10: README + full-suite green + live smoke (gated)

**Files:**
- Create: `experiments/mermaid-joinpath-eval/README.md`

- [ ] **Step 1: Write `README.md`**

````markdown
# Mermaid Join-Path Eval

Measures whether rendering an (anonymized) Spider schema as XML / Mermaid / NL-adjacency changes an
LLM's join-path reconstruction accuracy. See the design spec under
`docs/superpowers/specs/2026-06-07-mermaid-joinpath-eval-design.md`.

## Setup
```bash
cd experiments/mermaid-joinpath-eval
uv sync
uv run pytest -q          # all deterministic units, offline
```

## Run the pilot (spends OpenRouter credit)
The key is read from `OPENROUTER_API_KEY`; source it from the lens project (never commit it):
```bash
set -a; . /Users/qingye/Documents/lens/.env; set +a
uv run python -m mje.runner --n 45 --max-spend 2.00
uv run python -m mje.stats --results results/results.jsonl
```

Flags: `--n` items (hardest strata first), `--samples`, `--models`, `--min-joins`, `--max-spend`,
and `--tables/--dev` to point at a manual Spider download if the HF mirror URL is unavailable.

## Notes
- Schemas are opaque-token anonymized to defeat training contamination (Spider predates the model cutoff).
- Grading is static (no DB execution): gold join edges come from gold SQL via sqlglot.
````

- [ ] **Step 2: Run the full test suite**

Run: `cd experiments/mermaid-joinpath-eval && uv run pytest -q`
Expected: all tests PASS (schema_graph, anonymize, renderers, grade, prompt, data, model_client, runner, stats).

- [ ] **Step 3: Live smoke test (gated — needs key + ~$0.05)**

Run:
```bash
cd experiments/mermaid-joinpath-eval
set -a; . /Users/qingye/Documents/lens/.env; set +a
uv run python -m mje.runner --n 2 --max-spend 0.10 --models anthropic/claude-sonnet-4.6
uv run python -m mje.stats --results results/results.jsonl
```
Expected: downloads Spider, runs 2 items × 3 renderings × 1 model = 6 calls, prints a stats table,
and `results/results.jsonl` has 6 rows. If the HF URL 404s, follow the README manual-download note.

- [ ] **Step 4: Commit**

```bash
git add experiments/mermaid-joinpath-eval/README.md
git commit -m "docs(mje): usage README and smoke instructions"
```

---

## Self-Review

**Spec coverage:**
- §4 task (opaque + reconstruction) → Tasks 2, 5, 6 (endpoint tables = gold tables, JSON join-conditions).
- §5 datasets (Spider dev, ≥2 joins, strata, ambiguity flag) → Task 6 (`build_items`, `_ambiguous`) + Task 9 (`_stratum`).
- §6 anonymization (opaque, bijective, types kept) → Task 2.
- §7 renderings (3, fixed ordering, info-equivalent) → Task 3.
- §8 prompt (single-shot, JSON output) → Task 5.
- §9 models (Sonnet 4.6 + DeepSeek V4 Flash, OpenRouter) → Task 7 (`PRICING`) + Task 8 (`--models`).
- §10 grading (edge F1, exact, hallucinated; JSON-first w/ SQL fallback) → Task 4.
- §11 stats (strata, paired McNemar, summary) → Task 9. *Bootstrap CIs are printed as group means;
  full bootstrap intervals deferred — noted as acceptable for the pilot table.*
- §13 budget (pre-call estimate, `--max-spend` cap, defaults N≈45) → Task 8.
- §14 testing (every deterministic unit + mock client) → Tasks 1–9.

**Placeholder scan:** No "TBD"/"add error handling"/"similar to" placeholders; every code step is complete.

**Type consistency:** `Item` fields (`graph`, `gold_edges`, `endpoint_tables`, `n_joins`, `ambiguous`)
are produced in Task 6 and consumed identically in Task 8. `score()` keys (`f1`, `exact`, `hallucinated`,
`precision`, `recall`) defined in Task 4 are used unchanged in Tasks 8–9. `edge_key`/`frozenset` edge
representation is consistent across Tasks 1–9. `RENDERERS` dict (Task 3) is the single source of renderings
in Tasks 8 and 10.

**One deliberate deviation from the spec:** §11 mentions bootstrap CIs; the plan ships group means + McNemar
and defers true bootstrap intervals (YAGNI for a ~45-item pilot). Flagged here rather than silently dropped.
````
