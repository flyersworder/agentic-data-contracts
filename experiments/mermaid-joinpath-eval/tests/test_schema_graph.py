import json
from pathlib import Path

from mje.schema_graph import (
    SchemaGraph,
    edge_key,
    extract_gold_edges,
    gold_tables,
    parse_tables_json,
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
    q1 = (
        "SELECT T1.name FROM customers AS T1 JOIN orders AS T2 "
        "ON T1.id = T2.customer_id"
    )
    e1 = extract_gold_edges(q1, g)
    assert e1 == {edge_key(("customers", "id"), ("orders", "customer_id"))}

    q2 = (
        "SELECT c.name FROM customers c, orders o, line_items l "
        "WHERE c.id = o.customer_id AND o.id = l.order_id"
    )
    e2 = extract_gold_edges(q2, g)
    assert e2 == {
        edge_key(("customers", "id"), ("orders", "customer_id")),
        edge_key(("orders", "id"), ("line_items", "order_id")),
    }


def test_extract_gold_edges_qualifies_unqualified_columns():
    g = _shop()
    q = "SELECT title FROM line_items JOIN products ON product_id = products.id"
    e = extract_gold_edges(q, g)
    assert e == {edge_key(("line_items", "product_id"), ("products", "id"))}


def test_gold_tables_returns_from_join_tables():
    g = _shop()
    q = (
        "SELECT c.name FROM customers c JOIN orders o ON c.id = o.customer_id "
        "JOIN line_items l ON o.id = l.order_id"
    )
    assert gold_tables(q, g) == {"customers", "orders", "line_items"}
