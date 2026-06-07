import json
from pathlib import Path

from mje.anonymize import anonymize, map_edges, map_tables
from mje.schema_graph import edge_key, parse_tables_json

FIX = Path(__file__).parent / "fixtures" / "spider_mini" / "tables.json"


def _shop():
    return parse_tables_json(json.loads(FIX.read_text())[0])


def test_anonymize_renames_tables_and_columns_opaquely():
    g = _shop()
    ag, m = anonymize(g)
    assert set(ag.tables) == {"t0", "t1", "t2", "t3"}
    blob = " ".join(
        [t for t in ag.tables] + [c.name for cols in ag.tables.values() for c in cols]
    )
    for original in [
        "customers",
        "orders",
        "line_items",
        "products",
        "customer_id",
        "title",
    ]:
        assert original not in blob
    assert {c.type for c in ag.tables["t0"]} <= {"text", "number"}


def test_mapping_is_consistent_across_graph_and_edges():
    g = _shop()
    ag, m = anonymize(g)
    gold = {edge_key(("orders", "customer_id"), ("customers", "id"))}
    mapped = map_edges(gold, m)
    (pair,) = mapped
    for tname, cname in pair:
        assert tname in ag.tables
        assert cname in {c.name for c in ag.tables[tname]}


def test_map_tables():
    g = _shop()
    ag, m = anonymize(g)
    assert map_tables({"customers", "line_items"}, m) == {
        m.table["customers"],
        m.table["line_items"],
    }
