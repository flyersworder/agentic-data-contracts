import json
from pathlib import Path

from mje.anonymize import anonymize
from mje.grade import parse_pred_edges, score
from mje.schema_graph import edge_key, parse_tables_json

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
    pred = {edge_key(("t1", "c1"), ("t0", "c0"))}
    s = score(pred, gold, ag)
    assert s["f1"] == 1.0 and s["exact"] is True and s["hallucinated"] == 0


def test_score_partial_and_hallucination():
    ag = _anon()
    gold = {
        edge_key(("t0", "c0"), ("t1", "c1")),
        edge_key(("t1", "c0"), ("t2", "c1")),
    }
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
