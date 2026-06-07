import json
from pathlib import Path

from mje.anonymize import anonymize, map_edges, map_tables
from mje.data import Item
from mje.runner import run_eval
from mje.schema_graph import extract_gold_edges, gold_tables, parse_tables_json

FIX = Path(__file__).parent / "fixtures" / "spider_mini"


class _FakeClient:
    """Always returns the gold edges as a JSON array -> perfect score."""

    def __init__(self, items):
        self._by_id = {it.item_id: it for it in items}

    def call(self, model, messages, max_tokens=256, retries=3):
        user = messages[1]["content"]
        for it in self._by_id.values():
            if all(t in user for t in it.endpoint_tables):
                conds = [
                    f"{tuple(e)[0][0]}.{tuple(e)[0][1]}"
                    f" = {tuple(e)[1][0]}.{tuple(e)[1][1]}"
                    for e in it.gold_edges
                ]
                return json.dumps(conds), 100, 20
        return "[]", 100, 20


def _one_item():
    g = parse_tables_json(json.loads((FIX / "tables.json").read_text())[0])
    q = (
        "SELECT c.name FROM customers c, orders o, line_items l "
        "WHERE c.id = o.customer_id AND o.id = l.order_id"
    )
    gold = extract_gold_edges(q, g)
    used = gold_tables(q, g)
    ag, m = anonymize(g)
    return Item(
        "shop-1",
        "shop",
        q,
        ag,
        map_edges(gold, m),
        sorted(map_tables(used, m)),
        2,
        False,
    )


def test_run_eval_scores_all_renderings(tmp_path):
    items = [_one_item()]
    client = _FakeClient(items)
    out = tmp_path / "results.jsonl"
    rows = run_eval(
        items,
        models=["anthropic/claude-sonnet-4.6"],
        client=client,
        out_path=out,
        max_spend=1.0,
    )
    assert len(rows) == 3
    assert all(r["f1"] == 1.0 and r["exact"] for r in rows)
    assert {r["rendering"] for r in rows} == {"xml", "mermaid", "nl_adjacency"}
    assert out.exists() and len(out.read_text().strip().splitlines()) == 3


def test_budget_cap_aborts(tmp_path):
    items = [_one_item()]
    client = _FakeClient(items)
    rows = run_eval(
        items,
        models=["anthropic/claude-sonnet-4.6"],
        client=client,
        out_path=tmp_path / "r.jsonl",
        max_spend=0.0,
    )
    assert rows == []
