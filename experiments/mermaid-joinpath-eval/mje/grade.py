from __future__ import annotations

import json
import re

import sqlglot
from sqlglot import exp

from mje.schema_graph import SchemaGraph, edge_key

_COND = re.compile(
    r"([A-Za-z_]\w*)\.([A-Za-z_]\w*)\s*=\s*([A-Za-z_]\w*)\.([A-Za-z_]\w*)"
)


def _fk_keys(graph: SchemaGraph) -> set[frozenset]:
    return {edge_key(e.a, e.b) for e in graph.fk_edges}


def _extract_json_block(text: str) -> list[str] | None:
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

    try:
        ast = sqlglot.parse_one(text, read="sqlite")
        for eq in ast.find_all(exp.EQ):
            lhs, rhs = eq.left, eq.right
            if (
                isinstance(lhs, exp.Column)
                and isinstance(rhs, exp.Column)
                and lhs.table
                and rhs.table
            ):
                edges.add(edge_key((lhs.table, lhs.name), (rhs.table, rhs.name)))
    except Exception:
        pass
    return edges


def score(pred: set[frozenset], gold: set[frozenset], graph: SchemaGraph) -> dict:
    fk = _fk_keys(graph)
    tp = len(pred & gold)
    precision = tp / len(pred) if pred else 0.0
    recall = tp / len(gold) if gold else 0.0
    f1 = (
        (2 * precision * recall / (precision + recall)) if (precision + recall) else 0.0
    )
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
