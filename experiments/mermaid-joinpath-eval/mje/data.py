from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path

import requests

from mje.anonymize import anonymize, map_edges, map_tables
from mje.schema_graph import (
    SchemaGraph,
    extract_gold_edges,
    gold_tables,
    parse_tables_json,
)

# Spider dev/tables JSON — taoyds/spider raw GitHub source (evaluation_examples mirror).
# If this 404s, download Spider manually and pass --tables/--dev paths to the runner.
SPIDER_TABLES_URL = "https://raw.githubusercontent.com/taoyds/spider/master/evaluation_examples/examples/tables.json"
SPIDER_DEV_URL = "https://raw.githubusercontent.com/taoyds/spider/master/evaluation_examples/examples/dev.json"

# BIRD mini-dev JSON sources.
BIRD_TABLES_URL = "https://raw.githubusercontent.com/Harris-X/NLP2SQL/main/LLaMA-Factory/datasets/minidev/MINIDEV/dev_tables.json"
BIRD_DEV_URL = "https://huggingface.co/datasets/birdsql/bird_mini_dev/resolve/main/data/mini_dev_sqlite-00000-of-00001.json"


@dataclass
class Item:
    item_id: str
    db_id: str
    query: str
    graph: SchemaGraph  # anonymized
    gold_edges: set[frozenset]  # anonymized
    endpoint_tables: list[str]  # anonymized, sorted
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


def download_bird(dest: Path) -> tuple[Path, Path]:
    dest.mkdir(parents=True, exist_ok=True)
    tables_p, dev_p = dest / "dev_tables.json", dest / "mini_dev_sqlite.json"
    for url, p in [(BIRD_TABLES_URL, tables_p), (BIRD_DEV_URL, dev_p)]:
        if not p.exists():
            r = requests.get(url, timeout=120)
            r.raise_for_status()
            p.write_bytes(r.content)
    return tables_p, dev_p


def _ambiguous(raw_graph: SchemaGraph, used_tables: set[str]) -> bool:
    # Conservative proxy for join-path ambiguity (intentional for this experiment).
    # Limitations:
    #   (a) Treats the FK graph as a simple graph — two distinct FK column-pairs between
    #       the same table pair collapse to one edge, so ambiguity is under-counted.
    #   (b) If the used tables span multiple disconnected FK components the cycle test
    #       runs over the union of those components and may produce a false negative.
    # connected component spanning used_tables; cycle (edges >= nodes) => multiple paths
    adj: dict[str, set[str]] = {t: set() for t in raw_graph.tables}
    edge_set: set[frozenset] = set()
    for e in raw_graph.fk_edges:
        ta, tb = e.a[0], e.b[0]
        if ta != tb:
            adj[ta].add(tb)
            adj[tb].add(ta)
            edge_set.add(frozenset({ta, tb}))
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


def build_items(
    tables_path: Path,
    dev_path: Path,
    min_joins: int = 2,
    limit: int | None = None,
    query_key: str = "query",
) -> list[Item]:
    tables = {
        e["db_id"]: parse_tables_json(e)
        for e in json.loads(Path(tables_path).read_text())
    }
    dev = json.loads(Path(dev_path).read_text())

    items: list[Item] = []
    for i, ex in enumerate(dev):
        db_id, query = ex["db_id"], ex[query_key]
        graph = tables.get(db_id)
        if graph is None:
            continue
        try:
            gold = extract_gold_edges(query, graph)
            used = gold_tables(query, graph)
        except Exception:  # noqa: BLE001 — bad query must not abort whole build
            continue
        if len(gold) < min_joins:
            continue
        # Guard: skip items where gold edges reference non-existent tables (e.g. CTEs).
        valid_tables = {t.lower() for t in graph.tables}
        if any(tn.lower() not in valid_tables for pair in gold for (tn, _cn) in pair):
            continue
        ambiguous = _ambiguous(graph, used)
        anon_graph, m = anonymize(graph)
        items.append(
            Item(
                item_id=f"{db_id}-{i}",
                db_id=db_id,
                query=query,
                graph=anon_graph,
                gold_edges=map_edges(gold, m),
                endpoint_tables=sorted(map_tables(used, m)),
                n_joins=len(gold),
                ambiguous=ambiguous,
            )
        )
        if limit is not None and len(items) >= limit:
            break
    return items
