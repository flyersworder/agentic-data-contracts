"""Freeze the gold result of every LiveSQLBench Query task from PostgreSQL.

Run: LSB_DATA=... LSB_PG_DSN=... uv run --with psycopg2-binary \
         python prep/livesqlbench/gold.py <public livesqlbench_data.jsonl>

Runs each Query task's gold SQL (LSB_DATA/livesqlbench_base_full_v1_gt_kg_
testcases_20260613.jsonl) on the official PostgreSQL databases, normalised by
the official Soft-EX helpers, and writes LSB_DATA/gold_results_full_v1.json.
It also runs the same SQL on the DuckDB port and reports how many reproduce;
`freeze.py` makes the decision that counts.
"""

import collections
import json
import os
import sys
from pathlib import Path

PG_DSN = os.environ.get("LSB_PG_DSN")  # e.g. "host=... port=... user=... password=..."
if not PG_DSN:
    raise SystemExit("set LSB_PG_DSN to the LiveSQLBench PostgreSQL connection string")

import duckdb  # noqa: E402
import psycopg2  # noqa: E402

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from dce.benchmarks.lsb_grade import clean, normalise  # noqa: E402

DATA = Path(os.environ.get("LSB_DATA", Path.home() / "data" / "livesqlbench"))

PUB = sys.argv[1]
pub = {r["instance_id"]: r for r in map(json.loads, open(PUB, encoding="utf-8"))}
gt = {
    r["instance_id"]: r
    for r in map(
        json.loads,
        open(
            DATA / "livesqlbench_base_full_v1_gt_kg_testcases_20260613.jsonl",
            encoding="utf-8",
        ),
    )
}


def same(a, b, order):
    return a == b if order else set(a) == set(b)


pg_conns, dk_conns = {}, {}
gold = {}
stats = collections.Counter()
fails = collections.defaultdict(list)
for iid, p in pub.items():
    if p["category"] != "Query":
        continue
    db = p["selected_database"]
    sqls = clean(gt[iid]["sol_sql"])
    order = p["conditions"].get("order", False)
    if db not in pg_conns:
        pg_conns[db] = psycopg2.connect(PG_DSN, dbname=db)
        pg_conns[db].autocommit = True
        dk_conns[db] = duckdb.connect(
            str(DATA / "duckdb" / (db + ".duckdb")), read_only=True
        )
    try:
        with pg_conns[db].cursor() as cur:
            for s in sqls:
                cur.execute(s)
            pg_rows = normalise(cur.fetchall())
    except Exception as e:  # noqa: BLE001
        stats["pg_error"] += 1
        fails["pg_error"].append((iid, str(e).splitlines()[0][:120]))
        continue
    stats["gold_ok"] += 1
    stats["gold_empty"] += not pg_rows
    gold[iid] = {"db": db, "order": order, "rows": [list(r) for r in pg_rows]}
    try:
        rel = None
        for s in sqls:
            rel = dk_conns[db].execute(s)
        dk_rows = normalise(rel.fetchall() if rel is not None else [])
    except Exception as e:  # noqa: BLE001
        stats["duck_error"] += 1
        fails["duck_error"].append(
            (iid, type(e).__name__ + ": " + str(e).splitlines()[0][:110])
        )
        continue
    if same(dk_rows, pg_rows, order):
        stats["duck_match"] += 1
    else:
        stats["duck_differs"] += 1
        fails["duck_differs"].append(
            (iid, str(pg_rows[:2])[:90], str(dk_rows[:2])[:90])
        )
json.dump(
    gold,
    open(DATA / "gold_results_full_v1.json", "w", encoding="utf-8"),
    default=str,
)
print(dict(stats))
for k, v in fails.items():
    print("==", k, len(v))
    for x in v[:12]:
        print("  ", x)
