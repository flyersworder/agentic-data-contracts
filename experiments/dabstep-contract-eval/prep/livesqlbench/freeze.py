"""Classify every read-only task by whether its gold SQL reproduces the frozen
PostgreSQL result in DuckDB, over 20 fresh runs under the grader's connection
settings (`lsb_grade.connect_readonly`, memory limit included).

Run: LSB_DATA=... uv run --with sqlglot==30.19.0 python prep/livesqlbench/freeze.py

A task whose gold SQL reads the clock (CURRENT_DATE, NOW(), ...) is excluded
outright: its PostgreSQL gold was frozen on one day and is wrong on the next.

Excludes the hand-translation set (LSB_DATA/prep/pg_only_gold.txt), which
verify_translations.py checks separately. Writes LSB_DATA/prep/
freeze_verdicts.json, then the task set those verdicts and the translations
give, to LSB_DATA/tasks_frozen_full_v1.json.rebuilt -- beside the frozen file,
never over it.
"""

import collections
import json
import os
import re
import sys
from pathlib import Path

import sqlglot

#: The transpiler the frozen `via: sqlglot` forms came from; another version
#: translates some golds differently (four fail on 30.17.0).
SQLGLOT_VERSION = "30.19.0"
if sqlglot.__version__ != SQLGLOT_VERSION:
    raise SystemExit(f"run with sqlglot=={SQLGLOT_VERSION}, not {sqlglot.__version__}")

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from dce.benchmarks.lsb_grade import (  # noqa: E402
    clean,
    connect_readonly,
    matches,
    run_sql,
)

DATA = Path(os.environ.get("LSB_DATA", Path.home() / "data" / "livesqlbench"))
#: Five runs let a gold whose ordered LIMIT cuts through a tie pass by luck
#: (cold_chain_pharma_compliance_20 went 5/5, then 1/5 the next day).
RUNS = 20
#: Keywords, or functions only when called: `'replace now'` is a value.
CLOCK = re.compile(
    r"\b(current_date|current_time|current_timestamp|localtime|localtimestamp)\b"
    r"|\b(now|clock_timestamp|statement_timestamp|transaction_timestamp"
    r"|timeofday)\s*\(",
    re.IGNORECASE,
)


def connect(db: str):
    # The grader's own settings, so a gold that reproduces here reproduces
    # when an agent's answer is graded.
    return connect_readonly(DATA / "duckdb" / f"{db}.duckdb")


def reads_the_clock(sqls: list[str]) -> bool:
    # Cleaned first, so a comment that says "now" does not count.
    return any(CLOCK.search(sql) for sql in clean(sqls))


pg_only = set(
    re.findall(
        r"^##### (\S+)",
        (DATA / "prep" / "pg_only_gold.txt").read_text(encoding="utf-8"),
        re.M,
    )
)
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
gold = json.load(open(DATA / "gold_results_full_v1.json", encoding="utf-8"))


def candidate(con, sqls):
    try:
        return run_sql(con, sqls, seconds=600), "as_is"
    except Exception:  # noqa: BLE001
        trans = [sqlglot.transpile(s, read="postgres", write="duckdb")[0] for s in sqls]
        return run_sql(con, trans, seconds=600), "sqlglot"


verdicts = {}
for iid, g in sorted(gold.items()):
    if iid in pg_only:
        continue
    sqls = clean(gt[iid]["sol_sql"])
    if reads_the_clock(sqls):
        verdicts[iid] = {"verdict": "excluded", "via": "time_dependent"}
        continue
    strict = loose = 0
    how = "?"
    for _ in range(RUNS):
        con = connect(g["db"])
        try:
            rows, how = candidate(con, sqls)
        except Exception:  # noqa: BLE001
            rows = []
        con.close()
        strict += matches(g["rows"], rows, g["order"])
        loose += matches(g["rows"], rows, False)
    if strict == RUNS:
        v = "primary"
    elif loose == RUNS and g["order"]:
        v = "order_only"
    else:
        v = "excluded"
    verdicts[iid] = {
        "verdict": v,
        "via": how,
        "strict_runs": strict,
        "loose_runs": loose,
    }
json.dump(
    verdicts,
    open(DATA / "prep" / "freeze_verdicts.json", "w", encoding="utf-8"),
    indent=1,
)
c = collections.Counter(v["verdict"] for v in verdicts.values())
print(
    dict(c),
    "| flaky (some but not all strict):",
    sum(0 < v.get("strict_runs", 0) < RUNS for v in verdicts.values()),
)

translations = json.load(open(DATA / "prep" / "translations.json", encoding="utf-8"))
frozen = {}
for iid, g in sorted(gold.items()):
    if iid in verdicts:
        v = verdicts[iid]
        frozen[iid] = {
            "db": g["db"],
            "order": g["order"],
            "set": v["verdict"],
            "via": v["via"],
        }
    elif reads_the_clock(gt[iid]["sol_sql"]):
        frozen[iid] = {
            "db": g["db"],
            "order": g["order"],
            "set": "excluded",
            "via": "time_dependent",
        }
    elif translations.get(iid, {}).get("status") == "ok":
        frozen[iid] = {
            "db": g["db"],
            "order": g["order"],
            "set": "primary",
            "via": "hand_translation",
        }
    else:
        frozen[iid] = {
            "db": g["db"],
            "order": g["order"],
            "set": "excluded",
            "via": "untranslatable_tie",
        }
json.dump(
    frozen,
    open(DATA / "tasks_frozen_full_v1.json.rebuilt", "w", encoding="utf-8"),
    indent=1,
    sort_keys=True,
)
print(dict(collections.Counter(f["set"] for f in frozen.values())))
