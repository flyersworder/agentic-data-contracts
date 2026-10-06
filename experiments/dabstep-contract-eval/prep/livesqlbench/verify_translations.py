"""Verify the DuckDB translations of the PostgreSQL-only LiveSQLBench gold queries.

Run: LSB_DATA=... uv run python prep/livesqlbench/verify_translations.py [--threads 1]

Loads LSB_DATA/prep/translations.json, and for every "ok" entry runs clean(sql)
on a fresh read-only DuckDB connection RUNS times, comparing each result to the
frozen PostgreSQL gold rows with lsb_grade.matches. Pass --threads N to also run
each check with DuckDB limited to N threads (a tie-order sensitivity probe).
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from dce.benchmarks.lsb_grade import (  # noqa: E402
    clean,
    connect_readonly,
    matches,
    run_sql,
)

DATA = Path(os.environ.get("LSB_DATA", Path.home() / "data" / "livesqlbench"))
RUNS = 20  # as freeze.py


def connect(db: str):
    # The grader's own settings, as freeze.py.
    return connect_readonly(DATA / "duckdb" / f"{db}.duckdb")


def check(
    entry_sql: list[str], db: str, gold: dict, threads: int | None
) -> tuple[bool, str]:
    con = connect(db)
    try:
        if threads:
            con.execute(f"SET threads = {threads}")
        got = run_sql(con, clean(entry_sql), seconds=600)
    except Exception as exc:  # noqa: BLE001 - report any engine error
        return False, type(exc).__name__ + ": " + str(exc).splitlines()[0][:150]
    finally:
        con.close()
    ok = matches([tuple(r) for r in gold["rows"]], got, gold["order"])
    return ok, "" if ok else f"mismatch (gold {len(gold['rows'])} rows, got {len(got)})"


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--threads", type=int, default=None)
    args = parser.parse_args()

    translations = json.load(
        open(DATA / "prep" / "translations.json", encoding="utf-8")
    )
    gold = json.load(open(DATA / "gold_results_full_v1.json", encoding="utf-8"))

    passed, failed_checks = [], []
    for iid, entry in translations.items():
        if entry["status"] != "ok":
            continue
        results = [
            check(entry["sql"], gold[iid]["db"], gold[iid], args.threads)
            for _ in range(RUNS)
        ]
        n_ok = sum(ok for ok, _ in results)
        if n_ok == RUNS:
            passed.append(iid)
        else:
            msg = next(m for ok, m in results if not ok)
            failed_checks.append(iid)
            print(f"FAIL {iid}: {n_ok}/{RUNS} runs matched; {msg}")

    n_failed_status = sum(e["status"] == "failed" for e in translations.values())
    print(
        f"ok entries verified: {len(passed)}/{len(passed) + len(failed_checks)} "
        f"({RUNS} runs each)"
    )
    print(f"entries marked failed: {n_failed_status}")
    for iid, entry in translations.items():
        if entry["status"] == "failed":
            print(f"  {iid}: {entry['reason'][:110]}")
    return 1 if failed_checks else 0


if __name__ == "__main__":
    sys.exit(main())
