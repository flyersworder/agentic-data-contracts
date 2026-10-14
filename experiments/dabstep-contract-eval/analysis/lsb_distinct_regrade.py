"""How much the official DISTINCT stripping moves the LiveSQLBench scores.

The Soft-EX rules strip DISTINCT from the candidate query with the regex
`\\bDISTINCT\\b(?![^()]*\\bON\\b)`, which `lsb_grade.remove_distinct` vendors
unchanged. Two constructs it was not meant for: `a IS [NOT] DISTINCT FROM b`
becomes `a IS FROM b`, a parse error, and `COUNT(DISTINCT x)` becomes
`COUNT( x)`, a different number. The compiled contracts use both.

This re-grades every primary answer whose graded SQL holds either construct,
under two corrected cleaners, and prints accuracy per arm beside the
official score:

  A  protects IS [NOT] DISTINCT FROM (a plain bug fix);
  B  also protects DISTINCT inside an aggregate call.

The official score stays the measure every prediction was declared on; this
is a sensitivity check (FINDINGS.md, *A failure audit of the hard tasks*).
Prints aggregates only; reads the private gold results (`$LSB_PRIVATE`,
default ~/data/livesqlbench).

Run:  uv run python analysis/lsb_distinct_regrade.py
"""

from __future__ import annotations

import json
import os
import re
import sys
from collections import Counter
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from dce.benchmarks import lsb_grade  # noqa: E402
from dce.stats import load  # noqa: E402

PRIVATE = Path(os.environ.get("LSB_PRIVATE", Path.home() / "data/livesqlbench"))
IS_DISTINCT = re.compile(r"\bIS\s+(NOT\s+)?DISTINCT\s+FROM\b", re.I)
AGG_DISTINCT = re.compile(r"\(\s*DISTINCT\b", re.I)
OFFICIAL = lsb_grade.remove_distinct
VARIANTS = {"A": (IS_DISTINCT,), "B": (IS_DISTINCT, AGG_DISTINCT)}
RUNS = {
    "gpt-6-luna, 302 tasks": "livesqlbench/luna-r{}.jsonl",
    "qwen3.8-27b, 302 tasks": "livesqlbench/qwen38-r{}.jsonl",
    "experiment 2, 55 tasks": "livesqlbench/lsb-exp2-luna-r{}.jsonl",
}


def affected(sql: str) -> bool:
    return bool(IS_DISTINCT.search(sql) or AGG_DISTINCT.search(sql))


def protected(patterns):
    """`remove_distinct`, with the text `patterns` match left in place."""

    def remove(sqls: list[str]) -> list[str]:
        out = []
        for query in sqls:
            saved: list[str] = []

            def keep(m: re.Match) -> str:
                saved.append(m.group(0))
                return f"__KEEP{len(saved) - 1}__"

            for p in patterns:
                query = p.sub(keep, query)
            (query,) = OFFICIAL([query])
            for i, text in enumerate(saved):
                query = query.replace(f"__KEEP{i}__", text)
            out.append(query)
        return out

    return remove


def regrade(row: dict, variant: str, gold: dict, tasks: dict) -> bool:
    t = row["task_id"]
    lsb_grade.remove_distinct = protected(VARIANTS[variant])
    try:
        grade = lsb_grade.grade_answer(
            row["answer"],
            db_path=PRIVATE / "duckdb" / f"{tasks[t]['db']}.duckdb",
            gold_rows=gold[t]["rows"],
            ordered=bool(tasks[t]["order"]),  # primary tasks only
        )
    finally:
        lsb_grade.remove_distinct = OFFICIAL
    return grade.verdict == "correct"


def report(name: str, pattern: str, gold: dict, tasks: dict) -> None:
    runs: Counter[str] = Counter()
    correct: Counter[str] = Counter()
    touched: Counter[str] = Counter()
    new = {v: Counter() for v in VARIANTS}
    for i in (1, 2, 3):
        for r in load(ROOT / "results" / pattern.format(i)):
            if r["subset"] != "primary":
                continue
            arm, ok = r["arm"], r["verdict"] == "correct"
            runs[arm] += 1
            correct[arm] += ok
            sql = lsb_grade.extract_sql(r.get("answer"))
            if not sql or not affected(sql):
                for v in VARIANTS:
                    new[v][arm] += ok
                continue
            touched[arm] += 1
            for v in VARIANTS:
                new[v][arm] += regrade(r, v, gold, tasks)
    print(f"== {name}")
    for arm in sorted(runs):
        n = runs[arm]
        print(
            f"  {arm:18s} official {correct[arm] / n:.1%}  re-graded {touched[arm]:3d}"
            + "".join(f"  {v} {new[v][arm] / n:.1%}" for v in VARIANTS)
        )


if __name__ == "__main__":
    gold = json.loads((PRIVATE / "gold_results_full_v1.json").read_text("utf-8"))
    tasks = json.loads((PRIVATE / "tasks_frozen_full_v1.json").read_text("utf-8"))
    for name, pattern in RUNS.items():
        report(name, pattern, gold, tasks)
