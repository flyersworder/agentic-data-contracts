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
  B  also protects DISTINCT inside an aggregate call;
  C  strips DISTINCT from neither the candidate nor the gold.

The official evaluator strips DISTINCT from the gold SQL too, and the frozen
gold results were made that way (`prep/livesqlbench/gold.py`): on a gold
that counts distinct values, the "right" answer is the count without
DISTINCT. A and B keep that gold, so B is lopsided (DISTINCT kept on one
side only). C recomputes the gold in DuckDB from its unstripped SQL and
re-grades every primary answer where either side uses DISTINCT; it is the
consistent alternative, at the cost of trusting DuckDB for those golds. Only
golds frozen `as_is` are recomputed (the PostgreSQL text runs on DuckDB);
an answer to a translated gold keeps its official verdict, and the count of
those is printed.

The official score stays the measure every prediction was declared on; this
is a sensitivity check (FINDINGS.md, *Does the KB determine the gold?*).
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
DISTINCT = re.compile(r"\bDISTINCT\b", re.I)
OFFICIAL = lsb_grade.remove_distinct
MAPPING = "livesqlbench_base_full_v1_gt_kg_testcases_20260613.jsonl"
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


def grade_unstripped(
    answer: str | None, gold_sqls: list[str], db_path: Path, *, ordered: bool
) -> bool:
    """Variant C: comments and ROUND removed as usual, DISTINCT kept on both
    sides, the gold recomputed on the same database."""
    sql = lsb_grade.extract_sql(answer)
    if sql is None:
        return False
    tidy = lambda qs: lsb_grade.remove_round(lsb_grade.remove_comments(list(qs)))  # noqa: E731
    con = lsb_grade.connect_readonly(db_path)
    try:
        gold = lsb_grade.run_sql(con, tidy(gold_sqls), seconds=lsb_grade.GRADE_SECONDS)
        try:
            got = lsb_grade.run_sql(con, tidy([sql]), seconds=lsb_grade.GRADE_SECONDS)
        except Exception:  # noqa: BLE001 -- any failure grades incorrect, as officially
            return False
    finally:
        con.close()
    return lsb_grade.matches([tuple(r) for r in gold], got, ordered)


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


def report(name: str, pattern: str, gold: dict, tasks: dict, gold_sql: dict) -> None:
    runs: Counter[str] = Counter()
    correct: Counter[str] = Counter()
    touched: Counter[str] = Counter()
    kept: Counter[str] = Counter()  # C: translated gold, official verdict kept
    new = {v: Counter() for v in (*VARIANTS, "C")}
    for i in (1, 2, 3):
        for r in load(ROOT / "results" / pattern.format(i)):
            if r["subset"] != "primary":
                continue
            arm, ok = r["arm"], r["verdict"] == "correct"
            runs[arm] += 1
            correct[arm] += ok
            sql = lsb_grade.extract_sql(r.get("answer"))
            t = r["task_id"]
            uses = sql and (
                DISTINCT.search(sql) or DISTINCT.search(" ".join(gold_sql[t]))
            )
            if uses and tasks[t]["via"] != "as_is":
                kept[arm] += 1
                new["C"][arm] += ok
            elif uses:
                new["C"][arm] += grade_unstripped(
                    r["answer"],
                    gold_sql[t],
                    PRIVATE / "duckdb" / f"{tasks[t]['db']}.duckdb",
                    ordered=bool(tasks[t]["order"]),
                )
            else:
                new["C"][arm] += ok
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
            + "".join(f"  {v} {new[v][arm] / n:.1%}" for v in new)
            + f"  (C kept {kept[arm]})"
        )


if __name__ == "__main__":
    gold = json.loads((PRIVATE / "gold_results_full_v1.json").read_text("utf-8"))
    tasks = json.loads((PRIVATE / "tasks_frozen_full_v1.json").read_text("utf-8"))
    gold_sql = {}
    with open(PRIVATE / MAPPING, encoding="utf-8") as f:
        for line in f:
            r = json.loads(line)
            sqls = r["sol_sql"]
            gold_sql[r["instance_id"]] = sqls if isinstance(sqls, list) else [sqls]
    for name, pattern in RUNS.items():
        report(name, pattern, gold, tasks, gold_sql)
