"""Grade every frozen LiveSQLBench task's own gold SQL through the harness.

The gate before any agent run: the gold SQL, submitted exactly as an agent's
answer is (one ```sql block), must grade correct on all 375 tasks under
`LiveSQLBench.grade` -- the same connection settings, memory limit, time
limit and Soft-EX rules agents are graded by. The DuckDB form of each gold
is the one the freeze established (`via`): as-is, sqlglot's transpilation, or
the verified hand translation.

Run: LSB_DATA=... uv run --with sqlglot==30.19.0 \
         python prep/livesqlbench/check_grader.py
"""

from __future__ import annotations

import collections
import json
import os
import sys
from pathlib import Path

import sqlglot

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

from dce.benchmarks.livesqlbench import LiveSQLBench  # noqa: E402
from dce.benchmarks.lsb_grade import clean  # noqa: E402

SQLGLOT_VERSION = "30.19.0"  # as freeze.py
if sqlglot.__version__ != SQLGLOT_VERSION:
    raise SystemExit(f"run with sqlglot=={SQLGLOT_VERSION}, not {sqlglot.__version__}")

DATA = Path(os.environ.get("LSB_DATA", Path.home() / "data" / "livesqlbench"))


def main() -> None:
    bench = LiveSQLBench.from_data(DATA)
    frozen = json.load(open(DATA / "tasks_frozen_full_v1.json", encoding="utf-8"))
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
    translations = json.load(
        open(DATA / "prep" / "translations.json", encoding="utf-8")
    )
    counts: collections.Counter[str] = collections.Counter()
    failures = []
    for task in bench.tasks():
        via = frozen[task.task_id]["via"]
        sqls = list(gt[task.task_id]["sol_sql"])
        if via == "sqlglot":
            # Cleaned first, then transpiled: the form freeze.py ran.
            sqls = [
                sqlglot.transpile(s, read="postgres", write="duckdb")[0]
                for s in clean(sqls)
            ]
        elif via == "hand_translation":
            sqls = list(translations[task.task_id]["sql"])
        answer = "```sql\n" + ";\n".join(s.rstrip().rstrip(";") for s in sqls) + "\n```"
        grade = bench.grade(task, answer)
        counts[(task.subset, grade.verdict)] += 1
        if grade.verdict != "correct":
            failures.append((task.task_id, via, grade.failure))
    print(dict(counts))
    for failure in failures:
        print("  NOT CORRECT", failure)
    print(f"{len(failures)} of {sum(counts.values())} gold answers not graded correct")
    raise SystemExit(1 if failures else 0)


if __name__ == "__main__":
    main()
