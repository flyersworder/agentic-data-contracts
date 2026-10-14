"""Experiment 4's task set, in LiveSQLBench's layout, from each database's
questions.json.

  cd experiments/dabstep-contract-eval
  UV_FROZEN=1 uv run python prep/procedures/build_tasks.py

Run after every database's build.py. Writes to PROC_DATA (default
~/data/procedures), the files `LiveSQLBench.from_data` reads:

  hf-full-v1/livesqlbench_data.jsonl  instance_id, selected_database, query,
                                      high_level (false), conditions
  tasks_frozen_full_v1.json           {id: {db, order, set: "primary"}}
  gold_results_full_v1.json           {id: {db, order, rows}}

Each gold is its gold_sql run the way the grader runs an answer (`clean`,
then `run_sql`), and is refused unless it is non-empty, has no NULL, and
the gold SQL, graded as an answer, is correct.
"""

from __future__ import annotations

import json
import os
import sys
from decimal import Decimal
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parents[1]))

from dce.benchmarks.lsb_grade import (  # noqa: E402
    GRADE_SECONDS,
    clean,
    connect_readonly,
    grade_answer,
    run_sql,
)

DBS = ("reverse_logistics", "labor_certification_applications", "solar_panel")
OUT = Path(os.environ.get("PROC_DATA", Path.home() / "data" / "procedures"))


def _plain(value):
    return float(value) if isinstance(value, Decimal) else str(value)


def gold_rows(db_path: Path, sql: str) -> list[list]:
    """The rows as the gold file stores them: a JSON round trip, so the
    self-check below grades against exactly what a run will read."""
    con = connect_readonly(db_path)
    try:
        rows = run_sql(con, clean([sql]), seconds=GRADE_SECONDS)
    finally:
        con.close()
    return json.loads(json.dumps([list(r) for r in rows], default=_plain))


def main() -> None:
    public, frozen, gold, problems = [], {}, {}, []
    for db in DBS:
        db_path = OUT / "duckdb" / (db + ".duckdb")
        questions = json.loads(
            (HERE / db / "questions.json").read_text(encoding="utf-8")
        )
        for q in questions:
            iid, order = q["id"], bool(q["order"])
            rows = gold_rows(db_path, q["gold_sql"])
            if not rows or any(v is None for r in rows for v in r):
                problems.append(iid + ": gold is empty or has a NULL")
            answer = "```sql\n" + q["gold_sql"] + "\n```"
            verdict = grade_answer(
                answer, db_path=db_path, gold_rows=rows, ordered=order
            ).verdict
            if verdict != "correct":
                problems.append(iid + ": the gold SQL does not grade as correct")
            public.append(
                {
                    "instance_id": iid,
                    "selected_database": db,
                    "query": q["query"],
                    "high_level": False,
                    "conditions": {"order": order},
                }
            )
            frozen[iid] = {"db": db, "order": order, "set": "primary"}
            gold[iid] = {"db": db, "order": order, "rows": rows}
    if problems:
        raise SystemExit("\n".join(problems))
    (OUT / "hf-full-v1" / "livesqlbench_data.jsonl").write_text(
        "".join(json.dumps(p, ensure_ascii=False) + "\n" for p in public),
        encoding="utf-8",
    )
    (OUT / "tasks_frozen_full_v1.json").write_text(
        json.dumps(frozen, indent=1), encoding="utf-8"
    )
    (OUT / "gold_results_full_v1.json").write_text(json.dumps(gold), encoding="utf-8")
    print(len(public), "tasks written to", OUT)


if __name__ == "__main__":
    main()
