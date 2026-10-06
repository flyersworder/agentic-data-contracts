"""LiveSQLBench Base-Full-v1, ported to DuckDB, as a `Benchmark`.

Everything private stays in `LSB_DATA` (default `~/data/livesqlbench`) and
is never committed: the frozen gold results, the gold SQL and the 22 DuckDB
files. The public task file and each database's KB, column meanings and DDL
come from the Hugging Face dataset `birdsql/livesqlbench-base-full-v1`, laid
out under `LSB_DATA/hf-full-v1/`. `prep/livesqlbench/` rebuilds the rest.

Tasks are the frozen Query tasks: 302 `primary` and 73 `order_only`, each a
`subset`; the 36 excluded tasks are never loaded. Every arm's system prompt
carries the database's DDL and column meanings, so the arms differ only in
how the KB reaches the agent; until the compiler lands (PR C) the arms are
`schema_only` and `manual_prompt`. All their DuckDB connections, and the
grader's, run PostgreSQL's NULL ordering and integer division, because the
gold came from PostgreSQL, and have no file access (`lsb_grade.SAFE_INIT_SQL`).
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
from functools import cache
from pathlib import Path

from dce.benchmark import Grade, Task
from dce.benchmarks.lsb_grade import (
    SAFE_INIT_SQL,
    extract_sql,
    gold_digest,
    grade_answer,
)
from dce.tools import ArmSetup, _ungoverned_tools

DEFAULT_DATA = Path.home() / "data" / "livesqlbench"

#: Names the grading rules on every row. Bump it whenever `lsb_grade` changes
#: what counts as correct.
SCORER = "lsb-soft-ex-duckdb/2"

#: Every agent connection: the grader's own settings (`SAFE_INIT_SQL`), so
#: an agent explores the database under exactly the rules its answer is
#: graded by, with no file access to the gold beside it.
AGENT_INIT_SQL: tuple[str, ...] = SAFE_INIT_SQL

BASE_PROMPT = (
    "You are a data analyst answering questions over a DuckDB database.\n"
    "Explore the schema, write SQL, and verify your result before answering.\n"
    "The database sorts NULLs and divides integers as PostgreSQL does."
)

#: Appended to every task's query. Fixed before the first run; the grader
#: reads exactly what it asks for.
ANSWER_INSTRUCTION = (
    "End your answer with the final SQL query in a ```sql block. That query "
    "is what is graded: it must run on this DuckDB database and return the "
    "answer."
)


def render_column_meanings(meanings: dict) -> str:
    """One line per column, `- table.column: meaning`, in the file's order. A
    JSON column's meaning is an object; its field meanings follow as JSON."""
    lines = []
    for key, value in meanings.items():
        _, table, column = key.split("|", 2)
        if isinstance(value, dict):
            text = str(value.get("column_meaning", "")).strip()
            fields = value.get("fields_meaning")
            if fields:
                text += " Fields: " + json.dumps(fields, ensure_ascii=False)
        else:
            text = str(value).strip()
        lines.append(f"- {table}.{column}: {text}")
    return "\n".join(lines)


def render_manual(entries: list[dict]) -> str:
    """The KB as a prose manual: entries in `id` order, each its name, its
    description and its definition."""
    return "\n\n".join(
        f"### {e['knowledge']}\n\n{e['description']}\n\n{e['definition']}"
        for e in sorted(entries, key=lambda e: e["id"])
    )


def _read_jsonl(path: Path) -> list[dict]:
    return [
        json.loads(line)
        for line in path.read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]


@cache
def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


class LiveSQLBench:
    name = "livesqlbench"
    arms: tuple[str, ...] = ("schema_only", "manual_prompt")
    extra_arms: tuple[str, ...] = ()
    governed_arms: frozenset[str] = frozenset()
    roles = {"baseline": "schema_only", "manual": "manual_prompt"}
    primary = None
    group_label = "db"
    excluded_tasks: frozenset[str] = frozenset()
    subsets: tuple[str, ...] = ("primary", "order_only")
    task_set_label = "frozen task set"
    excluded_reason = "excluded"
    excluded_source = "LiveSQLBench.excluded_tasks"

    def __init__(
        self, *, data: Path, records: list[tuple[dict, dict]], gold: dict
    ) -> None:
        self._data = data
        self._records = records  # (public task, frozen entry), in file order
        self._gold = gold

    @classmethod
    def from_data(cls, data: Path) -> LiveSQLBench:
        frozen = json.loads(
            (data / "tasks_frozen_full_v1.json").read_text(encoding="utf-8")
        )
        gold = json.loads(
            (data / "gold_results_full_v1.json").read_text(encoding="utf-8")
        )
        public = _read_jsonl(data / "hf-full-v1" / "livesqlbench_data.jsonl")
        records = [
            (task, frozen[task["instance_id"]])
            for task in public
            if task["instance_id"] in frozen
            and frozen[task["instance_id"]]["set"] in cls.subsets
        ]
        # Checked here, before any spend: a task with no gold would grade as
        # a `scoring_error` on every arm, and a missing database file would
        # fail every task on it the same way.
        no_gold = sorted(
            task["instance_id"]
            for task, _ in records
            if task["instance_id"] not in gold
        )
        if no_gold:
            raise SystemExit(
                f"LiveSQLBench tasks with no frozen gold result: {no_gold}"
            )
        missing = sorted(
            {
                task["selected_database"]
                for task, _ in records
                if not (
                    data / "duckdb" / f"{task['selected_database']}.duckdb"
                ).is_file()
            }
        )
        if missing:
            raise SystemExit(
                f"LiveSQLBench databases missing from {data / 'duckdb'}: {missing}"
            )
        return cls(data=data, records=records, gold=gold)

    @classmethod
    def add_arguments(cls, parser: argparse.ArgumentParser) -> None:
        parser.add_argument(
            "--lsb-data",
            type=Path,
            default=Path(os.environ.get("LSB_DATA", DEFAULT_DATA)),
            help=(
                "(livesqlbench) the private data directory "
                "(default $LSB_DATA or ~/data/livesqlbench)"
            ),
        )

    @classmethod
    def from_args(cls, args: argparse.Namespace) -> LiveSQLBench:
        return cls.from_data(args.lsb_data)

    def tasks(self) -> list[Task]:
        return [
            Task(
                task_id=task["instance_id"],
                prompt=f"{task['query']}\n\n{ANSWER_INSTRUCTION}",
                group=task["selected_database"],
                meta={
                    "db": task["selected_database"],
                    "high_level": bool(task.get("high_level", False)),
                    "order": bool(entry["order"]),
                },
                subset=entry["set"],
            )
            for task, entry in self._records
        ]

    def _db_dir(self, db: str) -> Path:
        return self._data / "hf-full-v1" / db

    def pristine_db(self, task: Task) -> Path:
        return self._data / "duckdb" / f"{task.meta['db']}.duckdb"

    def _context(self, db: str) -> str:
        folder = self._db_dir(db)
        schema = (folder / f"{db}_schema.txt").read_text(encoding="utf-8").strip()
        meanings = json.loads(
            (folder / f"{db}_column_meaning_base.json").read_text(encoding="utf-8")
        )
        return (
            f"{BASE_PROMPT}\n\n## Database schema\n\n{schema}\n\n"
            f"## Column meanings\n\n{render_column_meanings(meanings)}"
        )

    def build_arm(self, arm: str, task: Task, db: Path) -> ArmSetup:
        name = task.meta["db"]
        prompt = self._context(name)
        if arm == "schema_only":
            pass
        elif arm == "manual_prompt":
            entries = _read_jsonl(self._db_dir(name) / f"{name}_kb.jsonl")
            prompt += "\n\n## Knowledge base\n\n" + render_manual(entries)
        else:
            raise ValueError(f"unknown arm: {arm!r}; expected one of {self.arms}")
        return ArmSetup(prompt, _ungoverned_tools(db, init_sql=AGENT_INIT_SQL), None)

    def arm_digest(self, arm: str, task: Task) -> str:
        # Every file the arm's prompt is built from, for the task's database,
        # so a re-downloaded schema or KB cannot change an arm unrecorded.
        name = task.meta["db"]
        files = [f"{name}_schema.txt", f"{name}_column_meaning_base.json"]
        if arm == "manual_prompt":
            files.append(f"{name}_kb.jsonl")
        folder = self._db_dir(name)
        joined = "\n".join(f"{f} {_sha256(folder / f)}" for f in files)
        return hashlib.sha256(joined.encode("utf-8")).hexdigest()

    def _gold_rows(self, task: Task):
        return self._gold[task.task_id]["rows"]

    def gold_ref(self, task: Task) -> str | None:
        return gold_digest(self._gold_rows(task))

    def grade(self, task: Task, answer: str) -> Grade:
        # An order-only task's gold reproduces in DuckDB only as a set, so it
        # is graded without order whatever its `order` flag says.
        ordered = task.subset == "primary" and bool(task.meta["order"])
        return grade_answer(
            answer,
            db_path=self.pristine_db(task),
            gold_rows=self._gold_rows(task),
            ordered=ordered,
        )

    def normalize(self, answer: str) -> str:
        # The SQL the grader ran, so a verdict can be re-checked from the row.
        return extract_sql(answer) or ""

    def golds_hash(self) -> str:
        digests = {
            task["instance_id"]: gold_digest(self._gold[task["instance_id"]]["rows"])
            for task, _ in self._records
        }
        return hashlib.sha256(
            json.dumps(digests, sort_keys=True).encode("utf-8")
        ).hexdigest()

    def row_fields(self, task: Task) -> dict:
        return {
            "db": task.meta["db"],
            "high_level": task.meta["high_level"],
            "order": task.meta["order"],
        }

    @staticmethod
    def scorer() -> str:
        return SCORER

    @staticmethod
    def rescore(row: dict) -> str | None:
        # The gold is not in the row, by design, so a row cannot be re-graded
        # offline; re-grading means re-running `lsb_grade` with LSB_DATA.
        return None

    @staticmethod
    def e2e_correct(row: dict) -> bool:
        # The answer is SQL graded by its result; there is no separate
        # end-to-end reading.
        return False
