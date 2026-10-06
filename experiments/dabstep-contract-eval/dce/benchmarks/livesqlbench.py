"""LiveSQLBench Base-Full-v1, ported to DuckDB, as a `Benchmark`.

Everything private stays in `LSB_DATA` (default `~/data/livesqlbench`) and
is never committed: the frozen gold results, the gold SQL and the 22 DuckDB
files. The public task file and each database's KB, column meanings and DDL
come from the Hugging Face dataset `birdsql/livesqlbench-base-full-v1`, laid
out under `LSB_DATA/hf-full-v1/`. `prep/livesqlbench/` rebuilds the rest.

Tasks are the frozen Query tasks: 302 `primary` and 73 `order_only`, each a
`subset`; the 36 excluded tasks are never loaded. Every arm's system prompt
carries the database's DDL and column meanings, so the arms differ only in
how the KB reaches the agent; the arms are `schema_only`, `manual_prompt`,
`manual_compiled` (the manual plus the compiled definitions) and `contract`
(the compiled contract behind governed tools), the last two loading
`contracts/livesqlbench/<db>/`. All their DuckDB connections, and the
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

from agentic_data_contracts import contract_digest
from dce.benchmark import Grade, Task
from dce.benchmarks.lsb_grade import (
    PG_COMPAT,
    SAFE_INIT_SQL,
    extract_sql,
    gold_digest,
    grade_answer,
)
from dce.compile.emit import render_compiled
from dce.compile.entries import KBEntry
from dce.compile.inputs import CompileInputs
from dce.frozen import load_contract_at
from dce.tools import ArmSetup, _ungoverned_tools, governed_arm

DEFAULT_DATA = Path.home() / "data" / "livesqlbench"
#: Where the compile commit puts the frozen contracts (spec, Layout).
DEFAULT_CONTRACTS = Path(__file__).resolve().parents[2] / "contracts" / "livesqlbench"

#: Names the grading rules on every row. Bump it whenever `lsb_grade` changes
#: what counts as correct.
SCORER = "lsb-soft-ex-duckdb/3"

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


def _meaning_text(value) -> str:
    if isinstance(value, dict):
        text = str(value.get("column_meaning", "")).strip()
        fields = value.get("fields_meaning")
        if fields:
            text += " Fields: " + json.dumps(fields, ensure_ascii=False)
        return text
    return str(value).strip()


def render_column_meanings(meanings: dict) -> str:
    """One line per column, `- table.column: meaning`, in the file's order. A
    JSON column's meaning is an object; its field meanings follow as JSON."""
    lines = []
    for key, value in meanings.items():
        _, table, column = key.split("|", 2)
        lines.append(f"- {table}.{column}: {_meaning_text(value)}")
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


#: LiveSQLBench's KB types as the compiler's neutral kinds (spec, Compiler).
KIND_BY_TYPE: dict[str, str] = {
    "calculation_knowledge": "calculation",
    "domain_knowledge": "predicate",
    "value_illustration": "illustration",
}

#: How the compiler is told the database's dialect switches.
DIALECT_NOTE = "The database sorts NULLs and divides integers as PostgreSQL does."


def compile_inputs(data: Path, db: str) -> CompileInputs:
    """The compiler's inputs for one database: its KB, column meanings, DDL
    and DuckDB file, and nothing else (no task, gold or test-case file)."""
    folder = data / "hf-full-v1" / db
    kb_path = folder / f"{db}_kb.jsonl"
    meanings_path = folder / f"{db}_column_meaning_base.json"
    schema_path = folder / f"{db}_schema.txt"
    db_path = data / "duckdb" / f"{db}.duckdb"
    entries = []
    for raw in _read_jsonl(kb_path):
        children = raw.get("children_knowledge")
        entries.append(
            KBEntry(
                id=int(raw["id"]),
                kind=KIND_BY_TYPE[raw["type"]],
                name=str(raw["knowledge"]),
                text=f"{raw['description']}\n\n{raw['definition']}",
                # -1 in the file means no children.
                children=tuple(int(c) for c in children)
                if isinstance(children, list)
                else (),
            )
        )
    meanings = json.loads(meanings_path.read_text(encoding="utf-8"))
    return CompileInputs(
        benchmark="livesqlbench",
        db_name=db,
        entries=tuple(entries),
        ddl=schema_path.read_text(encoding="utf-8").strip(),
        column_meanings=render_column_meanings(meanings),
        dialect_note=DIALECT_NOTE,
        db_path=db_path,
        init_sql=PG_COMPAT,
        sources=(kb_path, meanings_path, schema_path, db_path),
        out_subdir=f"livesqlbench/{db}",
        column_descriptions={
            (key.split("|", 2)[1].lower(), key.split("|", 2)[2].lower()): (
                _meaning_text(value)
            )
            for key, value in meanings.items()
        },
    )


class LiveSQLBench:
    name = "livesqlbench"
    arms: tuple[str, ...] = (
        "schema_only",
        "manual_prompt",
        "manual_compiled",
        "contract",
    )
    extra_arms: tuple[str, ...] = ()
    governed_arms: frozenset[str] = frozenset({"contract"})
    roles = {
        "baseline": "schema_only",
        "manual": "manual_prompt",
        "manual_plus": "manual_compiled",
        "contract": "contract",
    }
    primary = None
    group_label = "db"
    excluded_tasks: frozenset[str] = frozenset()
    subsets: tuple[str, ...] = ("primary", "order_only")
    task_set_label = "frozen task set"
    excluded_reason = "excluded"
    excluded_source = "LiveSQLBench.excluded_tasks"

    def __init__(
        self,
        *,
        data: Path,
        records: list[tuple[dict, dict]],
        gold: dict,
        contracts: Path = DEFAULT_CONTRACTS,
    ) -> None:
        self._data = data
        self._contracts = contracts
        self._records = records  # (public task, frozen entry), in file order
        self._gold = gold

    @classmethod
    def from_data(cls, data: Path, contracts: Path = DEFAULT_CONTRACTS) -> LiveSQLBench:
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
        return cls(data=data, records=records, gold=gold, contracts=contracts)

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

    def _contract_dir(self, db: str) -> Path:
        return self._contracts / db

    def _contract(self, db: str):
        return load_contract_at(self._contract_dir(db) / "contract.yml")

    def build_arm(self, arm: str, task: Task, db: Path) -> ArmSetup:
        name = task.meta["db"]
        prompt = self._context(name)
        if arm == "contract":
            # No manual: the contract carries the KB text (spec, Arms).
            return governed_arm(
                prompt, db, self._contract(name), init_sql=AGENT_INIT_SQL
            )
        if arm in ("manual_prompt", "manual_compiled"):
            entries = _read_jsonl(self._db_dir(name) / f"{name}_kb.jsonl")
            prompt += "\n\n## Knowledge base\n\n" + render_manual(entries)
            if arm == "manual_compiled":
                prompt += "\n\n## Compiled definitions\n\n" + render_compiled(
                    self._contract_dir(name)
                )
        elif arm != "schema_only":
            raise ValueError(f"unknown arm: {arm!r}; expected one of {self.arms}")
        return ArmSetup(prompt, _ungoverned_tools(db, init_sql=AGENT_INIT_SQL), None)

    def arm_digest(self, arm: str, task: Task) -> str:
        # Every file the arm's prompt is built from, for the task's database,
        # so a re-downloaded schema or KB cannot change an arm unrecorded.
        name = task.meta["db"]
        files = [f"{name}_schema.txt", f"{name}_column_meaning_base.json"]
        if arm in ("manual_prompt", "manual_compiled"):
            files.append(f"{name}_kb.jsonl")
        folder = self._db_dir(name)
        joined = "\n".join(f"{f} {_sha256(folder / f)}" for f in files)
        if arm in ("manual_compiled", "contract"):
            joined += f"\ncontract {contract_digest(self._contract(name))}"
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

    def check_arms(self, arms: tuple[str, ...], tasks: list[Task]) -> None:
        if not {"manual_compiled", "contract"} & set(arms):
            return
        missing = []
        for db in sorted({t.meta["db"] for t in tasks}):
            path = self._contract_dir(db) / "contract.yml"
            if not path.is_file():
                missing.append(db)
                continue
            try:
                self._contract(db)
            except Exception as exc:
                raise SystemExit(
                    f"compiled contract for {db} does not load: {exc}"
                ) from exc
        if missing:
            raise SystemExit(
                f"no compiled contract for {missing} under {self._contracts}; run "
                "`python -m dce.compile --benchmark livesqlbench` first"
            )

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
