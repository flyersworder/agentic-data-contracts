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
from dce.scale import insert_mid
from dce.tools import ArmSetup, _ungoverned_tools, governed_arm

DEFAULT_DATA = Path.home() / "data" / "livesqlbench"
#: Where the compile commit puts the frozen contracts (spec, Layout).
DEFAULT_CONTRACTS = Path(__file__).resolve().parents[2] / "contracts" / "livesqlbench"

#: The extra contract arms' contracts, by arm: the claudeopus5.5 compile with
#: declared joins, and the contract a whole-database author session wrote
#: (`dce.author`). Each arm is `contract` with its own contract.
EXTRA_CONTRACTS: dict[str, Path] = {
    "contract_opus": DEFAULT_CONTRACTS.parent.parent
    / "contracts-opus-joins"
    / "livesqlbench",
    "contract_authored": DEFAULT_CONTRACTS.parent.parent
    / "contracts-authored"
    / "livesqlbench",
    # Experiment 4b: the contract `dce.author` wrote from the gapped KB and the
    # data, draw 1 (prep/procedures4b/RUBRIC.md).
    "contract_gap": DEFAULT_CONTRACTS.parent.parent
    / "contracts-procedures4b"
    / "draw-1"
    / "livesqlbench",
}

#: The corrected arms: `manual_prompt` reading a corrected KB, and a governed
#: arm reading a corrected contract. The corrections are derived from the
#: gold, so they live with it under `LSB_DATA/corrected/` (`kb/<db>_kb.jsonl`,
#: `contracts/<db>/`) and never in the repository; a row records them only by
#: digest.
CORRECTED_ARMS: tuple[str, ...] = ("manual_corrected", "contract_corrected")

#: Experiment 4's writing factor: `manual_prompt` reading a KB whose new
#: entries carry the same knowledge written as a business manual writes it
#: (`LSB_DATA/styled/kb/<db>_kb.jsonl`; prep/procedures/DESIGN.md).
STYLED_ARMS: tuple[str, ...] = ("manual_styled",)

#: Experiment 4b's ungoverned arms (docs/superpowers/specs/
#: 2026-10-19-experiment-4b-design.md), run with `--lsb-data` at the 4b data:
#: `manual_gap` the manual over the gapped KB (`manual_prompt`'s prompt, named
#: for the analysis); `manual_full` the KB with each gap's correction
#: (`LSB_DATA/full/kb/`); `manual_noted` the gapped KB plus the author's
#: resolutions in words (`notes.md` beside `contract_gap`'s contract); and
#: `contract_text` that contract's two files in the prompt over plain tools.
GAP_ARMS: tuple[str, ...] = (
    "manual_gap",
    "manual_full",
    "manual_noted",
    "contract_text",
)
#: Introduces the contract files in `contract_text`, as on DABStep.
CONTRACT_TEXT_NOTE = (
    "The data contract for this database follows: its two files, verbatim. "
    "They define the tables, the metrics as SQL, and how the domain's rules "
    "are to be read."
)

#: Experiment 6 (docs/superpowers/specs/2026-10-20-experiment-6-disclosure-
#: scale-design.md): the base contract diluted with other databases'
#: contracts, at five sizes. Each arm-level is an arm of its own,
#: `<kind>_<level>`: the governed arm; the merged files in the prompt; the
#: same with the base first (the position control); and the prose, the base's
#: KB among the padding databases' KBs. Merged contracts live under
#: `contracts-scale/merged/<level>/` (`first/<level>/` for the control),
#: `levels.json` names each level's padding databases in order, and a padding
#: database's KB is `LSB_DATA/padding/<db>/<db>_kb.jsonl` unless the data
#: directory holds it as one of its own.
SCALE_ROOT = DEFAULT_CONTRACTS.parent.parent / "contracts-scale"
SCALE_LEVELS: tuple[str, ...] = ("1x", "2x", "4x", "8x", "all")
SCALE_KINDS: tuple[str, ...] = (
    "contract_scaled",
    "contract_text_scaled",
    "contract_text_first",
    "manual_scaled",
)
SCALE_ARMS: tuple[str, ...] = tuple(
    f"{kind}_{level}" for kind in SCALE_KINDS for level in SCALE_LEVELS
)
SCALE_GOVERNED: frozenset[str] = frozenset(
    f"contract_scaled_{level}" for level in SCALE_LEVELS
)
#: The scale arms' runaway guard, never binding before `REQUEST_LIMIT`: 50
#: requests of the largest level (about 450k tokens) and their growth.
SCALE_TOKEN_BUDGET = 35_000_000
#: The most characters a token holds in these prompts (3.9-4.2 measured on
#: the three routes), with margin: a request reporting fewer tokens than its
#: prompt's characters over this was truncated by the route.
SCALE_CHARS_PER_TOKEN_MAX = 6


def _scale_kind(arm: str) -> tuple[str, str] | None:
    """(kind, level) of a scale arm, else None."""
    if arm not in SCALE_ARMS:
        return None
    kind, level = arm.rsplit("_", 1)
    return kind, level


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
    extra_arms: tuple[str, ...] = (
        *EXTRA_CONTRACTS,
        *CORRECTED_ARMS,
        *STYLED_ARMS,
        *GAP_ARMS,
        *SCALE_ARMS,
    )
    governed_arms: frozenset[str] = frozenset(
        {"contract", *EXTRA_CONTRACTS, "contract_corrected", *SCALE_GOVERNED}
    )
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
        extra_contracts: dict[str, Path] | None = None,
        scale: Path = SCALE_ROOT,
    ) -> None:
        self._data = data
        self._contracts = contracts
        self._scale = scale
        self._roots = {
            "contract": contracts,
            "contract_corrected": data / "corrected" / "contracts",
            **(extra_contracts or EXTRA_CONTRACTS),
            **{
                arm: scale / ("first" if k == "contract_text_first" else "merged") / lv
                for arm in SCALE_ARMS
                for k, lv in [arm.rsplit("_", 1)]
                if k != "manual_scaled"
            },
        }
        self._records = records  # (public task, frozen entry), in file order
        self._gold = gold

    @classmethod
    def from_data(
        cls,
        data: Path,
        contracts: Path = DEFAULT_CONTRACTS,
        *,
        extra_contracts: dict[str, Path] | None = None,
        dbs: tuple[str, ...] | list[str] | None = None,
        scale: Path = SCALE_ROOT,
    ) -> LiveSQLBench:
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
        if dbs:
            known = {task["selected_database"] for task, _ in records}
            unknown = sorted(set(dbs) - known)
            if unknown:
                raise SystemExit(f"no LiveSQLBench tasks on databases {unknown}")
            records = [r for r in records if r[0]["selected_database"] in set(dbs)]
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
        return cls(
            data=data,
            records=records,
            gold=gold,
            contracts=contracts,
            extra_contracts=extra_contracts,
            scale=scale,
        )

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
        parser.add_argument(
            "--lsb-dbs",
            nargs="+",
            default=None,
            help="(livesqlbench) run only the tasks on these databases",
        )
        parser.add_argument(
            "--lsb-contracts",
            type=Path,
            default=DEFAULT_CONTRACTS,
            help=(
                "(livesqlbench) the contracts the contract and manual_compiled "
                "arms load, one folder per database (default: the compiled ones)"
            ),
        )
        parser.add_argument(
            "--lsb-scale",
            type=Path,
            default=SCALE_ROOT,
            help="(livesqlbench) experiment 6's merged contracts and levels.json",
        )

    @classmethod
    def from_args(cls, args: argparse.Namespace) -> LiveSQLBench:
        return cls.from_data(
            args.lsb_data,
            contracts=getattr(args, "lsb_contracts", DEFAULT_CONTRACTS),
            dbs=getattr(args, "lsb_dbs", None),
            scale=getattr(args, "lsb_scale", SCALE_ROOT),
        )

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

    def _contract_dir(self, db: str, arm: str = "contract") -> Path:
        return self._roots[arm] / db

    def _contract(self, db: str, arm: str = "contract"):
        return load_contract_at(self._contract_dir(db, arm) / "contract.yml")

    def build_arm(self, arm: str, task: Task, db: Path) -> ArmSetup:
        setup = self._build_arm(arm, task, db)
        if _scale_kind(arm):
            setup.token_budget = SCALE_TOKEN_BUDGET
            setup.min_input_tokens = (
                len(setup.system_prompt) // SCALE_CHARS_PER_TOKEN_MAX
            )
        return setup

    def _build_arm(self, arm: str, task: Task, db: Path) -> ArmSetup:
        name = task.meta["db"]
        prompt = self._context(name)
        if arm in self.governed_arms:
            # No manual: the contract carries the KB text (spec, Arms).
            return governed_arm(
                prompt, db, self._contract(name, arm), init_sql=AGENT_INIT_SQL
            )
        kind = (_scale_kind(arm) or ("", ""))[0]
        if arm == "contract_text" or kind in (
            "contract_text_scaled",
            "contract_text_first",
        ):
            folder = self._contract_dir(
                name, "contract_gap" if arm == "contract_text" else arm
            )
            files = "\n\n".join(
                f"### {f}\n\n```yaml\n"
                f"{(folder / f).read_text(encoding='utf-8').strip()}\n```"
                for f in ("contract.yml", "semantic.yml")
            )
            prompt += f"\n\n## Data contract\n\n{CONTRACT_TEXT_NOTE}\n\n{files}"
        elif arm in (
            "manual_prompt",
            "manual_compiled",
            "manual_corrected",
            *STYLED_ARMS,
            "manual_gap",
            "manual_full",
            "manual_noted",
        ):
            entries = _read_jsonl(self._kb_path(name, arm))
            prompt += "\n\n## Knowledge base\n\n" + render_manual(entries)
            if arm == "manual_noted":
                notes = self._notes_path(name).read_text(encoding="utf-8").strip()
                prompt += "\n\n## Data notes\n\n" + notes
            if arm == "manual_compiled":
                prompt += "\n\n## Compiled definitions\n\n" + render_compiled(
                    self._contract_dir(name)
                )
        elif kind == "manual_scaled":
            prompt += "\n\n" + self._scaled_kb(name, arm.rsplit("_", 1)[1])
        elif arm != "schema_only":
            raise ValueError(f"unknown arm: {arm!r}; expected one of {self.arms}")
        return ArmSetup(prompt, _ungoverned_tools(db, init_sql=AGENT_INIT_SQL), None)

    def _kb_path(self, db: str, arm: str) -> Path:
        if arm == "manual_corrected":
            return self._data / "corrected" / "kb" / f"{db}_kb.jsonl"
        if arm in STYLED_ARMS:
            return self._data / "styled" / "kb" / f"{db}_kb.jsonl"
        if arm == "manual_full":
            return self._data / "full" / "kb" / f"{db}_kb.jsonl"
        return self._db_dir(db) / f"{db}_kb.jsonl"

    def _levels(self) -> dict:
        return json.loads((self._scale / "levels.json").read_text(encoding="utf-8"))

    def _padding_kb(self, db: str) -> Path:
        own = self._db_dir(db) / f"{db}_kb.jsonl"
        return own if own.is_file() else self._data / "padding" / db / f"{db}_kb.jsonl"

    def _scaled_kb_files(self, db: str, level: str) -> list[tuple[str, Path]]:
        """(database, KB file) for the base and each padding database of
        `level`, in padding order, base first."""
        padding = self._levels()[db][level]
        return [(db, self._kb_path(db, "manual_prompt"))] + [
            (p, self._padding_kb(p)) for p in padding
        ]

    def _scaled_kb(self, db: str, level: str) -> str:
        """The base's KB among the padding databases' KBs, each under a
        heading of its database's name, the base at the padding's size
        midpoint as in the merged contract."""
        base, *padding = [
            f"## Knowledge base: {name}\n\n" + render_manual(_read_jsonl(path))
            for name, path in self._scaled_kb_files(db, level)
        ]
        return "\n\n".join(insert_mid(padding, [base]))

    def _notes_path(self, db: str) -> Path:
        return self._contract_dir(db, "contract_gap") / "notes.md"

    def arm_digest(self, arm: str, task: Task) -> str:
        # Every file the arm's prompt is built from, for the task's database,
        # so a re-downloaded schema or KB cannot change an arm unrecorded.
        name = task.meta["db"]
        files = [f"{name}_schema.txt", f"{name}_column_meaning_base.json"]
        if arm in ("manual_prompt", "manual_compiled", "manual_gap", "manual_noted"):
            files.append(f"{name}_kb.jsonl")
        folder = self._db_dir(name)
        joined = "\n".join(f"{f} {_sha256(folder / f)}" for f in files)
        if arm == "manual_corrected":
            joined += f"\ncorrected kb {_sha256(self._kb_path(name, arm))}"
        if arm in STYLED_ARMS:
            joined += f"\nstyled kb {_sha256(self._kb_path(name, arm))}"
        if arm in GAP_ARMS:
            joined += f"\narm {arm}"
        if arm == "manual_full":
            joined += f"\nfull kb {_sha256(self._kb_path(name, arm))}"
        if arm == "manual_noted":
            joined += f"\nnotes {_sha256(self._notes_path(name))}"
        if arm == "contract_text":
            joined += (
                f"\ncontract {contract_digest(self._contract(name, 'contract_gap'))}"
            )
        kind = (_scale_kind(arm) or ("", ""))[0]
        if kind in ("contract_text_scaled", "contract_text_first"):
            joined += f"\ncontract {contract_digest(self._contract(name, arm))}"
        if kind == "manual_scaled":
            joined += f"\narm {arm}" + "".join(
                f"\nkb {p} {_sha256(path)}"
                for p, path in self._scaled_kb_files(name, arm.rsplit("_", 1)[1])
            )
        if arm == "manual_compiled":
            joined += f"\ncontract {contract_digest(self._contract(name))}"
        elif arm in self.governed_arms:
            joined += f"\ncontract {contract_digest(self._contract(name, arm))}"
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
        dbs = sorted({t.meta["db"] for t in tasks})
        if "manual_corrected" in arms:
            missing = [
                db for db in dbs if not self._kb_path(db, "manual_corrected").is_file()
            ]
            if missing:
                raise SystemExit(
                    f"manual_corrected: no corrected KB for {missing} under "
                    f"{self._data / 'corrected' / 'kb'}"
                )
        for arm in STYLED_ARMS:
            missing = [db for db in dbs if not self._kb_path(db, arm).is_file()]
            if arm in arms and missing:
                raise SystemExit(
                    f"{arm}: no styled KB for {missing} under "
                    f"{self._data / 'styled' / 'kb'}"
                )
        if "manual_full" in arms:
            missing = [
                db for db in dbs if not self._kb_path(db, "manual_full").is_file()
            ]
            if missing:
                raise SystemExit(
                    f"manual_full: no full KB for {missing} under "
                    f"{self._data / 'full' / 'kb'}"
                )
        if "manual_noted" in arms:
            missing = [db for db in dbs if not self._notes_path(db).is_file()]
            if missing:
                raise SystemExit(
                    f"manual_noted: no notes.md for {missing} under "
                    f"{self._roots['contract_gap']}"
                )
        if {"contract_text", "manual_noted"} & set(arms) and "contract_gap" not in arms:
            self._check_contracts("contract_gap", dbs, appendix=False)
        for arm in arms:
            if (_scale_kind(arm) or ("", ""))[0] == "manual_scaled":
                self._check_scaled_kb(arm, dbs)
        for arm in ("contract", *self.extra_arms):
            if (
                arm == "manual_corrected"
                or arm in STYLED_ARMS
                or arm in GAP_ARMS
                or arm.startswith("manual_scaled_")
            ):
                continue
            if arm == "contract":
                if not {"manual_compiled", "contract"} & set(arms):
                    continue
            elif arm not in arms:
                continue
            self._check_contracts(arm, dbs, appendix="manual_compiled" in arms)

    def _check_scaled_kb(self, arm: str, dbs: list[str]) -> None:
        level = arm.rsplit("_", 1)[1]
        try:
            files = [f for db in dbs for f in self._scaled_kb_files(db, level)]
        except (OSError, KeyError, ValueError) as exc:
            raise SystemExit(
                f"{arm}: no level {level} in {self._scale / 'levels.json'} "
                f"for {dbs}: {type(exc).__name__}: {exc}"
            ) from exc
        missing = [str(path) for _, path in files if not path.is_file()]
        if missing:
            raise SystemExit(f"{arm}: KB files missing: {missing}")

    def _check_contracts(self, arm: str, dbs: list[str], *, appendix: bool) -> None:
        missing, broken = [], []
        for db in dbs:
            path = self._contract_dir(db, arm) / "contract.yml"
            if not path.is_file():
                missing.append(db)
                continue
            # Everything an arm reads at build time: `from_yaml` alone does
            # not open `semantic.yml`; the digest and the appendix do.
            try:
                contract_digest(self._contract(db, arm))
                if appendix and arm == "contract":
                    render_compiled(self._contract_dir(db))
            except Exception as exc:
                broken.append(
                    f"{arm}: compiled contract for {db} does not load: "
                    f"{type(exc).__name__}: {exc}"
                )
        if missing:
            raise SystemExit(
                f"{arm}: no compiled contract for {missing} under "
                f"{self._roots[arm]}; run `python -m dce.compile --benchmark "
                "livesqlbench` (or `dce.author`) first"
            )
        if broken:
            raise SystemExit("\n".join(broken))

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
