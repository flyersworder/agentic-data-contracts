"""DataAgentBench (ucbepic/DataAgentBench at `dab_fetch.COMMIT`) as a
`Benchmark`: experiment 7, phase 1 (docs/superpowers/specs/
2026-11-01-experiment-7-dataagentbench-design.md).

Named `dataagentbench`, not `dab`, to keep it apart from DABStep. Everything
DAB ships stays in `DAB_DATA` (default `~/data/dataagentbench`, built by
`dce.benchmarks.dab_data`) and is never committed: the descriptions, the
queries, the golds and the validators. The hints are quoted only as the 37
items scored in experiment 7 (`prep/dataagentbench/hint-items.json`). The
adapted system prompt below is DAB's; like every file derived from DAB, it is
not covered by this repository's MIT license
(`contracts-dataagentbench/NOTICE.md`). Tasks are the 40 queries of
the nine phase-1 datasets, `<dataset>/<n>`, grouped by dataset; each dataset
is one DuckDB file, a schema per logical database.

Every arm gets DAB's system prompt adapted to the harness's tools and its
answer step, and DAB's user message: the query, then the database
description with `SESSION_NOTE` after it. The arms differ only in knowledge
(and, for the governed arms, the tools):

- `dab_desc`: the description only, DAB's no-hints condition;
- `dab_hints`: the hints appended to the description as DAB's
  `run_agent.py` does (`description + "\\n\\n" + hints.strip()`);
- `dab_contract`: the governed contract of draw A1;
- `dab_contract_text`: draw A1's two files pasted into the prompt, plain
  tools, as the earlier `contract_text` arms;
- `dab_hints_contract`: the hints, and the governed contract of draw B.

The contracts live under `contracts-dataagentbench/<draw>/<dataset>/`;
`check_arms` stops a sweep by arm and dataset when one is missing.
Grading is the query's own `validate.py` (`dce.benchmarks.dab_grade`).
"""

from __future__ import annotations

import argparse
import dataclasses
import hashlib
import json
import os
import re
from functools import cache
from pathlib import Path

from agentic_data_contracts import contract_digest
from dce.benchmark import Grade, Task
from dce.benchmarks import dab_data, dab_fetch, dab_grade
from dce.frozen import load_contract_at
from dce.tools import ArmSetup, _ungoverned_tools, governed_arm

DEFAULT_DATA = dab_data.DEFAULT_DATA
DEFAULT_CONTRACTS = Path(__file__).resolve().parents[2] / "contracts-dataagentbench"

ARMS: tuple[str, ...] = (
    "dab_desc",
    "dab_hints",
    "dab_contract",
    "dab_contract_text",
    "dab_hints_contract",
)
GOVERNED_ARMS: frozenset[str] = frozenset({"dab_contract", "dab_hints_contract"})
HINT_ARMS: frozenset[str] = frozenset({"dab_hints", "dab_hints_contract"})
#: Which draw each contract arm reads (spec, Authoring): draw A1 for the two
#: contract arms, fixed before any draw exists; draw B, authored with the
#: hints, for `dab_hints_contract`.
CONTRACT_DRAWS: dict[str, str] = {
    "dab_contract": "draw-a1",
    "dab_contract_text": "draw-a1",
    "dab_hints_contract": "draw-b",
}

SCORER = dab_grade.SCORER

# -- the prompt -----------------------------------------------------------------
#
# DAB's SYSTEM_PROMPT (common_scaffold/prompts/prompt_builder.py at the pinned
# commit), adapted only for the harness's tool names and its answer step: the
# storage instructions, execute_python, MongoDB, list_db's db_name and
# return_answer are gone; the final answer is the agent's final message. The
# tools section and its examples are the arm's own tools, so a governed arm's
# prompt names the governed tools. Fixed in the materials commit.

PLAIN_TOOLS = """- list_tables: list the tables of every database, one <db_name>.<table_name> per line.
  Required args: {}
- describe_table: list a table's columns and their types.
  Required args: {"table": "<db_name>.<table_name>"}
- execute_sql: run a DuckDB SQL query. Returns the result as CSV (a header, then the rows) or an error string.
  Required args: {"sql": "<SQL query>"}"""  # noqa: E501

PLAIN_EXAMPLES = """- execute_sql:
{"tool": "execute_sql", "args": {"sql": "SELECT * FROM some_db_name.some_table LIMIT some_limit;"}}

- list_tables:
{"tool": "list_tables", "args": {}}"""  # noqa: E501

GOVERNED_TOOLS = """- describe_table: list a table's columns and their types.
  Required args: {"schema": "<db_name>", "table": "<table_name>"}
- preview_table: show sample rows of a table.
  Required args: {"schema": "<db_name>", "table": "<table_name>"}
- lookup_domain: read one of the data contract's domains.
  Required args: {"name": "<domain_name>"}
- list_metrics: list the data contract's metrics.
  Required args: {}
- lookup_metric: read one of the data contract's metrics.
  Required args: {"metric_name": "<metric_name>"}
- lookup_relationships: read the data contract's relationships of a table.
  Required args: {"table": "<db_name>.<table_name>"}
- trace_metric_impacts: trace which metrics a table or column affects.
  Required args: as the tool states.
- inspect_query: check a DuckDB SQL query against the data contract without running it.
  Required args: {"sql": "<SQL query>"}
- run_query: run a DuckDB SQL query. Returns the result or an error string.
  Required args: {"sql": "<SQL query>"}"""  # noqa: E501

GOVERNED_EXAMPLES = """- run_query:
{"tool": "run_query", "args": {"sql": "SELECT some_column FROM some_db_name.some_table LIMIT some_limit;"}}

- describe_table:
{"tool": "describe_table", "args": {"schema": "some_db_name", "table": "some_table"}}"""  # noqa: E501

SYSTEM_PROMPT = """You are a data analysis agent. Use only the tools listed below to answer the user's query, based on the provided DATABASE DESCRIPTION for logical database names, and the results of previous tool calls.

TOOLS (system will execute):
{TOOLS}

KEY RULES (must follow exactly):
1. Always use tool calls until you answer. Do not output plain text, explanations, or reasoning.
2. Include all required arguments for the tool you call.
3. In SQL, always name a table as <db_name>.<table_name>. Refer the DATABASE DESCRIPTION for db_name.
4. Wrap mixed-case or uppercase column names in double quotes.
5. Return the final answer only as your final message, with no tool call. Do not include extra text, explanation, or formatting.

EXAMPLES:
{EXAMPLES}

- final answer:
your final message: ...final plain-text answer...

If you cannot proceed, give a short explanatory message as your final message.

Do not output explanations, reasoning, or any natural language outside of the required tool calls and the final answer."""  # noqa: E501

#: The one sentence added to every arm's description (spec, "One database
#: session per dataset"): DAB's agent reaches each database through its own
#: system and joins in Python; here they are schemas of one DuckDB file.
SESSION_NOTE = (
    "All of these databases are schemas of one DuckDB database, each schema "
    "named after its logical database (<db_name>.<table_name>), so every "
    "query, including one that joins across databases, is written in DuckDB "
    "SQL, whatever system a database is described as stored in."
)

#: Introduces the contract files in `dab_contract_text`, as on DABStep and
#: LiveSQLBench.
CONTRACT_TEXT_NOTE = (
    "The data contract for this database follows: its two files, verbatim. "
    "They define the tables, the metrics as SQL, and how the domain's rules "
    "are to be read."
)


#: DAB's hints, per dataset. Read only by the hint arms and draw B's author.
HINTS_FILE = "db_description_withhint.txt"


def dataset_description(folder: Path) -> str:
    """The description every arm reads: DAB's, stripped as run_agent.py
    strips it, then the one-session sentence."""
    text = (folder / "db_description.txt").read_text(encoding="utf-8")
    return text.strip() + "\n\n" + SESSION_NOTE


def with_hints(description: str, folder: Path) -> str:
    """The hint arms' description. run_agent.py:
    `db_description += "\\n\\n" + hints.strip()`."""
    hints = (folder / HINTS_FILE).read_text(encoding="utf-8")
    return description + "\n\n" + hints.strip()


def system_prompt(governed: bool) -> str:
    tools, examples = (
        (GOVERNED_TOOLS, GOVERNED_EXAMPLES)
        if governed
        else (PLAIN_TOOLS, PLAIN_EXAMPLES)
    )
    return SYSTEM_PROMPT.replace("{TOOLS}", tools).replace("{EXAMPLES}", examples)


def user_message(query: str, description: str) -> str:
    """DAB's user message (`prompt_builder.init_messages`)."""
    return f"QUERY:\n{query.strip()}\n\nDATABASE DESCRIPTION:\n{description.strip()}"


@cache
def _sha256(path: Path) -> str:
    return dab_fetch.sha256_file(path)


def _sha256_text(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


_QUERY_DIR = re.compile(r"query(\d+)")


class DataAgentBench:
    name = "dataagentbench"
    arms = ARMS
    extra_arms: tuple[str, ...] = ()
    governed_arms = GOVERNED_ARMS
    roles = {"baseline": "dab_desc", "manual": "dab_hints", "contract": "dab_contract"}
    primary = None
    group_label = "dataset"
    excluded_tasks: frozenset[str] = frozenset()
    subsets: tuple[str, ...] = ()
    task_set_label = "phase-1 task set"
    excluded_reason = "excluded"
    excluded_source = "DataAgentBench.excluded_tasks"

    def __init__(
        self,
        *,
        data: Path,
        records: list[tuple[str, int, str]],
        contracts: Path = DEFAULT_CONTRACTS,
    ) -> None:
        self._data = data
        self._src = data / "src"
        self._records = records  # (dataset, query number, query text)
        self._contracts = contracts

    @classmethod
    def from_data(
        cls,
        data: Path,
        contracts: Path = DEFAULT_CONTRACTS,
        *,
        datasets: tuple[str, ...] | list[str] | None = None,
    ) -> DataAgentBench:
        src = data / "src"
        records = []
        problems = []
        for dataset in datasets or dab_fetch.DATASETS:
            folder = src / dab_fetch.dataset_dir(dataset)
            numbers = sorted(
                int(m[1])
                for p in folder.glob("query*")
                if p.is_dir() and (m := _QUERY_DIR.fullmatch(p.name))
            )
            expected = dab_fetch.QUERY_COUNTS.get(dataset)
            if not numbers or (expected is not None and len(numbers) != expected):
                problems.append(f"{dataset}: queries {numbers} under {folder}")
                continue
            if not (data / "duckdb" / f"{dataset}.duckdb").is_file():
                problems.append(f"{dataset}: no {data / 'duckdb' / dataset}.duckdb")
            for n in numbers:
                qdir = folder / f"query{n}"
                try:
                    dab_grade.check_validator(qdir / "validate.py")
                except (OSError, SyntaxError, dab_grade.ValidatorRefused) as exc:
                    problems.append(f"{dataset}/{n}: {exc}")
                if not (qdir / "ground_truth.csv").is_file():
                    problems.append(f"{dataset}/{n}: no ground_truth.csv")
                info = json.loads((qdir / "query.json").read_text(encoding="utf-8"))
                query = info if isinstance(info, str) else info["query"]
                records.append((dataset, n, query))
        # Before any spend: a missing database or a refused validator would
        # fail every task it touches the same way.
        if problems:
            raise SystemExit(
                "DataAgentBench data not ready (run `python -m "
                "dce.benchmarks.dab_data`):\n" + "\n".join(problems)
            )
        return cls(data=data, records=records, contracts=contracts)

    @classmethod
    def add_arguments(cls, parser: argparse.ArgumentParser) -> None:
        parser.add_argument(
            "--dab-data",
            type=Path,
            default=Path(os.environ.get("DAB_DATA", DEFAULT_DATA)),
            help=(
                "(dataagentbench) the built data directory "
                "(default $DAB_DATA or ~/data/dataagentbench)"
            ),
        )
        parser.add_argument(
            "--dab-datasets",
            nargs="+",
            default=None,
            help="(dataagentbench) run only these datasets",
        )
        parser.add_argument(
            "--dab-contracts",
            type=Path,
            default=DEFAULT_CONTRACTS,
            help="(dataagentbench) the contracts root, <draw>/<dataset>/",
        )

    @classmethod
    def from_args(cls, args: argparse.Namespace) -> DataAgentBench:
        return cls.from_data(
            args.dab_data,
            contracts=getattr(args, "dab_contracts", DEFAULT_CONTRACTS),
            datasets=getattr(args, "dab_datasets", None),
        )

    # -- tasks and prompts ----------------------------------------------------

    def _folder(self, dataset: str) -> Path:
        return self._src / dab_fetch.dataset_dir(dataset)

    def _query_dir(self, task: Task) -> Path:
        return self._folder(task.meta["dataset"]) / f"query{task.meta['query']}"

    def _description(self, dataset: str) -> str:
        return dataset_description(self._folder(dataset))

    def tasks(self) -> list[Task]:
        return [
            Task(
                task_id=f"{dataset}/{n}",
                prompt=user_message(query, self._description(dataset)),
                group=dataset,
                meta={"dataset": dataset, "query": n},
            )
            for dataset, n, query in self._records
        ]

    def pristine_db(self, task: Task) -> Path:
        return self._data / "duckdb" / f"{task.meta['dataset']}.duckdb"

    def _contract_dir(self, arm: str, dataset: str) -> Path:
        return self._contracts / CONTRACT_DRAWS[arm] / dataset

    def _contract(self, arm: str, dataset: str):
        return load_contract_at(self._contract_dir(arm, dataset) / "contract.yml")

    def _query_text(self, task: Task) -> str:
        return next(
            q
            for d, n, q in self._records
            if d == task.meta["dataset"] and n == task.meta["query"]
        )

    def build_arm(self, arm: str, task: Task, db: Path) -> ArmSetup:
        if arm not in ARMS:
            raise ValueError(f"unknown arm: {arm!r}; expected one of {ARMS}")
        dataset = task.meta["dataset"]
        user_prompt = None
        if arm in HINT_ARMS:
            description = with_hints(self._description(dataset), self._folder(dataset))
            user_prompt = user_message(self._query_text(task), description)
        if arm in GOVERNED_ARMS:
            setup = governed_arm(system_prompt(True), db, self._contract(arm, dataset))
            return dataclasses.replace(setup, user_prompt=user_prompt)
        prompt = system_prompt(False)
        if arm == "dab_contract_text":
            folder = self._contract_dir(arm, dataset)
            files = "\n\n".join(
                f"### {f}\n\n```yaml\n"
                f"{(folder / f).read_text(encoding='utf-8').strip()}\n```"
                for f in ("contract.yml", "semantic.yml")
            )
            prompt += f"\n\n## Data contract\n\n{CONTRACT_TEXT_NOTE}\n\n{files}"
        return ArmSetup(
            prompt,
            _ungoverned_tools(db, all_schemas=True),
            None,
            user_prompt=user_prompt,
        )

    def arm_digest(self, arm: str, task: Task) -> str:
        # Every input the arm's prompts are built from, for the task's dataset.
        dataset = task.meta["dataset"]
        folder = self._folder(dataset)
        lines = [
            f"arm {arm}",
            f"system {_sha256_text(system_prompt(arm in GOVERNED_ARMS))}",
            f"session {_sha256_text(SESSION_NOTE)}",
            f"db_description.txt {_sha256(folder / 'db_description.txt')}",
        ]
        if arm in HINT_ARMS:
            lines.append(f"{HINTS_FILE} {_sha256(folder / HINTS_FILE)}")
        if arm in CONTRACT_DRAWS:
            lines.append(f"contract {contract_digest(self._contract(arm, dataset))}")
        return _sha256_text("\n".join(lines))

    def check_arms(self, arms: tuple[str, ...], tasks: list[Task]) -> None:
        datasets = sorted({t.meta["dataset"] for t in tasks})
        for arm in arms:
            if arm not in CONTRACT_DRAWS:
                continue
            missing, broken = [], []
            for dataset in datasets:
                folder = self._contract_dir(arm, dataset)
                if (
                    not (folder / "contract.yml").is_file()
                    or not (folder / "semantic.yml").is_file()
                ):
                    missing.append(dataset)
                    continue
                try:
                    # `from_yaml` alone does not open `semantic.yml`; the
                    # digest does.
                    contract_digest(self._contract(arm, dataset))
                except Exception as exc:
                    broken.append(f"{dataset}: {type(exc).__name__}: {exc}")
            if missing:
                raise SystemExit(
                    f"{arm}: no contract for {missing} under "
                    f"{self._contracts / CONTRACT_DRAWS[arm]}"
                )
            if broken:
                raise SystemExit(f"{arm}: contract does not load: " + "; ".join(broken))

    # -- grading ----------------------------------------------------------------

    def _gold(self, task: Task) -> str:
        path = self._query_dir(task) / "ground_truth.csv"
        return dab_grade.gold_string(path.read_text(encoding="utf-8"))

    def gold_ref(self, task: Task) -> str | None:
        # A digest, never the gold: result rows are committed, golds are not.
        return _sha256_text(self._gold(task))

    def grade(self, task: Task, answer: str) -> Grade:
        return dab_grade.grade_answer(
            self._query_dir(task), answer, helper_root=self._src
        )

    def normalize(self, answer: str) -> str:
        # The validator reads the answer exactly as given.
        return answer

    def golds_hash(self) -> str:
        digests = {}
        for task in self.tasks():
            qdir = self._query_dir(task)
            digests[task.task_id] = [self.gold_ref(task), _sha256(qdir / "validate.py")]
        return _sha256_text(json.dumps(digests, sort_keys=True))

    def row_fields(self, task: Task) -> dict:
        return {"dataset": task.meta["dataset"], "query": task.meta["query"]}

    @staticmethod
    def scorer() -> str:
        return SCORER

    @staticmethod
    def rescore(row: dict) -> str | None:
        # The gold is not in the row, by design; re-grading needs DAB_DATA.
        return None

    @staticmethod
    def e2e_correct(row: dict) -> bool:
        return False
