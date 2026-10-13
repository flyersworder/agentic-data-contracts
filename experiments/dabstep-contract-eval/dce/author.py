"""python -m dce.author -- one session writes a database's whole contract.

    uv run python -m dce.author --db insider_trading museum_artifact cross_border \
        --max-spend 15

The compiler (`dce.compile`) turns a KB into a contract one entry at a time.
The author is the other way a contract gets made: a data engineer who reads
the whole KB, explores the data, and writes the contract as a whole --
metrics that combine entries, conventions the data imposes, joins declared
with `from_sql`. The session sees the schema, the column meanings and the
KB, nothing else: no task, question, gold or result. Its database access is
the compiler's (`CompileDB`: read-only, no file access, bounded).

A submission is accepted only when the contract loads, every metric runs,
every declared join passes the library's gate, every table is allowed, the
standard operations stay forbidden, and every KB entry is mapped to where
the contract holds it -- so the arm cannot know less than the manual. Output
goes to `<out>/livesqlbench/<db>/`, with a gzipped log of the session.
"""

from __future__ import annotations

import argparse
import gzip
import json
import os
import tempfile
import threading
from collections.abc import Callable
from pathlib import Path

import yaml

from dce.compile.db import CompileDB
from dce.compile.emit import FORBIDDEN_OPERATIONS
from dce.compile.inputs import CompileInputs
from dce.compile.run import _sha256
from dce.compile.session import DriverResult

AUTHOR_MODEL = "claudeopus5.5"
MAX_SUBMISSIONS = 6
#: Model requests per database: room to explore the data, then to write and
#: fix the contract.
AUTHOR_REQUEST_LIMIT = 80
LOG = "author-log.jsonl.gz"
#: Sessions that failed outside the model (a timeout, a dropped connection):
#: billed, not finished. The database is authored again on the next run.
SPEND_LOG = "author-spend.jsonl"
#: Seconds one model request may take. A whole contract is one response,
#: longer than the agents' shared 300 s allows.
AUTHOR_TIMEOUT_S = 900
#: The form a contract takes: the hand-authored DABStep contract, shown as an
#: example of structure only (another benchmark; nothing of LiveSQLBench).
EXAMPLE_DIR = Path(__file__).resolve().parents[1] / "contract"
HEADER = (
    "# Authored by dce.author -- do not edit; re-author instead.\n"
    "# benchmark: {benchmark}, database: {db}, author: {model}\n"
)
ERROR_CHARS = 4000

SYSTEM_PROMPT = """\
You are a data engineer. You write the data contract an analyst agent will \
use to answer questions over one database. The agent sees the database \
schema, the column meanings and your contract, and nothing else: the \
documentation below reaches it only through what you write.

A contract is two YAML files. contract.yml declares the tables the agent may \
read, the operations it may not run, the domains (topics, each with a \
description) and rules. semantic.yml holds the metrics (name, description, \
sql_expression, source_model; for a metric over several tables, from_sql -- \
a FROM clause only, tables written main.<table>, with aliases -- and grain, \
what one row of it is) and table and column descriptions. The agent looks \
metrics up by name and reuses their SQL; it reads descriptions as written.

Write the contract a careful engineer would:
- Carry every rule, definition, threshold and value meaning the \
documentation states. Keep each definition's wording or make it more \
precise; never drop or weaken a stated rule.
- Turn every computable definition into a metric whose SQL runs on this \
database. Combine definitions where the documentation builds one on \
another, and declare joins with from_sql where a definition spans tables.
- Check the data with run_sql where it decides how a definition must be \
written (how values are stored, units, NULLs, JSON paths, formats) and say \
what you found in the description.
- Write DuckDB SQL. {dialect_note}
- Allow every table in schema main, and forbid at least: {forbidden}.

Submit with submit_contract(contract_yml, semantic_yml, kb_coverage). \
kb_coverage maps every documentation entry id to where the contract holds \
it: "metric:<name>", "table:<table>", "column:<table>.<column>" or \
"domain:<name>". A submission that fails a check comes back with the \
reasons; you have {submissions} submissions. When one returns "Accepted.", \
reply "done".

## The form, by example

A contract written for another database (payments; nothing in it applies \
here), to show the structure and the level of care:

### contract.yml

{example_contract}

### semantic.yml

{example_semantic}
"""

USER_PROMPT = """\
Write the contract for the database `{db}`.

## Database schema

{ddl}

## Column meanings

{meanings}

## Documentation ({n} entries)

{kb}
"""


def system_prompt(inputs: CompileInputs) -> str:
    return SYSTEM_PROMPT.format(
        dialect_note=inputs.dialect_note,
        forbidden=", ".join(FORBIDDEN_OPERATIONS),
        submissions=MAX_SUBMISSIONS,
        example_contract=(EXAMPLE_DIR / "contract.yml").read_text(encoding="utf-8"),
        example_semantic=(EXAMPLE_DIR / "semantic.yml").read_text(encoding="utf-8"),
    )


def user_prompt(inputs: CompileInputs) -> str:
    return USER_PROMPT.format(
        db=inputs.db_name,
        ddl=inputs.ddl,
        meanings=inputs.column_meanings or "(none)",
        n=len(inputs.entries),
        kb="\n".join(
            f"[{e.id}] {e.name}: {' '.join(e.text.split())}" for e in inputs.entries
        ),
    )


def check_submission(
    contract_yml: str,
    semantic_yml: str,
    kb_coverage: dict,
    *,
    inputs: CompileInputs,
    db: CompileDB,
    tables: dict[str, list[tuple[str, str]]],
) -> list[str]:
    """Why a submission cannot be accepted; empty when it can."""
    from agentic_data_contracts import DataContract, contract_digest
    from agentic_data_contracts.semantic.yaml_source import YamlSource
    from agentic_data_contracts.validation import (
        metric_query,
        validate_metric_from_sql,
    )

    problems: list[str] = []
    try:
        contract = yaml.safe_load(contract_yml)
        semantic = yaml.safe_load(semantic_yml)
    except yaml.YAMLError as exc:
        return [f"contract.yml or semantic.yml is not YAML: {exc}"]
    if not isinstance(contract, dict) or not isinstance(contract.get("semantic"), dict):
        return ["contract.yml must be a mapping with a `semantic` section"]
    if not isinstance(semantic, dict):
        return ["semantic.yml must be a mapping"]
    contract["semantic"]["source"] = {"type": "yaml", "path": "./semantic.yml"}

    allowed = {
        t.lower()
        for block in contract["semantic"].get("allowed_tables") or []
        if str(block.get("schema", "")).lower() == "main"
        for t in block.get("tables") or []
    }
    for table in sorted(t for t in tables if t.lower() not in allowed):
        problems.append(f"allowed_tables must include main.{table}")
    forbidden = {
        str(op).upper() for op in contract["semantic"].get("forbidden_operations") or []
    }
    for op in FORBIDDEN_OPERATIONS:
        if op not in forbidden:
            problems.append(f"forbidden_operations must include {op}")

    with tempfile.TemporaryDirectory() as tmp:
        folder = Path(tmp)
        (folder / "contract.yml").write_text(yaml.safe_dump(contract), encoding="utf-8")
        (folder / "semantic.yml").write_text(yaml.safe_dump(semantic), encoding="utf-8")
        try:
            loaded = DataContract.from_yaml(folder / "contract.yml")
            contract_digest(loaded)
            metrics = YamlSource(folder / "semantic.yml").get_metrics()
        except Exception as exc:  # noqa: BLE001 - every failure is reported
            return problems + [
                f"the contract does not load: {type(exc).__name__}: {exc}"
            ]

    for metric in metrics:
        if not metric.sql_expression.strip():
            continue
        query = metric_query(metric) or (
            f"SELECT {metric.sql_expression}\nFROM {metric.source_model}"
        )
        out = db.run_sql(f"SELECT * FROM ({query}\n) AS q LIMIT 5")
        if out.startswith("ERROR:"):
            problems.append(f"metric {metric.name!r} does not run: {out[7:300]}")
    problems += validate_metric_from_sql(loaded, metrics, dialect="duckdb")

    names = {m.name for m in metrics}
    domains = {str(d.get("name")) for d in contract["semantic"].get("domains") or []}
    columns = {
        (str(t.get("table", "")).lower(), str(c.get("name", "")).lower())
        for t in semantic.get("tables") or []
        for c in t.get("columns") or []
    }
    described = {str(t.get("table", "")).lower() for t in semantic.get("tables") or []}
    coverage = {str(k): str(v) for k, v in (kb_coverage or {}).items()}
    for entry in inputs.entries:
        where = coverage.get(str(entry.id))
        if where is None:
            problems.append(
                f"kb_coverage has no place for entry {entry.id} ({entry.name})"
            )
            continue
        kind, _, target = where.partition(":")
        ok = {
            "metric": target in names,
            "domain": target in domains,
            "table": target.lower() in described,
            "column": tuple(target.lower().partition(".")[::2]) in columns,
        }.get(kind, False)
        if not ok:
            problems.append(
                f"entry {entry.id}: {where!r} names nothing in the contract"
            )
    return problems


class _Session:
    def __init__(self, inputs: CompileInputs, db: CompileDB, tables) -> None:
        self.inputs, self.db, self.tables = inputs, db, tables
        self.submissions = 0
        self.accepted: tuple[str, str] | None = None
        self.errors: list[str] = []
        self.queries: list[dict] = []
        self._lock = threading.Lock()

    def tools(self) -> list[Callable]:
        def run_sql(sql: str) -> str:
            """Run a read-only SQL query on the database; at most 50 rows come back."""
            out = self.db.run_sql(sql)
            self.queries.append({"sql": sql, "error": out.startswith("ERROR:")})
            return out

        def submit_contract(
            contract_yml: str, semantic_yml: str, kb_coverage: dict[str, str]
        ) -> str:
            """Submit the whole contract: contract.yml and semantic.yml as YAML
            text, and kb_coverage mapping every documentation entry id to
            "metric:<name>", "table:<table>", "column:<table>.<column>" or
            "domain:<name>"."""
            with self._lock:
                if self.accepted is not None:
                    return "Already accepted."
                if self.submissions >= MAX_SUBMISSIONS:
                    return "No submissions left. Stop."
                self.submissions += 1
                problems = check_submission(
                    contract_yml,
                    semantic_yml,
                    kb_coverage,
                    inputs=self.inputs,
                    db=self.db,
                    tables=self.tables,
                )
                if not problems:
                    self.accepted = (contract_yml, semantic_yml)
                    return "Accepted."
                text = "\n".join(f"- {p}" for p in problems)[:ERROR_CHARS]
                self.errors.append(text)
                left = MAX_SUBMISSIONS - self.submissions
                return f"ERROR:\n{text}\nFix these and submit again ({left} left)."

        return [run_sql, submit_contract]


def _header(inputs: CompileInputs, model: str) -> dict:
    from dce.agent import _commit_sha

    return {
        "type": "header",
        "benchmark": inputs.benchmark,
        "db": inputs.db_name,
        "model": model,
        "commit_sha": _commit_sha(),
        "inputs": {p.name: _sha256(p) for p in inputs.sources},
    }


def _logged_usd(out_dir: Path) -> float:
    lines: list[str] = []
    if (out_dir / LOG).exists():
        lines += (
            gzip.decompress((out_dir / LOG).read_bytes()).decode("utf-8").splitlines()
        )
    if (out_dir / SPEND_LOG).exists():
        lines += (out_dir / SPEND_LOG).read_text(encoding="utf-8").splitlines()
    return sum((json.loads(x).get("usage") or {}).get("usd") or 0.0 for x in lines)


def author_database(
    inputs: CompileInputs,
    out_dir: Path,
    driver,
    *,
    spend_cap: float,
    spent: float = 0.0,
    model: str = AUTHOR_MODEL,
) -> tuple[str, float]:
    """("done" | "failed" | "error" | "skipped" | "capped", spent). A
    database with a log is finished, accepted or not, and is skipped:
    re-authoring it is a decision, made by deleting its directory. A session
    that failed outside the model ("error") writes no log: its cost goes to
    the spend log and the database is authored again on the next run."""
    if (out_dir / LOG).exists():
        return "skipped", spent
    if spent >= spend_cap:
        return "capped", spent
    db = CompileDB(inputs.db_path, init_sql=inputs.init_sql)
    try:
        tables = db.tables()
        session = _Session(inputs, db, tables)
        result: DriverResult = driver(
            system_prompt=system_prompt(inputs),
            user_prompt=user_prompt(inputs),
            tools=session.tools(),
        )
    finally:
        db.close()
    out_dir.mkdir(parents=True, exist_ok=True)
    spent += result.usage.get("usd") or 0.0
    if result.usage.get("fatal") and session.accepted is None:
        with (out_dir / SPEND_LOG).open("a", encoding="utf-8") as fh:
            line = {"usage": result.usage, "submissions": session.submissions}
            fh.write(json.dumps(line, ensure_ascii=False) + "\n")
        return "error", spent
    if session.accepted is not None:
        head = HEADER.format(benchmark=inputs.benchmark, db=inputs.db_name, model=model)
        contract_yml, semantic_yml = session.accepted
        contract = yaml.safe_load(contract_yml)
        contract["semantic"]["source"] = {"type": "yaml", "path": "./semantic.yml"}
        dump = {"sort_keys": False, "allow_unicode": True, "width": 100}
        (out_dir / "contract.yml").write_text(
            head + yaml.safe_dump(contract, **dump), encoding="utf-8"
        )
        (out_dir / "semantic.yml").write_text(
            head + yaml.safe_dump(yaml.safe_load(semantic_yml), **dump),
            encoding="utf-8",
        )
    record = {
        "type": "session",
        "accepted": session.accepted is not None,
        "submissions": session.submissions,
        "errors": session.errors,
        "queries": session.queries,
        "usage": result.usage,
        "messages": result.messages,
    }
    lines = [_header(inputs, model), record]
    (out_dir / LOG).write_bytes(
        gzip.compress(
            "".join(json.dumps(x, ensure_ascii=False) + "\n" for x in lines).encode(
                "utf-8"
            ),
            mtime=0,
        )
    )
    return ("done" if session.accepted else "failed"), spent


def main(argv: list[str] | None = None) -> None:
    import functools

    from dce.benchmarks.livesqlbench import compile_inputs
    from dce.compile import run as run_module

    parser = argparse.ArgumentParser(prog="dce.author")
    parser.add_argument("--db", nargs="+", required=True)
    parser.add_argument(
        "--lsb-data",
        type=Path,
        default=Path(os.environ.get("LSB_DATA", Path.home() / "data" / "livesqlbench")),
    )
    parser.add_argument("--out", type=Path, default=Path("contracts-authored"))
    parser.add_argument("--model", default=AUTHOR_MODEL)
    parser.add_argument("--max-spend", type=float, required=True)
    args = parser.parse_args(argv)

    root = args.out / "livesqlbench"
    spent = sum(_logged_usd(p) for p in root.glob("*")) if root.exists() else 0.0
    driver = functools.partial(
        run_module.claude_driver,
        model=args.model,
        request_limit=AUTHOR_REQUEST_LIMIT,
        timeout=AUTHOR_TIMEOUT_S,
    )
    for name in args.db:
        status, spent = author_database(
            compile_inputs(args.lsb_data, name),
            root / name,
            driver,
            spend_cap=args.max_spend,
            spent=spent,
            model=args.model,
        )
        print(f"{name}: {status} (spent ${spent:.2f} of ${args.max_spend:.2f})")
        if status == "capped":
            raise SystemExit(2)


if __name__ == "__main__":
    main()
