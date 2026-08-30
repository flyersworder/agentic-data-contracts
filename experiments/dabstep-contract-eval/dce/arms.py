"""The three context configurations under comparison.

Arms differ only in what context reaches the agent. Arms `schema_only` and
`manual_prompt` deliberately bypass the library entirely — if they used its
`run_query`, they would inherit contract enforcement and arm `contract` would no
longer be the only governed arm.

WORKING COPY, NOT THE PRISTINE FILE: every arm must be pointed at a working
copy of the database, never at `data/dabstep.duckdb` directly. Two reasons,
not one:

  * Arm `contract`'s `DuckDBAdapter` opens its connection read-write and holds
    it open for the arm's lifetime. If any other arm — built in the same
    process, as Task 7's runner does — then opens the *same* physical file
    with a different connection configuration, DuckDB raises
    `ConnectionException: Can't open a connection to same database file with
    a different configuration than existing connections`. Making every arm's
    `duckdb.connect()` call agree on configuration (below, all arms now open
    read-write, no `read_only=True`) is necessary to avoid that crash, but it
    is not sufficient by itself — see the next point — so this file also
    exposes the working-copy helpers rather than treating "matching configs"
    as the whole fix.
  * Arms `schema_only` and `manual_prompt` are ungoverned: nothing stops an
    agent under either arm from issuing `DROP TABLE` or any other DDL/DML.
    That is precisely the failure this library exists to prevent, and running
    ungoverned SQL directly against the pristine source-of-truth file would
    let one bad query silently corrupt every task that runs after it in the
    same sweep. `make_working_copy` / `check_and_restore` below give a runner
    a disposable copy plus a way to detect and undo that corruption instead.

A runner (Task 7) is expected to call `make_working_copy` once per process,
pass the returned path as every `build_arm` call's `db_path`, and call
`check_and_restore` after each task to both repair and record any mutation.
"""

from __future__ import annotations

import csv
import hashlib
import io
import json
import shutil
from dataclasses import dataclass
from pathlib import Path

import duckdb
from pydantic_ai import Tool

from dce.frozen import load_contract

ARMS: tuple[str, ...] = ("schema_only", "manual_prompt", "contract")

# A harness property, not a contract limit: `semantic.limits.max_rows` is not
# part of the library's contract schema, and the frozen contract declares no
# row limit. Applied identically to all three arms — arms A/B cap
# `execute_sql`'s own fetch below, and arm C's `run_query` is post-truncated
# by `_truncate_run_query` (the library's `run_query` does not cap rows on its
# own) — so no arm is advantaged by seeing an uncapped result the others
# cannot.
MAX_ROWS = 10_000

BASE_PROMPT = (
    "You are a data analyst answering questions over a DuckDB database.\n"
    "Explore the schema, write SQL, and verify your result before answering.\n"
    "Your final message must be the answer alone, formatted exactly as the "
    "question's guidelines require. Do not show working in the final message."
)


@dataclass
class ArmSetup:
    system_prompt: str
    tools: list[Tool]
    session: object | None
    adapter: object | None = None  # arm `contract`'s DuckDBAdapter, else None

    def close(self) -> None:
        """Release the persistent database connection this arm holds, if any.

        Arms `schema_only` and `manual_prompt` open and close a connection
        per tool call, holding nothing between calls, so there is nothing for
        them to release. Arm `contract`'s `DuckDBAdapter` keeps one
        connection open for the life of the session; a caller done with an
        arm must call this so that connection does not leak (one leaked
        read-write handle per `build_arm("contract", ...)` call is also what
        provoked the cross-arm connection conflict this module works around).
        """
        if self.adapter is not None:
            self.adapter.connection.close()


def _sha256(path: Path) -> str:
    """Digest a file's contents in fixed-size chunks (safe for a large DB)."""
    digest = hashlib.sha256()
    with open(path, "rb") as fh:
        for chunk in iter(lambda: fh.read(1 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()


def make_working_copy(pristine_db: Path, working_db: Path) -> Path:
    """Copy the pristine database to a working file, once per process.

    Every arm must open `working_db`, never `pristine_db`, directly — see the
    module docstring. Returns `working_db` so this composes into a `build_arm`
    call: `build_arm(arm, make_working_copy(pristine, working), docs)`.
    """
    shutil.copyfile(pristine_db, working_db)
    return working_db


@dataclass
class IntegrityCheck:
    """Outcome of one post-task comparison of the working DB against pristine."""

    corrupted: bool
    pristine_digest: str
    working_digest: str


def check_and_restore(working_db: Path, pristine_db: Path) -> IntegrityCheck:
    """Call after every task. Arms `schema_only` and `manual_prompt` are
    ungoverned, so nothing in this experiment stops one of them from mutating
    the warehouse (a `DROP TABLE`, say). If the working copy's digest no
    longer matches the pristine file's, that mutation is exactly the failure
    this library exists to prevent: restore the working copy from the
    pristine file and return the event, so a runner can stamp it into the
    result row instead of silently carrying corrupted data into the next
    task.
    """
    pristine_digest = _sha256(pristine_db)
    working_digest = _sha256(working_db)
    corrupted = working_digest != pristine_digest
    if corrupted:
        shutil.copyfile(pristine_db, working_db)
    return IntegrityCheck(corrupted, pristine_digest, working_digest)


def _ungoverned_tools(db_path: Path) -> list[Tool]:
    """Plain schema access + query execution. No contract, no validation.

    Every connection here opens `db_path` read-write (no `read_only=True`) so
    its configuration matches arm `contract`'s `DuckDBAdapter` when both are
    built in the same process — see the module docstring for why a mismatch
    crashes every ungoverned tool call outright.

    `describe_table` runs `DESCRIBE` directly against the same physical
    database as every other arm, unfiltered and uncached — arm C's contract
    is only permitted to declare the column names it declares because every
    arm can see them this way, so this must not gain an arm-specific schema
    cache, a curated table list, or any filtering of `DESCRIBE` output.
    """

    def list_tables() -> str:
        """List the tables in the database, one name per line."""
        con = None
        try:
            con = duckdb.connect(str(db_path))
            return "\n".join(r[0] for r in con.execute("SHOW TABLES").fetchall())
        except Exception as exc:  # surfaced to the model, same as execute_sql
            return f"ERROR: {exc}"
        finally:
            if con is not None:
                con.close()

    def describe_table(table: str) -> str:
        """Return each column's name and type for the given table."""
        con = None
        try:
            con = duckdb.connect(str(db_path))
            rows = con.execute(f"DESCRIBE {table}").fetchall()
            return "\n".join(f"{r[0]}: {r[1]}" for r in rows)
        except Exception as exc:
            return f"ERROR: {exc}"
        finally:
            if con is not None:
                con.close()

    def execute_sql(sql: str) -> str:
        """Run a SQL query and return its result as CSV (header + rows)."""
        con = None
        try:
            con = duckdb.connect(str(db_path))
            cur = con.execute(sql)
            cols = [d[0] for d in cur.description]
            # Fetch one row past the cap so truncation can be detected and
            # reported rather than silently swallowed (see I5 in the design
            # notes: a truncated aggregate must not be answered as complete).
            rows = cur.fetchmany(MAX_ROWS + 1)
            truncated = len(rows) > MAX_ROWS
            rows = rows[:MAX_ROWS]

            buf = io.StringIO()
            writer = csv.writer(buf)
            writer.writerow(cols)
            writer.writerows(rows)
            text = buf.getvalue().rstrip("\n")
            if truncated:
                text += f"\n-- truncated at {MAX_ROWS} rows"
            return text
        except Exception as exc:  # surfaced to the model, same as arm C's errors
            return f"ERROR: {exc}"
        finally:
            if con is not None:
                con.close()

    return [Tool(list_tables), Tool(describe_table), Tool(execute_sql)]


def _truncate_run_query(tool_def, max_rows: int):
    """Cap `run_query`'s row count the same way `execute_sql` is capped.

    The library's `run_query` returns every row the query produced — measured
    on one identical query, arm A's `execute_sql` returned 10,001 lines
    (`MAX_ROWS` + header) while arm C's `run_query` returned 138,236 rows.
    `MAX_ROWS` is meant to apply identically everywhere, so this wraps the
    `run_query` `ToolDef` before it reaches `create_pydantic_ai_tools`,
    truncating its JSON payload's `rows` list and appending the same
    `-- truncated at N rows` marker `execute_sql` appends, rather than
    changing the library's own (frozen) implementation.

    Response text is not always bare JSON: `run_query` prepends
    `WARNINGS:`/`LOG:` sections before the JSON blob, and a blocked call
    returns plain `BLOCKED —` text with no JSON at all. Locating the first
    `{` and parsing from there handles the former; a `JSONDecodeError` or the
    absence of `{` leaves the response untouched, which is correct for the
    latter (nothing to truncate in a block message).
    """
    from agentic_data_contracts.tools.factory import ToolDef

    inner = tool_def.callable

    async def _capped(args: dict) -> dict:
        result = await inner(args)
        content = result.get("content") or []
        if not content or content[0].get("type") != "text":
            return result

        text = content[0]["text"]
        brace = text.find("{")
        if brace == -1:
            return result
        prefix, blob = text[:brace], text[brace:]
        try:
            data = json.loads(blob)
        except json.JSONDecodeError:
            return result

        rows = data.get("rows")
        if not isinstance(rows, list) or len(rows) <= max_rows:
            return result

        data["rows"] = rows[:max_rows]
        blob_out = json.dumps(data, default=str)
        marker = f"-- truncated at {max_rows} rows"
        new_text = f"{prefix}{blob_out}\n{marker}"
        return {**result, "content": [{"type": "text", "text": new_text}]}

    return ToolDef(
        name=tool_def.name,
        description=tool_def.description,
        input_schema=tool_def.input_schema,
        callable=_capped,
    )


# RETRY BUDGET — NOT FIXABLE IN THIS FILE, BUT CREATED BY IT: pydantic-ai's
# per-run tool-retry budget defaults to `Agent(retries=1)`. Arm `contract`'s
# governed tools raise `ModelRetry` on a validation block (bad SQL,
# `SELECT *` under `no_select_star`, a forbidden operation, a missing
# required filter, ...), and pydantic-ai counts every `ModelRetry` against
# that one shared budget. Arms `schema_only` and `manual_prompt` never raise
# at all — a bad query comes back as an ordinary `"ERROR: ..."` string and the
# model just keeps iterating. Measured end to end: arm A finished a task after
# 7 model calls; arm C raised `UnexpectedModelBehavior` and ended the run
# after 2, because two governed queries were blocked in a row (trivially
# reachable — reaching for `SELECT *` twice is simply dead under the default
# budget). That is not a difference in context, it is a difference in how
# many chances the model gets, and it would show up directly as an accuracy
# gap that has nothing to do with the contract.
#
# Any consumer building arm `contract` via `build_arm` MUST construct its
# `Agent` with `retries=` set high enough that arm C is not budget-limited
# relative to arms A/B (e.g. at least as many retries as whatever iteration
# cap, if any, bounds A/B's own loop). This cannot be enforced from inside
# `_governed_tools` — the budget lives on the `Agent`, not on the tools — so
# it is stated here, at the point where the asymmetry is created, for whoever
# wires the `Agent` next (Task 7).
def _governed_tools(db_path: Path):
    from agentic_data_contracts import create_pydantic_ai_tools
    from agentic_data_contracts.adapters.duckdb import DuckDBAdapter
    from agentic_data_contracts.core.session import ContractSession
    from agentic_data_contracts.tools.factory import create_tools

    contract = load_contract()
    session = ContractSession(contract)
    adapter = DuckDBAdapter(database=str(db_path))
    tool_defs = create_tools(contract, adapter=adapter, session=session)
    tool_defs = [
        _truncate_run_query(t, MAX_ROWS) if t.name == "run_query" else t
        for t in tool_defs
    ]
    tools = create_pydantic_ai_tools(contract, tools=tool_defs, session=session)
    return tools, session, adapter


def build_arm(arm: str, db_path: Path, docs: dict[str, str]) -> ArmSetup:
    if arm == "schema_only":
        return ArmSetup(BASE_PROMPT, _ungoverned_tools(db_path), None)

    if arm == "manual_prompt":
        prompt = (
            f"{BASE_PROMPT}\n\n## Domain manual\n\n{docs['manual']}\n\n"
            f"## Payments table reference\n\n{docs['payments_readme']}"
        )
        return ArmSetup(prompt, _ungoverned_tools(db_path), None)

    if arm == "contract":
        tools, session, adapter = _governed_tools(db_path)
        prompt = (
            f"{BASE_PROMPT}\n\n"
            "A data contract governs this database. Look up the domain and the "
            "metrics that apply before writing SQL, and validate every query "
            "with inspect_query before running it.\n\n"
            f"{load_contract().to_system_prompt()}"
        )
        return ArmSetup(prompt, tools, session, adapter)

    raise ValueError(f"unknown arm: {arm!r}; expected one of {ARMS}")
