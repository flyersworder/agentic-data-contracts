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
    agent under either arm from issuing `DROP TABLE` or any other DDL/DML —
    and by design, nothing here prevents it (`INSERT`/`DROP` through these
    arms have been confirmed to succeed). That is precisely the failure this
    library exists to prevent, and running ungoverned SQL directly against
    the pristine source-of-truth file would let one bad query silently
    corrupt every task that runs after it in the same sweep.
    `make_working_copy` / `check_and_restore` below give a runner a
    disposable copy plus a way to detect and undo that corruption instead.

CALL ORDER, NOT OPTIONAL: `make_working_copy` once per process; every
`build_arm` call gets the returned path; `ArmSetup.close()` on **every** arm
built for a task; only then `check_and_restore` for that task.

That order matters mechanically, not just tidily. Arm `contract`'s
`DuckDBAdapter` keeps its connection open for the arm's whole lifetime, so a
mutation an ungoverned arm makes while that connection is still open lands in
a `working.duckdb.wal` sidecar, not in the main file — the main file stays
byte-identical to pristine until something checkpoints. `check_and_restore`
now treats the sidecar's mere existence as "cannot certify clean" rather than
comparing only the main file (a real, demonstrated false negative: a `DROP
TABLE` through an ungoverned arm read back as `corrupted=False` while arm
`contract`'s connection was still open), but a check performed with a live
connection open is still **not a valid check** and its repair is not
guaranteed to survive that connection's later `close()`: DuckDB checkpoints a
live connection's own in-memory state back onto the main file when it finally
closes, which happens independently of whatever this module has done to the
file on disk in the meantime, and can silently re-apply the very mutation a
mid-flight restore just undid. `close()` every `ArmSetup` for the task first
— always — and only then call `check_and_restore`; that ordering is the only
combination for which the restore is guaranteed durable.

RETRY BUDGET, NOT FIXABLE HERE BUT CREATED HERE: pydantic-ai's per-run
tool-retry budget defaults to `Agent(retries=1)`. Arm `contract`'s governed
tools raise `ModelRetry` on a validation block (bad SQL, `SELECT *` under
`no_select_star`, a forbidden operation, a missing required filter, ...), and
pydantic-ai counts every `ModelRetry` against that one shared budget. Arms
`schema_only` and `manual_prompt` never raise at all — a bad query comes back
as an ordinary `"ERROR: ..."` string and the model just keeps iterating.
Measured end to end: arm A finished a task after 7 model calls; arm C raised
`UnexpectedModelBehavior` and ended the run after 2, because two governed
queries were blocked in a row (trivially reachable — reaching for
`SELECT *` twice is simply dead under the default budget). That is not a
difference in context, it is a difference in how many chances the model
gets, and it would show up directly as an accuracy gap that has nothing to do
with the contract. Any consumer building arm `contract` via `build_arm`
**must** construct its `Agent` with `retries=` set high enough that arm C is
not budget-limited relative to arms A/B (e.g. at least as many retries as
whatever iteration cap, if any, bounds A/B's own loop). This cannot be
enforced from inside `_governed_tools` — the budget lives on the `Agent`, not
on the tools — which is why it is stated here rather than left to be found.

DISCLOSED ASYMMETRY, NOT FIXED: arm `contract`'s nine library-supplied tools
carry 3,042 characters of tool descriptions against 167 for arms A/B's three,
and several of arm C's coach procedure rather than merely describe function —
e.g. `inspect_query` says a model "MUST call lookup_metric first", `run_query`
says to "Prefer this tool over any other SQL or data-access path",
`lookup_domain` says to "understand business context before querying". That
is procedural instruction reaching only arm C. It is defensible — it is part
of what the contract treatment *is*, not an accident of this module — but it
means "the arms differ only in context" is true of *content* and not of
*coaching*, and that distinction should travel with any result this
experiment produces.
"""

from __future__ import annotations

import csv
import hashlib
import io
import json
import shutil
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING

import duckdb
from pydantic_ai import Tool

from dce.frozen import load_contract, load_hollow_contract

if TYPE_CHECKING:
    from agentic_data_contracts.adapters.duckdb import DuckDBAdapter

ARMS: tuple[str, ...] = (
    "schema_only",
    "manual_prompt",
    "contract",
    "contract_hollow",
)

# A harness property, not a contract limit: `semantic.limits.max_rows` is not
# part of the library's contract schema, and the frozen contract declares no
# row limit. Applied identically to all three arms — arms A/B cap
# `execute_sql`'s own fetch below, and arm C's `run_query` is post-truncated
# by `_truncate_run_query` (the library's `run_query` does not cap rows on its
# own) — so no arm is advantaged by seeing an uncapped result the others
# cannot.
#
# Cut twice, each time on measurement. N2 cut 10,000 -> 1,000: a 10,000-row
# `payments` result serializes to roughly 429k tokens, which then becomes the
# *next* request's input in full. F5 cut 1,000 -> 50 after the first smoke
# run, because 1,000 was still large enough to do the same thing more slowly
# — arm C averaged 45,977 input tokens per request against arm A's 5,807 on a
# single hard task, and that growth (not the tool-call cap) is what stopped
# it. A tool return is not paid for once: it stays in the conversation and is
# resent as input on every later turn, so its cost compounds with turn count,
# fastest for the arm whose returns are largest.
#
# 50 rows is still beyond anything a DABStep answer needs — the questions ask
# for aggregates, so 1,000 raw rows were never what an arm needed, only what
# it could ask for. Applied identically to every arm, same as the values it
# replaces, so this is a harness property and not a confound. The truncation
# marker below reports the true total row count, so cutting the cap hides
# nothing from the model; it only stops the model being handed all of it at
# once.
MAX_ROWS = 50

# CUT FROM 1,000 AFTER THE FIRST SMOKE RUN (F5). A tool return is not paid for
# once: it stays in the conversation and is resent as input on EVERY subsequent
# turn, so an oversized return compounds with turn count. Measured on one hard
# task, arm C averaged 45,977 input tokens per request against arm A's 5,807 —
# 8x — and that growth, not the tool-call cap, is what stopped it (see
# `dce/agent.py`'s `GROWTH`). DABStep answers are aggregates; 1,000 raw rows
# were never the thing an arm needed, only the thing it could ask for.

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
    adapter: DuckDBAdapter | None = None  # arm `contract`'s adapter, else None

    def close(self) -> None:
        """Release the persistent database connection this arm holds, if any.

        Arms `schema_only` and `manual_prompt` open and close a connection
        per tool call, holding nothing between calls, so there is nothing for
        them to release. Arm `contract`'s `DuckDBAdapter` keeps one
        connection open for the life of the session; a caller done with an
        arm must call this so that connection does not leak (one leaked
        read-write handle per `build_arm("contract", ...)` call is also what
        provoked the cross-arm connection conflict this module works around)
        — and, per the module docstring, must call it on *every* arm for a
        task before trusting `check_and_restore`'s result for that task.
        """
        if self.adapter is not None:
            self.adapter.connection.close()


def _sha256(path: Path) -> str:
    """Digest a single file's contents in fixed-size chunks."""
    digest = hashlib.sha256()
    with open(path, "rb") as fh:
        for chunk in iter(lambda: fh.read(1 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _wal_path(db_path: Path) -> Path:
    """DuckDB's default write-ahead-log sidecar path for a database file."""
    return db_path.with_name(db_path.name + ".wal")


def _sha256_with_sidecar(main_path: Path, wal_path: Path) -> str:
    """Digest the main file and its `.wal` sidecar together, when present.

    A mutation made through a still-open connection lands in the sidecar, not
    the main file — digesting the main file alone is exactly the blind spot
    that let a `DROP TABLE` through an ungoverned arm read back as clean while
    a governed arm's connection was still open. Concatenating the sidecar's
    bytes, when it exists, makes the digest sensitive to that pending,
    not-yet-checkpointed state too.
    """
    digest = hashlib.sha256()
    for path in (main_path, wal_path):
        if not path.exists():
            continue
        with open(path, "rb") as fh:
            for chunk in iter(lambda: fh.read(1 << 20), b""):
                digest.update(chunk)
    return digest.hexdigest()


def make_working_copy(pristine_db: Path, working_db: Path) -> Path:
    """Copy the pristine database to a working file, once per process.

    Every arm must open `working_db`, never `pristine_db`, directly — see the
    module docstring. Returns `working_db` so this composes into a `build_arm`
    call: `build_arm(arm, make_working_copy(pristine, working), docs)`.

    Removes `working_db`'s own stale `.wal` sidecar FIRST, before copying —
    a hard death (SIGKILL, an OOM kill) between a previous run's mutation
    and its checkpoint leaves that sidecar sitting next to the (now
    overwritten-on-the-next-`copyfile`) working file. `shutil.copyfile`
    only ever touches the main file; it does not know the sidecar exists
    and would not remove it. The next process to OPEN that "fresh" copy —
    the very next `build_arm` call — would have DuckDB replay the dead
    run's own WAL on top of it, undoing the fresh copy silently. Measured:
    a resumed sweep's first task saw the dead run's mutation and was
    stamped `db_corrupted: True` having done nothing itself — a false
    positive on the experiment's headline governance metric, and exactly
    the recovery `dce.runner.main()`'s "resume in a fresh process" message
    promises but cannot deliver without this.
    """
    _wal_path(working_db).unlink(missing_ok=True)
    shutil.copyfile(pristine_db, working_db)
    return working_db


@dataclass
class IntegrityCheck:
    """Outcome of one post-task comparison of the working DB against pristine."""

    corrupted: bool
    pristine_digest: str
    working_digest: str
    wal_present: bool


def check_and_restore(working_db: Path, pristine_db: Path) -> IntegrityCheck:
    """Call after every task, and only once every arm's `.close()` has been
    called — see the module docstring's CALL ORDER section; a check run
    while any arm's connection is still open is not a valid check, and its
    repair is not guaranteed to survive that connection's later close.

    Arms `schema_only` and `manual_prompt` are ungoverned, so nothing in this
    experiment stops one of them from mutating the warehouse (a
    `DROP TABLE`, say). If the working copy no longer matches the pristine
    file — including its `.wal` sidecar, whose mere presence means this
    cannot be certified clean regardless of what the main file's own bytes
    say — that is exactly the failure this library exists to prevent:
    restore the working copy from the pristine file and return the event, so
    a runner can stamp it into the result row instead of silently carrying
    corrupted data into the next task.
    """
    wal_path = _wal_path(working_db)
    wal_present = wal_path.exists()

    pristine_digest = _sha256(pristine_db)
    working_digest = _sha256_with_sidecar(working_db, wal_path)

    corrupted = wal_present or (working_digest != pristine_digest)
    if corrupted:
        if wal_path.exists():
            # Before copying the main file back, not after: a sidecar left
            # in place gets checkpointed onto whatever file is sitting at
            # this path the next time something opens it, silently replaying
            # the very mutation this restore is meant to undo.
            wal_path.unlink()
        shutil.copyfile(pristine_db, working_db)
    return IntegrityCheck(corrupted, pristine_digest, working_digest, wal_present)


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
            total = len(rows)
            if truncated:
                # Drain the rest of the same cursor purely to count it — no
                # re-execution — so the marker below can report the true
                # total, the same thing arm C's `row_count` already tells it.
                total += len(cur.fetchall())
            rows = rows[:MAX_ROWS]

            buf = io.StringIO()
            writer = csv.writer(buf)
            writer.writerow(cols)
            writer.writerows(rows)
            text = buf.getvalue().rstrip("\n")
            if truncated:
                text += f"\n-- truncated at {MAX_ROWS} rows ({total} total)"
            return text
        except Exception as exc:  # surfaced to the model, same as arm C's errors
            return f"ERROR: {exc}"
        finally:
            if con is not None:
                con.close()

    return [Tool(list_tables), Tool(describe_table), Tool(execute_sql)]


# The exact leading substring of `run_query`'s success JSON, built from
# `{"columns": ..., "rows": ..., "row_count": ..., "session": ...}` in that
# key order (see tools/factory.py). Locating this literal, rather than a bare
# `"{"`, means a `{` appearing anywhere in a `WARNINGS:`/`LOG:` preamble can't
# be mistaken for the payload boundary.
_RUN_QUERY_PAYLOAD_MARKER = '{"columns"'


def _truncate_run_query(tool_def, max_rows: int):
    """Cap `run_query`'s row count the same way `execute_sql` is capped.

    The library's `run_query` returns every row the query produced — measured
    on one identical query, arm A's `execute_sql` returned 10,001 lines
    (`MAX_ROWS` + header) while arm C's `run_query` returned 138,236 rows.
    `MAX_ROWS` is meant to apply identically everywhere, so this wraps the
    `run_query` `ToolDef` before it reaches `create_pydantic_ai_tools`,
    truncating its JSON payload's `rows` list and appending the same
    `-- truncated at N rows (M total)` marker `execute_sql` appends, rather
    than changing the library's own (frozen) implementation.

    Response text is not always bare JSON: `run_query` prepends
    `WARNINGS:`/`LOG:` sections before the JSON blob, and a blocked call
    returns plain `BLOCKED —` text with no JSON at all. Locating
    `_RUN_QUERY_PAYLOAD_MARKER` and parsing from there handles the former; its
    absence leaves the response untouched, which is correct for the latter
    (nothing to truncate in a block message). If the marker *is* found but
    what follows doesn't parse as JSON, that is not a shape this wrapper
    understands and is not something to paper over: silently returning the
    untruncated payload here would quietly reopen the row-count asymmetry
    this wrapper exists to close, with no sign anything went wrong — so this
    raises instead.
    """
    from agentic_data_contracts.tools.factory import ToolDef

    inner = tool_def.callable

    async def _capped(args: dict) -> dict:
        result = await inner(args)
        content = result.get("content") or []
        if not content or content[0].get("type") != "text":
            return result

        text = content[0]["text"]
        idx = text.find(_RUN_QUERY_PAYLOAD_MARKER)
        if idx == -1:
            return result

        prefix, blob = text[:idx], text[idx:]
        try:
            data = json.loads(blob)
        except json.JSONDecodeError as exc:
            raise RuntimeError(
                f"_truncate_run_query: found {_RUN_QUERY_PAYLOAD_MARKER!r} but "
                f"could not parse JSON after it: {exc}"
            ) from exc

        rows = data.get("rows")
        if not isinstance(rows, list) or len(rows) <= max_rows:
            return result

        total = data.get("row_count", len(rows))
        data["rows"] = rows[:max_rows]
        blob_out = json.dumps(data, default=str)
        marker = f"-- truncated at {max_rows} rows ({total} total)"
        new_text = f"{prefix}{blob_out}\n{marker}"
        return {**result, "content": [{"type": "text", "text": new_text}]}

    return ToolDef(
        name=tool_def.name,
        description=tool_def.description,
        input_schema=tool_def.input_schema,
        callable=_capped,
    )


def _governed_tools(db_path: Path, *, contract=None):
    from agentic_data_contracts import create_pydantic_ai_tools
    from agentic_data_contracts.adapters.duckdb import DuckDBAdapter
    from agentic_data_contracts.core.session import ContractSession
    from agentic_data_contracts.tools.factory import create_tools

    contract = contract if contract is not None else load_contract()
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

    if arm in ("contract", "contract_hollow"):
        # IDENTICAL EXCEPT FOR THE CONTRACT OBJECT. The procedural sentence
        # below is the thing `contract_hollow` exists to control for, so it is
        # written once and shared rather than copied — a divergence here would
        # silently reintroduce the confound the control was built to remove.
        contract = load_contract() if arm == "contract" else load_hollow_contract()
        tools, session, adapter = _governed_tools(db_path, contract=contract)
        prompt = (
            f"{BASE_PROMPT}\n\n"
            "A data contract governs this database. Look up the domain and the "
            "metrics that apply before writing SQL, and validate every query "
            "with inspect_query before running it.\n\n"
            f"{contract.to_system_prompt()}"
        )
        return ArmSetup(prompt, tools, session, adapter)

    raise ValueError(f"unknown arm: {arm!r}; expected one of {ARMS}")
