"""The three context configurations under comparison.

Arms differ only in what context reaches the agent. Arms `schema_only` and
`manual_prompt` deliberately bypass the library entirely — if they used its
`run_query`, they would inherit contract enforcement and arm `contract` would no
longer be the only governed arm.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import duckdb
from pydantic_ai import Tool

from dce.frozen import load_contract

ARMS: tuple[str, ...] = ("schema_only", "manual_prompt", "contract")

# A harness property, not a contract limit: `semantic.limits.max_rows` is not
# part of the library's contract schema, and the frozen contract declares no
# row limit. Applied identically to all three arms so no arm is advantaged by
# result truncation.
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


def _ungoverned_tools(db_path: Path) -> list[Tool]:
    """Plain schema access + query execution. No contract, no validation.

    `describe_table` runs `DESCRIBE` directly against the same physical
    database as every other arm, unfiltered and uncached — arm C's contract is
    only permitted to declare the column names it declares because every arm
    can see them this way, so this must not gain an arm-specific schema cache,
    a curated table list, or any filtering of `DESCRIBE` output.
    """

    def list_tables() -> str:
        con = duckdb.connect(str(db_path), read_only=True)
        try:
            return "\n".join(r[0] for r in con.execute("SHOW TABLES").fetchall())
        finally:
            con.close()

    def describe_table(table: str) -> str:
        con = duckdb.connect(str(db_path), read_only=True)
        try:
            rows = con.execute(f"DESCRIBE {table}").fetchall()
            return "\n".join(f"{r[0]}: {r[1]}" for r in rows)
        finally:
            con.close()

    def execute_sql(sql: str) -> str:
        con = duckdb.connect(str(db_path), read_only=True)
        try:
            cur = con.execute(sql)
            cols = [d[0] for d in cur.description]
            rows = cur.fetchmany(MAX_ROWS)
            return "\n".join([",".join(cols), *(",".join(map(str, r)) for r in rows)])
        except Exception as exc:  # surfaced to the model, same as arm C's errors
            return f"ERROR: {exc}"
        finally:
            con.close()

    return [Tool(list_tables), Tool(describe_table), Tool(execute_sql)]


def _governed_tools(db_path: Path):
    from agentic_data_contracts import create_pydantic_ai_tools
    from agentic_data_contracts.adapters.duckdb import DuckDBAdapter
    from agentic_data_contracts.core.session import ContractSession

    contract = load_contract()
    session = ContractSession(contract)
    tools = create_pydantic_ai_tools(
        contract,
        adapter=DuckDBAdapter(database=str(db_path)),
        session=session,
    )
    return tools, session


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
        tools, session = _governed_tools(db_path)
        prompt = (
            f"{BASE_PROMPT}\n\n"
            "A data contract governs this database. Look up the domain and the "
            "metrics that apply before writing SQL, and validate every query "
            "with inspect_query before running it.\n\n"
            f"{load_contract().to_system_prompt()}"
        )
        return ArmSetup(prompt, tools, session)

    raise ValueError(f"unknown arm: {arm!r}; expected one of {ARMS}")
