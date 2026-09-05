"""Check the contract's declarations against the live warehouse.

Run:
    uv run python examples/revenue_agent/setup_db.py     # once — builds the DuckDB
    uv run python examples/revenue_agent/check_drift.py

What this shows
---------------
A contract can declare a column that does not exist. The declaration sits inside
``contract_digest``, so the contract stays "frozen" around a column the warehouse
renamed — and every gate stays green, because the declarations did not change.
The *world* did. ``check_schema_drift`` is the preflight that notices, and it
belongs in CI, where a schema migration trips it, rather than in an agent's turn.

This is the third of three validation verbs the library contributes, and they
answer different questions:

  * ``validate_examples``   — is this SQL still *allowed* and *plannable*?
  * ``check_example_answers`` — does it still return the *right number*?
  * ``check_schema_drift``  — do the *declarations* still describe reality?

The gap the third one fills is narrow and easy to miss. A live ``EXPLAIN`` (see
``verify_examples.py``) catches a renamed column the moment some SQL references
it. A column that is *declared and never queried* is invisible to that: the
documentation goes stale, the agent reads it, and nothing anywhere disagrees.

Gate on ``report.ok``, not ``report.has_drift``
----------------------------------------------
An unresolved ``*`` wildcard, an adapter that raised, a semantic source that
could not be read — none of these are drift, and none of them are a pass. They
land in ``report.unchecked``, and ``ok`` is False whenever anything is there. A
connection failure that reads as a clean bill of health is the exact silent
success this whole feature exists to eliminate.
"""

from __future__ import annotations

import sys
from pathlib import Path

from agentic_data_contracts import DataContract
from agentic_data_contracts.adapters.duckdb import DuckDBAdapter
from agentic_data_contracts.semantic.yaml_source import YamlSource
from agentic_data_contracts.validation import check_schema_drift

HERE = Path(__file__).parent


def main() -> int:
    contract = DataContract.from_yaml(HERE / "contract.yml")
    adapter = DuckDBAdapter(str(HERE / "sample_data.duckdb"))
    source = YamlSource(str(HERE / "semantic.yml"))

    print("=== Contract vs. live schema ===")
    report = check_schema_drift(contract, adapter, source)
    print(report.summary())
    print(f"ok: {report.ok}")

    # A clean report proves nothing unless you have seen the check fail. This is
    # the same defect the library was written for, reproduced against this
    # contract: the loader that built a table once emitted `column0/column1/...`
    # instead of real names, the declarations outlived the fix, and only a human
    # reading a diff caught it.
    print("\n=== The same check, against a stale declaration ===")
    stale = _with_phantom_columns(HERE / "semantic.yml")
    broken = check_schema_drift(contract, adapter, YamlSource.from_raw(stale))
    print(broken.summary())
    print(f"ok: {broken.ok}")

    # The agent-facing half of the same reconciliation: `describe_table` holds
    # the declarations and the live schema in one call, so it says when the
    # documentation it just served disagrees with the warehouse.
    print("\n=== What the agent sees (describe_table note) ===")
    print(_describe_note(contract, adapter, stale))

    return 0 if report.ok else 1


def _with_phantom_columns(path: Path) -> dict[str, object]:
    """The semantic document with two columns that no longer exist."""
    import yaml

    raw = yaml.safe_load(path.read_text())
    orders = next(t for t in raw["tables"] if t["table"] == "orders")
    orders["columns"] = [
        {"name": "revenue", "type": "DECIMAL(10,2)", "description": "PHANTOM"},
        {"name": "order_date", "type": "DATE", "description": "PHANTOM"},
        *orders["columns"],
    ]
    return raw


def _describe_note(
    contract: DataContract, adapter: DuckDBAdapter, raw: dict[str, object]
) -> str:
    import asyncio
    import json

    from agentic_data_contracts import create_tools

    tools = create_tools(
        contract, adapter=adapter, semantic_source=YamlSource.from_raw(raw)
    )
    describe = next(t for t in tools if t.name == "describe_table")
    result = asyncio.run(describe.callable({"schema": "analytics", "table": "orders"}))
    payload = json.loads(result["content"][0]["text"])
    return payload.get("note", "(no note — declarations agree with the warehouse)")


if __name__ == "__main__":
    sys.exit(main())
