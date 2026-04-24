import contextvars
import json
from pathlib import Path

import pytest

from agentic_data_contracts.adapters.duckdb import DuckDBAdapter
from agentic_data_contracts.core.contract import DataContract
from agentic_data_contracts.tools.factory import create_tools


@pytest.fixture
def contract(fixtures_dir: Path) -> DataContract:
    return DataContract.from_yaml(fixtures_dir / "principals_contract.yml")


@pytest.fixture
def adapter() -> DuckDBAdapter:
    db = DuckDBAdapter(":memory:")
    db.connection.execute(
        """
        CREATE SCHEMA IF NOT EXISTS analytics;
        CREATE SCHEMA IF NOT EXISTS hr;
        CREATE SCHEMA IF NOT EXISTS raw;
        CREATE TABLE analytics.orders (id INTEGER);
        INSERT INTO analytics.orders VALUES (1);
        CREATE TABLE hr.salaries (id INTEGER, salary DECIMAL(10,2));
        INSERT INTO hr.salaries VALUES (1, 100000.00);
        CREATE TABLE raw.audit_log (id INTEGER);
        INSERT INTO raw.audit_log VALUES (1);
        """
    )
    return db


@pytest.mark.asyncio
async def test_webex_room_multiple_users_one_tool_instance(
    contract: DataContract, adapter: DuckDBAdapter
) -> None:
    """Simulates a Webex room: one long-lived bot, many users, identity per message.

    This is the canonical scenario the callable-principal design was built for.
    One `create_tools()` call; identity flipped via contextvars between messages;
    per-user access rules apply correctly.
    """
    current_sender: contextvars.ContextVar[str | None] = contextvars.ContextVar(
        "current_sender", default=None
    )

    tools = create_tools(
        contract,
        adapter=adapter,
        caller_principal=lambda: current_sender.get(),
    )
    inspect = next(t for t in tools if t.name == "inspect_query").callable

    # Message 1: alice asks about hr.salaries → allowed.
    current_sender.set("alice@co.com")
    body = json.loads(
        (await inspect({"sql": "SELECT salary FROM hr.salaries"}))["content"][0]["text"]
    )
    assert body["valid"] is True, body

    # Message 2: bob asks the same thing → blocked, with bob in message.
    current_sender.set("bob@co.com")
    body = json.loads(
        (await inspect({"sql": "SELECT salary FROM hr.salaries"}))["content"][0]["text"]
    )
    assert body["valid"] is False
    assert any("caller: 'bob@co.com'" in v for v in body["violations"])

    # Message 3: intern asks about raw.audit_log → blocked (blocklist hit).
    current_sender.set("intern@co.com")
    body = json.loads(
        (await inspect({"sql": "SELECT id FROM raw.audit_log"}))["content"][0]["text"]
    )
    assert body["valid"] is False
    assert any("caller: 'intern@co.com'" in v for v in body["violations"])

    # Message 4: alice again, audit_log → allowed (not in blocklist).
    current_sender.set("alice@co.com")
    body = json.loads(
        (await inspect({"sql": "SELECT id FROM raw.audit_log"}))["content"][0]["text"]
    )
    assert body["valid"] is True, body

    # Message 5: nobody set — should fail closed on restricted tables.
    current_sender.set(None)
    body = json.loads(
        (await inspect({"sql": "SELECT salary FROM hr.salaries"}))["content"][0]["text"]
    )
    assert body["valid"] is False
    assert any("<no caller identified>" in v for v in body["violations"])

    # Open table: works regardless of identity.
    for sender in ["alice@co.com", "bob@co.com", "intern@co.com", None]:
        current_sender.set(sender)
        body = json.loads(
            (await inspect({"sql": "SELECT id FROM analytics.orders"}))["content"][0][
                "text"
            ]
        )
        assert body["valid"] is True, (sender, body)
