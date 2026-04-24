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
        CREATE SCHEMA IF NOT EXISTS sealed;
        CREATE TABLE analytics.orders (id INTEGER, amount DECIMAL(10,2));
        INSERT INTO analytics.orders VALUES (1, 10.00);
        CREATE TABLE hr.salaries (id INTEGER, salary DECIMAL(10,2));
        INSERT INTO hr.salaries VALUES (1, 100000.00);
        CREATE TABLE raw.audit_log (id INTEGER, event VARCHAR);
        INSERT INTO raw.audit_log VALUES (1, 'login');
        CREATE TABLE sealed.top_secret (id INTEGER, payload VARCHAR);
        INSERT INTO sealed.top_secret VALUES (1, 'classified');
        """
    )
    return db


def _tool(tools: list, name: str):
    return next(t for t in tools if t.name == name).callable


@pytest.mark.asyncio
class TestInspectQueryForwarding:
    async def test_alice_inspect_passes(
        self, contract: DataContract, adapter: DuckDBAdapter
    ) -> None:
        tools = create_tools(contract, adapter=adapter, caller_principal="alice@co.com")
        inspect = _tool(tools, "inspect_query")
        body = json.loads(
            (await inspect({"sql": "SELECT salary FROM hr.salaries"}))["content"][0][
                "text"
            ]
        )
        assert body["valid"] is True

    async def test_bob_inspect_blocks_with_caller_in_message(
        self, contract: DataContract, adapter: DuckDBAdapter
    ) -> None:
        tools = create_tools(contract, adapter=adapter, caller_principal="bob@co.com")
        inspect = _tool(tools, "inspect_query")
        body = json.loads(
            (await inspect({"sql": "SELECT salary FROM hr.salaries"}))["content"][0][
                "text"
            ]
        )
        assert body["valid"] is False
        assert any("caller: 'bob@co.com'" in v for v in body["violations"])


@pytest.mark.asyncio
class TestDescribeTable:
    async def test_allowed_principal_succeeds(
        self, contract: DataContract, adapter: DuckDBAdapter
    ) -> None:
        tools = create_tools(contract, adapter=adapter, caller_principal="alice@co.com")
        describe = _tool(tools, "describe_table")
        text = (await describe({"schema": "hr", "table": "salaries"}))["content"][0][
            "text"
        ]
        body = json.loads(text)
        assert body["schema"] == "hr"
        assert body["table"] == "salaries"

    async def test_restricted_for_other_principal(
        self, contract: DataContract, adapter: DuckDBAdapter
    ) -> None:
        tools = create_tools(contract, adapter=adapter, caller_principal="bob@co.com")
        describe = _tool(tools, "describe_table")
        text = (await describe({"schema": "hr", "table": "salaries"}))["content"][0][
            "text"
        ]
        assert "restricted" in text
        assert "caller: 'bob@co.com'" in text

    async def test_restricted_for_unidentified(
        self, contract: DataContract, adapter: DuckDBAdapter
    ) -> None:
        tools = create_tools(contract, adapter=adapter)
        describe = _tool(tools, "describe_table")
        text = (await describe({"schema": "hr", "table": "salaries"}))["content"][0][
            "text"
        ]
        assert "caller: '<no caller identified>'" in text

    async def test_undeclared_table_unchanged_message(
        self, contract: DataContract, adapter: DuckDBAdapter
    ) -> None:
        tools = create_tools(contract, adapter=adapter, caller_principal="alice@co.com")
        describe = _tool(tools, "describe_table")
        text = (await describe({"schema": "nope", "table": "nothing"}))["content"][0][
            "text"
        ]
        assert "not in the allowed tables list" in text


@pytest.mark.asyncio
class TestPreviewTable:
    async def test_allowed_principal_succeeds(
        self, contract: DataContract, adapter: DuckDBAdapter
    ) -> None:
        tools = create_tools(contract, adapter=adapter, caller_principal="alice@co.com")
        preview = _tool(tools, "preview_table")
        text = (await preview({"schema": "hr", "table": "salaries"}))["content"][0][
            "text"
        ]
        body = json.loads(text)
        # DuckDB returns Decimal; json.dumps(..., default=str) renders it as a string.
        assert body["rows"][0]["salary"] == "100000.00"

    async def test_restricted_for_other_principal(
        self, contract: DataContract, adapter: DuckDBAdapter
    ) -> None:
        tools = create_tools(contract, adapter=adapter, caller_principal="bob@co.com")
        preview = _tool(tools, "preview_table")
        text = (await preview({"schema": "hr", "table": "salaries"}))["content"][0][
            "text"
        ]
        assert "restricted" in text
        assert "caller: 'bob@co.com'" in text


@pytest.mark.asyncio
class TestSemanticToolsUnaffected:
    """Explicit negative tests: metric/domain tools ignore caller_principal."""

    async def test_list_metrics_unaffected(
        self, contract: DataContract, adapter: DuckDBAdapter
    ) -> None:
        # Same call, different principals → same output.
        for principal in ["alice@co.com", "bob@co.com", None]:
            tools = create_tools(contract, adapter=adapter, caller_principal=principal)
            list_metrics = _tool(tools, "list_metrics")
            text = (await list_metrics({}))["content"][0]["text"]
            # principals_contract.yml has no semantic source → this exact reply.
            assert text == "No semantic source configured."


def test_create_tools_accepts_callable_principal(
    contract: DataContract, adapter: DuckDBAdapter
) -> None:
    # Must accept a zero-arg callable (Webex pattern) without raising.
    tools = create_tools(
        contract, adapter=adapter, caller_principal=lambda: "alice@co.com"
    )
    assert len(tools) == 9
