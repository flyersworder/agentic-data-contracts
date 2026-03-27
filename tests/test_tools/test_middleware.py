from pathlib import Path

import pytest

from agentic_data_contracts.adapters.duckdb import DuckDBAdapter
from agentic_data_contracts.core.contract import DataContract
from agentic_data_contracts.core.session import ContractSession
from agentic_data_contracts.tools.middleware import contract_middleware


@pytest.fixture
def contract(fixtures_dir: Path) -> DataContract:
    return DataContract.from_yaml(fixtures_dir / "valid_contract.yml")


@pytest.fixture
def adapter() -> DuckDBAdapter:
    db = DuckDBAdapter(":memory:")
    db.connection.execute(
        """
        CREATE SCHEMA IF NOT EXISTS analytics;
        CREATE TABLE analytics.orders (
            id INTEGER, amount DECIMAL(10,2), tenant_id VARCHAR
        );
        INSERT INTO analytics.orders VALUES (1, 100.00, 'acme');
        """
    )
    return db


@pytest.mark.asyncio
async def test_middleware_allows_valid_query(
    contract: DataContract, adapter: DuckDBAdapter
) -> None:
    @contract_middleware(contract, adapter=adapter)
    async def my_query(args: dict) -> dict:
        result = adapter.execute(args["sql"])
        rows = [dict(zip(result.columns, row)) for row in result.rows]
        return {"content": [{"type": "text", "text": str(rows)}]}

    result = await my_query(
        {"sql": "SELECT id, amount FROM analytics.orders WHERE tenant_id = 'acme'"}
    )
    text = result["content"][0]["text"]
    assert "100" in text


@pytest.mark.asyncio
async def test_middleware_blocks_invalid_query(
    contract: DataContract, adapter: DuckDBAdapter
) -> None:
    @contract_middleware(contract, adapter=adapter)
    async def my_query(args: dict) -> dict:
        return {"content": [{"type": "text", "text": "should not reach here"}]}

    result = await my_query({"sql": "SELECT * FROM analytics.orders"})
    text = result["content"][0]["text"]
    assert "BLOCKED" in text
    assert "should not reach here" not in text


@pytest.mark.asyncio
async def test_middleware_tracks_session(
    contract: DataContract, adapter: DuckDBAdapter
) -> None:
    session = ContractSession(contract)

    @contract_middleware(contract, adapter=adapter, session=session)
    async def my_query(args: dict) -> dict:
        return {"content": [{"type": "text", "text": "ok"}]}

    await my_query({"sql": "DELETE FROM analytics.orders"})
    assert session.retries == 1


@pytest.mark.asyncio
async def test_middleware_checks_session_limits(
    contract: DataContract, adapter: DuckDBAdapter
) -> None:
    session = ContractSession(contract)
    session.record_retry()
    session.record_retry()
    session.record_retry()

    @contract_middleware(contract, adapter=adapter, session=session)
    async def my_query(args: dict) -> dict:
        return {"content": [{"type": "text", "text": "ok"}]}

    result = await my_query(
        {"sql": "SELECT id FROM analytics.orders WHERE tenant_id = 'x'"}
    )
    text = result["content"][0]["text"]
    assert "limit" in text.lower() or "exceeded" in text.lower()
