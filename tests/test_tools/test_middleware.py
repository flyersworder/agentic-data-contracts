import threading
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


@pytest.mark.asyncio
async def test_middleware_offloads_validate_off_event_loop(
    contract: DataContract, adapter: DuckDBAdapter
) -> None:
    """The async wrapper must run the blocking EXPLAIN dry-run (inside
    validator.validate) on a worker thread, not the event-loop thread."""
    seen: dict[str, int] = {}
    original_explain = adapter.explain

    def tracking_explain(sql: str):  # type: ignore[no-untyped-def]
        seen["explain"] = threading.get_ident()
        return original_explain(sql)

    setattr(adapter, "explain", tracking_explain)

    @contract_middleware(contract, adapter=adapter)
    async def my_query(args: dict) -> dict:
        return {"content": [{"type": "text", "text": "ok"}]}

    await my_query(
        {"sql": "SELECT id, amount FROM analytics.orders WHERE tenant_id = 'acme'"}
    )

    assert seen["explain"] != threading.get_ident(), (
        "EXPLAIN ran on the event-loop thread"
    )


def test_contract_middleware_warns_that_token_budget_is_inert(
    caplog: pytest.LogCaptureFixture, adapter: DuckDBAdapter
) -> None:
    """This wrapper receives an args dict only, same as the SDK path.

    It is exported from the package root, so a declared budget going
    unenforced here needs to be as loud as it is on the other blind paths.
    """
    import logging

    from agentic_data_contracts.core.schema import (
        AllowedTable,
        DataContractSchema,
        ResourceConfig,
        SemanticConfig,
    )

    budgeted = DataContract(
        DataContractSchema(
            name="budgeted",
            semantic=SemanticConfig(
                allowed_tables=[
                    AllowedTable.model_validate(
                        {"schema": "analytics", "tables": ["orders"]}
                    ),
                ],
            ),
            resources=ResourceConfig(token_budget=50_000),
        )
    )
    with caplog.at_level(logging.WARNING):
        contract_middleware(budgeted, adapter=adapter)
    assert "token_budget" in caplog.text
    assert "NOT be enforced" in caplog.text
