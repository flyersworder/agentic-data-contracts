"""run_query tells the agent's own failed SQL apart from a contract refusal (#134).

Both spend the one ``max_retries`` budget, but an execution error is worded
``ERROR —`` rather than ``BLOCKED —`` and counted in ``session.execution_errors``,
while every contract refusal is counted in ``session.blocks``. The failing SQL
below passes Layer 1 and the EXPLAIN dry-run and fails only when executed, like
the casts of JSON text to a number that surfaced the bug.
"""

from typing import Any

from agentic_data_contracts.adapters.duckdb import DuckDBAdapter
from agentic_data_contracts.core.contract import DataContract
from agentic_data_contracts.core.schema import (
    AllowedTable,
    DataContractSchema,
    Enforcement,
    ResourceConfig,
    ResultCheck,
    SemanticConfig,
    SemanticRule,
)
from agentic_data_contracts.core.session import ContractSession
from agentic_data_contracts.tools.factory import create_tools

FAILING_CAST = (
    "SELECT CAST(CAST(id AS VARCHAR) || 'x' AS INTEGER) AS n FROM analytics.orders"
)


def _contract(*, max_retries: int = 5, rules: list[SemanticRule] | None = None):
    return DataContract(
        DataContractSchema(
            name="test",
            semantic=SemanticConfig(
                allowed_tables=[
                    AllowedTable.model_validate(
                        {"schema": "analytics", "tables": ["orders"]}
                    )
                ],
                rules=rules or [],
            ),
            resources=ResourceConfig(max_retries=max_retries),
        )
    )


def _adapter() -> DuckDBAdapter:
    db = DuckDBAdapter(":memory:")
    db.connection.execute(
        """
        CREATE SCHEMA analytics;
        CREATE TABLE analytics.orders AS
            SELECT range AS id, range - 2 AS amount FROM range(5);
        """
    )
    return db


def _run_query(dc: DataContract, session: ContractSession) -> Any:
    tools = create_tools(dc, adapter=_adapter(), session=session)
    return next(t for t in tools if t.name == "run_query").callable


def _text(result: dict[str, Any]) -> str:
    return result["content"][0]["text"]


async def test_execution_error_is_worded_as_an_error() -> None:
    dc = _contract()
    session = ContractSession(dc)
    result = await _run_query(dc, session)({"sql": FAILING_CAST})

    text = _text(result)
    assert text.startswith("ERROR — Query execution failed:")
    assert "Conversion Error" in text
    assert "BLOCKED" not in text
    assert "Remaining:" in text
    assert result["is_error"] is True
    assert result["_kind"] == "error"


async def test_execution_error_is_counted_apart_from_blocks() -> None:
    dc = _contract()
    session = ContractSession(dc)
    await _run_query(dc, session)({"sql": FAILING_CAST})

    assert session.execution_errors == 1
    assert session.blocks == 0
    assert session.retries == 1


async def test_execution_errors_spend_the_retry_budget() -> None:
    dc = _contract(max_retries=2)
    session = ContractSession(dc)
    run_query = _run_query(dc, session)
    await run_query({"sql": FAILING_CAST})
    await run_query({"sql": FAILING_CAST})

    result = await run_query({"sql": "SELECT id FROM analytics.orders"})
    assert _text(result).startswith("BLOCKED — Session limit exceeded")


async def test_validation_block_is_counted_as_a_block() -> None:
    dc = _contract()
    session = ContractSession(dc)
    result = await _run_query(dc, session)({"sql": "SELECT id FROM analytics.secrets"})

    assert _text(result).startswith("BLOCKED —")
    assert (session.blocks, session.execution_errors, session.retries) == (1, 0, 1)


async def test_result_check_block_is_counted_as_a_block() -> None:
    rule = SemanticRule(
        name="no_negative",
        description="No negative amounts",
        enforcement=Enforcement.BLOCK,
        result_check=ResultCheck(column="amount", min_value=0),
    )
    dc = _contract(rules=[rule])
    session = ContractSession(dc)
    result = await _run_query(dc, session)(
        {"sql": "SELECT id, amount FROM analytics.orders"}
    )

    assert _text(result).startswith("BLOCKED — Result check violations")
    assert (session.blocks, session.execution_errors, session.retries) == (1, 0, 1)


async def test_database_rejecting_the_sql_at_explain_is_an_execution_error() -> None:
    # A missing column fails the EXPLAIN dry-run, before execution. The database
    # refused the SQL, not the contract: the validator runs EXPLAIN only once
    # every policy check has passed.
    dc = _contract()
    session = ContractSession(dc)
    result = await _run_query(dc, session)(
        {"sql": "SELECT nosuchcol FROM analytics.orders"}
    )

    text = _text(result)
    assert text.startswith("ERROR — Schema validation failed:")
    assert "nosuchcol" in text
    assert "Remaining:" in text
    assert result["_kind"] == "error"
    assert (session.blocks, session.execution_errors, session.retries) == (0, 1, 1)


async def test_unparseable_sql_stays_a_block() -> None:
    # Fail-closed: the contract cannot check SQL it cannot read, and the
    # database might run it, so refusing it is the contract's decision.
    dc = _contract()
    session = ContractSession(dc)
    result = await _run_query(dc, session)({"sql": "SELEC id FROM analytics.orders"})

    assert _text(result).startswith("BLOCKED —")
    assert result["_kind"] == "blocked"
    assert (session.blocks, session.execution_errors, session.retries) == (1, 0, 1)


async def test_middleware_counts_a_database_rejection_as_an_execution_error() -> None:
    from agentic_data_contracts.tools.middleware import contract_middleware

    dc = _contract()
    session = ContractSession(dc)

    @contract_middleware(dc, adapter=_adapter(), session=session)
    async def my_query(args: dict) -> dict:  # pragma: no cover - never reached
        return {"content": [{"type": "text", "text": "ran"}]}

    result = await my_query({"sql": "SELECT nosuchcol FROM analytics.orders"})

    assert _text(result).startswith("ERROR — Schema validation failed:")
    assert result["_kind"] == "error"
    assert (session.blocks, session.execution_errors) == (0, 1)
