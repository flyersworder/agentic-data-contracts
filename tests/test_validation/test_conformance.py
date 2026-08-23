from pathlib import Path

import pytest

from agentic_data_contracts.core.contract import DataContract
from agentic_data_contracts.core.recorder import ToolRecorder
from agentic_data_contracts.core.session import ContractSession
from agentic_data_contracts.validation.conformance import Attempt
from agentic_data_contracts.validation.examples import VerifiedExample


@pytest.fixture
def contract(fixtures_dir: Path) -> DataContract:
    return DataContract.from_yaml(fixtures_dir / "valid_contract.yml")


def _example(**kw):
    return VerifiedExample(sql=kw.pop("sql", "SELECT 1"), **kw)


def test_from_session_captures_the_call_log_and_cost(contract):
    rec = ToolRecorder()
    session = ContractSession(contract, recorder=rec)
    rec.log("run_query", {"sql": "SELECT 1"}, "ok", scalar=5.0)
    session.record_cost(0.02)

    attempt = Attempt.from_session(_example(), session, final_text="five")

    assert [c.tool for c in attempt.calls] == ["run_query"]
    assert attempt.cost_usd == 0.02
    assert attempt.elapsed_seconds >= 0.0
    assert attempt.final_text == "five"


def test_from_session_coerces_foreign_tool_calls_to_a_list(contract):
    session = ContractSession(contract, recorder=ToolRecorder())
    attempt = Attempt.from_session(
        _example(), session, foreign_tool_calls=("mcp__bigquery__execute_sql",)
    )

    assert attempt.foreign_tool_calls == ["mcp__bigquery__execute_sql"]
    attempt.foreign_tool_calls.append("another")  # must not raise


def test_from_session_refuses_a_reused_recorder(contract):
    session = ContractSession(contract, recorder=ToolRecorder())
    Attempt.from_session(_example(), session)

    with pytest.raises(ValueError, match="already consumed"):
        Attempt.from_session(_example(), session)


def test_from_session_requires_a_recorder(contract):
    with pytest.raises(ValueError, match="recorder"):
        Attempt.from_session(_example(), ContractSession(contract))
