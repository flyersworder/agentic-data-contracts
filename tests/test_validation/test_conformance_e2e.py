"""End-to-end proof that the conformance evaluator's pieces compose.

Tiers 1 and 2 test the verdicts and the recorder separately (unit-level, with
hand-built ``Attempt``/``ToolCall`` fixtures). This module proves they compose
in the real path: real contract tools (``create_tools``), a real
``ToolRecorder``, a real ``ContractSession``, and real verdicts
(``evaluate_conformance``) -- driven by a plain scripted async function instead
of a model, so the tests are deterministic and free.
"""

from pathlib import Path

import pytest

from agentic_data_contracts.adapters.duckdb import DuckDBAdapter
from agentic_data_contracts.core.contract import DataContract
from agentic_data_contracts.core.recorder import ToolRecorder
from agentic_data_contracts.core.session import ContractSession
from agentic_data_contracts.semantic.yaml_source import YamlSource
from agentic_data_contracts.tools.factory import create_tools
from agentic_data_contracts.validation import Attempt, evaluate_conformance
from agentic_data_contracts.validation.examples import VerifiedExample


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
            id INTEGER, amount DECIMAL(10,2), tenant_id VARCHAR, created_at DATE
        );
        INSERT INTO analytics.orders VALUES
            (1, 100.00, 'acme', DATE '2026-01-05'),
            (2, 200.00, 'acme', DATE '2026-01-06');
        """
    )
    return db


@pytest.fixture
def semantic(fixtures_dir: Path) -> YamlSource:
    return YamlSource(fixtures_dir / "semantic_source.yml")


def _run(contract, adapter, semantic, recorder):
    """Build a real session + tools wired to a real recorder.

    ``semantic_source`` is passed explicitly: ``valid_contract.yml`` points its
    semantic source at a dbt manifest path that does not resolve, so relying on
    auto-load would silently turn every metric lookup into an error.
    """
    session = ContractSession(contract, recorder=recorder)
    tools = {
        t.name: t.callable
        for t in create_tools(
            contract, adapter=adapter, semantic_source=semantic, session=session
        )
    }
    return session, tools


GOOD_SQL = "SELECT COUNT(id) FROM analytics.orders WHERE tenant_id = 'acme'"


@pytest.mark.asyncio
async def test_a_compliant_scripted_agent_passes(contract, adapter, semantic):
    """Lookup the governed metric, then run the query it was invited to run.

    A right answer reached the right way must pass cleanly.
    """
    example = VerifiedExample(
        sql=GOOD_SQL,
        question="How many orders does acme have?",
        id="orders-count",
        expected=2.0,
        expects_metrics=["total_revenue"],
    )
    rec = ToolRecorder()
    session, tools = _run(contract, adapter, semantic, rec)

    await tools["lookup_metric"]({"metric_name": "total_revenue"})
    await tools["run_query"]({"sql": example.sql})

    report = evaluate_conformance([Attempt.from_session(example, session)])
    assert report.ok is True


@pytest.mark.asyncio
async def test_skipping_the_declared_lookup_is_violated(contract, adapter, semantic):
    """Same right answer, but never consulting the declared metric definition.

    The number still matches -- that is the whole point: a correct answer
    reached without the governed lookup still fails the protocol axis.
    """
    example = VerifiedExample(
        sql=GOOD_SQL,
        question="How many orders does acme have?",
        expected=2.0,
        expects_metrics=["total_revenue"],
    )
    rec = ToolRecorder()
    session, tools = _run(contract, adapter, semantic, rec)

    await tools["run_query"]({"sql": example.sql})

    report = evaluate_conformance([Attempt.from_session(example, session)])
    assert report.ok is False
    assert len(report.protocol_failures) == 1


@pytest.mark.asyncio
async def test_an_out_of_band_answer_is_contaminated(contract, adapter, semantic):
    """No tool calls at all; the attempt just declares a final answer."""
    example = VerifiedExample(sql="SELECT 1", question="q", expected=2.0)
    rec = ToolRecorder()
    session, _ = _run(contract, adapter, semantic, rec)

    report = evaluate_conformance(
        [Attempt.from_session(example, session, final_answer=3.0)]
    )
    assert len(report.contaminated) == 1
    assert report.ok is False


@pytest.mark.asyncio
async def test_a_rerun_after_a_blocked_attempt_still_passes(
    contract, adapter, semantic
):
    """Friction is recorded; it must not fail the gate on its own.

    The agent runs a blocked query (SELECT * is forbidden by no_select_star),
    then the good query, then reruns the SAME good query again. The identical
    rerun must cluster with the first success rather than create ambiguity.
    """
    example = VerifiedExample(
        sql=GOOD_SQL,
        question="q",
        expected=2.0,
    )
    rec = ToolRecorder()
    session, tools = _run(contract, adapter, semantic, rec)

    await tools["run_query"]({"sql": "SELECT * FROM analytics.orders"})  # blocked
    await tools["run_query"]({"sql": example.sql})
    await tools["run_query"]({"sql": example.sql})  # identical rerun

    report = evaluate_conformance([Attempt.from_session(example, session)])
    assert report.ok is True
    assert report.results[0].answer_source == "sole_scalar"
    assert any("blocked" in r for r in report.results[0].reasons)
