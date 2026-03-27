from pathlib import Path

import pytest

from agentic_data_contracts.core.contract import DataContract

try:
    from agent_contracts import Contract

    from agentic_data_contracts.bridge.compiler import compile_to_contract

    HAS_AGENT_CONTRACTS = True
except ImportError:
    HAS_AGENT_CONTRACTS = False

pytestmark = pytest.mark.skipif(
    not HAS_AGENT_CONTRACTS,
    reason="ai-agent-contracts not installed",
)


@pytest.fixture
def dc(fixtures_dir: Path) -> DataContract:
    return DataContract.from_yaml(fixtures_dir / "valid_contract.yml")


def test_compile_returns_contract(dc: DataContract) -> None:
    contract = compile_to_contract(dc)
    assert isinstance(contract, Contract)
    assert contract.name == "revenue-analysis"


def test_compile_resources(dc: DataContract) -> None:
    contract = compile_to_contract(dc)
    assert contract.resources.cost_usd == 5.00
    assert contract.resources.tokens == 50000
    assert contract.resources.iterations == 3


def test_compile_temporal(dc: DataContract) -> None:
    contract = compile_to_contract(dc)
    assert contract.temporal.max_duration is not None
    assert contract.temporal.max_duration.total_seconds() == 300


def test_compile_block_rules_become_termination_conditions(dc: DataContract) -> None:
    contract = compile_to_contract(dc)
    assert len(contract.termination_conditions) >= 2
    types = [tc.type for tc in contract.termination_conditions]
    assert "contract_rule_violation" in types


def test_compile_warn_rules_become_success_criteria(dc: DataContract) -> None:
    contract = compile_to_contract(dc)
    warn_criteria = [
        sc for sc in contract.success_criteria if sc.name == "use_approved_metrics"
    ]
    assert len(warn_criteria) == 1


def test_compile_success_criteria_weights(dc: DataContract) -> None:
    contract = compile_to_contract(dc)
    named = {sc.name: sc for sc in contract.success_criteria}
    assert "query_uses_semantic_definitions" in named
    assert named["query_uses_semantic_definitions"].weight == pytest.approx(0.4)


def test_compile_capabilities_instructions(dc: DataContract) -> None:
    contract = compile_to_contract(dc)
    assert contract.capabilities is not None
    assert contract.capabilities.instructions is not None
    assert "analytics.orders" in contract.capabilities.instructions


def test_compile_metadata(dc: DataContract) -> None:
    contract = compile_to_contract(dc)
    assert (
        "source_of_truth" in contract.metadata or "allowed_tables" in contract.metadata
    )
