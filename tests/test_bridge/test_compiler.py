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
    # valid_contract.yml declares a dbt manifest the fixtures do not ship, so
    # the tests below that are not about the semantic source pass
    # semantic_source=None, the explicit no-source opt-out.
    return DataContract.from_yaml(fixtures_dir / "valid_contract.yml")


def _loadable(fixtures_dir: Path) -> DataContract:
    return DataContract.from_yaml_string(
        f"""
version: "1.0"
name: loadable
semantic:
  source:
    type: yaml
    path: {fixtures_dir / "semantic_source.yml"}
  allowed_tables:
    - schema: analytics
      tables: [orders]
  domains:
    - name: revenue
      summary: Revenue
      description: Revenue is recognized at fulfillment.
"""
    )


def test_compile_loads_the_configured_source_by_default(fixtures_dir: Path) -> None:
    # Same default as to_system_prompt(): the instructions name the metrics.
    contract = compile_to_contract(_loadable(fixtures_dir))
    assert contract.capabilities is not None
    instructions = contract.capabilities.instructions
    assert instructions is not None
    assert '<metric name="total_revenue" />' in instructions


def test_compile_explicit_none_renders_without_a_source(fixtures_dir: Path) -> None:
    contract = compile_to_contract(_loadable(fixtures_dir), semantic_source=None)
    assert contract.capabilities is not None
    instructions = contract.capabilities.instructions
    assert instructions is not None
    assert "<metric name=" not in instructions


def test_compile_unloadable_source_fails_closed(dc: DataContract) -> None:
    from agentic_data_contracts.core.contract import SemanticSourceUnavailableError

    with pytest.raises(SemanticSourceUnavailableError):
        compile_to_contract(dc)


def test_compile_returns_contract(dc: DataContract) -> None:
    contract = compile_to_contract(dc, semantic_source=None)
    assert isinstance(contract, Contract)
    assert contract.name == "revenue-analysis"


def test_compile_resources(dc: DataContract) -> None:
    contract = compile_to_contract(dc, semantic_source=None)
    assert contract.resources.cost_usd == 5.00
    assert contract.resources.tokens == 50000
    assert contract.resources.iterations == 3


def test_compile_temporal(dc: DataContract) -> None:
    contract = compile_to_contract(dc, semantic_source=None)
    assert contract.temporal.max_duration is not None
    assert contract.temporal.max_duration.total_seconds() == 300


def test_compile_block_rules_become_termination_conditions(dc: DataContract) -> None:
    contract = compile_to_contract(dc, semantic_source=None)
    assert len(contract.termination_conditions) >= 2
    types = [tc.type for tc in contract.termination_conditions]
    assert "contract_rule_violation" in types


def test_compile_warn_rules_become_success_criteria(dc: DataContract) -> None:
    contract = compile_to_contract(dc, semantic_source=None)
    warn_criteria = [
        sc for sc in contract.success_criteria if sc.name == "use_approved_metrics"
    ]
    assert len(warn_criteria) == 1


def test_compile_success_criteria_weights(dc: DataContract) -> None:
    contract = compile_to_contract(dc, semantic_source=None)
    named = {sc.name: sc for sc in contract.success_criteria}
    assert "query_uses_semantic_definitions" in named
    assert named["query_uses_semantic_definitions"].weight == pytest.approx(0.4)


def test_compile_capabilities_instructions(dc: DataContract) -> None:
    contract = compile_to_contract(dc, semantic_source=None)
    assert contract.capabilities is not None
    assert contract.capabilities.instructions is not None
    assert "analytics.orders" in contract.capabilities.instructions


def test_compile_metadata(dc: DataContract) -> None:
    contract = compile_to_contract(dc, semantic_source=None)
    assert (
        "source_of_truth" in contract.metadata or "allowed_tables" in contract.metadata
    )


def test_compile_omits_source_of_truth_for_frozen_contract(fixtures_dir: Path) -> None:
    """A frozen contract carries its semantics inline (path=None); the compiler
    must omit source_of_truth rather than write a literal None pointer."""
    dc = DataContract.from_yaml(fixtures_dir / "roundtrip_contract.yml")
    dc.freeze_semantic_source()
    contract = compile_to_contract(dc, semantic_source=None)
    assert "source_of_truth" not in contract.metadata
