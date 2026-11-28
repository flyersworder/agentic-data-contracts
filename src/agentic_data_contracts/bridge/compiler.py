"""Bridge layer — compiles DataContract to ai-agent-contracts Contract."""

from __future__ import annotations

from datetime import timedelta
from typing import TYPE_CHECKING

from agent_contracts import (
    Capabilities,
    Contract,
    ResourceConstraints,
    SuccessCriterion,
    TemporalConstraints,
    TerminationCondition,
)

from agentic_data_contracts.core.contract import (
    _LOAD_FROM_CONFIG,
    DataContract,
    _Default,
)

if TYPE_CHECKING:
    from agentic_data_contracts.semantic.base import SemanticSource


def compile_to_contract(
    dc: DataContract,
    *,
    semantic_source: SemanticSource | None | _Default = _LOAD_FROM_CONFIG,
) -> Contract:
    """Compile a DataContract into an ai-agent-contracts Contract.

    ``semantic_source`` is passed to ``DataContract.to_system_prompt`` for the
    contract's instructions, with the same default: left out, the contract's
    configured source is loaded (and an unloadable declared source raises
    ``SemanticSourceUnavailableError``); ``None`` renders without one.
    """

    res = dc.schema.resources
    resources = ResourceConstraints(
        cost_usd=res.cost_limit_usd if res else None,
        tokens=res.token_budget if res else None,
        iterations=res.max_retries if res else None,
    )

    temporal_cfg = dc.schema.temporal
    temporal = TemporalConstraints(
        max_duration=(
            timedelta(seconds=temporal_cfg.max_duration_seconds)
            if temporal_cfg and temporal_cfg.max_duration_seconds
            else None
        ),
    )

    termination_conditions: list[TerminationCondition] = []
    for rule in dc.block_rules():
        termination_conditions.append(
            TerminationCondition(
                type="contract_rule_violation",
                condition=f"Rule '{rule.name}': {rule.description}",
                priority=1,
            )
        )

    success_criteria: list[SuccessCriterion] = []
    for rule in dc.warn_rules():
        success_criteria.append(
            SuccessCriterion(
                name=rule.name,
                condition=rule.description,
                weight=0.3,
                required=False,
            )
        )
    for sc in dc.schema.success_criteria:
        success_criteria.append(
            SuccessCriterion(
                name=sc.name,
                condition=sc.name,
                weight=sc.weight,
                required=False,
            )
        )

    instructions = dc.to_system_prompt(semantic_source)
    capabilities = Capabilities(instructions=instructions)

    metadata: dict[str, object] = {
        "allowed_tables": dc.allowed_table_names(),
        "forbidden_operations": dc.schema.semantic.forbidden_operations,
    }
    # A frozen contract carries its semantics inline (path=None); omit
    # source_of_truth rather than write a literal None pointer.
    if dc.schema.semantic.source and dc.schema.semantic.source.path is not None:
        metadata["source_of_truth"] = dc.schema.semantic.source.path

    for rule in dc.log_rules():
        metadata[f"log_rule_{rule.name}"] = rule.description

    return Contract(
        id=f"data-contract-{dc.name}",
        name=dc.name,
        resources=resources,
        temporal=temporal,
        capabilities=capabilities,
        termination_conditions=termination_conditions,
        success_criteria=success_criteria,
        metadata=metadata,
    )
