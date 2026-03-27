"""Bridge layer — compiles DataContract to ai-agent-contracts Contract."""

from __future__ import annotations

from datetime import timedelta

from agent_contracts import (
    Capabilities,
    Contract,
    ResourceConstraints,
    SuccessCriterion,
    TemporalConstraints,
    TerminationCondition,
)

from agentic_data_contracts.core.contract import DataContract


def compile_to_contract(dc: DataContract) -> Contract:
    """Compile a DataContract into an ai-agent-contracts Contract."""

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

    instructions = dc.to_system_prompt()
    capabilities = Capabilities(instructions=instructions)

    metadata: dict[str, object] = {
        "allowed_tables": dc.allowed_table_names(),
        "forbidden_operations": dc.schema.semantic.forbidden_operations,
    }
    if dc.schema.semantic.source:
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
