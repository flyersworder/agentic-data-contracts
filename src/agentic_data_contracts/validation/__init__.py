from agentic_data_contracts.core.recorder import ToolCall, ToolRecorder
from agentic_data_contracts.validation.attribution import (
    INTERACTION_KEY,
    AttributionResult,
    attribute_change,
    check_attribution,
)
from agentic_data_contracts.validation.checkers import (
    ENFORCEABLE_OPERATIONS,
    BlockedColumnsChecker,
    CheckResult,
    MaxJoinsChecker,
    NoSelectStarChecker,
    OperationBlocklistChecker,
    RelationshipChecker,
    RequiredFilterChecker,
    RequireLimitChecker,
    ResultCheckRunner,
    TableAllowlistChecker,
    extract_tables,
)
from agentic_data_contracts.validation.conformance import (
    Attempt,
    ConformanceReport,
    ConformanceResult,
    evaluate_conformance,
)
from agentic_data_contracts.validation.drift import (
    SchemaDrift,
    SchemaDriftReport,
    UncheckedTable,
    check_schema_drift,
)
from agentic_data_contracts.validation.examples import (
    ExampleAnswerReport,
    ExampleAnswerResult,
    ExampleResult,
    ExampleValidationReport,
    VerifiedExample,
    check_example_answers,
    validate_examples,
)
from agentic_data_contracts.validation.explain import ExplainAdapter, ExplainResult
from agentic_data_contracts.validation.reconciliation import (
    ReconciliationResult,
    reconcile_decomposition,
)
from agentic_data_contracts.validation.validator import (
    Checker,
    ValidationResult,
    Validator,
)

__all__ = [
    "Attempt",
    "AttributionResult",
    "BlockedColumnsChecker",
    "CheckResult",
    "Checker",
    "ConformanceReport",
    "ConformanceResult",
    "ENFORCEABLE_OPERATIONS",
    "ExampleAnswerReport",
    "ExampleAnswerResult",
    "ExampleResult",
    "ExampleValidationReport",
    "ExplainAdapter",
    "ExplainResult",
    "INTERACTION_KEY",
    "MaxJoinsChecker",
    "NoSelectStarChecker",
    "OperationBlocklistChecker",
    "ReconciliationResult",
    "RelationshipChecker",
    "RequireLimitChecker",
    "RequiredFilterChecker",
    "ResultCheckRunner",
    "SchemaDrift",
    "SchemaDriftReport",
    "SensitivityReport",
    "SensitivityResult",
    "TableAllowlistChecker",
    "ToolCall",
    "ToolRecorder",
    "UncheckedTable",
    "ValidationResult",
    "Validator",
    "VerifiedExample",
    "attribute_change",
    "check_attribution",
    "check_example_answers",
    "check_schema_drift",
    "check_sensitivity",
    "evaluate_conformance",
    "extract_tables",
    "reconcile_decomposition",
    "validate_examples",
    "validate_sensitivity_tables",
]


def __getattr__(name: str) -> object:
    """Lazy load sensitivity module exports to avoid circular imports.

    The sensitivity module imports from semantic.base, which imports from
    adapters.base, which imports from validation.explain. This creates a
    circular dependency when validation/__init__.py is loaded early by
    adapters/base.py. To break the cycle, sensitivity exports are loaded
    on-demand.
    """
    if name in (
        "SensitivityReport",
        "SensitivityResult",
        "check_sensitivity",
        "validate_sensitivity_tables",
    ):
        from agentic_data_contracts.validation.sensitivity import (
            SensitivityReport,
            SensitivityResult,
            check_sensitivity,
            validate_sensitivity_tables,
        )

        _map = {
            "SensitivityReport": SensitivityReport,
            "SensitivityResult": SensitivityResult,
            "check_sensitivity": check_sensitivity,
            "validate_sensitivity_tables": validate_sensitivity_tables,
        }
        return _map[name]
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
