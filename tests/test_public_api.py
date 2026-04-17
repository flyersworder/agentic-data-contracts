"""Tests for public API exports."""


def test_top_level_imports() -> None:
    from agentic_data_contracts import (
        ClaudePromptRenderer,
        DataContract,
        MetricDefinition,
        MetricImpact,
        PromptRenderer,
        Relationship,
        SemanticSource,
        contract_middleware,
        create_tools,
    )

    assert DataContract is not None
    assert create_tools is not None
    assert contract_middleware is not None
    assert PromptRenderer is not None
    assert ClaudePromptRenderer is not None
    assert MetricDefinition is not None
    assert MetricImpact is not None
    assert Relationship is not None
    assert SemanticSource is not None


def test_core_imports() -> None:
    from agentic_data_contracts.core.contract import DataContract
    from agentic_data_contracts.core.schema import DataContractSchema
    from agentic_data_contracts.core.session import ContractSession, LimitExceededError

    assert DataContract is not None
    assert DataContractSchema is not None
    assert ContractSession is not None
    assert LimitExceededError is not None


def test_validation_imports() -> None:
    from agentic_data_contracts.validation.checkers import (  # noqa: I001
        CheckResult,
        NoSelectStarChecker,  # noqa: F401
        OperationBlocklistChecker,  # noqa: F401
        RequiredFilterChecker,  # noqa: F401
        TableAllowlistChecker,  # noqa: F401
    )
    from agentic_data_contracts.validation.explain import ExplainAdapter, ExplainResult  # noqa: F401
    from agentic_data_contracts.validation.validator import ValidationResult, Validator  # noqa: F401
    from agentic_data_contracts.validation import Checker  # noqa: F401

    assert CheckResult is not None
    assert Validator is not None
    assert Checker is not None


def test_adapter_imports() -> None:
    from agentic_data_contracts.adapters.base import (
        Column,
        DatabaseAdapter,
        QueryResult,  # noqa: F401
        TableSchema,  # noqa: F401
    )

    assert DatabaseAdapter is not None
    assert Column is not None


def test_semantic_imports() -> None:
    from agentic_data_contracts.semantic.base import MetricDefinition, SemanticSource  # noqa: F401, I001
    from agentic_data_contracts.semantic.yaml_source import YamlSource

    assert SemanticSource is not None
    assert YamlSource is not None


def test_tools_imports() -> None:
    from agentic_data_contracts.tools.factory import ToolDef, create_tools
    from agentic_data_contracts.tools.middleware import contract_middleware

    assert ToolDef is not None
    assert create_tools is not None
    assert contract_middleware is not None
