"""Tests for public API exports."""


def test_top_level_imports() -> None:
    from agentic_data_contracts import (
        SEMANTIC_KEYS,
        DataContract,
        MetricDefinition,
        MetricImpact,
        PromptRenderer,
        Relationship,
        SemanticSource,
        XmlPromptRenderer,
        contract_middleware,
        create_tools,
    )

    assert DataContract is not None
    assert create_tools is not None
    assert contract_middleware is not None
    assert PromptRenderer is not None
    assert XmlPromptRenderer is not None
    assert MetricDefinition is not None
    assert MetricImpact is not None
    assert Relationship is not None
    assert SemanticSource is not None
    # README and the v0.41.0 CHANGELOG both promise this name at the top level.
    # v0.43.0 added decomposition_convention as interpreted vocabulary.
    assert SEMANTIC_KEYS == {
        "metrics",
        "tables",
        "relationships",
        "metric_impacts",
        "decomposition_convention",
    }


def test_claude_prompt_renderer_is_gone() -> None:
    """v0.39.0 renamed it with no alias — a stale import must fail loudly."""
    import agentic_data_contracts

    assert not hasattr(agentic_data_contracts, "ClaudePromptRenderer")
    assert "ClaudePromptRenderer" not in agentic_data_contracts.__all__


def test_ard_publish_imports() -> None:
    from agentic_data_contracts import (
        SemanticSourceUnavailableError,
        build_ai_catalog,
        build_catalog_entry,
        contract_canonical_bytes,
        contract_digest,
    )

    assert SemanticSourceUnavailableError is not None
    assert build_catalog_entry is not None
    assert build_ai_catalog is not None
    assert contract_digest is not None
    assert contract_canonical_bytes is not None
    # Governance failures must not be swallowed by generic file-error handling.
    assert not issubclass(SemanticSourceUnavailableError, FileNotFoundError)


def test_core_imports() -> None:
    from agentic_data_contracts.core.contract import DataContract
    from agentic_data_contracts.core.schema import DataContractSchema
    from agentic_data_contracts.core.session import (
        ContractSession,
        ContractSessionLimitError,
        LimitExceededError,
    )

    assert DataContract is not None
    assert DataContractSchema is not None
    assert ContractSession is not None
    assert LimitExceededError is not None
    assert ContractSessionLimitError is not None


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
    from agentic_data_contracts.semantic.base import (  # noqa: F401, I001
        ExtensibleSemanticSource,
        MetricDefinition,
        SemanticSource,
    )
    from agentic_data_contracts.semantic.yaml_source import SEMANTIC_KEYS, YamlSource

    assert SemanticSource is not None
    assert ExtensibleSemanticSource is not None
    assert YamlSource is not None
    assert SEMANTIC_KEYS


def test_tools_imports() -> None:
    from agentic_data_contracts.tools.factory import ToolDef, create_tools
    from agentic_data_contracts.tools.middleware import contract_middleware

    assert ToolDef is not None
    assert create_tools is not None
    assert contract_middleware is not None


def test_answer_checking_exports() -> None:
    """v0.44.0 promises these three names at the top level and in .validation."""
    from agentic_data_contracts import (
        ExampleAnswerReport,
        ExampleAnswerResult,
        check_example_answers,
    )
    from agentic_data_contracts.validation import (
        ExampleAnswerReport as VExampleAnswerReport,
    )
    from agentic_data_contracts.validation import (
        ExampleAnswerResult as VExampleAnswerResult,
    )
    from agentic_data_contracts.validation import (
        check_example_answers as v_check_example_answers,
    )

    assert ExampleAnswerReport is VExampleAnswerReport
    assert ExampleAnswerResult is VExampleAnswerResult
    assert check_example_answers is v_check_example_answers
    assert callable(check_example_answers)
    # The empty-is-not-ok rule is part of the published contract.
    assert ExampleAnswerReport(results=[]).ok is False
