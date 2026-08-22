"""Agentic Data Contracts — YAML-first data contract governance for AI agents."""

from agentic_data_contracts.adapters.base import SqlNormalizer
from agentic_data_contracts.ard import (
    build_ai_catalog,
    build_catalog_entry,
    contract_canonical_bytes,
    contract_digest,
)
from agentic_data_contracts.core.contract import (
    DataContract,
    SemanticSourceUnavailableError,
)
from agentic_data_contracts.core.principal import Principal, resolve_principal
from agentic_data_contracts.core.prompt import PromptRenderer, XmlPromptRenderer
from agentic_data_contracts.semantic.base import (
    ExtensibleSemanticSource,
    MetricDefinition,
    MetricImpact,
    Relationship,
    SemanticSource,
)
from agentic_data_contracts.semantic.yaml_source import SEMANTIC_KEYS
from agentic_data_contracts.tools.factory import RowFormat, create_tools
from agentic_data_contracts.tools.middleware import contract_middleware
from agentic_data_contracts.tools.sdk import create_sdk_mcp_server
from agentic_data_contracts.validation import (
    ExampleAnswerReport,
    ExampleAnswerResult,
    check_example_answers,
)

# Optional [langchain] extra — module top-level imports langchain_core and
# langchain.agents, so this fails fast when the extra isn't installed. We
# fall through to ``None`` so ``from agentic_data_contracts import …`` keeps
# working for users on the base install.
try:
    from agentic_data_contracts.tools.langchain import (
        ContractMiddleware,
        create_langchain_tools,
    )
except ImportError:  # pragma: no cover — exercised only without the extra
    # ty gives each imported name a declared type of the class/function itself,
    # so rebinding to ``None`` reads as implicit shadowing. Which names it flags
    # is not derivable from their signatures and shifts between ty versions —
    # v0.0.72 dropped two of the six suppressions this block and the next one
    # used to carry. Don't add or remove these by hand: ty errors on a missing
    # suppression and reports `unused-ignore-comment` on a dead one, so the
    # pinned hook keeps the list honest in both directions.
    ContractMiddleware = None  # ty: ignore[invalid-assignment]
    create_langchain_tools = None

# Optional [pydantic-ai] extra — module top-level imports pydantic_ai, so this
# fails fast when the extra isn't installed. We fall through to ``None`` so
# ``from agentic_data_contracts import …`` keeps working on the base install.
try:
    from agentic_data_contracts.tools.pydantic_ai import (
        ContractDeps,
        contract_run_kwargs,
        create_pydantic_ai_tools,
        create_pydantic_ai_toolset,
    )
except ImportError:  # pragma: no cover — exercised only without the extra
    ContractDeps = None  # ty: ignore[invalid-assignment]
    contract_run_kwargs = None  # ty: ignore[invalid-assignment]
    create_pydantic_ai_tools = None  # ty: ignore[invalid-assignment]
    create_pydantic_ai_toolset = None

__all__ = [
    "SEMANTIC_KEYS",
    "ContractDeps",
    "ContractMiddleware",
    "DataContract",
    "ExampleAnswerReport",
    "ExampleAnswerResult",
    "ExtensibleSemanticSource",
    "MetricDefinition",
    "MetricImpact",
    "Principal",
    "PromptRenderer",
    "Relationship",
    "RowFormat",
    "SemanticSource",
    "SemanticSourceUnavailableError",
    "SqlNormalizer",
    "XmlPromptRenderer",
    "build_ai_catalog",
    "build_catalog_entry",
    "check_example_answers",
    "contract_canonical_bytes",
    "contract_digest",
    "contract_middleware",
    "contract_run_kwargs",
    "create_langchain_tools",
    "create_pydantic_ai_toolset",
    "create_pydantic_ai_tools",
    "create_sdk_mcp_server",
    "create_tools",
    "resolve_principal",
]
