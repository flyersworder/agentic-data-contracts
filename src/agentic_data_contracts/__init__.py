"""Agentic Data Contracts — YAML-first data contract governance for AI agents."""

from agentic_data_contracts.adapters.base import SqlNormalizer
from agentic_data_contracts.core.contract import DataContract
from agentic_data_contracts.core.principal import Principal, resolve_principal
from agentic_data_contracts.core.prompt import ClaudePromptRenderer, PromptRenderer
from agentic_data_contracts.semantic.base import (
    MetricDefinition,
    MetricImpact,
    Relationship,
    SemanticSource,
)
from agentic_data_contracts.tools.factory import create_tools
from agentic_data_contracts.tools.middleware import contract_middleware
from agentic_data_contracts.tools.sdk import create_sdk_mcp_server

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
    ContractMiddleware = None  # ty: ignore[invalid-assignment]
    create_langchain_tools = None  # ty: ignore[invalid-assignment]

# Optional [pydantic-ai] extra — module top-level imports pydantic_ai, so this
# fails fast when the extra isn't installed. We fall through to ``None`` so
# ``from agentic_data_contracts import …`` keeps working on the base install.
try:
    from agentic_data_contracts.tools.pydantic_ai import create_pydantic_ai_tools
except ImportError:  # pragma: no cover — exercised only without the extra
    create_pydantic_ai_tools = None  # ty: ignore[invalid-assignment]

__all__ = [
    "ClaudePromptRenderer",
    "ContractMiddleware",
    "DataContract",
    "MetricDefinition",
    "MetricImpact",
    "Principal",
    "PromptRenderer",
    "Relationship",
    "SemanticSource",
    "SqlNormalizer",
    "contract_middleware",
    "create_langchain_tools",
    "create_pydantic_ai_tools",
    "create_sdk_mcp_server",
    "create_tools",
    "resolve_principal",
]
