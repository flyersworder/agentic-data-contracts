"""Pydantic AI integration — wraps ToolDefs as a list of ``pydantic_ai.Tool``.

The returned list plugs directly into ``pydantic_ai.Agent(tools=...)``.

Enforcement is applied **in-tool**, mirroring ``create_langchain_tools``'s
default path: each wrapped tool pre-checks ``ContractSession`` limits and the
underlying callables self-validate SQL (see ``run_query`` in
``tools/factory.py``). Two enforcement signals are mapped onto Pydantic AI's
error contract, which distinguishes recoverable from terminal failures:

- **Validation block** (``BLOCKED —`` envelope from a tool — bad SQL, a
  forbidden operation, a missing required filter, a failed result-check) is
  *recoverable*: re-raised as ``pydantic_ai.ModelRetry`` so the model can
  rewrite its arguments and try again.
- **Session-limit exhaustion** (``max_retries`` / ``max_duration`` / cost
  budget) is *terminal*: retrying cannot help, so it is raised as
  ``ContractSessionLimitError`` (a plain ``RuntimeError`` subclass) which
  propagates out of the run instead of consuming a model retry slot. This
  matches how ``factory.run_query`` already separates the two cases — it
  records a retry on a validation block but not on a limit breach.

Pass ``apply_middleware=False`` to skip the per-tool session pre-check (the
underlying ``run_query`` still self-checks its own limits).

Requires the ``[pydantic-ai]`` extra:
``pip install agentic-data-contracts[pydantic-ai]``.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any

from pydantic_ai import ModelRetry, RunContext, Tool
from pydantic_ai.toolsets import FunctionToolset, ToolsetFunc

from agentic_data_contracts.adapters.base import DatabaseAdapter
from agentic_data_contracts.core.contract import DataContract
from agentic_data_contracts.core.principal import Principal
from agentic_data_contracts.core.session import (
    ContractSession,
    ContractSessionLimitError,
    LimitExceededError,
)
from agentic_data_contracts.semantic.base import SemanticSource
from agentic_data_contracts.tools.factory import ToolDef, create_tools

_BLOCKED_PREFIX = "BLOCKED —"
# Substring marking a *terminal* session-budget breach inside a BLOCKED
# envelope (vs. a recoverable validation/permission block). Both this adapter's
# own pre-check and ``factory.run_query``'s self-check emit it, so the sniff
# below must treat it as terminal regardless of which layer produced it.
_SESSION_LIMIT_MARKER = "Session limit exceeded"


def _with_remaining(message: str, session: ContractSession) -> str:
    """Append the canonical ``Remaining: {budget}`` suffix used by
    ``run_query`` in ``tools/factory.py`` so wrapper-emitted blocks carry
    the same diagnostic footprint as run_query's own blocks."""
    return f"{message}\nRemaining: {json.dumps(session.remaining(), default=str)}"


def _unwrap_mcp_text(envelope: dict[str, Any]) -> str:
    """Pull the first text block out of an MCP-style content envelope.

    Defensive: tolerates missing keys, non-text blocks, and empty content.
    Falls back to ``""`` so the model always sees a stable string type
    rather than a stringified dict.
    """
    try:
        content = envelope.get("content") or []
        for block in content:
            if isinstance(block, dict) and block.get("type") == "text":
                return str(block.get("text", ""))
        return ""
    except (AttributeError, TypeError):
        return ""


def create_pydantic_ai_tools(
    contract: DataContract,
    *,
    adapter: DatabaseAdapter | None = None,
    semantic_source: SemanticSource | None = None,
    session: ContractSession | None = None,
    caller_principal: Principal = None,
    tools: list[ToolDef] | None = None,
    apply_middleware: bool = True,
) -> list[Tool]:
    """Create a list of ``pydantic_ai.Tool``s from a ``DataContract``.

    Args:
        contract: The data contract to enforce.
        adapter: Optional database adapter for query execution.
        semantic_source: Optional semantic source (auto-loaded if not given).
        session: Optional ``ContractSession`` for tracking enforcement state.
            One is created automatically if omitted.
        caller_principal: Optional principal identifying the caller, used for
            per-principal table/rule gating (passed through to ``create_tools``).
        tools: Pre-built ``ToolDef`` list (if ``None``, created via
            ``create_tools``).
        apply_middleware: When ``True`` (default), each tool pre-checks
            ``session.check_limits()`` and raises ``ContractSessionLimitError``
            on overrun. Set ``False`` to skip the pre-check.

    Returns:
        A list of ``pydantic_ai.Tool`` instances; order matches the
        underlying ``create_tools()`` output.
    """
    if session is None:
        session = ContractSession(contract)

    if tools is None:
        tools = create_tools(
            contract,
            adapter=adapter,
            semantic_source=semantic_source,
            session=session,
            caller_principal=caller_principal,
        )

    return [_to_pydantic_ai_tool(t, session, apply_middleware) for t in tools]


def _to_pydantic_ai_tool(
    tool_def: ToolDef,
    session: ContractSession,
    apply_middleware: bool,
) -> Tool:
    """Wrap one ``ToolDef`` into a ``pydantic_ai.Tool`` via ``Tool.from_schema``.

    ``Tool.from_schema`` passes the model's arguments as keyword arguments and
    does not re-validate them against the JSON schema; the underlying factory
    callables read ``args.get(...)`` defensively, so collecting ``**kwargs``
    into a dict is safe.
    """
    inner = tool_def.callable

    async def _fn(**kwargs: Any) -> str:
        if apply_middleware:
            try:
                session.check_limits()
            except LimitExceededError as e:
                # Terminal — do NOT raise ModelRetry; retrying cannot help.
                raise ContractSessionLimitError(
                    _with_remaining(
                        f"{_BLOCKED_PREFIX} {_SESSION_LIMIT_MARKER}: {e}", session
                    )
                ) from e

        text = _unwrap_mcp_text(await inner(kwargs))

        # Every BLOCKED path in tools/factory.py uses the canonical
        # "BLOCKED —" em-dash prefix. A session-budget breach is terminal even
        # when it surfaces from a tool's own self-check (run_query's limit
        # check under apply_middleware=False), so it must NOT become a
        # recoverable ModelRetry. Everything else BLOCKED (bad SQL, forbidden
        # op, permission gate, failed result-check) is recoverable: surfaced as
        # ModelRetry so the model can rewrite its arguments or switch tools.
        if text.startswith(_BLOCKED_PREFIX):
            if _SESSION_LIMIT_MARKER in text:
                raise ContractSessionLimitError(text)
            raise ModelRetry(text)

        return text

    return Tool.from_schema(
        function=_fn,
        name=tool_def.name,
        description=tool_def.description,
        json_schema=tool_def.input_schema,
        takes_ctx=False,
    )


@dataclass
class ContractDeps:
    """Per-user run dependencies for the deps-aware Pydantic AI toolset.

    Used as ``Agent(deps_type=ContractDeps)`` and passed on each turn via
    ``agent.run(..., deps=ContractDeps(session=..., caller_principal=...))``.

    The **caller owns** each user's ``ContractSession``: create it once per user,
    keep it keyed by user id, and pass the *same* object on every turn so
    cumulative limits (``max_duration`` from the first call, retries, cost)
    accumulate across the conversation. The toolset never creates sessions.
    """

    session: ContractSession
    caller_principal: Principal = None


def create_pydantic_ai_toolset(
    contract: DataContract,
    *,
    adapter: DatabaseAdapter | None = None,
    semantic_source: SemanticSource | None = None,
    apply_middleware: bool = True,
) -> ToolsetFunc[ContractDeps]:
    """Create a deps-aware toolset factory so ONE shared ``Agent`` serves many users.

    Returns a ``ToolsetFunc`` — register it on a single shared agent via the
    public ``agent.toolset(...)`` API (or ``@agent.toolset``). On each run it
    reads the per-user :class:`ContractDeps` from ``RunContext.deps`` and rebuilds
    the contract's tools bound to that user's ``ContractSession`` + principal, so
    you do not build a separate tools list (or Agent) per user::

        agent = Agent(model, deps_type=ContractDeps)
        agent.toolset(per_run_step=False)(
            create_pydantic_ai_toolset(contract, adapter=adapter)
        )
        await agent.run(prompt, deps=ContractDeps(session=user_session,
                                                  caller_principal=user))

    Register with ``per_run_step=False`` (the decorator-factory form above is the
    typed public API for passing it). ``agent.toolset`` defaults to
    ``per_run_step=True``, which re-invokes the factory — rebuilding the 9 tools +
    a ``Validator`` — on *every* model step. The deps (session and principal) are
    stable within a single run, so the tools only need building once per run;
    ``per_run_step=False`` evaluates the factory once per ``run()`` and avoids the
    per-step rebuild. (The rebuild does no I/O, so the cost is small either way,
    but once-per-run is the right default for this factory.)

    Enforcement is identical to :func:`create_pydantic_ai_tools` (a validation
    block becomes ``ModelRetry``; a session-budget breach becomes the terminal
    ``ContractSessionLimitError``). The shared config (adapter connection pool,
    semantic source) stays shared across all users; only the per-user session and
    principal vary, threaded in via ``deps``.
    """

    def _factory(ctx: RunContext[ContractDeps]) -> FunctionToolset[ContractDeps]:
        deps = ctx.deps
        # Fail loudly on mis-wiring — never silently skip enforcement.
        if not isinstance(deps, ContractDeps):
            raise TypeError(
                "create_pydantic_ai_toolset requires Agent(deps_type=ContractDeps)"
                " and run(..., deps=ContractDeps(...)); got"
                f" {type(deps).__name__}."
            )
        if deps.session is None:
            raise ValueError("ContractDeps.session must not be None.")

        tools = create_pydantic_ai_tools(
            contract,
            adapter=adapter,
            semantic_source=semantic_source,
            session=deps.session,
            caller_principal=deps.caller_principal,
            apply_middleware=apply_middleware,
        )
        return FunctionToolset(tools)

    return _factory
