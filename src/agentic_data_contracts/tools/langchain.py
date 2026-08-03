"""LangChain / deepagents integration — wraps ToolDefs as a list of BaseTool.

The returned list plugs directly into:

- ``deepagents.create_deep_agent(tools=...)``
- ``langchain.agents.create_agent(tools=...)``
- any LangChain runnable that accepts ``list[BaseTool]``.

Two integration paths are offered:

1. **In-tool enforcement (default)** — ``create_langchain_tools(...)`` returns
   ``StructuredTool``s whose coroutine pre-checks ``ContractSession`` limits
   and converts ``BLOCKED —`` envelopes from the underlying callables into
   ``ToolException``. The agent runtime renders those as
   ``ToolMessage(status="error")``.

2. **Graph-level enforcement** — ``ContractMiddleware`` subclasses
   ``langchain.agents.middleware.AgentMiddleware`` and intercepts tool calls
   at the graph boundary. Pair with ``apply_middleware=False`` to avoid
   double work.

Both paths observe token usage from the run's message history, so a contract's
``resources.token_budget`` is enforced either way; the accounting is shared
(:func:`_observe_usage`) precisely so the two cannot disagree about a session's
total when a caller wires both.

Important divergence from ``contract_middleware`` in ``tools.middleware``:
that decorator validates SQL on *every* tool that has an ``args["sql"]``
key — including ``inspect_query``, whose explicit purpose is to *report*
violations as JSON without blocking. The in-tool path here therefore only
runs ``session.check_limits()`` and the ``BLOCKED —`` prefix sniff; SQL
validation is left to the underlying tools (``run_query`` self-validates
inside ``factory.create_tools``).

Requires the ``[langchain]`` extra: ``pip install agentic-data-contracts[langchain]``.
"""

from __future__ import annotations

import asyncio
import json
from collections.abc import Awaitable, Callable
from typing import Any

from langchain.agents.middleware.types import AgentMiddleware, ToolCallRequest

# Imported at module level, not inside the coroutine, because that is what makes
# the injection work: LangGraph's ToolNode decides whether to hand a tool its
# ToolRuntime by calling `get_type_hints()` on the coroutine, and this module
# uses `from __future__ import annotations`, so the hint is the *string*
# "ToolRuntime | None" and must resolve against these module globals.
from langchain.tools import ToolRuntime
from langchain_core.messages import ToolMessage
from langchain_core.tools import BaseTool, StructuredTool, ToolException
from langgraph.types import Command

from agentic_data_contracts.adapters.base import DatabaseAdapter, SqlNormalizer
from agentic_data_contracts.core.contract import DataContract
from agentic_data_contracts.core.session import ContractSession, LimitExceededError
from agentic_data_contracts.semantic.base import SemanticSource
from agentic_data_contracts.tools.factory import (
    RowFormat,
    ToolDef,
    create_tools,
)
from agentic_data_contracts.validation.validator import Validator

_BLOCKED_PREFIX = "BLOCKED —"


def _with_remaining(message: str, session: ContractSession) -> str:
    """Append the canonical ``Remaining: {budget}`` suffix used by
    ``run_query``'s own ``_with_remaining`` helper, so wrapper-emitted
    blocks carry the same diagnostic footprint as run_query's own blocks."""
    return f"{message}\nRemaining: {json.dumps(session.remaining(), default=str)}"


def _unwrap_mcp_text(envelope: dict[str, Any]) -> str:
    """Pull the first text block out of an MCP-style content envelope.

    Defensive: tolerates missing keys, non-text blocks, and empty content.
    Falls back to ``""`` so the agent always sees a stable string type
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


def _messages_in(state: Any) -> list[Any]:
    """Pull the message list out of a graph state, tolerating its absence.

    Both callers receive state from the framework rather than constructing it,
    and both can legitimately be handed nothing: a middleware invoked outside a
    graph, or a tool invoked directly with no ``ToolRuntime`` to inject.
    """
    if not state:
        return []
    try:
        return list(state["messages"] or [])
    except (KeyError, TypeError):
        return []


def _usage_scope_for(state: Any, config: Any) -> str:
    """Identify the conversation whose running total is being observed.

    Two sources, tried in order, because neither covers every wiring:

    1. ``thread_id`` from the runtime config — present whenever a checkpointer
       is configured, and stable across turns.
    2. The id of the first message in state. LangGraph's ``add_messages``
       reducer assigns a UUID as messages enter state, and the first one stays
       put across turns, so it identifies a conversation even with no
       checkpointer — which is the shape the README's own LangChain example
       uses.

    Falling back to a bare constant is the *last* resort, reached only when
    there is neither a thread id nor an identified first message.

    Two known under-counts, both of them collisions -- distinct runs landing on
    one key, where ``observe_tokens``' monotone guard then keeps only the
    larger:

    1. That bare constant, when two conversations share it.
    2. A **nested run inheriting its parent's thread id** -- a sub-agent or
       subgraph, which ``create_deep_agent`` produces. Its own history is
       shorter than the parent's, so its usage is dropped whole rather than
       merely mixed: a 5000-token parent turn followed by an 800-token
       sub-agent turn accrues 5000, not 5800.

    The tempting fix for (2) -- appending the config's ``checkpoint_ns`` to
    separate nested runs -- is wrong, and measurably so: that value is
    ``tools:<task-id>`` and carries a *fresh* id on every tool call, so it
    would mint a new scope each time and re-accrue the whole cumulative total,
    multiplying rather than separating. Fixing this needs a key that is stable
    within a run and distinct across nesting; there is no such field to hand.

    Both cases under-enforce rather than over-enforce, which is the safe
    direction to fail, and both are strictly better than the nothing this path
    observed before v0.35.0.

    Deriving the key from ``(state, config)`` rather than from a caller-specific
    object is what lets the middleware and the ``StructuredTool`` path agree:
    fed the same run, they compute the same key, so a session wired to both
    observes one cumulative total instead of two competing ones.
    """
    thread_id = ((config or {}).get("configurable") or {}).get("thread_id")
    if thread_id:
        return f"langchain:thread:{thread_id}"

    messages = _messages_in(state)
    if messages:
        first_id = getattr(messages[0], "id", None)
        if first_id:
            return f"langchain:conv:{first_id}"

    return "langchain"


def _observe_usage(session: ContractSession, state: Any, config: Any) -> None:
    """Feed the contract's ``token_budget`` from a run's message history.

    ``usage_metadata`` is **per message**, so the running total is the *sum*
    over the list -- do not "simplify" this to the last message's total, which
    would undercount by an order of magnitude. The sum grows monotonically as
    messages append, which is what ``ContractSession.observe_tokens`` expects.

    Scoped per conversation, never by a constant: one middleware instance (and
    one tool list) serves every conversation an agent handles, and a shared key
    silently drops the second conversation's usage whenever its running sum sits
    below the first's peak -- under-enforcing *and* reporting a false
    ``tokens_remaining`` to the model, which is the failure this feature exists
    to remove. See :func:`_usage_scope_for`.

    Two accepted limits. Concurrent conversations still share one
    ``ContractSession``, so the budget is a combined ceiling across them rather
    than per-conversation -- correct for a per-user session, worth knowing if
    you share one more widely. And a middleware that trims or summarises history
    (``SummarizationMiddleware``, ``trim_messages``) shrinks the sum; the
    monotone guard refuses to subtract, so accrual pauses until the new sum
    passes the old peak. That under-enforces rather than over-enforces, which is
    the safe direction.
    """
    total = 0
    for message in _messages_in(state):
        usage = getattr(message, "usage_metadata", None)
        if isinstance(usage, dict):
            total += int(usage.get("total_tokens") or 0)
    if total:
        session.observe_tokens(total, scope=_usage_scope_for(state, config))


def create_langchain_tools(
    contract: DataContract,
    *,
    adapter: DatabaseAdapter | None = None,
    semantic_source: SemanticSource | None = None,
    session: ContractSession | None = None,
    tools: list[ToolDef] | None = None,
    apply_middleware: bool = True,
    row_format: RowFormat = "compact",
) -> list[BaseTool]:
    """Create a list of LangChain ``BaseTool``s from a ``DataContract``.

    Args:
        contract: The data contract to enforce.
        adapter: Optional database adapter for query execution.
        semantic_source: Optional semantic source (auto-loaded if not given).
        session: Optional ``ContractSession`` for tracking enforcement state.
            One is created automatically if omitted.
        tools: Pre-built ``ToolDef`` list (if ``None``, created via
            ``create_tools``).
        row_format: How ``run_query`` / ``preview_table`` render result
            rows — ``"compact"`` (default) for positional arrays aligned
            to ``columns``, ``"records"`` for one dict per row. Ignored
            when ``tools`` is supplied.
        apply_middleware: When ``True`` (default), each tool pre-checks
            ``session.check_limits()``. Set ``False`` if you are pairing
            this with ``ContractMiddleware`` to avoid duplicate
            limit-check work — note that the ``BLOCKED —`` prefix sniff
            is always active regardless of this flag, so error semantics
            (raising ``ToolException`` on a blocked envelope) are
            preserved either way.

    Returns:
        A list of ``BaseTool`` instances; order matches the underlying
        ``create_tools()`` output.
    """
    if session is None:
        session = ContractSession(contract)

    if tools is None:
        tools = create_tools(
            contract,
            adapter=adapter,
            semantic_source=semantic_source,
            session=session,
            row_format=row_format,
        )

    return [_to_structured_tool(t, session, apply_middleware) for t in tools]


def _to_structured_tool(
    tool_def: ToolDef,
    session: ContractSession,
    apply_middleware: bool,
) -> BaseTool:
    """Wrap one ``ToolDef`` into a ``StructuredTool``.

    The coroutine returns ``(content_str, raw_envelope)`` so the original
    MCP dict survives on ``ToolMessage.artifact`` while the model sees
    plain text on ``ToolMessage.content``.

    The ``runtime`` parameter is how a declared ``token_budget`` reaches this
    path: LangGraph's ``ToolNode`` injects a ``ToolRuntime`` carrying the run's
    message history and thread id. It triggers on the parameter being *named*
    ``runtime`` **or** annotated ``ToolRuntime`` — either alone suffices, so
    renaming this parameter would keep working while dropping the annotation
    would not. The annotation is therefore the load-bearing half. It never
    reaches the model — ``infer_schema=False`` means the advertised schema is
    ``tool_def.input_schema`` verbatim, not this signature.

    It is optional, and defaults to ``None``, because injection only happens
    inside a graph: ``await tool.ainvoke({"sql": ...})`` is a supported way to
    call these tools and passes no runtime at all. A required parameter would
    break every such caller to catch a mis-wiring that degrades to the previous
    behaviour anyway — usage simply goes unobserved, exactly as it did before.
    """
    inner = tool_def.callable

    async def _coroutine(
        runtime: ToolRuntime | None = None, **kwargs: Any
    ) -> tuple[str, dict[str, Any]]:
        # Observe before checking, so a breach that this very run caused blocks
        # now rather than one tool call later. Unconditional, unlike the limit
        # check below: `apply_middleware=False` delegates *enforcement* to
        # ContractMiddleware, and observation is not enforcement. Both feeding
        # one session is harmless -- they derive the same scope key from the
        # same run, so the second observation of a total is a no-op.
        # Read defensively, matching ContractMiddleware._state_and_config: a
        # non-LangGraph harness that forwards raw tool-call args could put
        # anything under this key, and an AttributeError from accounting must
        # not be how a governed query fails.
        if runtime is not None:
            _observe_usage(
                session,
                getattr(runtime, "state", None),
                getattr(runtime, "config", None),
            )

        if apply_middleware:
            try:
                session.check_limits()
            except LimitExceededError as e:
                raise ToolException(
                    _with_remaining(
                        f"{_BLOCKED_PREFIX} Session limit exceeded: {e}",
                        session,
                    )
                ) from e

        envelope = await inner(kwargs)
        text = _unwrap_mcp_text(envelope)

        # Every BLOCKED path in tools/factory.py and tools/middleware.py
        # uses the canonical "BLOCKED —" em-dash prefix; sniffing it lets
        # us surface enforcement decisions as ToolException, which the
        # agent runtime renders as ToolMessage(status="error").
        if text.startswith(_BLOCKED_PREFIX):
            raise ToolException(text)

        return text, envelope

    return StructuredTool.from_function(
        name=tool_def.name,
        description=tool_def.description,
        coroutine=_coroutine,
        args_schema=tool_def.input_schema,
        infer_schema=False,
        response_format="content_and_artifact",
        # ``handle_tool_error=False`` lets ``ToolException`` propagate to the
        # agent loop's ``ToolNode``, which converts it to
        # ``ToolMessage(status="error")``. We don't do the conversion here.
        handle_tool_error=False,
    )


class ContractMiddleware(AgentMiddleware):
    """Graph-level contract enforcement for LangChain / deepagents.

    Intercepts every tool call and, when ``args["sql"]`` is present, runs
    ``Validator.validate(sql)`` plus ``ContractSession.check_limits()``
    before the tool is invoked. On violation, short-circuits with
    ``ToolMessage(status="error")``; otherwise delegates to the next
    handler in the chain.

    Pair with ``create_langchain_tools(..., apply_middleware=False)`` to
    avoid duplicate enforcement work.
    """

    def __init__(
        self,
        contract: DataContract,
        *,
        adapter: DatabaseAdapter | None = None,
        session: ContractSession | None = None,
    ) -> None:
        super().__init__()
        self._contract = contract
        self._session = session if session is not None else ContractSession(contract)
        sql_normalizer = adapter if isinstance(adapter, SqlNormalizer) else None
        self._validator = Validator(
            contract,
            dialect=adapter.dialect if adapter is not None else None,
            explain_adapter=adapter,
            sql_normalizer=sql_normalizer,
        )

    @staticmethod
    def _state_and_config(request: ToolCallRequest) -> tuple[Any, Any]:
        """Unpack the two things usage accounting needs from a request.

        ``runtime`` is typed non-optional on ``ToolCallRequest`` but read
        defensively: a hand-built request, or one from outside a graph, has
        neither a runtime nor a config.
        """
        runtime = getattr(request, "runtime", None)
        return getattr(request, "state", None), getattr(runtime, "config", None)

    def _observe_token_usage(self, request: ToolCallRequest) -> None:
        """Feed the contract's ``token_budget`` from the run's message history.

        The accounting lives in :func:`_observe_usage`, shared with the
        ``StructuredTool`` path in :func:`_to_structured_tool` — both observe
        the same run, and copying the summing or the scope derivation would let
        the two drift into disagreeing about one session's total.
        """
        _observe_usage(self._session, *self._state_and_config(request))

    def _check(self, request: ToolCallRequest) -> ToolMessage | None:
        """Run enforcement against a request. Returns a short-circuit
        ``ToolMessage`` on violation, ``None`` to continue."""
        tool_call = request.tool_call
        name = tool_call.get("name", "")
        args = tool_call.get("args") or {}
        tool_call_id = tool_call.get("id", "")

        self._observe_token_usage(request)

        # Session-limit breach: do NOT call ``record_retry()`` here. The
        # session is already past its cap; recording another retry would
        # increment past it for no benefit and risks double-counting if a
        # future ceiling is added. This mirrors ``run_query`` in ``factory``,
        # which similarly skips ``record_retry`` on limit-exceeded but does
        # record on validation-block (next branch).
        try:
            self._session.check_limits()
        except LimitExceededError as e:
            return ToolMessage(
                content=_with_remaining(
                    f"{_BLOCKED_PREFIX} Session limit exceeded: {e}",
                    self._session,
                ),
                name=name,
                tool_call_id=tool_call_id,
                status="error",
            )

        # Defensive ``isinstance`` guard: a malformed agent or a hand-built
        # ToolCallRequest could send ``args`` as a non-dict, or ``sql`` as a
        # non-string; either would crash inside ``Validator.validate`` /
        # sqlglot. ``langchain``'s args_schema validation should normally
        # prevent this, but the cost of guarding is one keyword.
        sql = args.get("sql") if isinstance(args, dict) else None
        if isinstance(sql, str) and sql:
            result = self._validator.validate(sql)
            if result.blocked:
                self._session.record_retry()
                return ToolMessage(
                    content=_with_remaining(
                        f"{_BLOCKED_PREFIX} Violations:\n"
                        + "\n".join(f"- {r}" for r in result.reasons),
                        self._session,
                    ),
                    name=name,
                    tool_call_id=tool_call_id,
                    status="error",
                )
        return None

    def wrap_tool_call(
        self,
        request: ToolCallRequest,
        handler: Callable[[ToolCallRequest], ToolMessage | Command[Any]],
    ) -> ToolMessage | Command[Any]:
        blocked = self._check(request)
        if blocked is not None:
            return blocked
        return handler(request)

    async def awrap_tool_call(
        self,
        request: ToolCallRequest,
        handler: Callable[[ToolCallRequest], Awaitable[ToolMessage | Command[Any]]],
    ) -> ToolMessage | Command[Any]:
        # _check runs the synchronous Validator.validate(), which makes a
        # blocking EXPLAIN/dry-run DB round-trip via explain_adapter.explain.
        # Offload it to a worker thread so a slow check cannot stall the event
        # loop in an async runtime. The sync wrap_tool_call path calls _check
        # directly.
        blocked = await asyncio.to_thread(self._check, request)
        if blocked is not None:
            return blocked
        return await handler(request)
