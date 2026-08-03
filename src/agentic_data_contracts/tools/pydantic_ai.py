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
from weakref import WeakKeyDictionary

from pydantic_ai import ModelRetry, RunContext, Tool
from pydantic_ai.toolsets import FunctionToolset, ToolsetFunc
from pydantic_ai.usage import RunUsage, UsageLimits

from agentic_data_contracts.adapters.base import DatabaseAdapter
from agentic_data_contracts.core.contract import DataContract
from agentic_data_contracts.core.principal import Principal
from agentic_data_contracts.core.session import (
    ContractSession,
    ContractSessionLimitError,
    LimitExceededError,
)
from agentic_data_contracts.semantic.base import SemanticSource
from agentic_data_contracts.tools.factory import (
    RowFormat,
    ToolDef,
    create_tools,
    validate_row_format,
)

_BLOCKED_PREFIX = "BLOCKED —"

# One carried ``RunUsage`` per ``ContractSession``, so a session's token tally
# can track a counter that spans every turn instead of per-run snapshots. Weak
# keys: the caller owns session lifetime (one per user, often held for a
# conversation), and this must not be what keeps one alive.
#
# Module-private rather than an attribute on ``ContractSession`` deliberately —
# ``RunUsage`` is a Pydantic AI type and ``core/`` stays framework-agnostic.
# Keyed on identity, which is also how the tool wrapper decides whether the
# counter it was handed is the carried one; see ``_usage_scope``.
_CARRIED_USAGE: WeakKeyDictionary[ContractSession, RunUsage] = WeakKeyDictionary()

# Scope for a carried counter. Constant on purpose: the counter is already
# global across the session's runs, so a per-run key would re-accrue the whole
# running total on each new run. Measured before this existed: a true spend of
# 600 was recorded as 900, and the inflated tally then blocked a run that was
# still within budget.
_CARRIED_SCOPE = "pydantic-ai:carried"
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
    row_format: RowFormat = "compact",
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
        row_format: How ``run_query`` / ``preview_table`` render result
            rows — ``"compact"`` (default) for positional arrays aligned
            to ``columns``, ``"records"`` for one dict per row. Ignored
            when ``tools`` is supplied.
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
            row_format=row_format,
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

    ``takes_ctx=True`` so the wrapper can read ``ctx.usage`` — this is the only
    place the contract's ``token_budget`` can be fed on this path. Which span
    that reading belongs to depends on how the caller wired the run, so the key
    it accrues under is chosen by :func:`_usage_scope` rather than fixed here.
    """
    inner = tool_def.callable

    async def _fn(ctx: RunContext[Any], **kwargs: Any) -> str:
        # Observe BEFORE the limit check, so a budget already exhausted by the
        # model's own consumption is caught on this call rather than the next.
        scope = _usage_scope(ctx, session)
        if scope is not None and ctx.usage.total_tokens:
            session.observe_tokens(ctx.usage.total_tokens, scope=scope)

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
        takes_ctx=True,
    )


def _usage_scope(ctx: RunContext[Any], session: ContractSession) -> str | None:
    """Pick the key under which this run's usage total accrues, or ``None``.

    ``observe_tokens`` accrues the delta *per scope*, so the key has to match
    the span of the counter being read — and which counter that is depends on
    how the caller wired the run:

    - **Carried** (``contract_run_kwargs``): ``ctx.usage`` *is* the
      ``RunUsage`` registered for this session, spanning every turn. One
      constant key, so the running total accrues once.
    - **Per run** (the default, and ``usage_limits_from_contract``):
      ``ctx.usage`` restarts at zero each ``agent.run()``, so it is keyed by
      ``run_id`` and each run contributes its own total.

    Decided by **identity**, not a flag, and that is what makes mis-wiring
    safe. A caller who ignores ``contract_run_kwargs``, or passes its
    ``usage_limits`` without its ``usage``, simply fails the ``is`` check and
    gets the per-run behaviour — bounded, exactly as before. The alternative
    failure, treating a per-run counter as carried, would under-count; treating
    a carried counter as per-run would inflate the tally and block runs still
    within budget. Neither is reachable by getting a boolean wrong, because
    there is no boolean.

    ``None`` means "do not observe": no ``run_id`` and no carried counter
    leaves nothing that identifies a span, and a shared fallback key would
    collapse every run into ``max()`` and silently drop the smaller ones.
    """
    if _CARRIED_USAGE.get(session) is ctx.usage:
        return _CARRIED_SCOPE
    run_id = ctx.run_id
    return str(run_id) if run_id is not None else None


def contract_run_kwargs(
    contract: DataContract, session: ContractSession
) -> dict[str, Any]:
    """Both halves of exact ``token_budget`` enforcement, for ``agent.run()``.

    Supersedes :func:`usage_limits_from_contract`, whose spend grows linearly
    with the number of turns rather than settling at the declared ceiling::

        session = ContractSession(contract)          # one per user
        tools = create_pydantic_ai_tools(contract, adapter=adapter,
                                         session=session)

        await agent.run(prompt, **contract_run_kwargs(contract, session))

    Returns ``{"usage": ..., "usage_limits": ...}``. Splat it; the two are only
    correct together, which is why they are returned together rather than as
    two functions a caller could half-adopt.

    **Why this is exact and the other is not.** ``usage_limits_from_contract``
    subtracts ``session.tokens_used``, and the session is fed only from inside
    the tool wrapper — so every model request after a run's last tool call, the
    final answer included, goes unobserved and the shortfall compounds. Here the
    same ``RunUsage`` is carried across every ``agent.run()`` for the session, so
    Pydantic AI's own counter sees *every* request, and the tool wrapper reads
    that one running total. Nothing is missed, so nothing has to be estimated.

    Two consequences follow, both measured on an agent spending 100 tokens per
    request against a 500-token budget:

    - **True spend settles instead of growing.** 600 against a 500-token
      budget and *flat* however many turns run, where the superseded helper is
      linear in turns: 1100 at 6, 1700 at 12, 2500 at 20. Flat-versus-linear is
      the real difference; any single pair of numbers understates it.
    - **A refused turn costs nothing.** With the whole history in the counter,
      ``check_before_request`` refuses *before* issuing a request. The bounded
      helper cannot do this: its per-run counter starts at zero, so every
      refused turn still bought one billed model request, forever.

    The limit is **flat** (``total_tokens_limit=token_budget``), not the
    remainder. Subtracting would take the spend off twice — once in the limit
    and once inside the carried counter — and lock the run out below its budget.
    That is exactly why ``usage_limits_from_contract``'s docstring says not to
    combine it with ``agent.run(usage=...)``.

    Still call it **per run**. The counter is stable, but the ``UsageLimits`` is
    rebuilt each time so a contract reloaded mid-conversation takes effect —
    and hoisting it is no longer a correctness bug the way it was when the
    limit carried a snapshot of spend.

    A contract with no ``token_budget`` gets a carried counter and no token
    ceiling: usage is then tracked honestly for ``remaining()`` without
    inventing a limit nothing declared.

    ``ContractSessionLimitError`` remains reachable — the session still holds
    ``max_retries``, ``cost_limit_usd`` and ``max_duration_seconds``, and can be
    fed from outside the run — so keep that handler alongside
    ``pydantic_ai.exceptions.UsageLimitExceeded``.

    **Adopt it from the start of a session.** The carried counter begins at
    zero and knows nothing of spend the session recorded before the first call
    — from a plain ``agent.run`` without these kwargs, from another adapter
    sharing the session, or from a direct ``observe_tokens()``. The flat
    ceiling is then measured against the counter alone, so that earlier spend
    is not deducted. In-tool ``check_limits()`` still sees the session's full
    tally and stops the next tool call, so this degrades rather than escapes,
    but mixing wirings mid-session gives up the exactness that is the whole
    point of this helper.

    Sequential turns only. One counter per session is shared mutable state, so
    concurrent runs for the same user race on it; that is the same shape
    :class:`ContractDeps` already describes for sessions themselves. The
    registration below is likewise get-then-set, so two threads calling this
    for the *same new* session both build a counter and one wins — the loser's
    run falls back to per-run scoping, which under-counts rather than inflating.
    """
    usage = _CARRIED_USAGE.get(session)
    if usage is None:
        usage = RunUsage()
        _CARRIED_USAGE[session] = usage
    elif usage.total_tokens:
        # Catch the session up before the next run starts. The tool wrapper can
        # only observe while a tool is executing, so it always misses whatever
        # the previous run spent after its last tool call -- the final answer
        # most of all. Enforcement does not depend on this (the carried counter
        # is what Pydantic AI checks), but `remaining()` is reported to the
        # model in every run_query response, and a tally that lags by the
        # largest request of each turn is a number the model would act on.
        session.observe_tokens(usage.total_tokens, scope=_CARRIED_SCOPE)

    resources = contract.schema.resources
    budget = resources.token_budget if resources is not None else None
    limits = UsageLimits() if budget is None else UsageLimits(total_tokens_limit=budget)
    return {"usage": usage, "usage_limits": limits}


def usage_limits_from_contract(
    contract: DataContract, session: ContractSession
) -> UsageLimits:
    """Translate a contract's ``token_budget`` into Pydantic AI ``UsageLimits``.

    **Superseded by :func:`contract_run_kwargs`.** The ceiling here is not a
    ceiling: spend grows *linearly with the number of turns* — measured at
    2.2x the budget by 6 turns, 3.4x by 12, 5.0x by 20 — because each run is
    granted a remainder computed from a tally that misses whatever the previous
    run spent after its last tool call. ``contract_run_kwargs`` is flat at 1.2x
    no matter how many turns run, and makes a refused turn cost nothing. Prefer
    it. This remains supported and correct for what it claims — it needs
    nothing but the limit, so it still suits a caller who cannot carry a
    counter across turns.

    Closes the window that in-tool enforcement cannot::

        await agent.run(
            prompt,
            deps=ContractDeps(session=session),
            usage_limits=usage_limits_from_contract(contract, session),
        )

    ``ContractSession.check_limits()`` runs *pre-tool-call*, so a breach stops
    the next tool call — and an agent that burns its budget without calling
    tools is never interrupted at all. ``UsageLimits`` is checked by Pydantic
    AI on every model request, no tool call required, which closes that hole
    **within a run**.

    **The ``session`` argument is required, and is the whole point.**
    ``total_tokens_limit`` is checked against ``RunUsage`` — usage for *one*
    ``agent.run()`` call — while a contract's ``token_budget`` is a ceiling for
    the user across every turn (:class:`ContractDeps` instructs callers to
    reuse one session so limits accumulate). Passing the raw budget would
    therefore grant it afresh on each turn: a 50,000-token contract would
    authorise 50,000 *per turn*, so ten turns spend 500,000 under a contract
    that says 50,000 — declared-but-unenforced, the failure this library exists
    to prevent. Limiting each run to what the session has *left* bounds the
    total instead.

    Call it **per run**, not once at wiring time: the value is a snapshot of
    remaining budget, and a stale one re-grants spend that already happened.

    **This bounds the budget; it does not enforce it exactly.** The number
    subtracted is ``session.tokens_used``, and the session is fed only from
    inside the tool wrapper — so every model request *after a run's last tool
    call*, including the final answer generation (typically the largest
    context), is never observed. Each run is therefore granted more than the
    true remainder, and the shortfall compounds per turn. Measured on a
    two-request-per-turn agent with a 500-token budget, real spend reached ~2x
    the ceiling *by the first refusal* — and that factor is not a constant, it
    tracks how often the agent calls tools (~1.4x for a three-tool-call turn).
    An agent that calls **no** tools is stopped within a run but remains
    unbounded across them, since the session never accrues at all: 12 turns of
    a 500-token budget spent 1200 with zero refusals. All of which is a large
    improvement on the unbounded behaviour without this helper, and is not the
    identity a quick read might assume. Issue #56 carries the design that would
    make it exact (carry one ``RunUsage`` across turns via
    ``agent.run(usage=...)``); it needs its own pass, because that counter is
    global while ``observe_tokens`` scopes per ``run_id`` and would
    double-count it today.

    Three consequences of wiring this, none of them obvious:

    - **Catch ``pydantic_ai.exceptions.UsageLimitExceeded`` as well as**
      :class:`ContractSessionLimitError` — not instead of it. The framework
      usually stops the run first, because a tool can only execute on a
      response that already passed ``check_tokens``. But the contract-side
      error still fires whenever the session is fed from outside the run — a
      session shared with another adapter, or a direct ``observe_tokens()``
      call — on either ``apply_middleware`` value. Dropping the existing
      handler would leave those unhandled.
    - **The session's token tally freezes** once the framework starts refusing
      runs, since no further tools execute to feed it. ``remaining()`` will sit
      at a stale figure while each refused turn still costs one billed model
      request, so cumulative spend keeps climbing past that ~2x.
    - **Do not combine with ``agent.run(usage=...)``.** A carried counter is
      subtracted twice — once here, once inside the counter — and the run locks
      out permanently below the declared budget.

    Concurrent runs sharing one session each snapshot the same ``tokens_used``
    and are each granted the full remainder. Correct for sequential turns,
    which is what :class:`ContractDeps` describes; worth knowing if you fan out.

    ``contract`` supplies the budget and ``session`` supplies the spend, so
    pass the session built for *that* contract — a mismatched pair silently
    enforces one contract's ceiling against another's usage. Not asserted:
    ``ContractSession`` holds its contract, but comparing identity would reject
    the legitimate case of an equivalent contract reloaded from YAML.

    To add limits this contract does not express, copy the result::

        dataclasses.replace(usage_limits_from_contract(dc, s), tool_calls_limit=5)

    What is deliberately not mapped:

    - **``max_retries`` is NOT mapped onto ``request_limit``.** They count
      different things — ``max_retries`` counts *blocked query attempts* here
      (``record_retry()`` fires on a validation block), while ``request_limit``
      counts *model requests*. A contract saying ``max_retries: 3`` means
      "three bad queries and you are done", not "three LLM calls". Conflating
      them would silently change what an existing contract means.
      ``request_limit`` is left at Pydantic AI's own default, which is its
      runaway guard and not ours to reinterpret.
    - **``cost_limit_usd`` and ``max_duration_seconds``** have no
      ``UsageLimits`` equivalent and stay session-side.

    Returns ``UsageLimits`` with ``total_tokens_limit`` unset when the contract
    declares no ``token_budget`` — inventing a ceiling nothing declared would be
    the mirror image of failing to enforce one that was.

    An exhausted budget yields ``total_tokens_limit=0``. Pydantic AI's
    pre-request check compares ``total_tokens > limit`` against a run that has
    not started, so ``0 > 0`` is false and one model request still goes out
    before the post-request check aborts the run — a per-refused-turn cost, not
    a one-off.

    Pydantic AI only. LangChain has no per-request ceiling to map onto, and the
    Claude Agent SDK path cannot observe usage at all — so this lives here, in
    a module that only imports under the ``[pydantic-ai]`` extra, rather than
    anywhere that would imply cross-framework coverage it does not have.
    """
    resources = contract.schema.resources
    budget = resources.token_budget if resources is not None else None
    if budget is None:
        return UsageLimits()
    return UsageLimits(total_tokens_limit=max(0, budget - session.tokens_used))


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
    row_format: RowFormat = "compact",
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
    # This function returns a factory that builds tools per run, so deferring
    # to create_tools would push a typo to the first agent run. Check now.
    validate_row_format(row_format)

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
            row_format=row_format,
        )
        return FunctionToolset(tools)

    return _factory
