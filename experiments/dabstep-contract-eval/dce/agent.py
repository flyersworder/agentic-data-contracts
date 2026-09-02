"""Run one (task, arm, model) and return a fully provenanced result row.

`run_task` guarantees that once the billable model call has happened (or a
cap has tripped), it returns — it does not raise. The only exception it
still lets propagate is `AgentConstructionError`, and only because nothing
billable has happened yet when that one fires. Two known, deliberately
unfixed gaps in that guarantee, recorded here rather than silently
tolerated:

  * `KeyboardInterrupt` during a live model call escapes everything —
    `run_sync` itself, the guarded tail, even `setup.close()`'s own
    `try/except Exception` (which does not catch `BaseException`
    subclasses). Measured: 5 interrupted calls burned ~$6.00 in real spend
    and produced 0 rows. This is accepted as-is: an interrupt is
    user-initiated, and a sweep the operator is actively killing does not
    need its own resume bookkeeping to survive the kill — but it means an
    operator who Ctrl-C's a running sweep should not assume `spent_so_far`
    reflects everything that was actually billed up to that point.
  * `TOKEN_BUDGET` (the runaway guard) has slack against the TRUE
    request-level ceiling: `UsageLimits.total_tokens_limit` is checked only
    BETWEEN requests (see `PER_REQUEST_INPUT_TOKEN_CAP`'s own note on the
    same gap), so one in-flight request can land up to roughly
    `PER_REQUEST_INPUT_TOKEN_CAP` (a quarter of `TOKEN_BUDGET`) over the
    nominal cap before the NEXT request is what actually stops. ~25% slack
    is accepted as part of `TOKEN_BUDGET`'s deliberately generous sizing
    (see its own module-level comment), not closed here.
"""

from __future__ import annotations

import json
import os
import subprocess
import time
from decimal import Decimal
from importlib.metadata import version
from pathlib import Path

from dce.arms import build_arm
from dce.frozen import digest, hollow_digest
from dce.grade import _clean, active_scorer, score
from dce.pricing import MODELS, cost
from dce.trace import write_trace

# A harness cap, not a contract limit — applied identically to every arm so
# no arm gets more iterations than another (see the retries note below, and
# `dce/arms.py`'s module docstring on why that symmetry matters).
#: OpenRouter's `reasoning.effort`, sent explicitly rather than inherited.
#:
#: F3's real lesson applied to a second knob: an unset parameter is not a
#: fixed parameter. Every pinned model supports `reasoning_effort`, every one
#: of them reasons by default, and the default differs by endpoint — while the
#: serving endpoint was, until the pin below, chosen per request. Measured on
#: `z-ai/glm-5.3-flash` for one trivial question: 133 reasoning tokens with no
#: parameter set, against 45-55 at any explicit effort. Reasoning tokens bill
#: at the OUTPUT rate (the pricier one), so this is a cost knob as well as a
#: quality knob.
#:
#: "medium" is the neutral middle of OpenRouter's scale, chosen so the guard
#: is an explicit recorded value rather than a provider default. Uniform
#: across every arm — it is a model setting, not an arm setting — and stamped
#: on every row as `reasoning_effort`.
#:
#: Reasoning cannot be switched off: `{"enabled": false}` returns HTTP 400
#: "Reasoning is mandatory for this endpoint and cannot be disabled."
REASONING_EFFORT: str = "medium"

MAX_TOOL_CALLS: int = 40

# RAISED FROM 25 AFTER THE FIRST SMOKE RUN (F2, see
# `docs/superpowers/specs/2026-08-30-dabstep-smoke-findings.md`). On the first
# hard task attempted, TWO of three arms exhausted the budget and returned an
# empty answer: a `hit_limit` row is unscoreable, so it costs full price and
# contributes nothing to accuracy. Raising the cap alone would not fix that —
# a bigger budget still ends in the same unscoreable row when it runs out —
# which is why `_force_final_answer` below exists alongside it.

# pydantic-ai's own current default for `UsageLimits.request_limit`, made
# explicit rather than inherited — verified against the installed library in
# `tests/test_agent.py`'s API-shape guards — so a future pydantic-ai release
# that changes its default cannot silently change how many model turns one
# task gets, and so the value in force is visible and can be stamped into
# the result row (see `build_result_row`'s `request_limit` field) instead of
# being an invisible library default.
REQUEST_LIMIT: int = 50

# N2's fix: `total_tokens_limit` is only checked BETWEEN requests, so nothing
# by itself stops one enormous single request. Demonstrated overshoot before
# these existed: a 10,000-row `payments` result serialising to ~429k tokens
# (see `dce/arms.py`'s `MAX_ROWS`, since lowered to 1,000 for the same
# reason) became the next request's input in full, costing ~$0.86 on
# `gpt-5.6-sol` alone — $1.80 total against a $0.25 cap that request never
# got a chance to enforce because it only fires between requests, not within
# one. Two closures, both applied identically to every arm/model so neither
# is a confound:
#
#   * `MAX_OUTPUT_TOKENS_PER_REQUEST` bounds one model turn's OUTPUT via
#     `ModelSettings(max_tokens=...)` — previously unset, so a single
#     response (reasoning tokens included) was unbounded.
#   * `UsageLimits.per_request_input_tokens_limit` (set in `run_task` as
#     `PER_REQUEST_INPUT_TOKEN_CAP`, below) bounds one request's INPUT.
#
# The output bound must clear a reasoning model's reasoning, not just its
# answer — see the F1 note directly below, which is why 4,000 was wrong.
MAX_OUTPUT_TOKENS_PER_REQUEST: int = 64_000
# RAISED AGAIN, 16,000 -> 64,000, AND THIS TIME THE FAILURE WAS ARM-DEPENDENT
# AND LARGE. F1 raised it 4,000 -> 16,000 after the same error killed one arm
# of the first smoke run. 16,000 was sized against `glm-5.3-flash`, which
# reasons ~380 tokens per run; it survived that sweep at 3 errors in 1,604
# rows (0.2%). `deepseek-v4-flash-0731` reasons two orders of magnitude
# harder: measured on the first 15 rows of its sweep, a single `schema_only`
# run spent 36,155 reasoning tokens across 23 turns, and single requests blew
# past 16,000 on their own.
#
# The resulting error rate was 3 of 7 `schema_only` rows and ZERO in the
# other three arms. That asymmetry is the whole problem, and it is not
# subtle: an arm with no context flounders, reasons enormously, and dies on a
# HARNESS parameter — while the arms carrying context finish comfortably. An
# `error` row scores as wrong, so leaving the cap here would have handed the
# contract arm a win manufactured by our own configuration, in exactly the
# direction that flatters this library. That is the same class of mistake as
# F12, where a stricter-than-official scorer produced a false positive in our
# favour.
#
# 64,000 is chosen against evidence rather than doubled by reflex: the
# largest WHOLE-RUN output observed on this model is 38,983 tokens across 23
# turns, so a single request reaching 64,000 would be extraordinary. It stays
# far below the pinned endpoint's 384,000 ceiling, so it is still a real
# per-request bound rather than a formality, and `TOKEN_BUDGET` plus
# `tool_calls_limit` remain the actual runaway guards.
#
# THE GENERAL LESSON, worth carrying beyond this experiment: a fixed output
# cap is not model-independent. It must clear the reasoning volume of the
# most reasoning-hungry (model, arm) pair in the comparison, and the arm that
# needs the most reasoning is reliably the one with the least context. Sized
# to one model, it silently becomes a confound on the next.

# RAISED FROM 4,000 AFTER THE FIRST SMOKE RUN (F1). The old value assumed
# "4,000 output tokens is generous for a data-analyst answer with some
# reasoning attached". That is false for a reasoning model: reasoning tokens
# count against `max_tokens`, and the cap can therefore fire BEFORE ANY ANSWER
# TEXT EXISTS. Measured against the live API, `z-ai/glm-5.3-flash` spent 49 of
# 50 completion tokens on reasoning for a trivial one-number reply. Arm B died
# on exactly this:
#
#     UnexpectedModelBehavior: Model token limit (4000) exceeded before any
#     response was generated.
#
# THE FAILURE WAS ARM-ASYMMETRIC, WHICH IS WHY THIS IS A CONFOUND AND NOT JUST
# A BUG. The cap is nominally uniform, but it killed only arm B — the arm whose
# larger prompt induced the longest reasoning. A cap that preferentially kills
# whichever arm carries the most context biases the comparison this experiment
# exists to make.

# ── The uniform per-task runaway guard ──────────────────────────────────
#
# THIS WAS GOTTEN WRONG ONCE ALREADY, ONE LEVEL UP (N1): an earlier version
# derived `total_tokens_limit` from a dollar figure (`per_task_usd`). Cost is
# a REPORTED OUTCOME of this experiment ("arm B costs more per turn than arm
# A" is one of the findings it exists to produce), not a quantity to
# equalise across arms by capping it uniformly in dollars — dollars-per-turn
# is exactly the dimension that differs across arms (arm B's system prompt
# alone carries the full manual + payments-readme text on every single
# turn), so a dollar-derived cap produces a wildly non-uniform *iteration*
# budget and bites hardest on the arm carrying the most context. Measured:
# at a $1.00 dollar-derived cap, `gpt-5.6-sol` x `manual_prompt` cleared only
# ~16.4 of the ~26 requests `tool_calls_limit=25` implies needing — the
# uniform iteration control was not actually uniform once translated through
# a per-model dollar figure.
#
# The fix is to express the guard in the dimension it must not interfere
# with: tokens, sized off the iteration budget it must never bind before —
# not off a dollar figure at all.
#
# REQUEST_BUDGET: the iteration budget `tool_calls_limit=max_tool_calls`
# implies (its 25 tool-call turns plus one final answer-only turn), plus a
# flat margin.
REQUEST_BUDGET: int = MAX_TOOL_CALLS + 5

# MAX_ARM_FLOOR: the largest of the three arms' measured per-request input
# floors — arm B (`manual_prompt`), whose system prompt carries the manual +
# payments-readme text verbatim on every turn (~6,096 tokens measured;
# rounded up here). Sizing off the WORST arm, not an average or the
# cheapest, is what makes the guard actually uniform across arms: every arm
# and every model shares the one `TOKEN_BUDGET` below, so none is
# disadvantaged by carrying more context than another.
MAX_ARM_FLOOR: int = 6_100

# GROWTH: a request does not cost merely MAX_ARM_FLOOR again on every turn —
# the full conversation so far (every prior tool call and tool return) is
# resent as input on each request under this API family, so real
# consumption grows with turn count rather than staying flat at the floor.
# RAISED FROM 4x AFTER THE FIRST SMOKE RUN (F6). 4x was "a deliberately blunt
# safety multiplier", never measured. Measured growth is ARM-DEPENDENT, because
# a tool return's size is a property of the arm's tools:
#
#     schema_only     5,807 tokens/request   0.95x the 6,100 floor
#     manual_prompt  11,844 tokens/request   1.94x
#     contract       45,977 tokens/request   7.54x
#
# At GROWTH=4 the consequence was the exact confound N1 above claims to have
# removed: `TOKEN_BUDGET` (732,000) bound arm C at 22 tool calls while arm A ran
# to 28. The token guard — not `tool_calls_limit` — became the binding iteration
# control, and it bound EARLIEST FOR THE ARM CARRYING THE MOST CONTEXT. Moving
# the guard out of dollars and into tokens was necessary but not sufficient: a
# guard uniform in tokens is still non-uniform in iterations whenever growth
# differs by arm.
#
# Two changes restore the intended property. `dce/arms.py`'s `MAX_ROWS` (cut
# 1,000 -> 50) attacks arm C's growth at its root, and 12x here — the worst
# observed 7.54x with a 1.6x margin — sizes the guard so it cannot bind before
# `tool_calls_limit` does for any arm. `tool_calls_limit` is once again the
# single uniform iteration control; this is a runaway stop and nothing more.
GROWTH: int = 12

# TOKEN_BUDGET is the uniform runaway guard itself: sized off the iteration
# budget, the worst arm's floor, and a growth margin — not off a dollar
# figure, and not per-model. It is deliberately generous: `--max-spend` at
# the sweep level is the real money bound (arm-agnostic, unlike this), and a
# per-task guard exists only to stop one pathological task, not to budget
# the run. If it ever fires, that row is a visible `hit_limit`, inspectable
# and re-runnable at a higher budget — a far better failure mode than a
# guard so tight it fires during normal operation and gets mistaken for the
# thing under test being worse. See `_token_budget_usd` for what this
# implies in dollars per model — reported for visibility, not driving the
# limit.
TOKEN_BUDGET: int = REQUEST_BUDGET * MAX_ARM_FLOOR * GROWTH

# A quarter of `TOKEN_BUDGET` — N2's per-request input bound, uniform across
# every arm/model for the same reason `TOKEN_BUDGET` itself is. One
# oversized tool return can consume at most a quarter of the task's entire
# runaway-guard budget in a single step, rather than being allowed to
# consume the whole thing (or more, pre-N2) at once.
PER_REQUEST_INPUT_TOKEN_CAP: int = TOKEN_BUDGET // 4


def arm_digest(arm: str) -> str:
    """The digest of the contract artifact `arm` actually loads.

    `contract_hollow` loads the mechanically derived hollow contract, so its
    rows must be pinned to `hollow_digest()`. Stamping `digest()` on every row
    regardless -- which this harness did for runs A, B and C -- leaves arm D's
    rows carrying tamper-evidence for a file that arm never read, which is the
    one provenance claim the stamp exists to support. The ungoverned arms load
    no contract at all and keep the real digest as a record of which frozen
    experiment they belong to.
    """
    return hollow_digest() if arm == "contract_hollow" else digest()


def _tool_call_names(messages: list) -> list[str]:
    """Ordered tool names attempted during a run, from its message history.

    Takes the raw message list (e.g. from `capture_run_messages`), not an
    `AgentRunResult` — a run that raises never produces a result object, so
    reading `result.all_messages()` would lose this on exactly the paths
    (cap trips, errors) where the tool-call sequence matters most.

    Whether arm C actually called the lookup tools, or ignored them and went
    straight to SQL, is the behavioural half of the result — an arm C that
    never calls `lookup_metric` is not testing what we think it is.
    """
    names: list[str] = []
    for message in messages:
        for part in getattr(message, "parts", []):
            if getattr(part, "part_kind", "") == "tool-call":
                names.append(part.tool_name)
    return names


FORCED_ANSWER_PROMPT = (
    "You have run out of tool calls. Do not attempt any further tool calls.\n"
    "Using only the evidence already gathered above, state your single best "
    "final answer to the original question now, in exactly the format the "
    "answer guidelines require and with no other text. If the evidence is "
    "incomplete, still give your best estimate rather than refusing."
)

#: Requests the forcing turn is allowed. Two, not one: a model that emits a
#: tool call anyway (despite `toolsets=[]` leaving it none to call) burns a
#: request producing nothing, and one retry is enough to recover from that
#: without reopening an unbounded loop.
FORCED_ANSWER_REQUEST_LIMIT: int = 2


#: A rate-limited request is not a result. `provider.allow_fallbacks: false`
#: (F3) deliberately turns an unhonourable pin into an error rather than a
#: silent re-route to another endpoint — which is right for validity, and
#: leaves us owning the retry. Measured: the deepseek-v4-flash sweep ran clean
#: for 1,135 rows and was then cut off by its endpoint, recording 466 HTTP 429s
#: as terminal `error` rows and finishing "early" because a throttled request
#: fails instantly. 29% of that sweep was unusable for a transport condition
#: that resolves by waiting.
#:
#: Bounded on purpose. Backing off forever would convert a quota exhaustion
#: into a hung sweep, which is worse than a recorded failure: after these
#: attempts the row still lands as an `error` and the sweep moves on, exactly
#: as before.
RATE_LIMIT_RETRIES: int = 4
RATE_LIMIT_BACKOFF_S: tuple[float, ...] = (20.0, 60.0, 180.0, 420.0)


def _is_rate_limited(exc: BaseException) -> bool:
    """True for a transport-level 429 from the provider.

    Matched on the status code rather than the message: pydantic-ai wraps the
    provider's body verbatim, so the text varies by vendor while `429` does
    not. A non-429 transport error stays terminal — retrying a 400 just spends
    the same money four more times.
    """
    if getattr(exc, "status_code", None) == 429:
        return True
    return "status_code: 429" in str(exc)


def _trim_dangling_tool_calls(messages: list) -> list:
    """Drop trailing messages whose tool calls were never answered.

    A cap trips mid-turn, so the captured transcript usually ends with a
    `ModelResponse` carrying `ToolCallPart`s that never ran. Sending that back
    as history is rejected by the provider — every tool call must have a
    matching result — so the forcing turn would fail for a reason that has
    nothing to do with the model. Trimming to the last complete exchange is
    what makes the transcript resendable.

    Returns a new list; never mutates the caller's.
    """
    answered = {
        getattr(part, "tool_call_id", None)
        for message in messages
        for part in getattr(message, "parts", [])
        if getattr(part, "part_kind", "") in ("tool-return", "retry-prompt")
    }
    trimmed = list(messages)
    while trimmed:
        parts = getattr(trimmed[-1], "parts", [])
        dangling = [
            part
            for part in parts
            if getattr(part, "part_kind", "") == "tool-call"
            and getattr(part, "tool_call_id", None) not in answered
        ]
        if not dangling:
            break
        trimmed.pop()
    return trimmed


def _tool_less_twin(agent):
    """An `Agent` on the same model and settings, with no tools at all.

    `run_sync(toolsets=[])` is NOT sufficient, which is the trap here: it
    clears only the EXTRA toolsets passed at run time, leaving the agent's own
    tools — the ones registered by `Agent(tools=...)`, which is how every arm
    here is built — fully callable. Measured against a real `Agent`: with
    `toolsets=[]` the model still emitted a `list_tables` call and the forcing
    turn died on the tool-call limit instead of answering. A fake agent cannot
    show this, because a fake has no toolset machinery to leave behind.

    Falls back to the agent itself if a twin cannot be built (a test double
    with no `.model`); the caller passes `toolsets=[]` either way, so the
    fallback is no worse than the naive approach and never raises.
    """
    try:
        from pydantic_ai import Agent

        return Agent(agent.model, model_settings=agent.model_settings)
    except Exception:
        return agent


def _force_final_answer(agent, messages: list, usage) -> str:
    """Re-ask once, with no tools, for the answer the run never committed to.

    THE PROBLEM THIS SOLVES (F2). A `hit_limit` row is unscoreable: the run is
    paid for in full and contributes nothing to accuracy. On the first hard task
    of the first smoke run, two of three arms ended exactly there with an empty
    answer. Raising `MAX_TOOL_CALLS` does not fix this — a larger budget still
    terminates in the same empty row when it runs out.

    The agent has usually done most of the work by the time a cap trips; what it
    has not done is commit to an answer. So we hand back the transcript it built
    and ask for the answer with `toolsets=[]` — no tools to call, nothing to
    explore, one thing left to do.

    `usage` is the SAME `RunUsage` the main run mutated, so this turn's tokens
    are billed onto the row rather than vanishing. Applied identically to every
    arm, and recorded on the row as `forced_answer`, so no analysis can mistake
    a forced answer for one the model volunteered.

    Returns the answer, or `""` if this turn cannot produce one. It is strictly
    a recovery path: every failure here leaves the caller's `hit_limit` verdict
    exactly as it was, so the forcing turn can only improve a row, never
    corrupt one.
    """
    import warnings

    from pydantic_ai.exceptions import CostNotFoundWarning
    from pydantic_ai.usage import UsageLimits

    history = _trim_dangling_tool_calls(messages)
    if not history:
        return ""
    runner = _tool_less_twin(agent)
    try:
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", category=CostNotFoundWarning)
            result = runner.run_sync(
                FORCED_ANSWER_PROMPT,
                message_history=history,
                # Belt and braces alongside `_tool_less_twin`: `toolsets=[]`
                # alone does NOT do this (see that function's docstring).
                toolsets=[],
                usage=usage,
                # BOTH LIMITS ARE OFFSETS, NOT ABSOLUTES. Passing the main
                # run's `usage` is what bills this turn onto the row, but
                # pydantic-ai checks limits against that SAME cumulative
                # object — so a bare `request_limit=2` is compared to the
                # requests the main run already spent and trips before this
                # turn ever reaches the model. (Measured: it raised
                # "next request would exceed the request_limit of 2" with
                # `usage.requests` already at 4, and the forcing turn made no
                # call at all. Only a real `Agent` run surfaces this; a fake
                # agent never accumulates usage the limits can collide with.)
                usage_limits=UsageLimits(
                    # No FURTHER tool calls beyond those already made.
                    tool_calls_limit=usage.tool_calls,
                    request_limit=usage.requests + FORCED_ANSWER_REQUEST_LIMIT,
                    per_request_input_tokens_limit=PER_REQUEST_INPUT_TOKEN_CAP,
                ),
            )
        return str(result.output).strip()
    except Exception:
        # Deliberately broad. This runs after a cap has ALREADY tripped, on a
        # row that is already priced and already returnable; a failure here
        # must degrade to the `hit_limit` the caller was about to record, not
        # replace it with an exception.
        return ""


def _inspect_rejections(messages: list) -> int:
    """Count `inspect_query` calls that reported a query as invalid.

    `ContractSession` (agentic_data_contracts.core.session) tracks no counter
    for this. `session.retries` only increments from `run_query`'s own
    blocked/errored paths (`tools/factory.py`, `run_query`'s three
    `session.record_retry()` call sites); `inspect_query` deliberately never
    calls `record_retry` — its handler treats reporting that a query WOULD be
    blocked as a completed inspection, not a governance block of the
    `inspect_query` call itself, and returns an "ok"-outcome payload
    (`{"valid": false, "violations": [...], ...}`) even when the SQL it
    inspected is invalid. So `ContractSession` genuinely does not expose an
    `inspect_query` rejection count under any name — there is no
    `rejected_queries` attribute, and no other counter tracks it either.

    This is advice, not enforcement — see `run_task`'s `enforcement_blocks`
    for the event where the contract actually stopped a query. A model can
    skip `inspect_query` entirely and still have every `run_query` call
    blocked; this counter would then read 0 for a task the contract worked
    hard on, which is why `run_task` also records `enforcement_blocks` and
    `retry_prompts` rather than treating this number as the whole story.

    The only place this information survives is the tool's own JSON return
    value, so this walks the run's message history for `inspect_query`
    tool-return parts and counts the ones whose parsed payload has
    `"valid": false` (strictly the boolean, not merely falsy — a missing,
    `null`, or `0` `valid` field is not a rejection, just an unreadable one).
    Arms without an `inspect_query` tool (`schema_only`, `manual_prompt`)
    never produce such a part, so this is naturally 0 for them without
    special-casing the arm. Matching on `tool_name`, not on the content
    string, means an unrelated tool (`execute_sql`, `run_query`, ...) whose
    output happens to *contain* the literal text `"valid": false` — in a
    column value, say — is never miscounted.
    """
    count = 0
    for message in messages:
        for part in getattr(message, "parts", []):
            if getattr(part, "part_kind", "") != "tool-return":
                continue
            if getattr(part, "tool_name", "") != "inspect_query":
                continue
            content = getattr(part, "content", None)
            if not isinstance(content, str):
                continue
            # A malformed inspect_query payload is not a shape this counter
            # understands, and the whole point of this function is to be the
            # one place that number comes from — silently coding it as "not
            # rejected" here would delete the library's own headline metric
            # without a trace, so only a genuine parse failure is tolerated
            # (an inspect_query response is not always bare JSON in general,
            # see `_truncate_run_query` in `dce/arms.py` for the sibling case
            # on `run_query`), and everything else propagates.
            try:
                data = json.loads(content)
            except (TypeError, ValueError):
                continue
            if isinstance(data, dict) and data.get("valid") is False:
                count += 1
    return count


def _retry_prompt_count(messages: list) -> int:
    """Count `retry-prompt` parts — the event pydantic-ai actually sent a
    tool's rejection back to the model as a retryable failure.

    This is closer to enforcement than `_inspect_rejections` is: a
    `retry-prompt` fires from a real `ModelRetry` (raised whenever a governed
    tool's response starts with `BLOCKED —`, `tools/pydantic_ai.py`'s
    `_to_pydantic_ai_tool`), or from pydantic-ai's own tool-argument
    validation — so it is not identical to `run_task`'s `enforcement_blocks`
    (`ContractSession.retries`, which only counts `run_query`'s three
    contract-specific blocked paths). The two numbers can differ; both are
    recorded rather than treated as interchangeable.
    """
    count = 0
    for message in messages:
        for part in getattr(message, "parts", []):
            if getattr(part, "part_kind", "") == "retry-prompt":
                count += 1
    return count


def _commit_sha() -> str:
    try:
        return subprocess.check_output(
            ["git", "rev-parse", "--short", "HEAD"],
            text=True,
            # Pin the working directory this call runs in — `subprocess.run`
            # otherwise inherits the caller's process CWD, which for a
            # library module is whatever directory happened to invoke the
            # runner, not necessarily this repo. Anchoring to this file's own
            # location means the stamped commit is always *this* repo's HEAD.
            cwd=Path(__file__).resolve().parent,
        ).strip()
    except Exception:
        return "unknown"


def _assert_cost_limit_is_unenforceable_for_pinned_models() -> None:
    """Lock in, as a startup check rather than a comment, that `UsageLimits`'
    `cost_limit` cannot be trusted for any of our four pinned model ids.

    pydantic-ai derives `RunUsage.cost` from `genai-prices`, which raises
    `LookupError` for every one of `dce.pricing.MODELS`' ids under the
    `openrouter` provider (verified directly, and re-verified here on every
    import): none of them are real OpenRouter model snapshots, being pinned
    synthetic ids for this experiment (see `dce/pricing.py`'s module
    docstring). pydantic-ai's own response to that lookup failure is a
    `CostNotFoundWarning`, not an error — `cost_limit` then silently never
    fires, and a pathological task can spend arbitrarily far past
    `per_task_usd`. `run_task` works around this with the fixed, token-based
    `TOKEN_BUDGET` (above) as the real runaway guard, rather than trusting
    `cost_limit` to do anything.

    This assertion is the tripwire for that assumption changing: if a future
    `genai-prices` release ever adds pricing data for one of these ids,
    `cost_limit` would start being enforced for *that* model while staying
    dead for the other three — a brand-new asymmetry across arms/models that
    `TOKEN_BUDGET` was sized without accounting for. Raising loudly here,
    once, at import time, means that day is a deliberate code change instead
    of a silent shift in what "capped at `per_task_usd`" actually means.

    A tripwire must never be able to bring down the thing it is watching.
    `genai-prices` is only a *transitive* dependency (pulled in by
    `pydantic-ai-slim`, not declared here), so importing it at module scope
    without a fallback would make `import dce.agent` — and therefore the
    whole sweep — fail outright the day a pydantic-ai release drops it or
    renames it. A missing `genai_prices` is silently treated as "cannot
    disprove `TOKEN_BUDGET`'s reason for existing", not as this assertion's
    problem to raise about. Likewise, `calc_price`'s failure mode is only
    documented as `LookupError` today; catching bare `Exception` here means
    a future release raising some other exception type for the same "can't
    price this" case still reads as "still unpriceable" rather than crashing
    every import — only a genuinely *successful*, exception-free
    `calc_price` call is treated as news worth stopping the run for.
    """
    try:
        import genai_prices
    except ImportError:
        return

    for model_id in MODELS:
        try:
            genai_prices.calc_price(
                genai_prices.Usage(input_tokens=1, output_tokens=1),
                model_ref=model_id,
                provider_id="openrouter",
            )
        except Exception:
            continue
        raise AssertionError(
            f"GOOD NEWS, ACTION NEEDED: genai-prices can now price "
            f"{model_id!r}. This is not a breakage — it means pydantic-ai's "
            "cost_limit would start being genuinely enforced for this model "
            "(it was previously a silent no-op; see this function's "
            "docstring), while staying a no-op for the rest of "
            "dce.pricing.MODELS until they are priced too. TOKEN_BUDGET's "
            "conservative token-based guard may now be redundant for this "
            "model — re-evaluate whether cost_limit alone is now trustworthy "
            "for it before continuing to layer TOKEN_BUDGET on top, and "
            "update this assertion (or MODELS) once you have."
        )


_assert_cost_limit_is_unenforceable_for_pinned_models()


def _token_budget_usd(model: str) -> float:
    """What `TOKEN_BUDGET` costs in dollars for `model`, at its more
    expensive rate — reported for visibility (stamped nowhere enforced;
    `cost_limit` cannot be trusted, see
    `_assert_cost_limit_is_unenforceable_for_pinned_models`), not used to
    derive the limit itself. `TOKEN_BUDGET` is fixed and uniform across
    every arm and model *precisely because* a dollar-derived guard once
    produced a wildly non-uniform iteration budget across arms (see
    `TOKEN_BUDGET`'s module-level comment) — this function exists only to
    answer "what would that fixed token guard cost on this model", not to
    feed back into sizing it.

    Priced off `max(price_in, price_out)` — the MORE EXPENSIVE of a model's
    two per-token rates — for the same reason `TOKEN_BUDGET`'s sizing does
    not privilege one arm: `total_tokens_limit` bounds input and output
    tokens together, undifferentiated, and output is the pricier side for
    every pinned model, so this is the true worst-case dollar figure the
    guard implies, not an optimistic one.
    """
    spec = MODELS[model]
    rate = max(spec.price_in, spec.price_out)
    return TOKEN_BUDGET * rate / 1_000_000


#: Absolute last-resort price when even `_token_budget_usd(model)` can't be
#: computed (an unrecognized model id) — the worst case ACROSS EVERY pinned
#: model, computed once off `MODELS` rather than hand-picked, so
#: `_priced_fallback_row`'s innermost fallback can never itself raise
#: (`MODELS[model]` would be the thing raising) and never has to fall back
#: further, to `$0.00` — the exact failure mode this whole guarded-tail
#: mechanism exists to eliminate.
WORST_CASE_TOKEN_BUDGET_USD: float = max(_token_budget_usd(m) for m in MODELS)


def build_result_row(
    *,
    task: dict,
    arm: str,
    model: str,
    answer: str,
    answer_normalized: str,
    gold: str,
    verdict: str,
    forced_answer: bool,
    trace_path: str | None,
    reasoning_tokens: int,
    in_tok: int,
    out_tok: int,
    cached_tok: int,
    turns: int,
    tool_calls: list[str],
    inspect_rejections: int,
    enforcement_blocks: int,
    retry_prompts: int,
    request_limit: int,
    token_cap: int,
    golds_hash: str,
) -> dict:
    return {
        "task_id": task["task_id"],
        "level": task.get("level", "unknown"),
        "arm": arm,
        "model": model,
        "answer": answer,
        # `dce.grade._clean(answer)` — the same bracket/quote-stripping,
        # whitespace-trimmed form `score()` itself starts from, alongside
        # the raw model answer. Not `score()`'s full comparison (which does
        # further numeric/list normalization on top of this), but enough to
        # see what the scorer actually started from and re-adjudicate a
        # scoring dispute from the stored row alone, without re-running the
        # model.
        "answer_normalized": answer_normalized,
        "gold": gold,
        "verdict": verdict,
        # F2: True when the answer came from `_force_final_answer` — a
        # tool-less turn taken after a cap tripped — rather than from the run
        # itself. Recorded because a forced answer is a different measurement
        # from a volunteered one, and an analysis that pooled the two silently
        # would be comparing two things under one name.
        "forced_answer": forced_answer,
        # Where this run's full transcript landed, relative to the traces
        # root — or None when traces are off or the write failed. A row is
        # the claim; the trace is the evidence behind it.
        "trace_path": trace_path,
        # F3 — the pinned endpoint and the reasoning effort in force, so a
        # results file says which model was actually served rather than only
        # which id was requested. THE OBSERVED PROVIDER IS DELIBERATELY NOT
        # RECORDED: OpenRouter returns it only in the response body, which
        # pydantic-ai discards (`ModelResponse.provider_name` is the string
        # "openrouter", and no response header carries it). It is not needed,
        # because `allow_fallbacks: False` makes the pin self-verifying —
        # verified live, an unhonourable pin returns HTTP 404 through
        # pydantic-ai rather than silently re-routing, so a row that exists at
        # all is a row the pin held for.
        "provider_tag": MODELS[model].provider_tag,
        "quantization": MODELS[model].quantization,
        "reasoning_effort": reasoning_effort_for(model),
        # Reasoning tokens actually spent — a subset of `output_tokens`, and
        # billed at the output rate, which is the pricier one.
        #
        # This varies enormously BY MODEL on the tool-calling path, so it is
        # measured per row rather than assumed. On one hard task (1480, arm C)
        # with tools bound:
        #
        #     glm-5.3-flash          ~20 reasoning tokens   (near zero)
        #     deepseek-v4-pro-0813  1,096 reasoning tokens  (49% of output)
        #
        # A trivial probe once suggested reasoning was absent entirely under
        # tool use; that was an artifact of a prompt too easy to need any.
        # Given a question that requires thought, every pinned model emits
        # reasoning alongside its tool calls, pydantic-ai surfaces it as a
        # `ThinkingPart`, and `dce.trace` preserves it. For `deepseek-v4-pro`
        # that is ~4.6 KB of the model's own reasoning per task — the single
        # most useful thing in a transcript for diagnosing a wrong answer.
        "reasoning_tokens": reasoning_tokens,
        "input_tokens": in_tok,
        "output_tokens": out_tok,
        "cached_tokens": cached_tok,
        "turns": turns,
        # Cache-aware since F4: `cached_tok` is priced at the pinned
        # endpoint's cache-read rate, the rest at the fresh-input rate. See
        # `dce.pricing.cost` for why charging everything fresh was not a
        # harmless over-estimate but a bias against the longest-context arm.
        "usd": cost(model, in_tok, out_tok, cached_tok),
        # Mirrors `usd` here — a real, priceable call happened, so there is
        # no gap between what was really spent and what the sweep's spend
        # cap should count against. `usd_guard` only diverges from `usd`
        # for `dce.runner._construction_error_row`, where a real call never
        # happened at all but the cap still charges a pessimistic ceiling
        # (see that function's docstring). `spent_so_far` sums THIS field;
        # `real_spent_so_far` sums `usd`.
        "usd_guard": cost(model, in_tok, out_tok, cached_tok),
        "tool_calls": tool_calls,
        "inspect_rejections": inspect_rejections,
        "enforcement_blocks": enforcement_blocks,
        "retry_prompts": retry_prompts,
        "request_limit": request_limit,
        # The `total_tokens_limit` in force for this task — `TOKEN_BUDGET`,
        # fixed and uniform across every arm/model (see its module-level
        # comment) — recorded so a `hit_limit` row shows which limit
        # actually bound (`tool_calls_limit`, `request_limit`, or this one)
        # without re-deriving it by hand.
        "token_cap": token_cap,
        "contract_digest": arm_digest(arm),
        "golds_hash": golds_hash,
        # WHICH scorer produced `verdict`. `dce.grade` prefers DABStep's
        # official `question_scorer` when `dabstep_benchmark` is importable
        # and falls back to its own normalizer otherwise; that decision is
        # made by whatever is installed at import time, so without this
        # field a mid-experiment install would re-grade later rows under
        # different rules with nothing in the results file to show it.
        "scorer": active_scorer(),
        "commit_sha": _commit_sha(),
        "adc_version": version("agentic-data-contracts"),
    }


class AgentConstructionError(RuntimeError):
    """Raised by `run_task` only when CONSTRUCTION failed — either
    `build_arm` (`dce/arms.py`) or the `agent_factory`/
    `_default_agent_factory` call (e.g. a missing `OPENROUTER_API_KEY`) —
    before any billable model call was made.

    This is deliberately the ONLY exception `run_task` still lets escape.
    Everything after the agent exists — the model call, scoring,
    transcript bookkeeping, `build_result_row` itself — is guarded and
    returns a priced fallback row instead of raising (see
    `_priced_fallback_row`), because by that point real, non-refundable
    spend may already have happened. `dce.runner.sweep` catches exactly
    this type (and nothing else) to build a genuinely-free
    `construction_error` row on resume; any other exception is a real bug
    in this module and must stop the sweep loudly rather than being
    silently swallowed as if it were as cheap as a missing API key.
    """


#: The `RunUsage.details` keys that carry reasoning tokens, in priority order.
#:
#: THE KEY IS PROVIDER-SPECIFIC, AND GETTING THIS WRONG IS SILENT. OpenAI-shaped
#: responses report `reasoning_tokens`; Anthropic's Messages API reports
#: `thinking_tokens` for the same quantity. Reading only the first name records
#: a confident `0` on every Anthropic row — not an error, not a warning, just a
#: column that looks like "this model does not reason" while the model reasons
#: and bills for it at the output rate. Measured on `claudesonnet5` through the
#: gateway with this module's own factory: 645 thinking tokens on a single
#: three-request run, recorded as 0 before this fix.
_REASONING_TOKEN_KEYS: tuple[str, ...] = ("reasoning_tokens", "thinking_tokens")


def _reasoning_tokens(usage) -> int:
    """Reasoning tokens accumulated over a run, or 0 when unreported.

    pydantic-ai keeps this in `RunUsage.details`, not as a first-class field,
    so it is read defensively: a provider that omits it, or a future release
    that renames the key, must yield 0 rather than break a paid row.

    Both known spellings are tried (see `_REASONING_TOKEN_KEYS`). They are
    summed rather than first-wins so a provider that ever reported both could
    not have half its reasoning silently dropped; in practice exactly one is
    present, and a missing key contributes nothing.
    """
    try:
        details = getattr(usage, "details", None) or {}
        return sum(int(details.get(key, 0) or 0) for key in _REASONING_TOKEN_KEYS)
    except Exception:
        return 0


def _spec_field(model: str, field: str) -> str:
    """A `ModelSpec` field, or "unknown" for an unpinned id.

    Used only from `_priced_fallback_row`, which is the last line of defense
    inside `run_task`'s own exception handler and must not be able to raise —
    a bare `MODELS[model]` lookup would defeat that for exactly the exotic
    unknown-model case the fallback exists to survive.
    """
    spec = MODELS.get(model)
    return getattr(spec, field, "unknown") if spec is not None else "unknown"


def reasoning_effort_for(model: str) -> str:
    """The `reasoning_effort` to stamp on a row for `model`.

    Every route now genuinely sends `REASONING_EFFORT`, so this returns it
    unconditionally — but through DIFFERENT PARAMETERS, which is why the
    function exists rather than the constant being inlined again. OpenRouter
    takes it as `reasoning.effort` in `extra_body`; Anthropic's Messages API
    takes it as `anthropic_effort` alongside adaptive thinking. The stamp
    records the effort in force, which is the same on both, and the mechanism
    is documented per-route in the two factories.

    An earlier revision stamped a `"unset:anthropic-default"` sentinel here,
    on the then-true belief that the Anthropic route could not be given an
    effort at all. It could; the sentinel would have recorded a real control
    as absent. Kept as a function so the next route that genuinely cannot
    honour the setting has somewhere to say so.
    """
    return REASONING_EFFORT


def _priced_fallback_row(
    *,
    task: dict,
    arm: str,
    model: str,
    gold: str,
    golds_hash: str,
    usage,
    verdict: str,
    note: str,
) -> dict:
    """A `build_result_row`-shaped row for when something AFTER the model
    call itself failed: transcript/session bookkeeping, scoring's own
    plumbing (not `score()` itself — that already has its own
    `scoring_error` verdict, see `run_task`), or `build_result_row`
    construction. By this point `agent.run_sync` has already run (or
    tripped a cap) and mutated `usage` in place with real counts — exactly
    the mechanism CR1 (commit 262e810) relies on for the cap-trip path —
    so this must not report a $0.00 / 0-token row for spend that already
    happened. This module's own history is the reason this exists: commit
    a2c5d1d was exactly this class of bug (`result.usage()` instead of the
    property) sitting in this same post-call region, and a future
    pydantic-ai rename (`cache_read_tokens`, say) would reproduce it
    verbatim without this backstop.
    """
    in_tok = getattr(usage, "input_tokens", 0) or 0
    out_tok = getattr(usage, "output_tokens", 0) or 0
    try:
        usd = cost(model, in_tok, out_tok, getattr(usage, "cache_read_tokens", 0) or 0)
    except Exception:
        # Can't even price it off real counts: charge the pessimistic
        # ceiling rather than $0.00, so an unknown-cost failure consumes
        # budget instead of reading as free. Caps the blast radius even if
        # the guards above this one regress later.
        try:
            usd = _token_budget_usd(model)
        except Exception:
            # `model` itself can't be priced at all (an unrecognized id) —
            # `_token_budget_usd`'s own `MODELS[model]` lookup just failed
            # the same way `cost`'s did. `WORST_CASE_TOKEN_BUDGET_USD` is
            # computed off `MODELS` directly rather than off `model`, so
            # THIS branch cannot itself raise — the exact value A1 exists
            # to eliminate is `$0.00` here, not an untested fallback that
            # might still be it.
            usd = WORST_CASE_TOKEN_BUDGET_USD
    return {
        "task_id": task["task_id"],
        "level": task.get("level", "unknown"),
        "arm": arm,
        "model": model,
        "answer": "",
        "answer_normalized": "",
        "gold": gold,
        "verdict": verdict,
        # Always False: this row exists because the bookkeeping tail raised,
        # and it carries no answer at all ("answer" is "" above), so there is
        # no answer here for the forcing turn to have produced. Present so
        # every row in a results file has the same keys.
        "forced_answer": False,
        # The tail raised before a trace path was known; the trace itself may
        # still exist on disk under `trace_name(task_id, arm, model)`.
        "trace_path": None,
        "provider_tag": _spec_field(model, "provider_tag"),
        "quantization": _spec_field(model, "quantization"),
        "reasoning_effort": reasoning_effort_for(model),
        "reasoning_tokens": _reasoning_tokens(usage),
        "input_tokens": in_tok,
        "output_tokens": out_tok,
        "cached_tokens": getattr(usage, "cache_read_tokens", 0) or 0,
        "turns": getattr(usage, "requests", 0) or 0,
        # A real (partial) call did happen here, unlike a construction
        # error — `usd_guard` mirrors `usd`, pessimistic fallback and all,
        # since there is no independently-known "real" figure to diverge
        # from once `cost()` itself couldn't be trusted.
        "usd": usd,
        "usd_guard": usd,
        "tool_calls": [],
        "inspect_rejections": 0,
        "enforcement_blocks": 0,
        "retry_prompts": 0,
        "request_limit": REQUEST_LIMIT,
        "token_cap": TOKEN_BUDGET,
        "contract_digest": arm_digest(arm),
        "golds_hash": golds_hash,
        # The scorer in force for this process — see `build_result_row`.
        # Present on every row shape so the analysis has one uniform
        # column rather than a field that appears only on some rows.
        "scorer": active_scorer(),
        "commit_sha": _commit_sha(),
        "adc_version": version("agentic-data-contracts"),
        "note": note,
    }


def _litellm_anthropic_agent(
    *, model: str, system_prompt: str, tools: list, retries: int
):
    """Build the agent for a `route="litellm_anthropic"` model.

    A SECOND WIRE PROTOCOL, NOT A SECOND BASE URL. These models are Anthropic
    models reached through an enterprise LiteLLM gateway, and they are
    built on `AnthropicModel` (the Messages API) rather than `OpenAIChatModel`.
    The gateway does expose an OpenAI-compatible `/v1/chat/completions` route
    that works, and taking it would have been a one-line change — it was
    measured and rejected, for a reason that is about validity and not
    convenience:

    **Anthropic prompt caching requires explicit `cache_control` breakpoints**,
    which OpenRouter injects for Anthropic models and the gateway's
    OpenAI-compatible route does not. Measured over that route: zero
    `cache_read_input_tokens` on every request. Replaying `results/sol-full.jsonl`
    — the same $2/$10 rates as this model — with every input token billed fresh
    puts a full sweep at $196.63 against $72.36, and, fatally, the penalty is
    ARM-DEPENDENT: 1.86x on `schema_only` against 3.40x on `manual_prompt`. A
    cost column skewed by a factor that varies with how much context an arm
    carries measures the route's caching policy rather than the arms — which is
    precisely the confound `README.md`'s smoke-run check exists to catch, and
    it would have been baked in rather than merely observed. Verified on this
    route instead: 5,677 cache-read tokens within one run.

    THREE PARAMETERS THE OPENROUTER PATH SENDS ARE NOT SENT HERE. The gateway
    fronts Bedrock, which rejects `temperature` at anything but 1 (HTTP 400,
    "Only temperature=1 is supported"), rejects `seed` outright ("bedrock does
    not support parameters: ['seed']"), and rejects OpenRouter's nested
    `reasoning: {effort: ...}` body ("Extra inputs are not permitted"). The
    first two mean BOTH of this harness's determinism controls are unavailable
    for this model and its runs are correspondingly less reproducible than the
    other four — a caveat for any writeup, stamped on every row via
    `supports_temperature`. The third is replaced by a native thinking budget.

    Everything the arms share — `retries`, the token guard, the tool-call cap,
    `max_tokens` — is held identical to the OpenRouter path, because those are
    the controls that keep the arm comparison honest and they are not
    route-specific.
    """
    from anthropic import AsyncAnthropic
    from httpx2 import AsyncClient
    from pydantic_ai import Agent
    from pydantic_ai.models.anthropic import AnthropicModel, AnthropicModelSettings
    from pydantic_ai.providers.anthropic import AnthropicProvider

    base_url = os.environ["LITELLM_BASE_URL"]
    api_key = os.environ["LITELLM_MASTER_KEY"]

    return Agent(
        AnthropicModel(
            model,
            provider=AnthropicProvider(
                anthropic_client=AsyncAnthropic(
                    api_key=api_key,
                    base_url=base_url,
                    # The gateway is an internal host behind the enterprise CA,
                    # which is not in certifi's store — so an unconfigured
                    # client fails with "unable to get local issuer
                    # certificate". `SSL_CERT_FILE` (certifi's bundle
                    # concatenated with the enterprise root) is the documented
                    # setup; see README. Not disabling verification: this
                    # request carries a live gateway key.
                    #
                    # `max_retries` is left at the Anthropic SDK's default
                    # rather than pinned to 0. The OpenRouter path inherits the
                    # OpenAI SDK's default retries the same way, and a sweep
                    # that turned transient 429s into terminal rows on ONE
                    # route would differ from the others in how often it loses
                    # paid work -- an asymmetry in the harness, not in the
                    # thing under test.
                    http_client=AsyncClient(timeout=300),
                )
            ),
        ),
        system_prompt=system_prompt,
        tools=tools,
        # Identical to the OpenRouter path — see its comment. A confound
        # control, not a robustness knob, and it must not vary by route.
        retries=retries,
        model_settings=AnthropicModelSettings(
            timeout=300,
            # Same bound, same reason as the OpenRouter path. This model's
            # ceiling is 128k, so the shared 64k value is genuinely available
            # to it rather than being silently clamped.
            max_tokens=MAX_OUTPUT_TOKENS_PER_REQUEST,
            # The two settings that make caching actually happen. NOT
            # `anthropic_cache` — pydantic-ai's automatic mode places
            # breakpoints differently; these two mark the instructions and the
            # trailing message block, which is the pairing verified to produce
            # cache reads both within and across runs.
            anthropic_cache_instructions=True,
            anthropic_cache_messages=True,
            # THE SAME "medium" THE OTHER FOUR MODELS RUN AT, reached through
            # this API's own knob rather than OpenRouter's. Anthropic has no
            # `reasoning_effort`; `anthropic_effort` is the equivalent, and it
            # takes the same scale, so this is a translation of
            # `REASONING_EFFORT` rather than a newly invented number -- which
            # is why no `budget_tokens` is guessed at here.
            #
            # Not optional, for the reason `REASONING_EFFORT`'s own comment
            # gives: an unset parameter is not a fixed parameter. Measured
            # unpinned, this model reasoned anyway (645 thinking tokens on one
            # three-request run) at whatever default Anthropic ships that day,
            # which is exactly the invisible drift the pinning exists to stop.
            #
            # `adaptive` mirrors how a production agent runs against this
            # same gateway, where the pairing was validated on reasoning-hard
            # cases; without it the model reasons shallowly.
            # `display` IS LOAD-BEARING AND ITS DEFAULT CHANGED UNDER US.
            # On Sonnet 5 (and the Opus 5 generation) `display` defaults to
            # "omitted", which returns `thinking` blocks with EMPTY text and a
            # signature only — the reasoning still happens and is still billed,
            # it is simply not shown. On Sonnet 4.6 and earlier the default was
            # "summarized", so a trace captured from an older model contains
            # reasoning and one captured here would not, with nothing in the
            # data to say why.
            #
            # Measured on this gateway, same prompt, same model: 0 characters
            # of thinking at the default, 456 with "summarized". Billing is
            # identical either way — `display` controls visibility only.
            #
            # This is a SUMMARY, not the raw chain of thought, which no current
            # model exposes. It is what `dce/trace.py` needs to answer its
            # founding question — did the agent retrieve the rule and ignore
            # it, or never retrieve it — which the tool-call record alone
            # cannot separate.
            anthropic_thinking={"type": "adaptive", "display": "summarized"},
            anthropic_effort=REASONING_EFFORT,
        ),
    )


def _default_agent_factory(
    *, model: str, system_prompt: str, tools: list, retries: int
):
    from pydantic_ai import Agent
    from pydantic_ai.models.openai import OpenAIChatModel
    from pydantic_ai.providers.openrouter import OpenRouterProvider
    from pydantic_ai.settings import ModelSettings

    spec = MODELS[model]
    if spec.route == "litellm_anthropic":
        return _litellm_anthropic_agent(
            model=model, system_prompt=system_prompt, tools=tools, retries=retries
        )
    return Agent(
        OpenAIChatModel(
            model, provider=OpenRouterProvider(api_key=os.environ["OPENROUTER_API_KEY"])
        ),
        system_prompt=system_prompt,
        tools=tools,
        # RETRIES ARE A CONFOUND CONTROL, NOT A ROBUSTNESS KNOB. The ungoverned
        # arms return "ERROR: ..." as an ordinary tool result and keep iterating;
        # arm C's governed tools raise ModelRetry, and pydantic-ai's DEFAULT tool
        # retry budget is 1 — so arm C dies with UnexpectedModelBehavior after two
        # bad queries while arm A iterates freely. Measured: arm A finished after
        # 7 model calls, arm C raised after 2. Set it high enough that arm C is
        # never budget-limited relative to arms A/B, and identically for all arms.
        #
        # `retries` is threaded in from `run_task`'s effective `max_tool_calls`
        # rather than read off the `MAX_TOOL_CALLS` module constant: a caller
        # that overrides `max_tool_calls` changes the cap actually in force,
        # and this budget must track that cap, not the default it started at,
        # or a caller doing exactly that silently reinstates the confound
        # this comment describes.
        retries=retries,
        # N2: bounds one model turn's OUTPUT tokens — previously unset, so a
        # single response (reasoning tokens included) was unbounded between
        # the between-request `total_tokens_limit` checks. See
        # `MAX_OUTPUT_TOKENS_PER_REQUEST`'s module-level comment.
        model_settings=ModelSettings(
            # `temperature` is DELIBERATELY NOT SET HERE — see `extra_body`
            # below. `seed` is safe: pydantic-ai's `SAMPLING_PARAMS` strip
            # covers temperature/top_p/penalties/logit_bias/logprobs, not seed.
            seed=0,
            timeout=300,
            max_tokens=MAX_OUTPUT_TOKENS_PER_REQUEST,
            # F3: pin the serving ENDPOINT, not merely the model id, and pin
            # the reasoning effort. `allow_fallbacks: False` is the load-
            # bearing half — verified against the live API to return HTTP 404
            # rather than silently re-routing when the pin cannot be honoured,
            # so a pin that stops being available fails loudly instead of
            # quietly changing the model mid-sweep.
            # `extra_body` carries the fields pydantic-ai will not send for
            # us: OpenRouter-specific routing, the reasoning effort, and —
            # unavoidably — temperature.
            #
            # WHY TEMPERATURE IS HERE AND NOT ABOVE. pydantic-ai silently
            # STRIPS `ModelSettings(temperature=...)` for any model whose
            # profile has reasoning enabled, warning "Sampling parameters
            # ['temperature'] are not supported when reasoning is enabled".
            # That rule comes from OpenAI's own reasoning models; it is wrong
            # for these OpenRouter models, whose cards list `temperature` and
            # `reasoning` as simultaneously supported. Reasoning cannot be
            # turned off to dodge it either (HTTP 400, "Reasoning is mandatory
            # for this endpoint"). So `temperature=0.0` in `ModelSettings` was
            # inert for EVERY model in this experiment — every run so far was
            # at the provider's default sampling temperature.
            #
            # `extra_body` bypasses the strip; verified against the live API,
            # where a deliberately out-of-range value came back as
            # "Expected temperature to be at most 2, received 99" rather than
            # being dropped.
            extra_body={
                "provider": {
                    "order": [spec.provider_tag],
                    "allow_fallbacks": False,
                },
                "reasoning": {"effort": REASONING_EFFORT},
                **({"temperature": 0.0} if spec.supports_temperature else {}),
            },
        ),
    )


def run_task(
    task: dict,
    arm: str,
    model: str,
    db_path: Path,
    docs: dict[str, str],
    gold: str,
    *,
    golds_hash: str,
    max_tool_calls: int = MAX_TOOL_CALLS,
    # No longer drives the real runaway guard (see `TOKEN_BUDGET`'s
    # module-level comment — deriving that guard from a dollar figure was
    # tried and found to reintroduce a non-uniform iteration budget across
    # arms). Kept only as the documented *intent* fed to the inert
    # `cost_limit` below, for whenever a pinned model ever does become
    # priceable.
    per_task_usd: float = 1.00,
    agent_factory=None,
    trace_dir=None,
) -> dict:
    import warnings

    from pydantic_ai import capture_run_messages
    from pydantic_ai.exceptions import (
        CostNotFoundWarning,
        UnexpectedModelBehavior,
        UsageLimitExceeded,
    )
    from pydantic_ai.usage import RunUsage, UsageLimits

    try:
        setup = build_arm(arm, db_path, docs)
    except Exception as exc:
        # `build_arm` failing is the OTHER construction-class failure,
        # alongside the agent-factory call below: nothing billable has
        # happened yet either way, so it deserves the identical
        # free-to-retry treatment. NOTE (recorded, not fixed here): for
        # arm "contract", `_governed_tools` opens a `DuckDBAdapter`
        # connection before it can fail later in the same call (building
        # tools, loading the contract's system prompt) — if that later
        # step is what raises, there is no `ArmSetup` yet to call
        # `.close()` on, and that connection can leak. This is the one
        # construction-class failure this module cannot currently close
        # cleanly; fixing it would mean making `dce/arms.py`'s own
        # construction self-cleaning on partial failure, out of scope
        # here.
        raise AgentConstructionError(f"{type(exc).__name__}: {exc}") from exc

    # Set as soon as a priced row exists (in either branch of the guarded
    # tail below), so the `finally` block can still attach `close_error`
    # to it — mutating the SAME dict object a `return row` further up has
    # already committed to returning; Python runs `finally` before the
    # return actually completes, so the mutation is visible to the caller.
    # Stays `None` only on the `AgentConstructionError` path, where no row
    # was ever built — nothing billable happened, so there is nothing here
    # to attach a close error to.
    row: dict | None = None
    try:
        factory = agent_factory or _default_agent_factory
        try:
            agent = factory(
                model=model,
                system_prompt=setup.system_prompt,
                tools=setup.tools,
                retries=max_tool_calls,
            )
        except Exception as exc:
            # The ONLY exception `run_task` still lets propagate — see
            # `AgentConstructionError`'s docstring. Nothing billable has
            # happened yet, so this is genuinely free to retry.
            raise AgentConstructionError(f"{type(exc).__name__}: {exc}") from exc

        prompt = (
            f"{task['question']}\n\nAnswer guidelines: {task.get('guidelines', '')}"
        )
        limits = UsageLimits(
            # THE uniform iteration control across arms — see `dce/arms.py`'s
            # module docstring. `total_tokens_limit`/`cost_limit` below are
            # runaway guards, not budgets, and must stay loose enough never
            # to bind before this one does in normal operation.
            tool_calls_limit=max_tool_calls,
            request_limit=REQUEST_LIMIT,
            # Real protection against runaway spend does not come from this
            # — see `_assert_cost_limit_is_unenforceable_for_pinned_models`
            # — but it is left in place for any pinned model that ever does
            # become priceable, and as documented intent. Suppressed below
            # (`CostNotFoundWarning`) rather than left to fire on every one
            # of a sweep's thousand-plus calls.
            cost_limit=Decimal(str(per_task_usd)),
            # A runaway guard (dead loop, degenerate huge request), NOT a
            # per-task budget, and NOT derived from a dollar figure — see
            # `TOKEN_BUDGET`'s module-level comment for why sizing this off
            # `per_task_usd` was tried and reverted (it reintroduced a
            # non-uniform iteration budget across arms). Fixed and uniform
            # across every arm and model.
            total_tokens_limit=TOKEN_BUDGET,
            # N2: bounds one single request's input — closes the gap this
            # limit alone leaves open (checked only BETWEEN requests). See
            # `PER_REQUEST_INPUT_TOKEN_CAP`'s module-level comment. Not
            # preemptive (`UsageLimits.count_tokens_before_request` is
            # unset): pydantic-ai documents that knob as supported for
            # Anthropic/Google/Bedrock/OpenAI-Responses, not the
            # OpenAI-Chat-Completions-style model `_default_agent_factory`
            # builds, so an oversized request is still sent and billed once
            # before this stops the run — it prevents a SECOND one, not the
            # first one's cost. Real protection against the first oversized
            # request is `dce/arms.py`'s `MAX_ROWS` (cut from 10,000 to
            # 1,000 for exactly this reason).
            per_request_input_tokens_limit=PER_REQUEST_INPUT_TOKEN_CAP,
        )
        token_cap = TOKEN_BUDGET

        answer, verdict = "", "unset"
        forced_answer = False
        # `Agent.run_sync` mutates `usage` in place as the run progresses,
        # so it holds real counts even when the call below raises — reading
        # token/turn counts off it (rather than off a would-be `result`,
        # which does not exist on the exception paths) is what makes a cap
        # trip or a mid-run error stop recording $0.00 and 0 tokens for the
        # arm most likely to hit a cap (arm C: nine tools against three).
        usage = RunUsage()
        # Likewise, `result.all_messages()` does not exist once `run_sync`
        # has raised. `capture_run_messages` captures the same transcript
        # into `messages` as the run proceeds, independent of whether it
        # eventually raises, so the tool-call sequence and inspect_query
        # rejections survive a cap trip or error too.
        with capture_run_messages() as messages:
            try:
                # `cost_limit` is passed above knowing it cannot fire for any
                # pinned model (`_assert_cost_limit_is_unenforceable_for_
                # pinned_models`) — pydantic-ai's own acknowledgment of that
                # is a `CostNotFoundWarning` on every single call, which
                # would otherwise fire well over a thousand times across a
                # full sweep for a fact already established once at import
                # time. Suppressed here, not globally, so it stays scoped to
                # the one call site that provokes it.
                # RETRY A 429, DO NOT RECORD IT. See `RATE_LIMIT_RETRIES`.
                # `usage` is threaded through unchanged, so a partially-billed
                # attempt still counts toward the row's cost — a retry must
                # not make spent money disappear.
                for attempt in range(RATE_LIMIT_RETRIES + 1):
                    try:
                        with warnings.catch_warnings():
                            warnings.simplefilter(
                                "ignore", category=CostNotFoundWarning
                            )
                            result = agent.run_sync(
                                prompt, usage_limits=limits, usage=usage
                            )
                        break
                    except Exception as exc:
                        if attempt >= RATE_LIMIT_RETRIES or not _is_rate_limited(exc):
                            raise
                        time.sleep(RATE_LIMIT_BACKOFF_S[attempt])
                answer = str(result.output).strip()
            except UsageLimitExceeded:
                # A cap trip is a harness artifact, not a wrong answer — see
                # the module-level `MAX_TOOL_CALLS` note and
                # `tests/test_agent.py`.
                verdict = "hit_limit"
                # F2: rather than bank an unscoreable empty row, spend one
                # tool-less turn asking for the answer the run never committed
                # to. Only a non-empty answer changes anything — `verdict`
                # returns to "unset" so the scoring block below grades it like
                # any other answer, and `forced_answer` records that it came
                # from here.
                forced = _force_final_answer(agent, list(messages), usage)
                if forced:
                    answer, verdict, forced_answer = forced, "unset", True
            except UnexpectedModelBehavior as exc:
                # THE OUTPUT CAP IS OUR LIMIT TOO, SO IT GETS THE SAME
                # TREATMENT AS THE OTHERS. `MAX_OUTPUT_TOKENS_PER_REQUEST`
                # kills a request from inside, so it surfaces here rather than
                # as `UsageLimitExceeded` — but it is exactly as much a
                # harness artifact as a tool-call trip, and F2's argument
                # applies verbatim: bank a scoreable row instead of an
                # unscoreable one.
                #
                # This matters more than it sounds. Raising the cap does not
                # solve it: 16,000 -> 64,000 simply let `deepseek-v4-flash`
                # reason 67,516 tokens on the same task instead of 36,155.
                # An arm with no context can spiral without bound, so ANY
                # finite cap binds on `schema_only` and on nothing else —
                # which would hand the contract arm a win manufactured by our
                # own configuration. Forcing an answer removes the asymmetry
                # without pretending the spiral did not happen: the row still
                # records `forced_answer`.
                #
                # Narrowed to the cap message on purpose. `UnexpectedModelBehavior`
                # also covers genuine misbehaviour (an exhausted tool-retry
                # budget, say), which must stay a visible `error`. If a
                # pydantic-ai release changes the wording, this degrades to
                # the old behaviour — an error row — never to a wrong one.
                if "token limit" not in str(exc).lower():
                    verdict = "error"
                    answer = f"{type(exc).__name__}: {exc}"
                else:
                    verdict = "hit_limit"
                    forced = _force_final_answer(agent, list(messages), usage)
                    if forced:
                        answer, verdict, forced_answer = forced, "unset", True
                    else:
                        answer = f"{type(exc).__name__}: {exc}"
            except Exception as exc:
                verdict = "error"
                answer = f"{type(exc).__name__}: {exc}"

        # The full transcript, written before the bookkeeping below can
        # raise. `write_trace` swallows its own failures and returns None, so
        # a lost trace can never cost the paid row that follows — see
        # `dce/trace.py`'s FAILURE POLICY.
        trace_path = write_trace(
            trace_dir,
            task_id=str(task["task_id"]),
            arm=arm,
            model=model,
            messages=list(messages),
        )

        # EVERYTHING FROM HERE THROUGH `build_result_row` RUNS AFTER THE
        # BILLABLE CALL HAS ALREADY HAPPENED (or the cap already tripped):
        # `usage` already holds real, non-refundable counts by this point.
        # A bug anywhere in this bookkeeping tail — `_tool_call_names`,
        # `_inspect_rejections`, `_retry_prompt_count`, the session
        # `getattr`, `_clean`, `build_result_row` itself — must not let
        # that already-spent money vanish into an untraceable $0.00 row
        # that a resumed sweep then retries forever for free. This
        # module's own history is the reason this is guarded as a whole,
        # not left to the individual calls: commit a2c5d1d was exactly
        # this class of bug (`result.usage()` instead of the property)
        # shipped in this same region once already, and a future
        # pydantic-ai rename would reproduce it verbatim without this.
        try:
            tool_calls = _tool_call_names(messages)
            rejections = _inspect_rejections(messages)
            retry_prompts = _retry_prompt_count(messages)
            # Read before `setup.close()` (in `finally`, below) even though
            # `ContractSession.retries` is a plain int attribute unaffected
            # by the connection's lifecycle — matching `dce/arms.py`'s CALL
            # ORDER discipline of treating "read everything, then close" as
            # the one safe sequence rather than relying on which specific
            # reads happen to be connection-independent today.
            # `ArmSetup.session` is typed `object | None` (see
            # `dce/arms.py`), so this reads it dynamically rather than
            # narrowing on `is not None` — a plain `object` has no
            # `.retries` either way, and `getattr` gives the same "0 for
            # schema_only/manual_prompt" result without a type-checker
            # false positive.
            enforcement_blocks = getattr(setup.session, "retries", 0)

            # Scored only if the model call itself completed — scoring a
            # cap trip or a mid-run error against `gold` would
            # misrepresent a harness artifact as a graded attempt. Kept
            # out of the try/except above (Important 3): `score` raising
            # must not overwrite a good `answer` with an exception text
            # and relabel it `error` — a scoring failure gets its own
            # verdict instead, and the real answer this run produced is
            # preserved either way.
            if verdict == "unset":
                try:
                    verdict = "correct" if score(answer, gold) else "incorrect"
                except Exception:
                    verdict = "scoring_error"

            answer_normalized = _clean(answer)

            row = build_result_row(
                task=task,
                arm=arm,
                model=model,
                answer=answer,
                answer_normalized=answer_normalized,
                gold=gold,
                verdict=verdict,
                forced_answer=forced_answer,
                trace_path=trace_path,
                reasoning_tokens=_reasoning_tokens(usage),
                in_tok=usage.input_tokens,
                out_tok=usage.output_tokens,
                cached_tok=usage.cache_read_tokens,
                turns=usage.requests,
                tool_calls=tool_calls,
                inspect_rejections=rejections,
                enforcement_blocks=enforcement_blocks,
                retry_prompts=retry_prompts,
                request_limit=REQUEST_LIMIT,
                token_cap=token_cap,
                golds_hash=golds_hash,
            )
        except Exception as exc:
            row = _priced_fallback_row(
                task=task,
                arm=arm,
                model=model,
                gold=gold,
                golds_hash=golds_hash,
                usage=usage,
                verdict="post_run_error",
                note=f"{type(exc).__name__}: {exc}",
            )
        return row
    finally:
        # Arm C's adapter holds a live DuckDB connection open for the arm's
        # whole lifetime. It MUST be closed here — on every path, including
        # the error and cap-trip paths above — before the runner's
        # post-task integrity check (`dce.arms.check_and_restore`): DuckDB
        # keeps mutations in a `.wal` sidecar while the connection is open,
        # so a check performed against a live connection is not a valid
        # check and its repair is not guaranteed to survive the connection's
        # later close. See `dce/arms.py`'s module docstring, CALL ORDER.
        #
        # Guarded, not bare: a close failure here must not replace whatever
        # `row`/exception the `try` above already produced — the money is
        # already spent and priced either way by this point. But a
        # swallowed close failure is not the same as a HANDLED one: if the
        # connection did not actually close, `dce.runner.sweep`'s
        # subsequent `check_and_restore` call runs against what may still
        # be a live connection — exactly the "not a valid check" case
        # `dce/arms.py`'s CALL ORDER section warns about. Stamping
        # `close_error` onto the row (when one exists) is what lets the
        # runner see that and refuse to trust that task's integrity result,
        # instead of silently misattributing a leaked connection's own
        # effects to the arm under test.
        try:
            setup.close()
        except Exception as exc:
            if row is not None:
                row["close_error"] = f"{type(exc).__name__}: {exc}"
