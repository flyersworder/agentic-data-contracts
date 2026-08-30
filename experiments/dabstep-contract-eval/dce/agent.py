"""Run one (task, arm, model) and return a fully provenanced result row."""

from __future__ import annotations

import json
import os
import subprocess
from decimal import Decimal
from importlib.metadata import version
from pathlib import Path

from dce.arms import build_arm
from dce.frozen import digest
from dce.grade import _clean, score
from dce.pricing import MODELS, cost

# A harness cap, not a contract limit — applied identically to every arm so
# no arm gets more iterations than another (see the retries note below, and
# `dce/arms.py`'s module docstring on why that symmetry matters).
MAX_TOOL_CALLS: int = 25

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
#   * `UsageLimits.per_request_input_tokens_limit` (set in `run_task`, sized
#     as a fraction of `_token_cap` — see `_PER_REQUEST_INPUT_FRACTION`)
#     bounds one request's INPUT.
#
# 4,000 output tokens is generous for a data-analyst answer with some
# reasoning attached, while still being nowhere near the size of a runaway
# tool-return-turned-input.
MAX_OUTPUT_TOKENS_PER_REQUEST: int = 4_000

# The fraction of the whole-task `_token_cap` that a single request's INPUT
# may consume on its own — see `MAX_OUTPUT_TOKENS_PER_REQUEST`'s comment
# above for why this exists. 1/4 means one oversized tool return can eat at
# most a quarter of the task's entire runaway-guard budget before
# `per_request_input_tokens_limit` stops it outright, rather than being
# allowed to consume the whole thing (or more, pre-N2) in a single step.
_PER_REQUEST_INPUT_FRACTION: float = 0.25


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
    `per_task_usd`. `run_task` works around this with its own `_token_cap`
    (below), derived from the price table this experiment already owns,
    rather than trusting `cost_limit` to do anything.

    This assertion is the tripwire for that assumption changing: if a future
    `genai-prices` release ever adds pricing data for one of these ids,
    `cost_limit` would start being enforced for *that* model while staying
    dead for the other three — a brand-new asymmetry across arms/models that
    `_token_cap` was written without accounting for. Raising loudly here,
    once, at import time, means that day is a deliberate code change instead
    of a silent shift in what "capped at `per_task_usd`" actually means.

    A tripwire must never be able to bring down the thing it is watching.
    `genai-prices` is only a *transitive* dependency (pulled in by
    `pydantic-ai-slim`, not declared here), so importing it at module scope
    without a fallback would make `import dce.agent` — and therefore the
    whole sweep — fail outright the day a pydantic-ai release drops it or
    renames it. A missing `genai_prices` is silently treated as "cannot
    disprove `_token_cap`'s reason for existing", not as this assertion's
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
            "dce.pricing.MODELS until they are priced too. _token_cap's "
            "conservative token-based cap may now be redundant for this "
            "model — re-evaluate whether cost_limit alone is now trustworthy "
            "for it before continuing to layer _token_cap on top, and update "
            "this assertion (or MODELS) once you have."
        )


_assert_cost_limit_is_unenforceable_for_pinned_models()


def _token_cap(model: str, per_task_usd: float) -> int:
    """A RUNAWAY GUARD, not a per-task budget — the ceiling this experiment
    puts on `total_tokens_limit` because `cost_limit` cannot be trusted (see
    `_assert_cost_limit_is_unenforceable_for_pinned_models`).

    This distinction is load-bearing and was gotten backwards once already:
    cost is a REPORTED OUTCOME of this experiment ("arm B costs more per
    turn than arm A" is one of the findings it exists to produce), not a
    quantity to equalise across arms by capping it uniformly in dollars.
    Arm C carries nine tool schemas against arms A/B's three, and arm B
    carries the full manual/readme text in its system prompt — both cost
    more per request than arm A, structurally, before a single token of
    actual reasoning happens. A uniform dollar cap on top of that turns
    "capped at $X" into a wildly non-uniform *iteration* budget across arms,
    and it bites hardest on exactly the arm carrying the most context — a
    confound `dce/arms.py`'s module docstring already fought hard to keep
    out of `retries`, reintroduced here by a different route. The uniform
    iteration control is, and stays, `tool_calls_limit=max_tool_calls`
    (identical across every arm — see `dce/arms.py`'s RETRY BUDGET section).
    Real spend control lives at the sweep level (`--max-spend`, arm-
    agnostic), not here. `_token_cap`'s only job is to stop a genuinely
    pathological run — an infinite loop, a single oversized request (see
    `MAX_OUTPUT_TOKENS_PER_REQUEST` / `_per_request_input_cap` for that
    narrower case) — from spending without limit; it must be loose enough
    that no arm's normal operation ever reaches it before `tool_calls_limit`
    does. `run_task`'s default `per_task_usd=1.00` (not the tighter values
    tried and rejected earlier) reflects that: it is sized to stay out of the
    way, not to be a felt constraint.

    Priced off `max(price_in, price_out)` — the MORE EXPENSIVE of a model's
    two per-token rates, deliberately, not the cheaper one — for a second,
    independent reason: `total_tokens_limit` bounds input and output tokens
    together, undifferentiated, and output is the pricier side for every
    pinned model. Pricing off `price_in` is not a bound on spend at all: an
    all-output run priced that way could cost several times `per_task_usd`
    before tripping (`deepseek-v4-pro-0813` up to 3x, `gpt-5.6-sol` up to 5x,
    measured). Do not "optimise" this back to `price_in` for a larger cap —
    that reintroduces exactly that hole, and a task truncated early (a
    visible `hit_limit` row, re-runnable at a higher budget) is always the
    better failure mode than a run that quietly costs several times its
    intended cap in money that cannot be recovered.
    """
    spec = MODELS[model]
    rate = max(spec.price_in, spec.price_out)
    return int(per_task_usd / rate * 1_000_000)


def _per_request_input_cap(model: str, per_task_usd: float) -> int:
    """A ceiling on ONE request's input tokens — closes the gap
    `total_tokens_limit` leaves open (see `MAX_OUTPUT_TOKENS_PER_REQUEST`'s
    module-level comment): that limit is only checked BETWEEN requests, so
    nothing stops a single oversized tool return from becoming the very next
    request's input in full, in one step, regardless of how much of
    `_token_cap`'s budget remains. Sized as `_PER_REQUEST_INPUT_FRACTION` of
    the same whole-task `_token_cap`, so one runaway request can consume at
    most a bounded slice of the task's total runaway-guard budget rather
    than potentially all of it (or, before N2, more than all of it) at once.
    """
    return max(1, int(_token_cap(model, per_task_usd) * _PER_REQUEST_INPUT_FRACTION))


def build_result_row(
    *,
    task: dict,
    arm: str,
    model: str,
    answer: str,
    answer_normalized: str,
    gold: str,
    verdict: str,
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
        "input_tokens": in_tok,
        "output_tokens": out_tok,
        "cached_tokens": cached_tok,
        "turns": turns,
        # `dce.pricing`'s table has no discounted rate for cache-read
        # tokens, so this prices every `cached_tok` at the full `price_in`
        # rate — this OVER-estimates real spend whenever `cached_tokens > 0`
        # (a real provider typically bills a cache read well below the
        # fresh-input rate).
        "usd": cost(model, in_tok, out_tok),
        "tool_calls": tool_calls,
        "inspect_rejections": inspect_rejections,
        "enforcement_blocks": enforcement_blocks,
        "retry_prompts": retry_prompts,
        "request_limit": request_limit,
        # The `total_tokens_limit` `_token_cap` derived for this task's
        # (model, per_task_usd) — recorded so a `hit_limit` row shows which
        # limit actually bound (`tool_calls_limit`, `request_limit`, or this
        # one) without re-deriving it from `dce.pricing.MODELS` by hand.
        "token_cap": token_cap,
        "contract_digest": digest(),
        "golds_hash": golds_hash,
        "commit_sha": _commit_sha(),
        "adc_version": version("agentic-data-contracts"),
    }


def _default_agent_factory(
    *, model: str, system_prompt: str, tools: list, retries: int
):
    from pydantic_ai import Agent
    from pydantic_ai.models.openai import OpenAIChatModel
    from pydantic_ai.providers.openrouter import OpenRouterProvider
    from pydantic_ai.settings import ModelSettings

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
            temperature=0.0,
            seed=0,
            timeout=300,
            max_tokens=MAX_OUTPUT_TOKENS_PER_REQUEST,
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
    # A runaway guard, not a per-task budget — see `_token_cap`'s docstring.
    # $1.00 is sized to stay out of the way of every arm's normal operation
    # (even arm B's larger per-request floor on the priciest pinned model),
    # not to be a felt constraint; real spend control is `--max-spend` at
    # the sweep level, arm-agnostic.
    per_task_usd: float = 1.00,
    agent_factory=None,
) -> dict:
    import warnings

    from pydantic_ai import capture_run_messages
    from pydantic_ai.exceptions import CostNotFoundWarning, UsageLimitExceeded
    from pydantic_ai.usage import RunUsage, UsageLimits

    setup = build_arm(arm, db_path, docs)
    try:
        factory = agent_factory or _default_agent_factory
        agent = factory(
            model=model,
            system_prompt=setup.system_prompt,
            tools=setup.tools,
            retries=max_tool_calls,
        )

        prompt = (
            f"{task['question']}\n\nAnswer guidelines: {task.get('guidelines', '')}"
        )
        token_cap = _token_cap(model, per_task_usd)
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
            # per-task budget — see `_token_cap`'s docstring for why sizing
            # this as a felt cost control was tried and reverted.
            total_tokens_limit=token_cap,
            # N2: bounds one single request's input — closes the gap this
            # limit alone leaves open (checked only BETWEEN requests). See
            # `_per_request_input_cap`'s docstring. Not preemptive
            # (`UsageLimits.count_tokens_before_request` is unset): pydantic-ai
            # documents that knob as supported for Anthropic/Google/Bedrock/
            # OpenAI-Responses, not the OpenAI-Chat-Completions-style model
            # `_default_agent_factory` builds, so an oversized request is
            # still sent and billed once before this stops the run — it
            # prevents a SECOND one, not the first one's cost. Real
            # protection against the first oversized request is
            # `dce/arms.py`'s `MAX_ROWS` (cut from 10,000 to 1,000 for
            # exactly this reason).
            per_request_input_tokens_limit=_per_request_input_cap(model, per_task_usd),
        )

        answer, verdict = "", "unset"
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
                with warnings.catch_warnings():
                    warnings.simplefilter("ignore", category=CostNotFoundWarning)
                    result = agent.run_sync(prompt, usage_limits=limits, usage=usage)
                answer = str(result.output).strip()
            except UsageLimitExceeded:
                # A cap trip is a harness artifact, not a wrong answer — see
                # the module-level `MAX_TOOL_CALLS` note and
                # `tests/test_agent.py`.
                verdict = "hit_limit"
            except Exception as exc:
                verdict = "error"
                answer = f"{type(exc).__name__}: {exc}"

        tool_calls = _tool_call_names(messages)
        rejections = _inspect_rejections(messages)
        retry_prompts = _retry_prompt_count(messages)
        # Read before `setup.close()` (in `finally`, below) even though
        # `ContractSession.retries` is a plain int attribute unaffected by
        # the connection's lifecycle — matching `dce/arms.py`'s CALL ORDER
        # discipline of treating "read everything, then close" as the one
        # safe sequence rather than relying on which specific reads happen
        # to be connection-independent today.
        # `ArmSetup.session` is typed `object | None` (see `dce/arms.py`), so
        # this reads it dynamically rather than narrowing on `is not None` —
        # a plain `object` has no `.retries` either way, and `getattr` gives
        # the same "0 for schema_only/manual_prompt" result without a
        # type-checker false positive.
        enforcement_blocks = getattr(setup.session, "retries", 0)

        # Scored only if the model call itself completed — scoring a cap
        # trip or a mid-run error against `gold` would misrepresent a
        # harness artifact as a graded attempt. Kept out of the try/except
        # above (Important 3): `score` raising must not overwrite a good
        # `answer` with an exception string and relabel it "error" — a
        # scoring failure gets its own verdict instead, and the real answer
        # this run produced is preserved either way.
        if verdict == "unset":
            try:
                verdict = "correct" if score(answer, gold) else "incorrect"
            except Exception:
                verdict = "scoring_error"

        answer_normalized = _clean(answer)
    finally:
        # Arm C's adapter holds a live DuckDB connection open for the arm's
        # whole lifetime. It MUST be closed here — on every path, including
        # the error and cap-trip paths above — before the runner's
        # post-task integrity check (`dce.arms.check_and_restore`): DuckDB
        # keeps mutations in a `.wal` sidecar while the connection is open,
        # so a check performed against a live connection is not a valid
        # check and its repair is not guaranteed to survive the connection's
        # later close. See `dce/arms.py`'s module docstring, CALL ORDER.
        setup.close()

    return build_result_row(
        task=task,
        arm=arm,
        model=model,
        answer=answer,
        answer_normalized=answer_normalized,
        gold=gold,
        verdict=verdict,
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
