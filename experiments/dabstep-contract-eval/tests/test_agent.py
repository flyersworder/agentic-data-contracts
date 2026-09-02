import inspect
from pathlib import Path
from typing import Any

import dce.agent as agent
import duckdb
import pytest
from dce.agent import (
    MAX_TOOL_CALLS,
    AgentConstructionError,
    _commit_sha,
    _inspect_rejections,
    _priced_fallback_row,
    _retry_prompt_count,
    _token_budget_usd,
    _tool_call_names,
    build_result_row,
    run_task,
)
from dce.pricing import MODELS

TASK = {
    "task_id": "7",
    "question": "What is X?",
    "guidelines": "Answer with a number.",
    "level": "hard",
}

ROW_KWARGS: dict[str, Any] = dict(
    task=TASK,
    arm="contract",
    model="deepseek/deepseek-v4-pro-0813",
    answer="0.12",
    answer_normalized="0.12",
    gold="0.12",
    verdict="correct",
    forced_answer=False,
    trace_path=None,
    reasoning_tokens=0,
    in_tok=30_000,
    out_tok=2_000,
    cached_tok=0,
    turns=3,
    tool_calls=["lookup_metric", "inspect_query", "run_query"],
    inspect_rejections=1,
    enforcement_blocks=0,
    retry_prompts=0,
    request_limit=50,
    token_cap=732_000,
    golds_hash="deadbeef",
)


# ── pydantic-ai API-shape guards ────────────────────────────────────────────
#
# Each of these checks one call shape `dce/agent.py` depends on against the
# actually-installed pydantic-ai, not against the plan or memory. The first
# round of this file passed 67/67 while `run_task` called `result.usage()` —
# a method that does not exist; `AgentRunResult.usage` is a property — because
# the fakes matched the plan's mistake instead of the real API. These guards
# exist so that class of error fails at collection/test time instead of on
# the first live model call.


def test_agent_run_result_usage_is_a_property_not_a_method():
    """`AgentRunResult.usage` is a property on the installed pydantic-ai.
    `run_task` no longer reads it directly (see
    `test_agent_run_sync_accepts_a_usage_object_to_mutate_in_place` for the
    mechanism it uses instead), but this guard is kept because the fact it
    checks is exactly the one that broke silently last time.
    """
    from pydantic_ai.agent import AgentRunResult

    assert isinstance(inspect.getattr_static(AgentRunResult, "usage"), property)


def test_agent_run_sync_accepts_a_usage_object_to_mutate_in_place():
    """`run_task` passes `usage=RunUsage()` into `agent.run_sync(...)` and
    reads token/turn counts off that object afterward — never off `result`,
    which does not exist on the exception paths (`UsageLimitExceeded`, or
    any other error). This is what makes a cap trip or a mid-run error stop
    recording $0.00 and 0 tokens: pydantic-ai mutates the passed `RunUsage`
    in place as the run proceeds, regardless of whether it eventually
    raises. Checked here as a real keyword parameter of `run_sync`, not
    assumed.
    """
    from pydantic_ai import Agent

    assert "usage" in inspect.signature(Agent.run_sync).parameters


def test_capture_run_messages_is_a_context_manager_yielding_a_list():
    """`run_task` wraps the model call in
    `with capture_run_messages() as messages:` to recover the transcript
    (for `tool_calls` / `inspect_rejections` / `retry_prompts`) even when the
    call raises — `result.all_messages()` is unavailable on exactly those
    paths, since a raising `run_sync` never returns a `result`.
    """
    from pydantic_ai import capture_run_messages

    with capture_run_messages() as messages:
        assert messages == []


def test_pinned_models_remain_unpriceable_by_genai_prices():
    """Explicit, named coverage for the assertion `dce.agent` runs at import
    time (`_assert_cost_limit_is_unenforceable_for_pinned_models`). CR2 found
    that `genai-prices` has no pricing data for any of the four ids in
    `dce.pricing.MODELS`, so pydantic-ai's `cost_limit` is a silent no-op
    (`CostNotFoundWarning`, not an error) for every model this experiment
    runs — which is why `run_task` relies on the fixed `TOKEN_BUDGET`
    instead of trusting `cost_limit`. A collection failure on this whole
    test file would already prove the assertion the hard way (it runs on
    import); this gives it a readable name and failure message instead.
    """
    import dce.agent as agent_module

    agent_module._assert_cost_limit_is_unenforceable_for_pinned_models()


# ── TOKEN_BUDGET / PER_REQUEST_INPUT_TOKEN_CAP ──────────────────────────
#
# N1 (a correction to a correction): an earlier version derived the runaway
# guard from a dollar figure (`per_task_usd`); at $1.00 that cleared 11 of
# 12 (model x arm) cells but left `gpt-5.6-sol x manual_prompt` short of the
# ~26 requests `tool_calls_limit=25` implies needing, because dollars-per-
# turn is exactly the dimension that differs across arms. The guard is now
# expressed in tokens, sized off the iteration budget it must never bind
# before, uniformly across every arm and model — see `TOKEN_BUDGET`'s
# module-level comment for the full reasoning.


def test_token_budget_is_the_iteration_budget_times_the_worst_arm_floor_times_growth():
    expected = agent.REQUEST_BUDGET * agent.MAX_ARM_FLOOR * agent.GROWTH
    assert agent.TOKEN_BUDGET == expected


def test_token_budget_clears_every_arms_iteration_budget_with_margin():
    """The property N1 exists to guarantee: TOKEN_BUDGET, divided by even
    the WORST arm's measured per-request input floor, must comfortably
    exceed `tool_calls_limit + 5` requests — for every arm, since
    TOKEN_BUDGET is uniform across all of them (not derived per model, so
    this is not parametrized over MODELS: the same token figure applies to
    every one).
    """
    needed_requests = MAX_TOOL_CALLS + 5
    arm_floors = {"schema_only": 122, "manual_prompt": 6_096, "contract": 1_415}
    for arm, floor in arm_floors.items():
        request_budget = agent.TOKEN_BUDGET / floor
        assert request_budget >= needed_requests, (arm, request_budget)


def test_per_request_input_token_cap_is_a_quarter_of_token_budget():
    # N2: closes the gap total_tokens_limit leaves open (checked only
    # BETWEEN requests) by bounding one request's input to a fraction of
    # the same whole-task TOKEN_BUDGET, so one oversized tool return cannot
    # alone consume the entire runaway-guard budget in a single step.
    assert agent.PER_REQUEST_INPUT_TOKEN_CAP == agent.TOKEN_BUDGET // 4


@pytest.mark.parametrize("model_id", list(MODELS))
def test_token_budget_usd_is_priced_off_the_pricier_rate(model_id):
    """Reported for visibility only — does not drive `TOKEN_BUDGET` itself,
    which is fixed and model-independent (see its module-level comment).
    Priced off `max(price_in, price_out)` for the same true-worst-case
    reason the retired `_token_cap` was.
    """
    spec = MODELS[model_id]
    expected = agent.TOKEN_BUDGET * max(spec.price_in, spec.price_out) / 1_000_000
    assert _token_budget_usd(model_id) == pytest.approx(expected)


# ── _commit_sha ──────────────────────────────────────────────────────────


def test_commit_sha_is_independent_of_process_cwd(tmp_path: Path, monkeypatch):
    """`subprocess.check_output` inherits the caller's process CWD unless
    told otherwise — `_commit_sha` must pin `cwd` to this repo's own
    location, or a caller running from another directory (or another repo
    entirely) would stamp a result row with the wrong `commit_sha`, or a
    bogus one.
    """
    monkeypatch.chdir(tmp_path)
    sha = _commit_sha()
    assert sha != "unknown"
    assert len(sha) >= 7


# ── build_result_row ─────────────────────────────────────────────────────


def test_result_row_carries_full_provenance():
    row = build_result_row(**ROW_KWARGS)
    for field in (
        "task_id",
        "level",
        "arm",
        "model",
        "answer",
        "answer_normalized",
        "gold",
        "verdict",
        "input_tokens",
        "output_tokens",
        "cached_tokens",
        "turns",
        "usd",
        "usd_guard",
        "tool_calls",
        "inspect_rejections",
        "enforcement_blocks",
        "retry_prompts",
        "request_limit",
        "token_cap",
        "contract_digest",
        "golds_hash",
        "scorer",
        "commit_sha",
        "adc_version",
    ):
        assert field in row, field


def test_result_row_prices_from_the_pinned_table():
    kwargs: dict[str, Any] = {
        **ROW_KWARGS,
        "answer": "x",
        "answer_normalized": "x",
        "gold": "y",
        "verdict": "incorrect",
        "in_tok": 1_000_000,
        "out_tok": 0,
        "cached_tok": 0,
        "tool_calls": [],
        "inspect_rejections": 0,
    }
    row = build_result_row(**kwargs)
    assert row["usd"] == pytest.approx(MODELS["deepseek/deepseek-v4-pro-0813"].price_in)


# ── run_task, fake-agent unit tests ─────────────────────────────────────
#
# These drive `run_task` with a bare fake in place of a `pydantic_ai.Agent`
# (via the `agent_factory` seam) — cheap and fully offline, but a fake
# bypasses pydantic-ai's own internal message-context machinery, so
# `capture_run_messages` never sees anything from one: `tool_calls` /
# `inspect_rejections` / `retry_prompts` are correctly `[]`/`0`/`0` for every
# test below, not because the code is broken but because nothing here is a
# real `Agent` run. Verifying those against a *real* transcript is the real
# `Agent` + `FunctionModel` tests further down.


def test_run_task_records_a_cap_trip_as_hit_limit_not_incorrect(tmp_path: Path):
    class Exploded:
        def run_sync(self, *a, usage=None, **k):
            from pydantic_ai.exceptions import UsageLimitExceeded

            # Mirrors what the real pydantic-ai does: mutate the passed
            # `RunUsage` in place before raising. This is CR1's fix, at the
            # unit level — a cap trip must not book $0.00 / 0 tokens.
            if usage is not None:
                usage.input_tokens = 156
                usage.output_tokens = 30
                usage.requests = 3
            raise UsageLimitExceeded("tool call limit")

    row = run_task(
        TASK,
        "schema_only",
        "z-ai/glm-5.3-flash",
        tmp_path / "x.duckdb",
        {"manual": "m", "payments_readme": "r"},
        gold="0.12",
        golds_hash="deadbeef",
        agent_factory=lambda **_: Exploded(),
    )
    # A cap trip is a harness artifact. Scoring it as a wrong answer would let
    # a too-tight cap masquerade as an arm being worse at reasoning.
    assert row["verdict"] == "hit_limit"
    # CR1: token/turn counts and the $ they imply must survive the raise —
    # not stay at their zero initialisers.
    assert row["input_tokens"] == 156
    assert row["output_tokens"] == 30
    assert row["turns"] == 3
    assert row["usd"] > 0


def _fake_result(output: str, usage=None):
    class R:
        def __init__(self):
            self.output = output

    if usage is not None:
        usage.input_tokens = 100
        usage.output_tokens = 10
    return R()


def test_run_task_scores_the_final_message(tmp_path: Path):
    class Fake:
        def run_sync(self, *a, usage=None, **k):
            return _fake_result("0.12", usage)

    row = run_task(
        TASK,
        "schema_only",
        "z-ai/glm-5.3-flash",
        tmp_path / "x.duckdb",
        {"manual": "m", "payments_readme": "r"},
        gold="0.12",
        golds_hash="deadbeef",
        agent_factory=lambda **_: Fake(),
    )
    assert row["verdict"] == "correct"
    assert row["answer"] == "0.12"
    assert row["answer_normalized"] == "0.12"
    assert row["input_tokens"] == 100
    assert row["output_tokens"] == 10


def test_run_task_gives_a_scoring_failure_its_own_verdict_without_touching_the_answer(
    tmp_path: Path, monkeypatch
):
    """A post-run failure in `score` must not overwrite a good `answer` with
    the exception text and relabel it `error` — that would make a scoring
    bug indistinguishable from the model actually failing. It gets its own
    `scoring_error` verdict, and the real answer survives.
    """
    import dce.agent as agent_module

    def _boom(_predicted, _gold):
        raise ValueError("scorer exploded")

    monkeypatch.setattr(agent_module, "score", _boom)

    class Fake:
        def run_sync(self, *a, usage=None, **k):
            return _fake_result("0.12", usage)

    row = run_task(
        TASK,
        "schema_only",
        "z-ai/glm-5.3-flash",
        tmp_path / "x.duckdb",
        {"manual": "m", "payments_readme": "r"},
        gold="0.12",
        golds_hash="deadbeef",
        agent_factory=lambda **_: Fake(),
    )
    assert row["verdict"] == "scoring_error"
    assert row["answer"] == "0.12"


# ── A1: the only exception run_task still lets propagate is construction ──


def test_run_task_raises_agent_construction_error_when_build_arm_fails(
    tmp_path: Path, monkeypatch
):
    """`build_arm` sits before the agent-factory call but is the OTHER
    construction-class failure: nothing billable has happened yet, so it
    deserves identical free-to-retry treatment, not a raw exception type
    that `dce.runner.sweep`'s narrowed `except AgentConstructionError`
    would fail to catch."""
    import dce.agent as agent_module

    def exploding_build_arm(*a, **k):
        raise RuntimeError("boom")

    monkeypatch.setattr(agent_module, "build_arm", exploding_build_arm)

    with pytest.raises(AgentConstructionError):
        run_task(
            TASK,
            "schema_only",
            "z-ai/glm-5.3-flash",
            tmp_path / "x.duckdb",
            {"manual": "m", "payments_readme": "r"},
            gold="0.12",
            golds_hash="deadbeef",
        )


def test_run_task_raises_agent_construction_error_when_the_factory_fails(
    tmp_path: Path,
):
    """Nothing billable has happened yet when the agent factory itself
    fails (e.g. a missing OPENROUTER_API_KEY) — this is the ONE exception
    `run_task` still lets escape, and it must be `AgentConstructionError`
    specifically so `dce.runner.sweep` can narrow its `except` to exactly
    this and let any other bug propagate and stop the sweep loudly."""

    def exploding_factory(**_):
        raise KeyError("OPENROUTER_API_KEY")

    with pytest.raises(AgentConstructionError):
        run_task(
            TASK,
            "schema_only",
            "z-ai/glm-5.3-flash",
            tmp_path / "x.duckdb",
            {"manual": "m", "payments_readme": "r"},
            gold="0.12",
            golds_hash="deadbeef",
            agent_factory=exploding_factory,
        )


def test_run_task_recovers_a_priced_row_when_the_post_call_tail_raises(
    tmp_path: Path, monkeypatch
):
    """The billable call has already completed by the time anything in the
    post-run tail runs — `usage` already holds real counts. A bug there
    (this module's own history, commit a2c5d1d, was exactly this class of
    bug) must return a priced fallback row instead of raising and losing
    that spend to an untraceable failure.

    `build_result_row` itself is the thing made to explode here: it is
    explicitly the LAST link in the guarded tail (the review's own
    enumeration includes "the whole build_result_row call"), so this
    exercises the outermost, hardest-to-reach case, not just an earlier
    step.
    """
    import dce.agent as agent_module

    def _boom(**_):
        raise AttributeError("simulated pydantic-ai rename")

    monkeypatch.setattr(agent_module, "build_result_row", _boom)

    class Fake:
        def run_sync(self, *a, usage=None, **k):
            return _fake_result("0.12", usage)

    row = run_task(
        TASK,
        "schema_only",
        "z-ai/glm-5.3-flash",
        tmp_path / "x.duckdb",
        {"manual": "m", "payments_readme": "r"},
        gold="0.12",
        golds_hash="deadbeef",
        agent_factory=lambda **_: Fake(),
    )
    assert row["verdict"] == "post_run_error"
    # CR1's principle, extended to this tail: usage was already mutated in
    # place by run_sync, so the fallback must be priced from real counts,
    # not report $0.00 / 0 tokens for spend that already happened.
    assert row["input_tokens"] == 100
    assert row["output_tokens"] == 10
    assert row["usd"] > 0
    assert row["level"] == "hard"
    assert "AttributeError" in row["note"]


def test_run_task_survives_a_setup_close_failure(tmp_path: Path, monkeypatch):
    """`setup.close()` runs in a `finally` on every path. A close failure
    (e.g. a DB error while releasing the connection) must not replace the
    row the guarded tail already built — the money is already priced by
    that point regardless of whether cleanup itself succeeds. But it must
    not be SILENT either: a close failure means the connection may still
    be open, which `dce.runner.sweep`'s subsequent `check_and_restore`
    call cannot validly run against (see `dce/arms.py`'s CALL ORDER) —
    `close_error` on the row is what lets the runner see that."""
    import dce.agent as agent_module

    class _ExplodingCloseSetup:
        system_prompt = "p"
        tools: list = []
        session = None

        def close(self):
            raise RuntimeError("close exploded")

    monkeypatch.setattr(
        agent_module, "build_arm", lambda *a, **k: _ExplodingCloseSetup()
    )

    class Fake:
        def run_sync(self, *a, usage=None, **k):
            return _fake_result("0.12", usage)

    row = run_task(
        TASK,
        "schema_only",
        "z-ai/glm-5.3-flash",
        tmp_path / "x.duckdb",
        {"manual": "m", "payments_readme": "r"},
        gold="0.12",
        golds_hash="deadbeef",
        agent_factory=lambda **_: Fake(),
    )
    assert row["verdict"] == "correct"
    assert row["answer"] == "0.12"
    assert row["close_error"] == "RuntimeError: close exploded"


def test_run_task_does_not_stamp_close_error_when_close_succeeds(tmp_path: Path):
    class Fake:
        def run_sync(self, *a, usage=None, **k):
            return _fake_result("0.12", usage)

    row = run_task(
        TASK,
        "schema_only",
        "z-ai/glm-5.3-flash",
        tmp_path / "x.duckdb",
        {"manual": "m", "payments_readme": "r"},
        gold="0.12",
        golds_hash="deadbeef",
        agent_factory=lambda **_: Fake(),
    )
    assert "close_error" not in row


def test_priced_fallback_row_prices_normally_when_possible():
    from dce.pricing import cost
    from pydantic_ai.usage import RunUsage

    usage = RunUsage()
    usage.input_tokens = 100
    usage.output_tokens = 10
    row = _priced_fallback_row(
        task=TASK,
        arm="contract",
        model="z-ai/glm-5.3-flash",
        gold="g",
        golds_hash="h",
        usage=usage,
        verdict="post_run_error",
        note="x",
    )
    assert row["usd"] == cost("z-ai/glm-5.3-flash", 100, 10)
    assert row["usd_guard"] == row["usd"]
    assert row["input_tokens"] == 100
    assert row["output_tokens"] == 10


def test_priced_fallback_row_never_raises_even_for_an_unknown_model():
    """`_priced_fallback_row` is itself the last line of defense in the
    guarded tail (called from inside `run_task`'s own exception handler),
    so it must not be able to raise even in the exotic case where `model`
    can't be priced at all — both `cost()` and `_token_budget_usd()` do a
    bare `MODELS[model]` lookup, so an unknown id fails both. It must still
    return a row rather than propagating, and that row must still be
    priced pessimistically (WORST_CASE_TOKEN_BUDGET_USD), not $0.00 — $0.00
    here is exactly the value A1 exists to eliminate."""
    from dce.agent import WORST_CASE_TOKEN_BUDGET_USD
    from pydantic_ai.usage import RunUsage

    usage = RunUsage()
    usage.input_tokens = 100
    usage.output_tokens = 10
    row = _priced_fallback_row(
        task=TASK,
        arm="contract",
        model="not-a-pinned-model",
        gold="g",
        golds_hash="h",
        usage=usage,
        verdict="post_run_error",
        note="x",
    )
    assert row["usd"] == WORST_CASE_TOKEN_BUDGET_USD
    assert row["usd_guard"] == WORST_CASE_TOKEN_BUDGET_USD
    assert row["input_tokens"] == 100


def test_run_task_threads_the_effective_cap_into_the_agent_factory(tmp_path: Path):
    """`Agent(retries=...)` must track `max_tool_calls` as actually passed to
    `run_task`, not the `MAX_TOOL_CALLS` module constant — a caller
    overriding `max_tool_calls` must not silently reinstate the retry-budget
    confound `dce/arms.py`'s module docstring describes.
    """
    seen = {}

    class Fake:
        def run_sync(self, *a, usage=None, **k):
            return _fake_result("0.12", usage)

    def factory(*, model, system_prompt, tools, retries):
        seen["retries"] = retries
        return Fake()

    run_task(
        TASK,
        "schema_only",
        "z-ai/glm-5.3-flash",
        tmp_path / "x.duckdb",
        {"manual": "m", "payments_readme": "r"},
        gold="0.12",
        golds_hash="deadbeef",
        max_tool_calls=50,
        agent_factory=factory,
    )
    assert seen["retries"] == 50


def test_run_task_sizes_usage_limits_as_a_runaway_guard_not_a_dollar_budget(
    tmp_path: Path,
):
    """N1: `tool_calls_limit` is the uniform iteration control across arms
    (25, untouched by `per_task_usd`); `total_tokens_limit` and
    `per_request_input_tokens_limit` are the fixed, model-independent
    `TOKEN_BUDGET` / `PER_REQUEST_INPUT_TOKEN_CAP` — not a dollar-derived
    figure, and identical regardless of which model or arm is passed —
    verified against the actual `UsageLimits` reaching `run_sync`.
    """
    seen = {}

    class Fake:
        def run_sync(self, *a, usage=None, usage_limits=None, **k):
            seen["limits"] = usage_limits
            return _fake_result("0.12", usage)

    for model in ("z-ai/glm-5.3-flash", "openai/gpt-5.6-sol"):
        run_task(
            TASK,
            "contract",
            model,
            tmp_path / "x.duckdb",
            {"manual": "m", "payments_readme": "r"},
            gold="0.12",
            golds_hash="deadbeef",
            agent_factory=lambda **_: Fake(),
        )
        limits = seen["limits"]
        assert limits.tool_calls_limit == 40
        assert limits.request_limit == 50
        assert limits.total_tokens_limit == agent.TOKEN_BUDGET
        assert (
            limits.per_request_input_tokens_limit == agent.PER_REQUEST_INPUT_TOKEN_CAP
        )


def test_run_task_stamps_the_golds_hash_it_was_given(tmp_path: Path):
    class Fake:
        def run_sync(self, *a, usage=None, **k):
            return _fake_result("0.12", usage)

    row = run_task(
        TASK,
        "schema_only",
        "z-ai/glm-5.3-flash",
        tmp_path / "x.duckdb",
        {"manual": "m", "payments_readme": "r"},
        gold="0.12",
        golds_hash="a1b2c3",
        agent_factory=lambda **_: Fake(),
    )
    assert row["golds_hash"] == "a1b2c3"


# ── _tool_call_names / _inspect_rejections / _retry_prompt_count ───────────
#
# Unit-level, against plain duck-typed message/part objects — no Agent, no
# database, no network. These are the parsers the whole `inspect_rejections`
# metric hinges on, so they get direct coverage here rather than living only
# in a throwaway verification script.


def _tool_call(name: str):
    class Part:
        part_kind = "tool-call"
        tool_name = name

    return Part()


def _tool_return(name: str, content: str):
    class Part:
        part_kind = "tool-return"
        tool_name = name
        content: str

    p = Part()
    p.content = content
    return p


def _retry_prompt(name: str | None = None):
    class Part:
        part_kind = "retry-prompt"
        tool_name = name

    return Part()


def _msg(*parts):
    class Msg:
        parts: list

    m = Msg()
    m.parts = list(parts)
    return m


def test_tool_call_names_reads_the_ordered_sequence():
    messages = [_msg(_tool_call("lookup_domain"), _tool_call("lookup_metric"))]
    assert _tool_call_names(messages) == ["lookup_domain", "lookup_metric"]


def test_tool_call_names_ignores_non_tool_call_parts():
    messages = [_msg(_tool_return("inspect_query", '{"valid": true}'), _retry_prompt())]
    assert _tool_call_names(messages) == []


@pytest.mark.parametrize(
    ("description", "messages", "expected"),
    [
        (
            "no inspect_query tool at all",
            [_msg(_tool_call("execute_sql"), _tool_return("execute_sql", "col\n1"))],
            0,
        ),
        (
            "an unrelated tool's return echoing the literal text",
            [_msg(_tool_return("execute_sql", '{"valid": false}'))],
            0,
        ),
        (
            "a run_query payload containing the literal text",
            [_msg(_tool_return("run_query", '{"valid": false, "rows": []}'))],
            0,
        ),
        (
            "one genuinely invalid inspect_query",
            [_msg(_tool_return("inspect_query", '{"valid": false, "violations": []}'))],
            1,
        ),
        (
            "bad, good, bad in sequence",
            [
                _msg(
                    _tool_return("inspect_query", '{"valid": false}'),
                    _tool_return("inspect_query", '{"valid": true}'),
                    _tool_return("inspect_query", '{"valid": false}'),
                )
            ],
            2,
        ),
        (
            "non-JSON content",
            [_msg(_tool_return("inspect_query", "BLOCKED — Session limit exceeded"))],
            0,
        ),
        (
            "valid field missing entirely",
            [_msg(_tool_return("inspect_query", '{"violations": []}'))],
            0,
        ),
        (
            "valid is null, not false",
            [_msg(_tool_return("inspect_query", '{"valid": null}'))],
            0,
        ),
        (
            "valid is 0, falsy but not the boolean False",
            [_msg(_tool_return("inspect_query", '{"valid": 0}'))],
            0,
        ),
        (
            "a retry-prompt part naming inspect_query is not a tool-return",
            [_msg(_retry_prompt("inspect_query"))],
            0,
        ),
    ],
    ids=lambda x: x if isinstance(x, str) else None,
)
def test_inspect_rejections_adversarial_matrix(description, messages, expected):
    assert _inspect_rejections(messages) == expected, description


def test_retry_prompt_count_counts_only_retry_prompt_parts():
    messages = [
        _msg(
            _tool_call("run_query"),
            _retry_prompt("run_query"),
            _tool_return("inspect_query", '{"valid": false}'),
        )
    ]
    assert _retry_prompt_count(messages) == 1


# ── real Agent + FunctionModel runs ──────────────────────────────────────
#
# Offline and deterministic (`FunctionModel` supplies canned responses; no
# network, no real model). Unlike the fake-agent tests above, these drive a
# genuine `pydantic_ai.Agent` through `build_arm("contract", ...)`'s real
# governed tools, so pydantic-ai's own internal message-context machinery
# populates `capture_run_messages` for real — this is what pins
# `inspect_query`'s actual payload shape, rather than one this file
# hand-assembles.


@pytest.fixture
def contract_db(tmp_path: Path) -> Path:
    path = tmp_path / "t.duckdb"
    con = duckdb.connect(str(path))
    con.execute("CREATE TABLE payments AS SELECT 1 AS psp_reference")
    con.close()
    return path


def _function_agent_factory(steps):
    """An `agent_factory` building a real `Agent` on a `FunctionModel` that
    plays back `steps` (a list of `(tool_name, args)` pairs) as tool calls,
    then returns a fixed text answer once `steps` is exhausted.
    """
    from pydantic_ai import Agent
    from pydantic_ai.messages import ModelResponse, TextPart, ToolCallPart
    from pydantic_ai.models.function import FunctionModel
    from pydantic_ai.usage import RequestUsage

    calls = {"n": 0}

    def fn(messages, info):
        n = calls["n"]
        calls["n"] += 1
        if n < len(steps):
            name, args = steps[n]
            return ModelResponse(
                parts=[ToolCallPart(tool_name=name, args=args)],
                usage=RequestUsage(input_tokens=20, output_tokens=5),
            )
        return ModelResponse(
            parts=[TextPart("0.12")],
            usage=RequestUsage(input_tokens=20, output_tokens=2),
        )

    def factory(*, model, system_prompt, tools, retries):
        return Agent(FunctionModel(fn), tools=tools, retries=retries)

    return factory


def test_run_task_records_the_tool_call_sequence_from_a_real_agent_run(
    contract_db: Path,
):
    # The sequence is how we tell whether arm C actually used progressive
    # disclosure or ignored the lookup tools — MotherDuck's central finding.
    steps = [
        ("lookup_domain", {"name": "fees"}),
        ("lookup_metric", {"metric_name": "transaction_fee"}),
        ("inspect_query", {"sql": "SELECT psp_reference FROM main.payments"}),
        ("run_query", {"sql": "SELECT psp_reference FROM main.payments"}),
    ]
    row = run_task(
        TASK,
        "contract",
        "z-ai/glm-5.3-flash",
        contract_db,
        {},
        gold="0.12",
        golds_hash="deadbeef",
        agent_factory=_function_agent_factory(steps),
    )
    assert row["tool_calls"] == [
        "lookup_domain",
        "lookup_metric",
        "inspect_query",
        "run_query",
    ]
    assert row["verdict"] == "correct"
    # A valid inspect_query call and a valid run_query call — no rejection,
    # no enforcement block.
    assert row["inspect_rejections"] == 0
    assert row["enforcement_blocks"] == 0


def test_run_task_pins_the_real_inspect_query_rejection_payload(contract_db: Path):
    steps = [("inspect_query", {"sql": "SELECT * FROM main.payments"})]
    row = run_task(
        TASK,
        "contract",
        "z-ai/glm-5.3-flash",
        contract_db,
        {},
        gold="0.12",
        golds_hash="deadbeef",
        agent_factory=_function_agent_factory(steps),
    )
    assert row["inspect_rejections"] == 1
    assert row["tool_calls"] == ["inspect_query"]


def test_run_task_records_enforcement_blocks_separately_from_inspect_rejections(
    contract_db: Path,
):
    """CR6: the row must not read `inspect_rejections: 0` and imply the
    contract caught nothing when it actually blocked a `run_query` call the
    model never ran past `inspect_query` first.
    """
    steps = [
        ("run_query", {"sql": "SELECT * FROM main.payments"}),  # blocked
        ("run_query", {"sql": "SELECT psp_reference FROM main.payments"}),  # ok
    ]
    row = run_task(
        TASK,
        "contract",
        "z-ai/glm-5.3-flash",
        contract_db,
        {},
        gold="0.12",
        golds_hash="deadbeef",
        agent_factory=_function_agent_factory(steps),
    )
    assert row["verdict"] == "correct"
    assert row["inspect_rejections"] == 0
    assert row["enforcement_blocks"] == 1
    assert row["retry_prompts"] == 1


def test_run_task_recovers_tokens_and_transcript_after_a_real_cap_trip(
    contract_db: Path,
):
    """CR1, end to end: a real `Agent` run that trips `tool_calls_limit`
    must not book $0.00 / 0 tokens / an empty transcript. Reproduces the
    reviewer's measurement (`RunUsage(input_tokens=156, output_tokens=30,
    requests=3, tool_calls=2)` recovered after the exception) with a
    real `pydantic_ai.Agent` over `build_arm("contract", ...)`'s governed
    tools, not a hand-rolled fake.
    """
    from pydantic_ai import Agent
    from pydantic_ai.messages import ModelResponse, ToolCallPart
    from pydantic_ai.models.function import FunctionModel
    from pydantic_ai.usage import RequestUsage

    def fn(messages, info):
        return ModelResponse(
            parts=[
                ToolCallPart(
                    tool_name="inspect_query",
                    args={"sql": "SELECT * FROM main.payments"},
                )
            ],
            usage=RequestUsage(input_tokens=50, output_tokens=5),
        )

    def factory(*, model, system_prompt, tools, retries):
        return Agent(FunctionModel(fn), tools=tools, retries=retries)

    row = run_task(
        TASK,
        "contract",
        "z-ai/glm-5.3-flash",
        contract_db,
        {},
        gold="0.12",
        golds_hash="deadbeef",
        max_tool_calls=2,
        agent_factory=factory,
    )
    assert row["verdict"] == "hit_limit"
    assert row["input_tokens"] > 0
    assert row["output_tokens"] > 0
    assert row["usd"] > 0
    assert row["tool_calls"]
    assert row["inspect_rejections"] == 2


# ── F1/F2/F5/F6: the caps re-planned after the first smoke run ──────────────
#
# Every constant below is pinned with the measurement that moved it, so a
# future edit reverting one fails against evidence rather than against taste.
# Source: docs/superpowers/specs/2026-08-30-dabstep-smoke-findings.md.


def test_max_output_tokens_leaves_room_for_a_reasoning_models_reasoning():
    """F1. 4,000 was sized for "a data-analyst answer with some reasoning
    attached" — false for a reasoning model, whose reasoning tokens count
    against `max_tokens` and can exhaust it BEFORE ANY ANSWER TEXT EXISTS.
    Measured live: `z-ai/glm-5.3-flash` spent 49 of 50 completion tokens on
    reasoning for a one-number reply, and arm B died with "Model token limit
    (4000) exceeded before any response was generated".

    The failure was arm-asymmetric — it killed only the arm whose larger
    prompt induced the longest reasoning — which makes the old value a
    confound, not merely a tight cap.

    RAISED AGAIN to 64,000 when the identical failure returned on
    `deepseek-v4-flash-0731`, which reasons two orders of magnitude harder
    than the model 16,000 was sized against. There it killed 3 of the first 7
    `schema_only` rows and nothing in any other arm: the arm with the LEAST
    context reasons the MOST, so a fixed cap penalises the very baseline the
    experiment needs to measure honestly — in the direction that flatters
    this library. See `test_output_cap_clears_the_worst_observed_reasoning_run`
    for the property that should have been asserted instead of a constant.
    """
    assert agent.MAX_OUTPUT_TOKENS_PER_REQUEST == 64_000


def test_default_agent_factory_threads_the_output_cap_into_model_settings():
    """The constant is only worth pinning if it reaches the model."""
    import os

    os.environ.setdefault("OPENROUTER_API_KEY", "test-key-never-called")
    built = agent._default_agent_factory(
        model="z-ai/glm-5.3-flash", system_prompt="s", tools=[], retries=3
    )
    assert (built.model_settings or {})[
        "max_tokens"
    ] == agent.MAX_OUTPUT_TOKENS_PER_REQUEST


def test_token_budget_cannot_bind_before_the_tool_call_cap_at_worst_growth():
    """F6 — the regression that matters, and the one the old test missed.

    The old guard divided `TOKEN_BUDGET` by each arm's per-request input
    FLOOR. A floor ignores growth: the full conversation is resent as input
    every turn, so real consumption per request rises with turn count, and it
    rises FASTEST for the arm whose tool returns are largest. Measured over
    one hard task:

        schema_only     5,807 tokens/request   0.95x the 6,100 floor
        manual_prompt  11,844 tokens/request   1.94x
        contract       45,977 tokens/request   7.54x

    At GROWTH=4 the consequence was the exact confound N1 claims to have
    removed: `TOKEN_BUDGET` bound arm C at 22 tool calls while arm A ran to
    28 — the token guard, not `tool_calls_limit`, became the binding
    iteration control, and it bound earliest for the arm carrying the most
    context. `tool_calls_limit` must be the single uniform iteration control;
    this asserts the token guard clears a full iteration budget even at the
    worst growth ever observed.
    """
    worst_observed_growth = 7.54
    per_request_at_worst = agent.MAX_ARM_FLOOR * worst_observed_growth
    requests_afforded = agent.TOKEN_BUDGET / per_request_at_worst
    assert requests_afforded >= agent.REQUEST_BUDGET, requests_afforded


def test_growth_multiplier_carries_margin_over_the_worst_observed_growth():
    """Companion to the above: the multiplier itself must exceed the worst
    growth actually seen, not merely happen to clear it after rounding."""
    assert agent.GROWTH >= 7.54 * 1.5


def test_max_tool_calls_was_raised_after_two_of_three_arms_ran_out():
    """F2. On the first hard task attempted, arm A exhausted 28 tool calls and
    arm C 22, both returning an empty answer. 25 was not a budget either could
    finish inside."""
    assert agent.MAX_TOOL_CALLS == 40
    assert agent.REQUEST_BUDGET == agent.MAX_TOOL_CALLS + 5


# ── F2: the forcing turn ────────────────────────────────────────────────────


def _msg(*parts):
    from pydantic_ai.messages import ModelResponse

    return ModelResponse(parts=list(parts))


def test_trim_dangling_tool_calls_drops_an_unanswered_trailing_call():
    """A cap trips mid-turn, so the captured transcript usually ends with tool
    calls that never ran. Providers reject a history containing a tool call
    with no matching result, so the forcing turn would fail for a reason with
    nothing to do with the model."""
    from pydantic_ai.messages import ModelRequest, ToolCallPart, ToolReturnPart

    answered_call = _msg(ToolCallPart(tool_name="run_query", args={}, tool_call_id="a"))
    answer = ModelRequest(
        parts=[ToolReturnPart(tool_name="run_query", content="ok", tool_call_id="a")]
    )
    dangling = _msg(ToolCallPart(tool_name="run_query", args={}, tool_call_id="b"))

    trimmed = agent._trim_dangling_tool_calls([answered_call, answer, dangling])
    assert trimmed == [answered_call, answer]


def test_trim_dangling_tool_calls_keeps_a_complete_transcript_intact():
    from pydantic_ai.messages import ModelRequest, ToolCallPart, ToolReturnPart

    msgs = [
        _msg(ToolCallPart(tool_name="run_query", args={}, tool_call_id="a")),
        ModelRequest(
            parts=[
                ToolReturnPart(tool_name="run_query", content="ok", tool_call_id="a")
            ]
        ),
    ]
    assert agent._trim_dangling_tool_calls(msgs) == msgs


def test_trim_dangling_tool_calls_counts_a_retry_prompt_as_an_answer():
    """A `ModelRetry` (arm C's governed tools raise these) answers a tool call
    just as a return does — treating it as unanswered would throw away the
    rejection that is arm C's whole mechanism."""
    from pydantic_ai.messages import ModelRequest, RetryPromptPart, ToolCallPart

    msgs = [
        _msg(ToolCallPart(tool_name="inspect_query", args={}, tool_call_id="a")),
        ModelRequest(
            parts=[
                RetryPromptPart(
                    content="blocked", tool_name="inspect_query", tool_call_id="a"
                )
            ]
        ),
    ]
    assert agent._trim_dangling_tool_calls(msgs) == msgs


def test_trim_dangling_tool_calls_returns_empty_for_an_all_dangling_transcript():
    from pydantic_ai.messages import ToolCallPart

    msgs = [_msg(ToolCallPart(tool_name="run_query", args={}, tool_call_id="b"))]
    assert agent._trim_dangling_tool_calls(msgs) == []


def test_force_final_answer_returns_empty_rather_than_raising():
    """Strictly a recovery path. It runs after a cap has already tripped on a
    row that is already priced and already returnable, so any failure here must
    degrade to the caller's `hit_limit` — never replace it with an exception."""
    from pydantic_ai.messages import ModelRequest, ToolReturnPart
    from pydantic_ai.usage import RunUsage

    class Exploding:
        def run_sync(self, *a, **k):
            raise RuntimeError("provider rejected the history")

    history = [
        ModelRequest(
            parts=[ToolReturnPart(tool_name="t", content="ok", tool_call_id="a")]
        )
    ]
    assert agent._force_final_answer(Exploding(), history, RunUsage()) == ""


def test_force_final_answer_returns_empty_for_an_empty_history():
    from pydantic_ai.usage import RunUsage

    class NeverCalled:
        def run_sync(self, *a, **k):  # pragma: no cover - must not run
            raise AssertionError("should not be called with no history")

    assert agent._force_final_answer(NeverCalled(), [], RunUsage()) == ""


def test_force_final_answer_limits_are_offsets_against_cumulative_usage():
    """Regression for a bug only a REAL `Agent` run surfaced.

    Passing the main run's `usage` is what bills the forcing turn onto the row
    — but pydantic-ai checks usage limits against that same CUMULATIVE object.
    So absolute limits are compared against what the main run already spent: a
    bare `request_limit=2` raised "the next request would exceed the
    request_limit of 2" with `usage.requests` already at 4, and the forcing
    turn never reached the model at all. Both limits must therefore be offsets
    from the counts already banked.
    """
    from pydantic_ai.messages import ModelRequest, ToolReturnPart
    from pydantic_ai.usage import RunUsage

    seen = {}

    class Fake:
        def run_sync(self, prompt, **kw):
            seen.update(kw, prompt=prompt)
            kw["usage"].input_tokens += 500
            kw["usage"].output_tokens += 20

            class R:
                output = "  12.91  "

            return R()

    usage = RunUsage()
    usage.input_tokens = 1_000
    usage.requests = 4
    usage.tool_calls = 3
    history = [
        ModelRequest(
            parts=[ToolReturnPart(tool_name="t", content="ok", tool_call_id="a")]
        )
    ]
    assert agent._force_final_answer(Fake(), history, usage) == "12.91"

    limits = seen["usage_limits"]
    assert limits.request_limit == 4 + agent.FORCED_ANSWER_REQUEST_LIMIT
    # No FURTHER tool calls beyond the 3 already made — not an absolute 0,
    # which would trip immediately.
    assert limits.tool_calls_limit == 3
    assert seen["message_history"] == history
    assert "final answer" in seen["prompt"]
    # The extra turn's tokens land on the row rather than vanishing.
    assert usage.input_tokens == 1_500 and usage.output_tokens == 20


def test_tool_less_twin_actually_has_no_tools():
    """The second bug the end-to-end test caught: `run_sync(toolsets=[])`
    clears only the EXTRA run-time toolsets, leaving the agent's own
    `Agent(tools=...)` tools callable. Measured: the model emitted a
    `list_tables` call on the forcing turn and died on the tool-call limit
    instead of answering.
    """
    from pydantic_ai import Agent
    from pydantic_ai.models.function import FunctionModel

    def a_tool() -> str:
        """A tool."""
        return "x"

    original = Agent(FunctionModel(lambda m, i: None), tools=[a_tool], retries=1)
    twin = agent._tool_less_twin(original)

    assert twin is not original
    assert twin.model is original.model

    # The property that matters, asserted through pydantic-ai's own view of
    # what the model will be offered.
    def tool_names(a):
        return {t.name for t in a._function_toolset.tools.values()}

    assert "a_tool" in tool_names(original)
    assert tool_names(twin) == set()


def test_tool_less_twin_falls_back_to_the_agent_it_cannot_clone():
    """A test double has no `.model`; the fallback must not raise, so the
    forcing turn degrades to the naive path rather than to an exception."""

    class Double:
        pass

    d = Double()
    assert agent._tool_less_twin(d) is d


def test_run_task_forces_an_answer_after_a_cap_trip_and_scores_it(tmp_path: Path):
    """The point of F2: a `hit_limit` row is paid for and unscoreable. Two of
    three arms ended exactly there on the first hard task. One tool-less turn
    turns that waste into a graded result."""
    from pydantic_ai.exceptions import UsageLimitExceeded
    from pydantic_ai.messages import ModelRequest, ToolReturnPart

    class CapThenAnswer:
        def __init__(self):
            self.n = 0

        def run_sync(self, *a, usage=None, **k):
            self.n += 1
            if self.n == 1:
                usage.input_tokens, usage.output_tokens, usage.requests = 900, 40, 5
                raise UsageLimitExceeded("tool call limit")

            class R:
                output = "0.12"

            return R()

    forced_history = [
        ModelRequest(
            parts=[ToolReturnPart(tool_name="t", content="ok", tool_call_id="a")]
        )
    ]
    monkey = agent._trim_dangling_tool_calls
    try:
        agent._trim_dangling_tool_calls = lambda _m: forced_history
        row = run_task(
            TASK,
            "contract",
            "z-ai/glm-5.3-flash",
            tmp_path / "x.duckdb",
            {"manual": "m", "payments_readme": "r"},
            gold="0.12",
            golds_hash="h",
            agent_factory=lambda **_: CapThenAnswer(),
        )
    finally:
        agent._trim_dangling_tool_calls = monkey

    assert row["verdict"] == "correct"
    assert row["answer"] == "0.12"
    # The row must say the answer was forced: a forced answer is a different
    # measurement from a volunteered one, and pooling them silently would be
    # comparing two things under one name.
    assert row["forced_answer"] is True


def test_run_task_keeps_hit_limit_when_the_forcing_turn_produces_nothing(
    tmp_path: Path,
):
    """The forcing turn can only improve a row, never corrupt one."""
    from pydantic_ai.exceptions import UsageLimitExceeded

    class CapThenFail:
        def __init__(self):
            self.n = 0

        def run_sync(self, *a, usage=None, **k):
            self.n += 1
            if self.n == 1:
                usage.input_tokens, usage.output_tokens, usage.requests = 900, 40, 5
                raise UsageLimitExceeded("tool call limit")
            raise RuntimeError("forcing turn also failed")

    row = run_task(
        TASK,
        "contract",
        "z-ai/glm-5.3-flash",
        tmp_path / "x.duckdb",
        {"manual": "m", "payments_readme": "r"},
        gold="0.12",
        golds_hash="h",
        agent_factory=lambda **_: CapThenFail(),
    )
    assert row["verdict"] == "hit_limit"
    assert row["forced_answer"] is False
    # The already-spent money still survives the cap trip (CR1).
    assert row["input_tokens"] == 900 and row["usd"] > 0


def test_every_row_shape_carries_forced_answer():
    """`stats.py` reads whole columns; a key present on some rows and absent
    on others is a silent `None` in the middle of an analysis."""
    from dce.runner import _construction_error_row

    real = build_result_row(**ROW_KWARGS)
    fallback = _priced_fallback_row(
        task=TASK,
        arm="contract",
        model="z-ai/glm-5.3-flash",
        gold="g",
        golds_hash="h",
        usage=None,
        verdict="post_run_error",
        note="x",
    )
    construction = _construction_error_row(
        TASK, "contract", "z-ai/glm-5.3-flash", "g", "h", RuntimeError("boom")
    )
    for row in (real, fallback, construction):
        assert "forced_answer" in row
        assert isinstance(row["forced_answer"], bool)


def test_forcing_turn_survives_a_real_cap_trip_end_to_end(contract_db: Path):
    """The one test the fakes cannot stand in for.

    `_trim_dangling_tool_calls` exists because a real cap trips MID-TURN,
    leaving tool calls in the transcript that never ran — and a provider
    rejects such a history outright. Whether the trim actually yields a
    resendable transcript is a property of pydantic-ai's real message
    plumbing, not of our fakes: a fake agent never registers anything with
    `capture_run_messages`, so every fake-driven forcing test above runs
    against an empty history and proves nothing about this.

    Here a real `Agent` on a `FunctionModel` calls a tool until
    `tool_calls_limit` trips, and the forcing turn must still extract an
    answer from what it built.
    """
    from pydantic_ai import Agent
    from pydantic_ai.messages import ModelResponse, TextPart, ToolCallPart
    from pydantic_ai.models.function import FunctionModel
    from pydantic_ai.usage import RequestUsage

    def fn(messages, info):
        # No tools available => the forcing turn. Answer it.
        if not info.function_tools:
            return ModelResponse(
                parts=[TextPart("12.91")],
                usage=RequestUsage(input_tokens=30, output_tokens=4),
            )
        # Otherwise keep calling a tool forever, so the cap is what stops us.
        return ModelResponse(
            parts=[ToolCallPart(tool_name="list_tables", args={})],
            usage=RequestUsage(input_tokens=20, output_tokens=5),
        )

    def factory(*, model, system_prompt, tools, retries):
        return Agent(FunctionModel(fn), tools=tools, retries=retries)

    row = run_task(
        TASK,
        "schema_only",
        "z-ai/glm-5.3-flash",
        contract_db,
        {"manual": "m", "payments_readme": "r"},
        gold="12.91",
        golds_hash="h",
        max_tool_calls=3,
        agent_factory=factory,
    )
    assert row["forced_answer"] is True
    assert row["answer"] == "12.91"
    assert row["verdict"] == "correct"
    # The forcing turn's own tokens are billed onto the row, not lost.
    assert row["input_tokens"] > 0 and row["usd"] > 0


# ── F3: the endpoint pin and the reasoning effort ───────────────────────────


def test_default_agent_factory_pins_the_endpoint_and_the_reasoning_effort():
    """Both travel in `extra_body`, which is how OpenRouter-specific fields
    reach the API through pydantic-ai's OpenAI-shaped client."""
    import os

    os.environ.setdefault("OPENROUTER_API_KEY", "test-key-never-called")
    built = agent._default_agent_factory(
        model="z-ai/glm-5.3-flash", system_prompt="s", tools=[], retries=3
    )
    body = (built.model_settings or {})["extra_body"]
    assert body["provider"]["order"] == [MODELS["z-ai/glm-5.3-flash"].provider_tag]
    # The load-bearing half. Without it OpenRouter silently re-routes to
    # another endpoint — a different quantization at a different price — and
    # nothing in the results file would show it.
    assert body["provider"]["allow_fallbacks"] is False
    assert body["reasoning"]["effort"] == agent.REASONING_EFFORT


@pytest.mark.parametrize(
    "model_id", [m for m, s in MODELS.items() if s.route == "openrouter"]
)
def test_every_openrouter_model_gets_its_own_pin_not_a_shared_one(model_id):
    import os

    os.environ.setdefault("OPENROUTER_API_KEY", "test-key-never-called")
    built = agent._default_agent_factory(
        model=model_id, system_prompt="s", tools=[], retries=1
    )
    body = (built.model_settings or {})["extra_body"]
    assert body["provider"]["order"] == [MODELS[model_id].provider_tag]


@pytest.mark.parametrize(
    "model_id", [m for m, s in MODELS.items() if s.route == "litellm_anthropic"]
)
def test_the_anthropic_route_caches_and_sends_none_of_the_rejected_params(
    model_id, monkeypatch
):
    """The gateway fronts Bedrock, which rejects three things the OpenRouter
    path sends: `temperature` at anything but 1, `seed` at all, and
    OpenRouter's nested `reasoning` body. All three were HTTP 400s against the
    live gateway, so this asserts the settings that actually go out carry none
    of them — and that the two cache settings which are this route's whole
    reason for existing are present. Without them the route bills every input
    token fresh, at a multiplier that varies BY ARM (1.86x-3.40x), which would
    put the confound into the data rather than merely near it.
    """
    monkeypatch.setenv("LITELLM_BASE_URL", "https://gateway.invalid")
    monkeypatch.setenv("LITELLM_MASTER_KEY", "test-key-never-called")

    built = agent._default_agent_factory(
        model=model_id, system_prompt="s", tools=[], retries=1
    )
    settings = built.model_settings or {}

    assert settings["anthropic_cache_instructions"] is True
    assert settings["anthropic_cache_messages"] is True

    # The effort is genuinely sent, through this API's own knob rather than
    # OpenRouter's -- so the `reasoning_effort` these rows stamp is true, and
    # the same value the other four models run at. Measured unpinned, this
    # model reasoned anyway at Anthropic's own default (645 thinking tokens on
    # one run), which is the invisible drift the pin exists to stop.
    assert settings["anthropic_effort"] == agent.REASONING_EFFORT
    # `display` must stay explicit: it defaults to "omitted" on this model
    # generation, which yields empty thinking blocks and silently strips the
    # reasoning out of every trace. Measured: 0 chars vs 456 with this set.
    assert settings["anthropic_thinking"] == {
        "type": "adaptive",
        "display": "summarized",
    }
    for rejected in ("temperature", "seed", "extra_body"):
        assert rejected not in settings, rejected

    # The controls that keep the arm comparison honest are NOT route-specific
    # and must not drift between the two factories.
    assert settings["max_tokens"] == agent.MAX_OUTPUT_TOKENS_PER_REQUEST
    assert settings["timeout"] == 300


@pytest.mark.parametrize(
    "model_id", [m for m, s in MODELS.items() if s.route == "litellm_anthropic"]
)
def test_the_anthropic_route_fails_loudly_on_missing_gateway_credentials(
    model_id, monkeypatch
):
    """A missing key must raise at construction, where `run_task` turns it into
    a `construction_error` row and the circuit breaker trips after five — not
    fall back to a default provider that would quietly send this key-bearing
    traffic somewhere else.
    """
    monkeypatch.delenv("LITELLM_BASE_URL", raising=False)
    monkeypatch.delenv("LITELLM_MASTER_KEY", raising=False)

    with pytest.raises(KeyError):
        agent._default_agent_factory(
            model=model_id, system_prompt="s", tools=[], retries=1
        )


def test_spec_field_returns_unknown_rather_than_raising_for_an_unpinned_model():
    """`_priced_fallback_row` is the last line of defense inside `run_task`'s
    own exception handler; a bare `MODELS[model]` there would raise on exactly
    the unknown-model case the fallback exists to survive."""
    assert agent._spec_field("z-ai/glm-5.3-flash", "provider_tag") == "z-ai"
    assert agent._spec_field("no/such-model", "provider_tag") == "unknown"
    assert agent._spec_field("no/such-model", "quantization") == "unknown"


def test_every_row_shape_records_the_endpoint_actually_pinned():
    """A results file must say which model was SERVED, not only which id was
    requested. Every row shape carries it, including the two error shapes — a
    key present on some rows and absent on others is a silent `None` in the
    middle of an analysis.
    """
    from dce.runner import _construction_error_row

    rows = [
        build_result_row(**ROW_KWARGS),
        _priced_fallback_row(
            task=TASK,
            arm="contract",
            model="z-ai/glm-5.3-flash",
            gold="g",
            golds_hash="h",
            usage=None,
            verdict="post_run_error",
            note="x",
        ),
        _construction_error_row(
            TASK, "contract", "z-ai/glm-5.3-flash", "g", "h", RuntimeError("boom")
        ),
    ]
    for row in rows:
        assert row["reasoning_effort"] == agent.REASONING_EFFORT
        assert row["provider_tag"] in {s.provider_tag for s in MODELS.values()}
        assert row["quantization"] in {"fp4", "fp8", "unknown"}


def test_result_row_prices_cache_reads_at_the_discounted_rate():
    """F4 at the row level: `build_result_row` must pass `cached_tok` through
    to `cost`, not price every input token fresh."""
    spec = MODELS["deepseek/deepseek-v4-pro-0813"]
    kwargs: dict[str, Any] = {
        **ROW_KWARGS,
        "in_tok": 1_000_000,
        "out_tok": 0,
        "cached_tok": 1_000_000,
    }
    row = build_result_row(**kwargs)
    assert row["usd"] == pytest.approx(spec.price_cached)
    # `usd_guard` feeds the sweep's spend cap and must agree with `usd` when a
    # real priced call happened — otherwise the cap counts a different number
    # from the one the report totals.
    assert row["usd_guard"] == row["usd"]


def test_temperature_is_sent_via_extra_body_because_model_settings_strips_it():
    """Regression for a silent inertness that affected EVERY run so far.

    pydantic-ai strips `ModelSettings(temperature=...)` for any model whose
    profile has reasoning enabled — its `SAMPLING_PARAMS` rule, borrowed from
    OpenAI's reasoning models. These OpenRouter models list `temperature` and
    `reasoning` as simultaneously supported, and reasoning cannot be disabled
    to dodge the rule (HTTP 400, "Reasoning is mandatory for this endpoint").
    So temperature has to travel in `extra_body`, which bypasses the strip.
    """
    import os

    os.environ.setdefault("OPENROUTER_API_KEY", "test-key-never-called")
    built = agent._default_agent_factory(
        model="z-ai/glm-5.3-flash", system_prompt="s", tools=[], retries=1
    )
    settings = built.model_settings or {}
    # If this ever moves back to ModelSettings it becomes inert again, and
    # nothing at run time would say so beyond one warning.
    assert "temperature" not in settings
    assert settings["extra_body"]["temperature"] == 0.0
    # `seed` is NOT in pydantic-ai's SAMPLING_PARAMS, so it survives there.
    assert settings["seed"] == 0


def test_temperature_is_omitted_for_the_model_that_does_not_support_it():
    """`openai/gpt-5.6-sol` does not list `temperature` among its OpenRouter
    supported parameters. Sending it anyway would be sending a knob we know is
    ignored, and would imply a uniform temperature=0 across the board that
    FINDINGS cannot claim."""
    import os

    os.environ.setdefault("OPENROUTER_API_KEY", "test-key-never-called")
    built = agent._default_agent_factory(
        model="openai/gpt-5.6-sol", system_prompt="s", tools=[], retries=1
    )
    body = (built.model_settings or {})["extra_body"]
    assert "temperature" not in body
    assert MODELS["openai/gpt-5.6-sol"].supports_temperature is False


def test_output_cap_clears_the_worst_observed_reasoning_run():
    """The cap must not bind arm-dependently.

    Sized to `glm-5.3-flash` (~380 reasoning tokens/run) it was 16,000 and
    survived that sweep at 0.2% errors. On `deepseek-v4-flash-0731`, which
    reasons two orders of magnitude harder, the same value killed 3 of the
    first 7 `schema_only` rows and none in any other arm — the arm with the
    least context reasons the most, so a fixed cap penalises exactly the
    baseline this experiment needs to measure honestly.

    38,983 is the largest whole-run output observed on that model (23 turns);
    a single request may not be capped below it.
    """
    from dce.agent import MAX_OUTPUT_TOKENS_PER_REQUEST

    worst_observed_whole_run = 38_983
    assert MAX_OUTPUT_TOKENS_PER_REQUEST > worst_observed_whole_run, (
        "a per-request cap below the worst observed WHOLE-RUN output will bind "
        "on the arm that reasons most, which is the arm with the least context"
    )


def test_output_cap_death_is_forced_like_any_other_cap_trip(tmp_path, monkeypatch):
    """`MAX_OUTPUT_TOKENS_PER_REQUEST` kills a request from INSIDE, so it
    arrives as `UnexpectedModelBehavior`, not `UsageLimitExceeded` — and so it
    used to bypass the forcing turn and bank an unscoreable `error` row.

    That asymmetry is not cosmetic. An arm with no context can reason without
    bound (measured on `deepseek-v4-flash-0731`: 67,516 tokens on one
    `schema_only` run, against 36,155 for the same task at the old cap). So
    ANY finite cap binds on that arm and on no other, which would hand the
    contract arm a win manufactured by our own configuration. Raising the cap
    does not fix it — 16,000 -> 64,000 only bought a bigger spiral.
    """

    class Exploded:
        def run_sync(self, *a, usage=None, **k):
            from pydantic_ai.exceptions import UnexpectedModelBehavior

            if usage is not None:
                usage.input_tokens = 900
                usage.output_tokens = 64_000
                usage.requests = 4
            raise UnexpectedModelBehavior(
                "Model token limit (64000) exceeded before any response was generated."
            )

    monkeypatch.setattr(agent, "_force_final_answer", lambda *a, **k: "0.12")
    row = run_task(
        TASK,
        "schema_only",
        "z-ai/glm-5.3-flash",
        tmp_path / "x.duckdb",
        {"manual": "m", "payments_readme": "r"},
        gold="0.12",
        golds_hash="deadbeef",
        agent_factory=lambda **_: Exploded(),
    )
    assert row["forced_answer"] is True
    assert row["verdict"] == "correct"
    # The spend still has to survive the raise, exactly as for a cap trip.
    assert row["output_tokens"] == 64_000


def test_genuine_model_misbehaviour_still_records_an_error(tmp_path, monkeypatch):
    """The narrowing must bite: only the cap message is forced. An exhausted
    tool-retry budget is real misbehaviour and has to stay a visible error, or
    forcing would paper over bugs as well as caps."""

    class Exploded:
        def run_sync(self, *a, usage=None, **k):
            from pydantic_ai.exceptions import UnexpectedModelBehavior

            raise UnexpectedModelBehavior("Tool exceeded max retries count of 1")

    monkeypatch.setattr(agent, "_force_final_answer", lambda *a, **k: "0.12")
    row = run_task(
        TASK,
        "schema_only",
        "z-ai/glm-5.3-flash",
        tmp_path / "x.duckdb",
        {"manual": "m", "payments_readme": "r"},
        gold="0.12",
        golds_hash="deadbeef",
        agent_factory=lambda **_: Exploded(),
    )
    assert row["verdict"] == "error"
    assert row["forced_answer"] is False
    assert "max retries" in row["answer"]


def test_a_rate_limited_request_is_retried_not_recorded(tmp_path, monkeypatch):
    """A 429 is a transport condition that resolves by waiting, not a result.

    `provider.allow_fallbacks: false` (F3) deliberately turns an unhonourable
    pin into an error rather than a silent re-route, which is right for
    validity and leaves us owning the retry. Without one, the
    deepseek-v4-flash sweep ran clean for 1,135 rows, was cut off by its
    endpoint, and recorded 466 HTTP 429s as terminal `error` rows — 29% of the
    sweep lost, and it "finished early" because a throttled request fails
    instantly.
    """
    calls = {"n": 0}

    class Throttled:
        def run_sync(self, *a, usage=None, **k):
            calls["n"] += 1
            if calls["n"] <= 2:
                raise RuntimeError(
                    "ModelHTTPError: status_code: 429, model_name: deepseek/x"
                )

            class R:
                output = "0.12"

            if usage is not None:
                usage.input_tokens, usage.output_tokens, usage.requests = 10, 5, 1
            return R()

    monkeypatch.setattr(agent.time, "sleep", lambda s: None)
    row = run_task(
        TASK,
        "schema_only",
        "z-ai/glm-5.3-flash",
        tmp_path / "x.duckdb",
        {"manual": "m", "payments_readme": "r"},
        gold="0.12",
        golds_hash="deadbeef",
        agent_factory=lambda **_: Throttled(),
    )
    assert calls["n"] == 3, "should have retried twice, then succeeded"
    assert row["verdict"] == "correct"


def test_a_persistent_rate_limit_still_terminates(tmp_path, monkeypatch):
    """Bounded on purpose: backing off forever turns a quota exhaustion into a
    hung sweep, which is worse than a recorded failure."""
    calls = {"n": 0}

    class AlwaysThrottled:
        def run_sync(self, *a, usage=None, **k):
            calls["n"] += 1
            raise RuntimeError("ModelHTTPError: status_code: 429, model_name: x")

    monkeypatch.setattr(agent.time, "sleep", lambda s: None)
    row = run_task(
        TASK,
        "schema_only",
        "z-ai/glm-5.3-flash",
        tmp_path / "x.duckdb",
        {"manual": "m", "payments_readme": "r"},
        gold="0.12",
        golds_hash="deadbeef",
        agent_factory=lambda **_: AlwaysThrottled(),
    )
    assert calls["n"] == agent.RATE_LIMIT_RETRIES + 1
    assert row["verdict"] == "error"
    assert "429" in row["answer"]


def test_a_non_429_transport_error_is_not_retried(tmp_path, monkeypatch):
    """Retrying a 400 just spends the same money four more times."""
    calls = {"n": 0}

    class BadRequest:
        def run_sync(self, *a, usage=None, **k):
            calls["n"] += 1
            raise RuntimeError("ModelHTTPError: status_code: 400, model_name: x")

    monkeypatch.setattr(agent.time, "sleep", lambda s: None)
    row = run_task(
        TASK,
        "schema_only",
        "z-ai/glm-5.3-flash",
        tmp_path / "x.duckdb",
        {"manual": "m", "payments_readme": "r"},
        gold="0.12",
        golds_hash="deadbeef",
        agent_factory=lambda **_: BadRequest(),
    )
    assert calls["n"] == 1
    assert row["verdict"] == "error"


def test_reasoning_tokens_are_read_under_both_provider_spellings():
    """The key is provider-specific and reading only one is SILENT. OpenAI-shaped
    responses report `reasoning_tokens`; Anthropic's Messages API reports
    `thinking_tokens` for the same quantity, billed the same way at the output
    rate. Reading only the first spelling recorded a confident 0 on every
    `claudesonnet5` row -- a column that reads "this model does not reason"
    while the model reasons and charges for it. Measured through the gateway
    with `_default_agent_factory` itself: 645 thinking tokens on one run,
    recorded as 0 before this was fixed.
    """

    class _Usage:
        def __init__(self, details):
            self.details = details

    assert agent._reasoning_tokens(_Usage({"reasoning_tokens": 120})) == 120
    assert agent._reasoning_tokens(_Usage({"thinking_tokens": 645})) == 645
    # Summed, not first-wins: a provider reporting both must not have half of
    # its reasoning silently dropped.
    assert (
        agent._reasoning_tokens(_Usage({"reasoning_tokens": 5, "thinking_tokens": 7}))
        == 12
    )
    # Still defensive -- a paid row must never be lost to a usage shape.
    assert agent._reasoning_tokens(_Usage({})) == 0
    assert agent._reasoning_tokens(_Usage(None)) == 0
    assert agent._reasoning_tokens(None) == 0
    assert agent._reasoning_tokens(_Usage({"thinking_tokens": None})) == 0
