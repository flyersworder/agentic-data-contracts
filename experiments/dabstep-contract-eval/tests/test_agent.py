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
    assert row["usd"] == pytest.approx(0.66)


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
        assert limits.tool_calls_limit == 25
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
