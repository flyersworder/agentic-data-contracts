"""Transcripts: they capture what a row cannot, and they never cost a row."""

import gzip
from pathlib import Path

from dce.trace import MAX_TRACE_BYTES, read_trace, trace_name, write_trace


def _messages():
    from pydantic_ai.messages import (
        ModelRequest,
        ModelResponse,
        ToolCallPart,
        ToolReturnPart,
    )

    return [
        ModelResponse(
            parts=[
                ToolCallPart(
                    tool_name="run_query",
                    args={"sql": "SELECT card_scheme FROM main.payments"},
                    tool_call_id="a",
                )
            ]
        ),
        ModelRequest(
            parts=[
                ToolReturnPart(
                    tool_name="run_query", content="card_scheme\nVisa", tool_call_id="a"
                )
            ]
        ),
    ]


def test_a_trace_preserves_tool_arguments_and_returns(tmp_path: Path):
    """The whole point. A result row keeps tool NAMES; diagnosis needs the SQL
    that was written and the rows that came back."""
    rel = write_trace(
        tmp_path / "t",
        task_id="1480",
        arm="contract",
        model="z-ai/glm",
        messages=_messages(),
    )
    assert rel is not None
    messages = read_trace(tmp_path / "t" / trace_name("1480", "contract", "z-ai/glm"))
    calls = [
        part
        for message in messages
        for part in message["parts"]
        if part["part_kind"] == "tool-call"
    ]
    returns = [
        part
        for message in messages
        for part in message["parts"]
        if part["part_kind"] == "tool-return"
    ]
    assert calls[0]["args"]["sql"] == "SELECT card_scheme FROM main.payments"
    assert "Visa" in str(returns[0]["content"])


def test_trace_name_is_the_same_key_the_runner_resumes_on(tmp_path: Path):
    """A trace must be findable from a result row without a separate index."""
    name = trace_name("1480", "contract", "z-ai/glm-5.3-flash")
    assert name.startswith("1480__contract__")
    # A model id carries a slash; unslugged it would silently create a `z-ai/`
    # directory and split traces across two places.
    assert "/" not in name
    assert name.endswith(".json.gz")


def test_traces_are_off_when_no_directory_is_given():
    assert (
        write_trace(None, task_id="1", arm="contract", model="m", messages=[]) is None
    )


def test_a_write_failure_loses_the_trace_and_never_raises(tmp_path: Path):
    """FAILURE POLICY. This runs inside `run_task`'s guarded tail, after the
    model call is already paid for. Losing a transcript is acceptable; letting
    it raise would lose the row and let a resumed sweep re-buy the same work.
    """
    blocked = tmp_path / "afile"
    blocked.write_text("not a directory")
    # mkdir under a regular file raises NotADirectoryError inside write_trace.
    assert (
        write_trace(
            blocked / "sub",
            task_id="1",
            arm="contract",
            model="m",
            messages=_messages(),
        )
        is None
    )


def test_an_unserializable_transcript_still_produces_a_readable_trace(tmp_path: Path):
    """A transcript pydantic-ai cannot dump is still worth something — better a
    repr than no evidence at all on the run that most needs explaining."""

    class Exotic:
        parts = ["not a real part"]

        def __repr__(self):
            return "<Exotic transcript>"

    rel = write_trace(
        tmp_path / "t", task_id="9", arm="contract", model="m", messages=[Exotic()]
    )
    assert rel is not None
    payload = read_trace(tmp_path / "t" / trace_name("9", "contract", "m"))
    assert payload["unserializable"] is True
    assert "Exotic" in payload["repr"][0]


def test_an_oversized_trace_is_marked_truncated_not_silently_short(
    tmp_path: Path, monkeypatch
):
    import dce.trace as trace_mod

    monkeypatch.setattr(trace_mod, "MAX_TRACE_BYTES", 200)
    rel = trace_mod.write_trace(
        tmp_path / "t", task_id="9", arm="contract", model="m", messages=_messages()
    )
    assert rel is not None
    raw = gzip.open(tmp_path / "t" / trace_name("9", "contract", "m"), "rb").read()
    assert b'"__truncated__": true' in raw
    assert MAX_TRACE_BYTES > 200  # the real cap is a backstop, not routine


def test_run_task_stamps_the_trace_path_on_the_row(tmp_path: Path):
    """The row is the claim; `trace_path` is the pointer to its evidence."""
    from dce.agent import run_task

    class Fake:
        def run_sync(self, *a, usage=None, **k):
            if usage is not None:
                usage.input_tokens, usage.output_tokens = 100, 10

            class R:
                output = "0.12"

            return R()

    row = run_task(
        {"task_id": "1480", "question": "q", "guidelines": "g", "level": "hard"},
        "contract",
        "z-ai/glm-5.3-flash",
        tmp_path / "x.duckdb",
        {"manual": "m", "payments_readme": "r"},
        gold="0.12",
        golds_hash="h",
        agent_factory=lambda **_: Fake(),
        trace_dir=tmp_path / "traces",
    )
    assert row["trace_path"] is not None
    assert row["trace_path"].endswith(
        trace_name("1480", "contract", "z-ai/glm-5.3-flash")
    )
    assert (
        tmp_path / "traces" / trace_name("1480", "contract", "z-ai/glm-5.3-flash")
    ).exists()


def test_a_row_still_lands_when_tracing_is_impossible(tmp_path: Path):
    """The guarantee that matters: a paid row survives a broken trace dir."""
    from dce.agent import run_task

    class Fake:
        def run_sync(self, *a, usage=None, **k):
            if usage is not None:
                usage.input_tokens, usage.output_tokens = 100, 10

            class R:
                output = "0.12"

            return R()

    blocked = tmp_path / "afile"
    blocked.write_text("not a directory")
    row = run_task(
        {"task_id": "1", "question": "q", "guidelines": "g", "level": "hard"},
        "contract",
        "z-ai/glm-5.3-flash",
        tmp_path / "x.duckdb",
        {"manual": "m", "payments_readme": "r"},
        gold="0.12",
        golds_hash="h",
        agent_factory=lambda **_: Fake(),
        trace_dir=blocked / "sub",
    )
    assert row["verdict"] == "correct"
    assert row["usd"] > 0
    assert row["trace_path"] is None


# ── reasoning ───────────────────────────────────────────────────────────────


def test_a_trace_preserves_a_thinking_part(tmp_path: Path):
    """Reasoning is the most valuable thing in a transcript for diagnosis — it
    is where the model says WHY it wrote the query it wrote.

    This asserts the trace does not drop it. The first real traces contained
    none, which briefly looked like tool use suppressing reasoning — it was
    really just `glm-5.3-flash` reasoning very little (~20 tokens) plus a probe
    prompt too easy to need any. Given a question that requires thought,
    `deepseek-v4-pro-0813` emits ~1,100 reasoning tokens and ~4.6 KB of
    content per task, all of which lands in the trace.
    """
    from pydantic_ai.messages import ModelResponse, TextPart, ThinkingPart

    messages = [
        ModelResponse(
            parts=[
                ThinkingPart(content="the fee rule has a null aci, so it matches all"),
                TextPart("5, 9, 20"),
            ]
        )
    ]
    write_trace(
        tmp_path / "t", task_id="1480", arm="contract", model="m", messages=messages
    )
    parts = read_trace(tmp_path / "t" / trace_name("1480", "contract", "m"))[0]["parts"]
    kinds = {p["part_kind"] for p in parts}
    assert "thinking" in kinds
    thinking = next(p for p in parts if p["part_kind"] == "thinking")
    assert "null aci" in thinking["content"]


def test_reasoning_tokens_are_recorded_on_the_row():
    """Reasoning spend varies by model far more than by effort setting, so it
    is recorded rather than assumed: measured on one hard task with tools
    bound, `glm-5.3-flash` spent ~20 reasoning tokens where
    `deepseek-v4-pro-0813` spent 1,096 — 49% of its output tokens, billed at
    the output rate.
    """
    from dce.agent import _reasoning_tokens, build_result_row

    from tests.test_agent import ROW_KWARGS

    row = build_result_row(**{**ROW_KWARGS, "reasoning_tokens": 137})
    assert row["reasoning_tokens"] == 137
    # A subset of what is billed, never more.
    assert row["reasoning_tokens"] <= row["output_tokens"] or row["output_tokens"] == 0

    class Usage:
        details = {"reasoning_tokens": 42}

    assert _reasoning_tokens(Usage()) == 42


def test_reasoning_chars_recover_what_a_deployment_cannot_report():
    """`qwen3.6-27b`'s deployment reports no reasoning token count at all --
    measured, its whole usage block is three integers -- so
    `reasoning_tokens` is 0 on every row of that model while it reasons and
    bills at the output rate. (`qwen3.8-27b`, a newer build on the same route,
    does report it.) `reasoning_chars` is the recoverable half, read
    off the `ThinkingPart`s pydantic-ai builds from `reasoning_content`.

    This asserts the two are INDEPENDENT: a run with no provider token count
    must still record its reasoning volume, or the finding this column exists
    to rescue is still uncomputable.
    """
    from dce.agent import _reasoning_chars, build_result_row

    from tests.test_agent import ROW_KWARGS

    class _Part:
        def __init__(self, kind, content):
            self.part_kind = kind
            self.content = content

    class _Msg:
        def __init__(self, parts):
            self.parts = parts

    messages = [
        _Msg([_Part("thinking", "abcde"), _Part("tool-call", "ignored")]),
        _Msg([_Part("thinking", "fg")]),
        # A part with no content at all must contribute nothing rather than
        # raise -- the same defensive contract `_reasoning_tokens` keeps.
        _Msg([_Part("thinking", None)]),
        _Msg([]),
    ]
    assert _reasoning_chars(messages) == 7

    # Only thinking parts count: a run that produced a long ANSWER and no
    # reasoning must read 0, not "some text happened".
    assert _reasoning_chars([_Msg([_Part("text", "a long answer" * 100)])]) == 0

    # And the column survives onto the row independently of the token count,
    # which is the whole point.
    row = build_result_row(
        **{**ROW_KWARGS, "reasoning_tokens": 0, "reasoning_chars": 22759}
    )
    assert row["reasoning_tokens"] == 0
    assert row["reasoning_chars"] == 22759


def test_every_self_hosted_reasoning_model_has_its_own_measured_ratio():
    """`reasoning_chars` becomes a token figure only through a chars-per-token
    ratio, and that ratio belongs to a tokenizer, not to a route. A single
    constant measured on `qwen3.6-27b` would silently convert a second model's
    characters with the first model's tokenizer -- so every model on the
    self-hosted route must carry its own entry, and adding one without
    measuring it fails here.

    Required for the whole route, not only for models whose deployment omits
    the count: whether a deployment reports it is a property of its serving
    build (`qwen3.6-27b` does not, `qwen3.8-27b` does) and can change under
    the same alias, so the fallback must already exist when it does.
    """
    from dce.agent import REASONING_CHARS_PER_TOKEN
    from dce.pricing import MODELS

    unreported = {m for m, spec in MODELS.items() if spec.route == "litellm_openai"}
    assert set(REASONING_CHARS_PER_TOKEN) == unreported
    for model, ratio in REASONING_CHARS_PER_TOKEN.items():
        # English-and-SQL reasoning under a BPE tokenizer sits near 4 chars a
        # token; a value far outside this band is a mismeasurement, which is
        # how the first single-request value of 2.72 went unnoticed.
        assert 3.0 < ratio < 5.0, model


def test_reasoning_tokens_degrade_to_zero_rather_than_breaking_a_paid_row():
    """`details` is not a first-class pydantic-ai field, so a provider that
    omits it — or a rename in a future release — must cost 0, not a row."""
    from dce.agent import _reasoning_tokens

    class NoDetails:
        pass

    class Weird:
        details = {"reasoning_tokens": "not-a-number"}

    assert _reasoning_tokens(NoDetails()) == 0
    assert _reasoning_tokens(Weird()) == 0
    assert _reasoning_tokens(None) == 0
