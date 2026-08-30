import inspect
from pathlib import Path

import pytest
from dce.agent import build_result_row, run_task

TASK = {
    "task_id": "7",
    "question": "What is X?",
    "guidelines": "Answer with a number.",
    "level": "hard",
}


def test_agent_run_result_usage_is_a_property_not_a_method():
    """Guards `dce.agent.run_task`'s `usage = result.usage` against a version
    change. On the installed pydantic-ai, `AgentRunResult.usage` is a
    property. An earlier draft of `run_task` called it as `result.usage()`,
    and the fakes below originally matched that mistake with a `usage()`
    method instead of the real API — every test passed while the real call
    would have raised `TypeError: 'RunUsage' object is not callable` on the
    first live model call. This checks the actual installed library, not the
    plan, so a future pydantic-ai release that changes this shape fails
    loudly here instead of silently reintroducing that bug.
    """
    from pydantic_ai.agent import AgentRunResult

    assert isinstance(inspect.getattr_static(AgentRunResult, "usage"), property)


def test_result_row_carries_full_provenance():
    row = build_result_row(
        task=TASK,
        arm="contract",
        model="deepseek/deepseek-v4-pro-0813",
        answer="0.12",
        gold="0.12",
        verdict="correct",
        in_tok=30_000,
        out_tok=2_000,
        cached_tok=0,
        tool_calls=["lookup_metric", "inspect_query", "run_query"],
        inspect_rejections=1,
    )
    for field in (
        "task_id",
        "level",
        "arm",
        "model",
        "answer",
        "gold",
        "verdict",
        "input_tokens",
        "output_tokens",
        "cached_tokens",
        "usd",
        "tool_calls",
        "inspect_rejections",
        "contract_digest",
        "commit_sha",
        "adc_version",
    ):
        assert field in row, field


def test_result_row_prices_from_the_pinned_table():
    row = build_result_row(
        task=TASK,
        arm="contract",
        model="deepseek/deepseek-v4-pro-0813",
        answer="x",
        gold="y",
        verdict="incorrect",
        in_tok=1_000_000,
        out_tok=0,
        cached_tok=0,
        tool_calls=[],
        inspect_rejections=0,
    )
    assert row["usd"] == pytest.approx(0.66)


def test_run_task_records_a_cap_trip_as_hit_limit_not_incorrect(tmp_path: Path):
    class Exploded:
        def run_sync(self, *a, **k):
            from pydantic_ai.exceptions import UsageLimitExceeded

            raise UsageLimitExceeded("tool call limit")

    row = run_task(
        TASK,
        "schema_only",
        "z-ai/glm-5.3-flash",
        tmp_path / "x.duckdb",
        {"manual": "m", "payments_readme": "r"},
        gold="0.12",
        agent_factory=lambda **_: Exploded(),
    )
    # A cap trip is a harness artifact. Scoring it as a wrong answer would let
    # a too-tight cap masquerade as an arm being worse at reasoning.
    assert row["verdict"] == "hit_limit"


def _fake_result(output: str, tool_names: list[str] | None = None):
    class Part:
        def __init__(self, name):
            self.part_kind = "tool-call"
            self.tool_name = name

    class Msg:
        def __init__(self, names):
            self.parts = [Part(n) for n in names]

    class U:
        input_tokens = 100
        output_tokens = 10
        cache_read_tokens = 0

    class R:
        def __init__(self):
            self.output = output
            # `usage` is a property on the real `AgentRunResult` (see
            # `test_agent_run_result_usage_is_a_property_not_a_method`), not
            # a method — this fake must match that shape, not the plan's
            # original (wrong) `usage()` method, or it validates nothing.
            self.usage = U()

        def all_messages(self):
            return [Msg(tool_names or [])]

    return R()


def test_run_task_scores_the_final_message(tmp_path: Path):
    class Fake:
        def run_sync(self, *a, **k):
            return _fake_result("0.12")

    row = run_task(
        TASK,
        "schema_only",
        "z-ai/glm-5.3-flash",
        tmp_path / "x.duckdb",
        {"manual": "m", "payments_readme": "r"},
        gold="0.12",
        agent_factory=lambda **_: Fake(),
    )
    assert row["verdict"] == "correct"
    assert row["answer"] == "0.12"


def test_run_task_records_the_tool_call_sequence(tmp_path: Path):
    # The sequence is how we tell whether arm C actually used progressive
    # disclosure or ignored the lookup tools — MotherDuck's central finding.
    class Fake:
        def run_sync(self, *a, **k):
            return _fake_result(
                "0.12", ["lookup_domain", "lookup_metric", "inspect_query", "run_query"]
            )

    row = run_task(
        TASK,
        "contract",
        "z-ai/glm-5.3-flash",
        tmp_path / "x.duckdb",
        {"manual": "m", "payments_readme": "r"},
        gold="0.12",
        agent_factory=lambda **_: Fake(),
    )
    assert row["tool_calls"] == [
        "lookup_domain",
        "lookup_metric",
        "inspect_query",
        "run_query",
    ]
