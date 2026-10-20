"""Experiment 6's run settings: an arm's own token budget, a context
overflow as its own verdict, and a truncated prompt failing the run."""

from pathlib import Path

from dce.agent import TOKEN_BUDGET, run_task
from dce.benchmarks.dabstep import DABStep
from dce.stats import HARNESS_VERDICTS
from dce.tools import ArmSetup

TASK = {
    "task_id": "7",
    "question": "What is X?",
    "guidelines": "Answer with a number.",
    "level": "hard",
}
TASK_OBJ = DABStep.task(TASK)


def _bench(setup: ArmSetup | None = None) -> DABStep:
    bench = DABStep(
        db=Path("unused.duckdb"),
        golds={"7": "0.12"},
        golds_hash="deadbeef",
        docs={"manual": "m", "payments_readme": "r"},
    )
    if setup is not None:
        bench.build_arm = lambda arm, task, db: setup  # type: ignore[method-assign]
    return bench


def _fake(seen=None, *, input_tokens=100, requests=1, raises=None):
    class Fake:
        def run_sync(self, *a, usage=None, usage_limits=None, **k):
            if seen is not None:
                seen["limits"] = usage_limits
            if usage is not None:
                usage.input_tokens = input_tokens
                usage.output_tokens = 10
                usage.requests = requests
            if raises is not None:
                raise raises

            class R:
                output = "0.12"

            return R()

    return Fake()


def test_an_arm_may_carry_its_own_token_budget(tmp_path: Path):
    seen: dict = {}
    setup = ArmSetup("prompt", [], None, token_budget=35_000_000)
    row = run_task(
        TASK_OBJ,
        "schema_only",
        "z-ai/glm-5.3-flash",
        _bench(setup),
        tmp_path / "x.duckdb",
        agent_factory=lambda **_: _fake(seen),
    )
    assert seen["limits"].total_tokens_limit == 35_000_000
    assert seen["limits"].per_request_input_tokens_limit == 35_000_000 // 4
    assert row["token_cap"] == 35_000_000


def test_without_one_the_budget_is_the_shared_one(tmp_path: Path):
    seen: dict = {}
    row = run_task(
        TASK_OBJ,
        "schema_only",
        "z-ai/glm-5.3-flash",
        _bench(ArmSetup("prompt", [], None)),
        tmp_path / "x.duckdb",
        agent_factory=lambda **_: _fake(seen),
    )
    assert seen["limits"].total_tokens_limit == TOKEN_BUDGET
    assert row["token_cap"] == TOKEN_BUDGET


def test_a_context_overflow_is_its_own_verdict_and_not_an_error(tmp_path: Path):
    overflow = RuntimeError(
        "ModelHTTPError: status_code: 400, body: litellm.ContextWindowExceededError:"
        " This model's maximum context length is 262144 tokens."
    )
    row = run_task(
        TASK_OBJ,
        "schema_only",
        "z-ai/glm-5.3-flash",
        _bench(ArmSetup("prompt", [], None)),
        tmp_path / "x.duckdb",
        agent_factory=lambda **_: _fake(raises=overflow),
    )
    # Deterministic: --retry-pass retries `error` rows only, so an overflow
    # is never paid for twice.
    assert row["verdict"] == "overflow"
    assert "overflow" in HARNESS_VERDICTS
    other = run_task(
        TASK_OBJ,
        "schema_only",
        "z-ai/glm-5.3-flash",
        _bench(ArmSetup("prompt", [], None)),
        tmp_path / "x.duckdb",
        agent_factory=lambda **_: _fake(raises=RuntimeError("connection reset")),
    )
    assert other["verdict"] == "error"


def test_a_prompt_the_route_truncated_fails_the_run(tmp_path: Path):
    setup = ArmSetup("prompt", [], None, min_input_tokens=1_000)
    row = run_task(
        TASK_OBJ,
        "schema_only",
        "z-ai/glm-5.3-flash",
        _bench(setup),
        tmp_path / "x.duckdb",
        agent_factory=lambda **_: _fake(input_tokens=1_500, requests=2),
    )
    assert row["verdict"] == "truncated"
    assert "truncated" in HARNESS_VERDICTS
    whole = run_task(
        TASK_OBJ,
        "schema_only",
        "z-ai/glm-5.3-flash",
        _bench(setup),
        tmp_path / "x.duckdb",
        agent_factory=lambda **_: _fake(input_tokens=2_500, requests=2),
    )
    assert whole["verdict"] == "correct"
