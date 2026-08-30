"""Run one (task, arm, model) and return a fully provenanced result row."""

from __future__ import annotations

import json
import os
import subprocess
from decimal import Decimal
from importlib.metadata import version
from pathlib import Path
from typing import Any

from dce.arms import build_arm
from dce.frozen import digest
from dce.grade import score
from dce.pricing import cost

# A harness cap, not a contract limit — applied identically to every arm so
# no arm gets more iterations than another (see the retries note below, and
# `dce/arms.py`'s module docstring on why that symmetry matters).
MAX_TOOL_CALLS: int = 25


def _tool_call_names(result: Any) -> list[str]:
    """Ordered tool names from a run's message history.

    Whether arm C actually called the lookup tools, or ignored them and went
    straight to SQL, is the behavioural half of the result — an arm C that
    never calls `lookup_metric` is not testing what we think it is.
    """
    names: list[str] = []
    for message in result.all_messages():
        for part in getattr(message, "parts", []):
            if getattr(part, "part_kind", "") == "tool-call":
                names.append(part.tool_name)
    return names


def _inspect_rejections(result: Any) -> int:
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

    The only place this information survives is the tool's own JSON return
    value, so this walks the run's message history for `inspect_query`
    tool-return parts and counts the ones whose parsed payload has
    `"valid": false`. Arms without an `inspect_query` tool (`schema_only`,
    `manual_prompt`) never produce such a part, so this is naturally 0 for
    them without special-casing the arm.
    """
    count = 0
    for message in result.all_messages():
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


def _commit_sha() -> str:
    try:
        return subprocess.check_output(
            ["git", "rev-parse", "--short", "HEAD"], text=True
        ).strip()
    except Exception:
        return "unknown"


def build_result_row(
    *,
    task: dict,
    arm: str,
    model: str,
    answer: str,
    gold: str,
    verdict: str,
    in_tok: int,
    out_tok: int,
    cached_tok: int,
    tool_calls: list[str],
    inspect_rejections: int,
) -> dict:
    return {
        "task_id": task["task_id"],
        "level": task.get("level", "unknown"),
        "arm": arm,
        "model": model,
        "answer": answer,
        "gold": gold,
        "verdict": verdict,
        "input_tokens": in_tok,
        "output_tokens": out_tok,
        "cached_tokens": cached_tok,
        "usd": cost(model, in_tok, out_tok),
        "tool_calls": tool_calls,
        "inspect_rejections": inspect_rejections,
        "contract_digest": digest(),
        "commit_sha": _commit_sha(),
        "adc_version": version("agentic-data-contracts"),
    }


def _default_agent_factory(*, model: str, system_prompt: str, tools: list):
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
        retries=MAX_TOOL_CALLS,
        model_settings=ModelSettings(temperature=0.0, seed=0, timeout=300),
    )


def run_task(
    task: dict,
    arm: str,
    model: str,
    db_path: Path,
    docs: dict[str, str],
    gold: str,
    *,
    max_tool_calls: int = MAX_TOOL_CALLS,
    per_task_usd: float = 0.25,
    agent_factory=None,
) -> dict:
    from pydantic_ai.exceptions import UsageLimitExceeded
    from pydantic_ai.usage import UsageLimits

    setup = build_arm(arm, db_path, docs)
    try:
        factory = agent_factory or _default_agent_factory
        agent = factory(
            model=model, system_prompt=setup.system_prompt, tools=setup.tools
        )

        prompt = (
            f"{task['question']}\n\nAnswer guidelines: {task.get('guidelines', '')}"
        )
        limits = UsageLimits(
            tool_calls_limit=max_tool_calls, cost_limit=Decimal(str(per_task_usd))
        )

        answer, verdict = "", "hit_limit"
        in_tok = out_tok = cached = 0
        tool_calls: list[str] = []
        rejections = 0
        try:
            result = agent.run_sync(prompt, usage_limits=limits)
            # `AgentRunResult.usage` is a property, not a method, on
            # pydantic-ai 2.36.0's `AgentRunResult` — see
            # `test_agent_run_result_usage_is_a_property_not_a_method` in
            # `tests/test_agent.py`, which asserts this against the installed
            # library so a future version that changes this shape fails
            # loudly here instead of via a `TypeError` mid-sweep.
            usage = result.usage
            in_tok = usage.input_tokens
            out_tok = usage.output_tokens
            cached = getattr(usage, "cache_read_tokens", 0) or 0
            answer = str(result.output).strip()
            tool_calls = _tool_call_names(result)
            rejections = _inspect_rejections(result)
            verdict = "correct" if score(answer, gold) else "incorrect"
        except UsageLimitExceeded:
            # A cap trip is a harness artifact, not a wrong answer — see the
            # module-level `MAX_TOOL_CALLS` note and `tests/test_agent.py`.
            verdict = "hit_limit"
        except Exception as exc:
            verdict = "error"
            answer = f"{type(exc).__name__}: {exc}"
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
        gold=gold,
        verdict=verdict,
        in_tok=in_tok,
        out_tok=out_tok,
        cached_tok=cached,
        tool_calls=tool_calls,
        inspect_rejections=rejections,
    )
