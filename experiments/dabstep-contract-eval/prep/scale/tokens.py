"""Experiment 6's sizes in tokens, and which arm-levels fit each model
(design: Materials, Context windows). Writes contracts-scale/fit.json.

Each arm-level's system prompt, built as a run builds it, is sent once to
gpt-6-luna and once to qwen3.8-27b; the reported input tokens are its size.
gpt-6-sol shares luna's tokenizer (the limit probe of 2026-10-20 reported
identical counts at every size, 12,244 to 596,253 tokens), so luna's count
stands for sol's. An arm-level fits a model if its largest prompt over the
three bases plus the 95th percentile of `contract_text`'s conversation
growth in experiment 4b fits the model's input limit:

* qwen3.8-27b: a 262,144-token window, less the harness's 64,000-token
  output reservation; growth 4,282.
* gpt-6-luna and gpt-6-sol: no limit found up to 596,253 tokens, the
  largest probe; growth 2,184 and 2,161. A prompt above the probe does not
  count as fitting.

    UV_FROZEN=1 uv run python prep/scale/tokens.py   (with .env loaded)
"""

from __future__ import annotations

import asyncio
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

from dce.agent import _default_agent_factory, _is_context_overflow  # noqa: E402
from dce.benchmarks import livesqlbench as lsb  # noqa: E402
from prep.scale.build import BASE_CONTRACTS, BASES, OUT  # noqa: E402

DATA = Path.home() / "data" / "scale"
LIMITS = {
    "gpt-6-luna": (596_253, 2_184),
    "gpt-6-sol": (596_253, 2_161),
    "qwen3.8-27b": (262_144 - 64_000, 4_282),
}
TOKENIZERS = {
    "gpt-6-luna": "gpt-6-luna",
    "gpt-6-sol": "gpt-6-luna",
    "qwen3.8-27b": "qwen3.8-27b",
}


async def measure(model: str, prompt: str) -> int | None:
    agent = _default_agent_factory(
        model=model, system_prompt=prompt, tools=[], retries=0
    )
    try:
        result = await agent.run("Reply with OK.")
    except Exception as exc:
        if _is_context_overflow(exc):
            return None
        raise
    return result.usage.input_tokens


async def main() -> None:
    bench = lsb.LiveSQLBench.from_data(DATA, contracts=BASE_CONTRACTS, dbs=BASES)
    tokens: dict[str, dict[str, dict[str, int | None]]] = {}
    for tokenizer in sorted(set(TOKENIZERS.values())):
        tokens[tokenizer] = {}
        for base in BASES:
            task = next(t for t in bench.tasks() if t.meta["db"] == base)
            for arm in lsb.SCALE_ARMS:
                setup = bench.build_arm(arm, task, bench.pristine_db(task))
                n = await measure(tokenizer, setup.system_prompt)
                setup.close()
                tokens[tokenizer].setdefault(arm, {})[base] = n
                print(tokenizer, base, arm, n, flush=True)
    fit: dict[str, dict[str, dict]] = {}
    for model, (limit, growth) in LIMITS.items():
        fit[model] = {}
        for arm, per in tokens[TOKENIZERS[model]].items():
            counts = [n for n in per.values() if n is not None]
            worst = max(counts) if len(counts) == len(per) else None
            fit[model][arm] = {
                "tokens": per,
                "fits": worst is not None and worst + growth <= limit,
            }
    (OUT / "fit.json").write_text(json.dumps(fit, indent=1) + "\n", encoding="utf-8")
    for model, arms in fit.items():
        print(model, "does not fit:", [a for a, v in arms.items() if not v["fits"]])


if __name__ == "__main__":
    asyncio.run(main())
