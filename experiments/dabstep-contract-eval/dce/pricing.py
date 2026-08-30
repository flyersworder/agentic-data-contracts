"""Pinned model snapshots and their OpenRouter prices.

Prices are USD per 1M tokens, recorded 2026-08-30. An unpinned model id would
let OpenRouter silently re-point to a new snapshot mid-sweep, putting two
different models in one results file with no column recording it.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class ModelSpec:
    id: str
    price_in: float  # USD per 1M input tokens
    price_out: float  # USD per 1M output tokens
    role: str


MODELS: dict[str, ModelSpec] = {
    m.id: m
    for m in (
        ModelSpec("deepseek/deepseek-v4-flash-0731", 0.065, 0.18, "weak"),
        ModelSpec("deepseek/deepseek-v4-pro-0813", 0.66, 1.98, "strong"),
        ModelSpec("z-ai/glm-5.3-flash", 0.075, 0.25, "cross_family_control"),
        ModelSpec("openai/gpt-5.6-sol", 2.00, 10.00, "frontier_subset"),
    )
}


def cost(model: str, in_tok: int, out_tok: int) -> float:
    """USD for a call. Raises KeyError on an unpinned or unknown model id."""
    spec = MODELS[model]
    return (in_tok * spec.price_in + out_tok * spec.price_out) / 1_000_000
