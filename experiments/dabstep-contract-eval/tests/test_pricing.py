import pytest
from dce.pricing import MODELS, cost


def test_all_four_models_are_pinned_snapshots():
    assert set(MODELS) == {
        "deepseek/deepseek-v4-flash-0731",
        "deepseek/deepseek-v4-pro-0813",
        "z-ai/glm-5.3-flash",
        "openai/gpt-5.6-sol",
    }


def test_cost_is_per_million_tokens():
    # pro-0813 is 0.66 in / 1.98 out per 1M tokens.
    assert cost("deepseek/deepseek-v4-pro-0813", 1_000_000, 0) == pytest.approx(0.66)
    assert cost("deepseek/deepseek-v4-pro-0813", 0, 1_000_000) == pytest.approx(1.98)
    assert cost("deepseek/deepseek-v4-pro-0813", 30_000, 2_000) == pytest.approx(
        0.0198 + 0.00396
    )


def test_unknown_model_raises_rather_than_guessing():
    # A silent 0.0 would let an unbudgeted model run to completion.
    with pytest.raises(KeyError):
        cost("deepseek/deepseek-v4-pro", 100, 100)
