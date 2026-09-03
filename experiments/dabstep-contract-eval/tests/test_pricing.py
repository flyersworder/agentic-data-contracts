import pytest
from dce.agent import REASONING_EFFORT, reasoning_effort_for
from dce.pricing import MODELS, cost


def test_every_model_is_a_pinned_snapshot():
    assert set(MODELS) == {
        "deepseek/deepseek-v4-flash-0731",
        "deepseek/deepseek-v4-pro-0813",
        "z-ai/glm-5.3-flash",
        "openai/gpt-5.6-sol",
        "claudesonnet5",
    }


def test_only_openrouter_models_carry_an_openrouter_endpoint_pin():
    """`claudesonnet5` is not an OpenRouter model, and the difference is not
    cosmetic: its `provider_tag` names the upstream the enterprise gateway
    resolves its alias to, and nothing sends that tag on the wire. The
    OpenRouter specs' `provider_tag` IS the pin, enforced per request; this
    one is a record of what the alias meant when it was read. Asserting the
    two are the same kind of thing would be the sort of quiet fiction the
    module docstring warns about.
    """
    by_route = {}
    for spec in MODELS.values():
        by_route.setdefault(spec.route, []).append(spec.id)
    assert set(by_route) == {"openrouter", "litellm_anthropic"}
    assert by_route["litellm_anthropic"] == ["claudesonnet5"]
    assert len(by_route["openrouter"]) == 4


def test_every_model_pins_an_endpoint_not_just_an_id():
    """F3. One model id fans out to many OpenRouter endpoints — 30 for
    `deepseek-v4-flash-0731` — that differ in quantization, price and context,
    and routing is chosen PER REQUEST (measured: two identical back-to-back
    calls served by Z.AI then DeepInfra). A model id alone therefore pins
    nothing that matters.
    """
    for spec in MODELS.values():
        assert spec.provider_tag, spec.id
        assert spec.quantization in {"fp4", "fp8", "unknown"}, spec.id


def test_cost_is_per_million_tokens():
    spec = MODELS["deepseek/deepseek-v4-pro-0813"]
    assert cost(spec.id, 1_000_000, 0) == pytest.approx(spec.price_in)
    assert cost(spec.id, 0, 1_000_000) == pytest.approx(spec.price_out)
    assert cost(spec.id, 30_000, 2_000) == pytest.approx(
        30_000 * spec.price_in / 1e6 + 2_000 * spec.price_out / 1e6
    )


def test_cached_tokens_are_billed_at_the_cache_read_rate():
    """F4, the correction that matters most to the headline.

    Cache reads are billed far below fresh input — measured live against
    `z-ai/glm-5.3-flash`, an all-cached repeat cost 21% of a fresh call. The
    old `cost()` charged every input token fresh.
    """
    spec = MODELS["z-ai/glm-5.3-flash"]
    all_fresh = cost(spec.id, 1_000_000, 0, 0)
    all_cached = cost(spec.id, 1_000_000, 0, 1_000_000)
    assert all_fresh == pytest.approx(spec.price_in)
    assert all_cached == pytest.approx(spec.price_cached)
    assert all_cached < all_fresh
    half = cost(spec.id, 1_000_000, 0, 500_000)
    assert half == pytest.approx((spec.price_in + spec.price_cached) / 2)


def test_ignoring_the_cache_discount_biases_the_longest_context_arm_hardest():
    """Why F4 is a bias and not a rounding error.

    These are the three arms of the one task the first smoke run completed,
    with their real measured token counts. The over-charge factor rises with
    cache-hit rate, and cache-hit rate rises with conversation length — so the
    old formula inflated the arms unequally, worst for arm C, which is this
    library's own arm. Charging everything fresh reported the contract:floor
    cost ratio as 11.0x when it is really 7.6x.
    """
    glm = "z-ai/glm-5.3-flash"
    measured = {  # arm: (input, cached, output)
        "schema_only": (58_076, 46_464, 5_578),
        "manual_prompt": (153_981, 128_192, 10_786),
        "contract": (781_609, 678_336, 18_505),
    }
    inflation = {}
    for arm, (i, c, o) in measured.items():
        correct = cost(glm, i, o, c)
        naive = cost(glm, i, o, 0)
        inflation[arm] = naive / correct

    # Not a uniform bias that cancels out of a comparison.
    assert inflation["contract"] > inflation["manual_prompt"] > inflation["schema_only"]
    assert inflation["schema_only"] == pytest.approx(1.94, abs=0.05)
    assert inflation["contract"] == pytest.approx(2.81, abs=0.05)

    ratio = cost(glm, *(measured["contract"][i] for i in (0, 2, 1))) / cost(
        glm, *(measured["schema_only"][i] for i in (0, 2, 1))
    )
    assert ratio == pytest.approx(7.6, abs=0.2)


def test_cached_tokens_cannot_produce_a_negative_or_inflated_price():
    """`dce.runner`'s spend guard sums these figures; a negative one would read
    as income and let a sweep run past its cap."""
    spec = MODELS["z-ai/glm-5.3-flash"]
    # More cache reads than input tokens: clamped, not negative.
    assert cost(spec.id, 100, 0, 10_000) == pytest.approx(100 * spec.price_cached / 1e6)
    assert cost(spec.id, 100, 0, -50) == pytest.approx(100 * spec.price_in / 1e6)
    assert cost(spec.id, 0, 0, 0) == 0.0


def test_cache_read_is_cheaper_than_fresh_input_for_every_pinned_model():
    for spec in MODELS.values():
        assert spec.price_cached < spec.price_in, spec.id


def test_unknown_model_raises_rather_than_guessing():
    # A silent 0.0 would let an unbudgeted model run to completion.
    with pytest.raises(KeyError):
        cost("deepseek/deepseek-v4-pro", 100, 100)


def test_reasoning_effort_is_an_explicit_value_not_a_provider_default():
    """F3's lesson applied to a second knob: an unset parameter is not a fixed
    parameter. Every pinned model reasons by default, the default differs by
    endpoint, and reasoning tokens bill at the OUTPUT rate."""
    assert REASONING_EFFORT in {"minimal", "low", "medium", "high"}


def test_every_route_really_sends_the_effort_its_rows_claim():
    """The stamp records the control that was applied, so every route has to
    actually apply it. They use different parameters to do so — OpenRouter's
    `reasoning.effort` in `extra_body`, Anthropic's `anthropic_effort` — and
    the per-route factory tests assert each one goes out. This asserts the
    stamp agrees with them.
    """
    for spec in MODELS.values():
        assert reasoning_effort_for(spec.id) == REASONING_EFFORT, spec.id

    # An unpinned id must not raise: this runs on `_priced_fallback_row`'s
    # non-raising path, the same contract `_spec_field` documents.
    assert reasoning_effort_for("not/a-real-model") == REASONING_EFFORT
