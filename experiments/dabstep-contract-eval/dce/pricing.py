"""Pinned model snapshots, their pinned OpenRouter endpoints, and their prices.

Prices are USD per 1M tokens, recorded 2026-08-30 from
`/api/v1/models/<id>/endpoints` — from THE PINNED ENDPOINT, not from the model
card's headline rate. That distinction is the whole point of this module.

An unpinned model id would let OpenRouter silently re-point to a new snapshot
mid-sweep. An unpinned ENDPOINT does the same thing one level down, and is
worse for being invisible: one model id fans out to many endpoints that differ
in quantization, price, and context window, and routing is chosen PER REQUEST.
Measured — two identical back-to-back calls to `z-ai/glm-5.3-flash` were served
by Z.AI and then DeepInfra.

The spread is not a rounding error:

    deepseek-v4-flash-0731   $0.03 - $0.44 / 1M input   (14.7x, 30 endpoints)
    deepseek-v4-pro-0813     $0.58 - $1.32 / 1M input   (2.3x,  16 endpoints)
    glm-5.3-flash            $0.05 - $0.15 / 1M input   (3.0x,  20 endpoints)
    gpt-5.6-sol              $1.00 - $5.50 / 1M input   (5.5x,   7 endpoints)

So `provider_tag` and the three prices belong to one another and must move
together; changing the pin without repricing silently corrupts every `usd` in a
results file. `tests/test_pricing.py` asserts they stay consistent.

NOT EVERY MODEL HERE IS AN OPENROUTER MODEL. Two are served by an enterprise
LiteLLM gateway instead, on two DIFFERENT wire protocols —
`claudesonnet5` over Anthropic's own Messages API, `qwen3.6-27b` over the
OpenAI-compatible one — and `ModelSpec.route` says which a spec needs.
Everything above about pinning still applies to both, but both pins are weaker
than an OpenRouter one and they are weak for different reasons; read each
entry's own comment rather than assuming the OpenRouter reasoning transfers.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class ModelSpec:
    id: str
    #: OpenRouter endpoint tag, passed as `provider.order` with
    #: `allow_fallbacks: false`. Verified to hard-fail (HTTP 404) rather than
    #: silently re-route when it cannot be honoured — see
    #: `tests/test_pricing.py`'s note on the negative control.
    provider_tag: str
    #: Quantization of the pinned endpoint as OpenRouter reports it. Recorded,
    #: not enforced: fp4 and fp8 are different models in effect, and "unknown"
    #: means OpenRouter has no metadata, not that the endpoint is unquantized.
    quantization: str
    price_in: float  # USD per 1M fresh input tokens
    price_out: float  # USD per 1M output tokens (reasoning tokens included)
    price_cached: float  # USD per 1M cache-READ input tokens
    #: Whether this model lists `temperature` among its OpenRouter
    #: `supported_parameters`. `openai/gpt-5.6-sol` does not — it runs at its
    #: own default sampling while the rest are held at 0. See
    #: `dce.agent._default_agent_factory` for why this has to be applied via
    #: `extra_body` rather than `ModelSettings`.
    supports_temperature: bool
    role: str
    #: Which serving route reaches this model, and therefore which client
    #: `dce.agent._default_agent_factory` builds for it. `"openrouter"` is the
    #: original route (OpenAI-compatible chat completions through
    #: `OpenRouterProvider`, with the endpoint pin in `extra_body`);
    #: `"litellm_anthropic"` is Anthropic's own Messages API fronted by an
    #: enterprise-internal LiteLLM gateway, which is a different wire protocol,
    #: not merely a different base URL; `"litellm_openai"` is that same gateway's
    #: OpenAI-compatible route, which IS the same wire protocol as
    #: `"openrouter"` but reached through a different provider and with none of
    #: OpenRouter's endpoint-pinning vocabulary available. Defaulted so the four
    #: OpenRouter specs above read exactly as they did before this field
    #: existed.
    route: str = "openrouter"


MODELS: dict[str, ModelSpec] = {
    m.id: m
    for m in (
        # `deepinfra/fp8`: an established provider at a known quantization and
        # the full 1.05M context. OpenInference is cheaper ($0.03) but less
        # established; first-party DeepSeek is 2.75x dearer here ($0.22) AND
        # unreachable under this account's data policy (see the module note in
        # `dce/agent.py` on `_PROVIDER_PIN`).
        # `baidu/fp8`: PINNED ON THROUGHPUT AS WELL AS PRICE, which the
        # original pin was not. F3 chose endpoints on quantization and cost
        # alone; that is incomplete for a model this verbose. Measured on an
        # identical 3,000-token reasoning request, all fp8:
        #
        #     baidu       156.8 tok/s   0.0650 / 0.1299 / 0.0130
        #     baseten     112.3 tok/s   0.1300 / 0.2600 / 0.0280
        #     deepinfra    77.9 tok/s   0.0800 / 0.1800 / 0.0160   <- old pin
        #     parasail     55.1 tok/s   0.1400 / 0.2800 / 0.0500
        #     akashml      24.6 tok/s   0.0650 / 0.1800 / 0.0160
        #
        # Baidu dominates the old pin on BOTH axes at the same quantization —
        # 2.0x the throughput and cheaper on all three prices — so there is no
        # trade-off to weigh. Throughput matters here in a way it did not for
        # `glm-5.3-flash`: this model emits ~12,600 output tokens per run
        # against glm's ~1,700, so wall-clock is dominated by generation and a
        # 2x slower endpoint is a 2x longer sweep.
        #
        # fp8 deliberately retained rather than taking a cheaper fp4 endpoint
        # (Relace/Sail at $0.065): fp4 and fp8 are different models in effect,
        # and glm is pinned to fp8, so this keeps quantization constant across
        # the two model families being compared.
        ModelSpec(
            "deepseek/deepseek-v4-flash-0731",
            provider_tag="baidu/fp8",
            quantization="fp8",
            price_in=0.065,
            price_out=0.1299,
            price_cached=0.013,
            supports_temperature=True,
            role="weak",
        ),
        # `alibaba`: the cheapest reachable tier for this model, at 1.0M
        # context. Quantization is "unknown" — as it is for the first-party
        # DeepSeek endpoint too, which this account's data policy blocks
        # outright. Accepted rather than paying 1.9x for `gmicloud/fp8`:
        # quantization affects every arm of a given model identically, so it
        # is a fidelity caveat on absolute scores, not a confound in the
        # arm-to-arm comparison this experiment exists to make. Recorded on
        # every row so the caveat lives in the data.
        ModelSpec(
            "deepseek/deepseek-v4-pro-0813",
            provider_tag="alibaba",
            quantization="unknown",
            price_in=0.5808,
            price_out=1.7424,
            price_cached=0.0581,
            supports_temperature=True,
            role="strong",
        ),
        # `z-ai`: first-party, fp8, 1.05M context, and the rate our earlier
        # (endpoint-blind) table already happened to carry.
        ModelSpec(
            "z-ai/glm-5.3-flash",
            provider_tag="z-ai",
            quantization="fp8",
            price_in=0.075,
            price_out=0.25,
            price_cached=0.015,
            supports_temperature=True,
            role="cross_family_control",
        ),
        # `openai`: the standard tier, because it is the only one measured
        # to WORK. `openai/flex` is half price and, measured, faster --
        # serially 69.9 vs 70.5 tok/s, and 413 vs 299 tok/s aggregate under
        # 6-way concurrency. It was pinned here on 2026-09-01 on the strength
        # of those numbers and reverted the same day: under sustained sweep
        # load it returned bodies with `choices`/`model`/`object` all null on
        # 39 of 68 rows (58%), uniformly across arms, which pydantic-ai
        # cannot parse and the harness records as a terminal error. See
        # `results/sol-flex-aborted.jsonl`.
        #
        # The lesson is about what that benchmark measured, not about flex.
        # Throughput under a burst is not reliability under sustained load:
        # flex is best-effort by design and sheds requests rather than
        # queueing them, and three bursts of six requests never sampled that.
        # The same model on this standard tier ran the 50-row capability
        # probe with zero failures.
        #
        # `openai/fast` is 2x dearer than standard; `azure/*` and
        # `amazon-bedrock/*` are 2.2x-2.75x dearer than that.
        ModelSpec(
            "openai/gpt-5.6-sol",
            provider_tag="openai",
            quantization="unknown",
            price_in=2.00,
            price_out=10.00,
            price_cached=0.20,
            # Not in this model's OpenRouter `supported_parameters`.
            supports_temperature=False,
            role="frontier_subset",
        ),
        # NOT AN OPENROUTER MODEL. Anthropic Claude Sonnet 5 reached through
        # an enterprise LiteLLM gateway
        # (an internal LiteLLM deployment), which fronts Bedrock EU and
        # resolves this alias to `eu.anthropic.claude-sonnet-5`.
        #
        # `provider_tag` carries the resolved upstream rather than an
        # OpenRouter endpoint tag, and nothing sends it on the wire: the
        # gateway alias IS the pin. That is a weaker guarantee than the
        # OpenRouter specs above enjoy — the gateway can be re-pointed at a new
        # Bedrock snapshot without the alias changing, and no response field
        # would reveal it. Recorded here so the caveat lives in the data.
        #
        # Prices are the gateway's OWN metering, read from `/model/info` on
        # 2026-09-02 (prod): $2.00 / $10.00 per MTok, $0.20 per MTok cache
        # read, $2.50 per MTok cache write. Read from the gateway rather than
        # from Anthropic's list page for exactly the reason this module's
        # docstring gives for reading OpenRouter's endpoint rather than its
        # model card: the biller's number is the real one. Note these are
        # Anthropic's *introductory* Sonnet 5 rates, which its published
        # schedule ended on 2026-08-31 — the gateway was still metering at the
        # introductory tier when this was read, and a re-check is cheap.
        #
        # `supports_temperature=False`, and it is not a supported-parameters
        # technicality: Bedrock rejects `temperature=0` outright with HTTP 400
        # ("Only temperature=1 is supported"), and rejects `seed` the same way.
        # BOTH of this harness's determinism knobs are therefore unavailable
        # for this model, which the other four do not have to disclose. See
        # `dce.agent._litellm_anthropic_agent`.
        ModelSpec(
            "claudesonnet5",
            provider_tag="bedrock-eu/eu.anthropic.claude-sonnet-5",
            quantization="unknown",
            price_in=2.00,
            price_out=10.00,
            price_cached=0.20,
            supports_temperature=False,
            role="frontier_subset",
            route="litellm_anthropic",
        ),
        # ALSO NOT AN OPENROUTER MODEL, AND NOT THE SAME ROUTE AS THE ONE
        # ABOVE. Qwen3.6-27B self-hosted on vLLM in-house and reached
        # through the same production LiteLLM gateway, but over its
        # OpenAI-compatible `/v1/chat/completions` route rather than
        # Anthropic's Messages API. Added to answer a question the other five
        # cannot: every model measured so far is a commercial frontier or
        # near-frontier model, so "the contract arm wins on every model" was
        # only ever tested where the model was strong. A 27B open-weights
        # model served on our own hardware is the case that matters
        # operationally — it is what a cost-constrained deployment would
        # actually run.
        #
        # THE PIN IS THE WEAKEST OF ANY MODEL HERE, and measurably so. The
        # gateway alias fans out across THREE vLLM replicas which are NOT
        # running the same build: three consecutive requests on 2026-09-04
        # reported `system_fingerprint` of `vllm-0.22.1-tp4-efbb389f`,
        # `vllm-0.26.0-tp4-41646db4` and `vllm-0.26.0-tp4-34e424af`. That is
        # the same per-request fan-out this module's docstring describes for
        # OpenRouter — except OpenRouter gives us `provider.order` +
        # `allow_fallbacks: false` to pin it and fail loudly, and this route
        # gives us nothing. `provider_tag` therefore records the gateway's
        # `litellm_params.model` and pins nothing on the wire, exactly as the
        # `claudesonnet5` entry's does. `system_fingerprint` IS returned per
        # response, so which replica served a row is knowable — it is simply
        # not controllable.
        #
        # Prices are the gateway's OWN metering, read from `/v1/model/info` on
        # 2026-09-04: input is genuinely metered at $0.00 (self-hosted, no
        # per-token input charge configured) and output at $0.13205/MTok.
        # `price_cached` is 0.0 because there IS no cache tier — the gateway
        # reports `cache_read_input_token_cost: null`, vLLM's prefix cache is
        # not surfaced as `cached_tokens`, and so every row's `cached_tokens`
        # is an honest 0 rather than an unreported discount. Note the
        # consequence for `test_cache_read_is_cheaper_than_fresh_input`: a
        # no-cache route cannot satisfy a STRICT inequality, and the test was
        # widened rather than this priced with a fictional cache rate.
        #
        # `supports_temperature=True`, and unlike `claudesonnet5` this is not a
        # partial claim: vLLM accepts BOTH `temperature=0` and `seed=0`
        # (verified live, HTTP 200 on each). Both of this harness's determinism
        # controls are therefore available here — this model is the MORE
        # reproducible of the two gateway models, not the less.
        ModelSpec(
            "qwen3.6-27b",
            provider_tag="hosted_vllm/qwen3.6-27b",
            quantization="unknown",
            price_in=0.0,
            price_out=0.13205,
            price_cached=0.0,
            supports_temperature=True,
            role="open_weights_self_hosted",
            route="litellm_openai",
        ),
        # Qwen3.8-27B, self-hosted on vLLM behind the same gateway and route as
        # `qwen3.6-27b`, whose caveats all apply. It is the model MotherDuck's
        # local-model report ran (`motherduck-local` in the paper's refs), so
        # it is the candidate that would put their number and ours on the
        # same model -- though they ran it at 4-bit, and this deployment is
        # FP8. The gateway's API does not report quantization; FP8 was
        # confirmed out of band on 2026-09-25, after the panel commit was
        # fixed, so the field below stays "unknown" to match what every panel
        # row records.
        #
        # Prices are the gateway's own metering, read from `/model/info` on
        # 2026-09-24, and identical to `qwen3.6-27b`'s: input $0.00, output
        # $0.13205/MTok, no cache tier.
        #
        # `supports_temperature=True`, verified live the same day: HTTP 200
        # on `temperature=0` with `seed=0`. The gateway lists two deployments
        # for this alias, both on H100 GPUs; four consecutive requests all
        # reported `system_fingerprint` `vllm-0.28.0-tp4-2a8529a2`, so the
        # fan-out that `qwen3.6-27b` shows across builds was not observed
        # here -- which four requests cannot rule out.
        ModelSpec(
            "qwen3.8-27b",
            provider_tag="hosted_vllm/qwen3.8-27b",
            quantization="unknown",
            price_in=0.0,
            price_out=0.13205,
            price_cached=0.0,
            supports_temperature=True,
            role="open_weights_self_hosted",
            route="litellm_openai",
        ),
    )
}


def cost(model: str, in_tok: int, out_tok: int, cached_tok: int = 0) -> float:
    """USD for a call. Raises KeyError on an unpinned or unknown model id.

    `cached_tok` is the cache-READ subset of `in_tok`, priced at
    `price_cached`; the remainder is priced at `price_in`. Charging every input
    token at the fresh rate — which this function did until F4 — overstates
    spend by a factor that RISES WITH CACHE-HIT RATE, and cache-hit rate rises
    with conversation length. Measured across one task's three arms: 1.94x,
    2.17x and 2.81x respectively. That is not a uniform bias that cancels out
    of a comparison; it inflates the longest-context arm the most, and it is
    this library's own arm. The discount is large — 5x on glm-5.3-flash, 10x on
    gpt-5.6-sol, 30x on deepseek-v4-pro-0813 — so ignoring it is not a rounding
    error either.

    `cached_tok` is clamped into `[0, in_tok]`: a provider reporting more cache
    reads than input tokens must not be able to produce a negative price, which
    `dce.runner`'s spend guard would read as income.
    """
    spec = MODELS[model]
    cached = max(0, min(cached_tok, in_tok))
    fresh = in_tok - cached
    return (
        fresh * spec.price_in + cached * spec.price_cached + out_tok * spec.price_out
    ) / 1_000_000
