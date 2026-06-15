from __future__ import annotations

import os
import time

# USD per million tokens. Source: OpenRouter pricing.
PRICING = {
    "anthropic/claude-sonnet-4.6": {"in": 3.0, "out": 15.0},
    "deepseek/deepseek-v4-flash": {"in": 0.0983, "out": 0.1966},
}


def cost(model: str, in_tok: int, out_tok: int) -> float:
    """Return cost in USD. PRICING values are USD per 1 M tokens."""
    p = PRICING[model]
    return (in_tok * p["in"] + out_tok * p["out"]) / 1_000_000


class ModelClient:
    def __init__(
        self,
        api_key: str | None = None,
        base_url: str = "https://openrouter.ai/api/v1",
        client=None,
    ):
        if client is not None:
            self._client = client
        else:
            from openai import OpenAI

            key = api_key or os.environ["OPENROUTER_API_KEY"]
            self._client = OpenAI(api_key=key, base_url=base_url)

    def call(
        self, model: str, messages: list[dict], max_tokens: int = 256, retries: int = 3
    ) -> tuple[str, int, int]:
        last = None
        for attempt in range(retries):
            try:
                resp = self._client.chat.completions.create(
                    model=model,
                    messages=messages,
                    temperature=0,
                    max_tokens=max_tokens,
                )
                text = resp.choices[0].message.content or ""
                u = resp.usage
                return text, int(u.prompt_tokens), int(u.completion_tokens)
            except Exception as e:  # noqa: BLE001
                last = e
                time.sleep(2**attempt)
        raise RuntimeError(f"model call failed after {retries} retries: {last}")
