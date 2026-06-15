from mje.model_client import PRICING, ModelClient, cost


class _FakeResp:
    def __init__(self):
        msg = type("M", (), {"content": '["t0.c0 = t1.c1"]'})
        self.choices = [type("C", (), {"message": msg})]
        self.usage = type("U", (), {"prompt_tokens": 1000, "completion_tokens": 50})


class _FakeChat:
    def __init__(self):
        self.completions = self

    def create(self, **kwargs):
        self.kwargs = kwargs
        return _FakeResp()


class _FakeClient:
    def __init__(self):
        self.chat = _FakeChat()


def test_cost_uses_pricing_table():
    c = cost("anthropic/claude-sonnet-4.6", 1_000_000, 1_000_000)
    p = PRICING["anthropic/claude-sonnet-4.6"]
    assert round(c, 6) == round(p["in"] + p["out"], 6)


def test_client_call_returns_text_and_usage():
    mc = ModelClient(api_key="x", client=_FakeClient())
    text, in_tok, out_tok = mc.call(
        "anthropic/claude-sonnet-4.6",
        [{"role": "user", "content": "hi"}],
        max_tokens=256,
    )
    assert text == '["t0.c0 = t1.c1"]'
    assert in_tok == 1000 and out_tok == 50
