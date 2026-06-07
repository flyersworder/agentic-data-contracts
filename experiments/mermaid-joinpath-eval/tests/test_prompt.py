from mje.prompt import build_messages


def test_build_messages_contains_rendering_and_tables():
    msgs = build_messages("RENDERING_TEXT", ["t0", "t2", "t5"])
    assert msgs[0]["role"] == "system"
    assert msgs[1]["role"] == "user"
    user = msgs[1]["content"]
    assert "RENDERING_TEXT" in user
    assert "t0" in user and "t2" in user and "t5" in user
    assert "JSON" in user or "json" in user
