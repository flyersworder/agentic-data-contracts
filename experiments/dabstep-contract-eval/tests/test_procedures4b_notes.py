"""Experiment 4b's notes: the author's resolutions in words, for
`manual_noted`, written by one model and checked by another, both blind to
questions, golds and results."""

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "prep" / "procedures4b"))

import notes  # noqa: E402


def test_writer_and_checker_are_different_models():
    assert notes.WRITER != notes.CHECKER


def test_the_prompt_carries_the_documentation_and_the_contract_only():
    text = notes.user_prompt(
        db="shop",
        kb=[{"id": 1, "knowledge": "Tier", "description": "d", "definition": "x"}],
        contract_yml="semantic: {}",
        semantic_yml="metrics: []",
    )
    assert "[1] Tier: d x" in text
    assert "semantic: {}" in text and "metrics: []" in text
    for word in ("question", "gold", "answer"):
        assert word not in text.lower()


def test_notes_must_be_words_not_sql():
    assert notes.check_notes("The score is stored one point higher.") == []
    assert notes.check_notes("") != []
    assert any("SQL" in p for p in notes.check_notes("SELECT x FROM t WHERE y"))


def test_verdicts_are_structured():
    assert notes.check_verdict({"complete": True, "missing": [], "extra": []}) == []
    assert notes.check_verdict({"complete": False, "missing": ["a"], "extra": []}) == []
    assert notes.check_verdict({"complete": True}) != []


def _fake_driver(calls, submit_with=None):
    def driver(*, system_prompt, user_prompt, tools):
        import inspect

        (tool,) = tools
        calls.append((tool.__name__, inspect.signature(tool, eval_str=True)))
        if submit_with is not None and tool.__name__ == "submit_notes":
            tool("The score is stored one point higher.")
        if tool.__name__ == "submit_verdict":
            tool({"complete": True, "missing": [], "extra": []})

        class Result:
            usage: dict = {}
            messages: list = []

        return Result()

    return driver


def _folder(tmp_path):
    (tmp_path / "contract.yml").write_text("semantic: {}", encoding="utf-8")
    (tmp_path / "semantic.yml").write_text("metrics: []", encoding="utf-8")
    return tmp_path


def test_the_submit_tools_have_typed_named_parameters(tmp_path):
    # An untyped parameter gives a tool schema some providers reject.
    calls = []
    driver = _fake_driver(calls, submit_with=True)
    notes.write_notes("shop", [], _folder(tmp_path), driver, driver)
    sigs = dict(calls)
    assert str(sigs["submit_notes"]) == "(notes: str) -> str"
    assert str(sigs["submit_verdict"]) == "(verdict: dict) -> str"
    assert "stored one point higher" in (tmp_path / "notes.md").read_text(
        encoding="utf-8"
    )


def test_no_notes_are_written_when_the_writer_submits_none(tmp_path):
    driver = _fake_driver([])
    with pytest.raises(SystemExit, match="no notes"):
        notes.write_notes("shop", [], _folder(tmp_path), driver, driver)
    assert not (tmp_path / "notes.md").exists()


def test_a_checker_that_gives_no_verdict_stops_the_run(tmp_path):
    def driver(*, system_prompt, user_prompt, tools):
        (tool,) = tools
        if tool.__name__ == "submit_notes":
            tool("A note.")

        class Result:
            usage: dict = {"error": "boom"}
            messages: list = []

        return Result()

    with pytest.raises(SystemExit, match="no verdict"):
        notes.write_notes("shop", [], _folder(tmp_path), driver, driver)
    assert not (tmp_path / "notes.md").exists()
