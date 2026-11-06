import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "analysis"))

import exact_match as em  # noqa: E402


def _row(answer: str, gold: str, verdict: str) -> dict:
    return {
        "benchmark": "dabstep",
        "task_id": "1",
        "arm": "a",
        "answer": answer,
        "gold": gold,
        "verdict": verdict,
    }


def test_the_four_views_are_named_in_order():
    assert tuple(em.VIEWS) == (
        "official",
        "end-to-end",
        "exact, full message",
        "exact, final paragraph",
    )


def test_exact_full_message_keeps_only_officially_correct_exact_answers():
    assert em.exact_full(_row("**0.5**", "0.5", "correct"))
    assert not em.exact_full(_row("0.50", "0.5", "correct"))
    assert not em.exact_full(_row("0.5", "0.5", "incorrect"))


def test_exact_full_message_does_not_recover_a_final_paragraph():
    row = _row("Working...\n\n0.5", "0.5", "incorrect")
    assert not em.exact_full(row)


def test_exact_final_paragraph_grades_the_recovered_paragraph_exactly():
    assert em.exact_paragraph(_row("Working...\n\n0.5", "0.5", "incorrect"))
    assert not em.exact_paragraph(_row("Working...\n\n0.50", "0.5", "incorrect"))


def test_exact_final_paragraph_agrees_with_exact_full_on_officially_correct_rows():
    assert em.exact_paragraph(_row("0.5", "0.5", "correct"))
    assert not em.exact_paragraph(_row("0.50", "0.5", "correct"))


def test_exact_final_paragraph_never_credits_what_end_to_end_rejects():
    assert not em.exact_paragraph(_row("Working...\n\n0.7", "0.5", "incorrect"))
    assert not em.exact_paragraph(_row("0.5", "0.5", "error"))


def test_the_contrasts_are_form_route_and_route_with_notes():
    assert em.CONTRASTS == {
        "form (5c)": ("exp5c-{m}-r{i}.jsonl", "contract_text_noted", "manual_summed"),
        "route (5)": ("exp5-{m}-r{i}.jsonl", "contract", "contract_text"),
        "route, notes in both (5d)": (
            "exp5d-{m}-r{i}.jsonl",
            "contract_noted",
            "contract_text_noted",
        ),
    }


def test_scores_count_correct_repeats_per_task_under_a_view():
    rows = [
        [
            _row("0.5", "0.5", "correct"),
            {**_row("x\n\n0.5", "0.5", "incorrect"), "arm": "b"},
        ],
        [_row("0.50", "0.5", "correct"), {**_row("0.5", "0.5", "correct"), "arm": "b"}],
    ]
    score = em.scores_from(rows, ("a", "b"), em.VIEWS["exact, full message"])
    assert score["a"]["1"] == 1 and score["b"]["1"] == 1
    score = em.scores_from(rows, ("a", "b"), em.VIEWS["end-to-end"])
    assert score["a"]["1"] == 2 and score["b"]["1"] == 2
