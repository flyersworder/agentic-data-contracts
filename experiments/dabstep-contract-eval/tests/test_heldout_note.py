import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "analysis"))

import heldout_note as hn  # noqa: E402


def _row(task, arm, verdict):
    return {"task_id": task, "arm": arm, "verdict": verdict}


def test_a_task_is_inspected_when_the_text_won_and_the_prose_lost_in_both_repeats():
    r1 = [
        _row("1", "contract_text", "correct"),
        _row("1", "manual_resolved", "incorrect"),
        _row("2", "contract_text", "correct"),
        _row("2", "manual_resolved", "incorrect"),
        _row("3", "contract_text", "correct"),
        _row("3", "manual_resolved", "correct"),
    ]
    r2 = [
        _row("1", "contract_text", "correct"),
        _row("1", "manual_resolved", "incorrect"),
        _row("2", "contract_text", "incorrect"),
        _row("2", "manual_resolved", "incorrect"),
        _row("3", "contract_text", "correct"),
        _row("3", "manual_resolved", "incorrect"),
    ]
    assert hn.inspected(r1, r2, among={"1", "2", "3"}) == {"1"}


def test_only_rule_matching_tasks_can_be_inspected():
    r = [
        _row("1", "contract_text", "correct"),
        _row("1", "manual_resolved", "incorrect"),
    ]
    assert hn.inspected(r, r, among={"2"}) == set()
