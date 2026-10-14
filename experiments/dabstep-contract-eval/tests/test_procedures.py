import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "analysis"))

import procedures as pr  # noqa: E402


def test_the_piece_is_read_from_the_task_id():
    assert pr.piece_of("solar_panel_R03") == "R"
    assert pr.piece_of("labor_certification_applications_C10") == "C"
    with pytest.raises(ValueError):
        pr.piece_of("solar_panel_7")


def test_scores_pool_models_and_repeats(tmp_path):
    rows = [
        {"task_id": "d_R01", "arm": "contract", "verdict": "correct"},
        {"task_id": "d_R01", "arm": "contract", "verdict": "incorrect"},
        {"task_id": "d_R01", "arm": "manual_prompt", "verdict": "correct"},
    ]
    score = pr.pool([rows[:2], rows[2:]])
    assert score["contract"]["d_R01"] == 1
    assert score["manual_prompt"]["d_R01"] == 1


def test_tie_rule():
    assert pr.is_tie(0.03, 0.5)
    assert not pr.is_tie(0.08, 0.5)
    assert not pr.is_tie(0.0, 0.05)
