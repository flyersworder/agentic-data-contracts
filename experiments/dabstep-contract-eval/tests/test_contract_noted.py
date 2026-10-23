import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "analysis"))

import contract_noted as cn  # noqa: E402


def test_each_model_reads_its_own_new_repeats():
    assert cn.FILES == {"luna": "exp5c-luna-r{}.jsonl", "qwen": "exp5c-qwen-r{}.jsonl"}


def test_the_three_arms_run_together():
    assert cn.ARMS == ("manual_summed", "contract_text", "contract_text_noted")


def test_the_reruns_pair_with_the_earlier_files_of_the_same_arm():
    # luna's manual_summed ran in experiment 5b; everything else in exp5.
    assert cn.EARLIER["luna"] == {
        "manual_summed": "exp5b-luna-r{}.jsonl",
        "contract_text": "exp5-luna-r{}.jsonl",
    }
    assert cn.EARLIER["qwen"] == {
        "manual_summed": "exp5-qwen-r{}.jsonl",
        "contract_text": "exp5-qwen-r{}.jsonl",
    }


def test_predictions_are_labelled_per_model():
    # (31) is registered on Qwen only, where the text arm fell behind.
    assert cn.LABELS == {"luna": ("(30)", None), "qwen": ("(30Q)", "(31Q)")}


def test_drift_is_flagged_beyond_five_points_either_way():
    assert not cn.drifted(0.05)
    assert not cn.drifted(-0.05)
    assert cn.drifted(0.051)
    assert cn.drifted(-0.06)


def test_scores_count_correct_runs_per_task(tmp_path):
    import json

    base = {"model": "m", "gold": "x"}
    rows = [
        {**base, "task_id": 1, "arm": "a", "verdict": "correct", "answer": "x"},
        {**base, "task_id": 2, "arm": "a", "verdict": "incorrect", "answer": "y"},
        {**base, "task_id": 1, "arm": "b", "verdict": "correct", "answer": "x"},
    ]
    for i in (1, 2, 3):
        path = tmp_path / f"f-r{i}.jsonl"
        path.write_text(
            "\n".join(json.dumps({**r, "order": n}) for n, r in enumerate(rows)),
            encoding="utf-8",
        )
    score = cn.scores(tmp_path, "f-r{}.jsonl", ("a",), e2e=False)
    assert score["a"]["1"] == 3
    assert score["a"]["2"] == 0
    assert "b" not in score
