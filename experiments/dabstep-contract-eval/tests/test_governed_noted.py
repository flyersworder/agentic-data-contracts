import sys
from collections import Counter
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "analysis"))

import governed_noted as gn  # noqa: E402


def test_each_model_reads_its_own_new_repeats():
    assert gn.FILES == {"luna": "exp5d-luna-r{}.jsonl", "qwen": "exp5d-qwen-r{}.jsonl"}


def test_the_three_arms_run_together():
    assert gn.ARMS == ("contract", "contract_text_noted", "contract_noted")


def test_the_reruns_pair_with_the_earlier_files_of_the_same_arm():
    assert gn.EARLIER["luna"] == {
        "contract": "exp5-luna-r{}.jsonl",
        "contract_text_noted": "exp5c-luna-r{}.jsonl",
    }
    assert gn.EARLIER["qwen"] == {
        "contract": "exp5-qwen-r{}.jsonl",
        "contract_text_noted": "exp5c-qwen-r{}.jsonl",
    }


def test_predictions_are_labelled_per_model():
    assert gn.LABELS == {"luna": ("(41)", "(42)"), "qwen": ("(41Q)", "(42Q)")}


def _score(per_task: dict[str, dict[str, int]]) -> dict[str, Counter[str]]:
    return {arm: Counter(tasks) for arm, tasks in per_task.items()}


def test_the_route_test_is_a_tie_within_five_points_and_p_at_least_0_10():
    tasks = {str(t) for t in range(40)}
    same = {t: 2 for t in tasks}
    score = _score({"contract_noted": same, "contract_text_noted": same})
    holds, gap, up, down, p = gn.route_tie(score, tasks)
    assert holds and gap == 0 and (up, down) == (0, 0)

    ahead = {t: 3 for t in tasks}
    score = _score({"contract_noted": ahead, "contract_text_noted": same})
    holds, gap, up, down, p = gn.route_tie(score, tasks)
    assert not holds and up == 40 and down == 0


def test_the_notes_test_on_qwen_is_one_sided_and_the_luna_one_a_tie():
    tasks = {str(t) for t in range(40)}
    low = {t: 1 for t in tasks}
    high = {t: 3 for t in tasks}
    score = _score({"contract_noted": high, "contract": low})
    assert gn.notes_test("qwen", score, tasks)[0]
    assert not gn.notes_test("luna", score, tasks)[0]
    flat = _score({"contract_noted": low, "contract": low})
    assert gn.notes_test("luna", flat, tasks)[0]
    assert not gn.notes_test("qwen", flat, tasks)[0]


def test_the_clustered_contrasts_cover_both_tests_on_every_subset():
    tasks = {str(t) for t in range(6)}
    score = _score(
        {
            "contract_noted": {t: 3 for t in tasks},
            "contract_text_noted": {t: 1 for t in tasks},
            "contract": {t: 2 for t in tasks},
        }
    )
    clusterings = {"shape": {t: f"s{int(t) % 3}" for t in tasks}}
    out = gn.clustered_contrasts(score, (("all", tasks),), clusterings)
    assert [label for label, _ in out] == [
        "all: contract_noted vs contract_text_noted",
        "all: contract_noted vs contract",
    ]
    first = out[0][1]
    assert first["n"] == 6 and first["tasks"] == (6, 0)
    assert first["clusters"]["shape"]["n"] == 3
