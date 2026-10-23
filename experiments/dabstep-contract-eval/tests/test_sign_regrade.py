import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "analysis"))

import sign_regrade as sr  # noqa: E402


def test_the_sign_is_read_from_the_number_the_scorer_reads():
    assert sr.sign_of("-5.24") == -1
    assert sr.sign_of("5.24") == 1
    assert sr.sign_of("The delta is 74.67") == 1
    assert sr.sign_of("−2.5") == -1
    assert sr.sign_of("$-1,234.5") == -1
    assert sr.sign_of("0.000") == 0
    assert sr.sign_of("no number") == 0


def test_a_hyphen_inside_a_word_is_not_a_minus():
    assert sr.sign_of("year-end 12.5") == 1


def test_a_correct_row_with_the_wrong_sign_is_demoted():
    row = {"verdict": "correct", "answer": "5.24", "gold": "-5.24"}
    assert sr.demote(row)["verdict"] == "incorrect"


def test_a_row_with_the_right_sign_or_no_number_is_untouched():
    assert (
        sr.demote({"verdict": "correct", "answer": "-5.24", "gold": "-5.24"})["verdict"]
        == "correct"
    )
    assert (
        sr.demote({"verdict": "correct", "answer": "A, B", "gold": "A, B"})["verdict"]
        == "correct"
    )


def test_other_benchmarks_and_wrong_rows_are_untouched():
    lsb = {
        "benchmark": "livesqlbench",
        "verdict": "correct",
        "answer": "5",
        "gold": "-5",
    }
    assert sr.demote(lsb) == lsb
    wrong = {"verdict": "incorrect", "answer": "5", "gold": "-5"}
    assert sr.demote(wrong) == wrong


def test_the_end_to_end_reading_also_needs_the_sign():
    assert not sr.strict_final_paragraph("working\n\n5.24", "-5.24")
    assert sr.strict_final_paragraph("working\n\n-5.24", "-5.24")
