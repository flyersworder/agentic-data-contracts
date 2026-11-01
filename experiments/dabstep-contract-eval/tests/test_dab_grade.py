"""Experiment 7's grader: DataAgentBench's own validate.py, run on the final
answer in an isolated subprocess, plus the overlong-pass flag."""

from pathlib import Path

import pytest
from dce.benchmark import Grade
from dce.benchmarks import dab_grade

VALIDATE = """import re


def validate(llm_output: str):
    if re.search(r"\\b42\\b", llm_output):
        return True, "found"
    return False, "not found"
"""

SLOW = """def validate(llm_output: str):
    while True:
        pass
"""

RAISES = """def validate(llm_output: str):
    raise ValueError("boom")
"""

HELPER = """import re
from common_scaffold.validate.levenshtein import levenshtein


def validate(llm_output: str):
    return levenshtein(llm_output, "abc") <= 1, "ok"
"""

ECHO = """def validate(llm_output: str):
    return llm_output == "a\\r\\nb\\u00e9", "exact"
"""


def query_dir(tmp_path: Path, source: str = VALIDATE, gold: str = "42\n") -> Path:
    folder = tmp_path / "query_toy" / "query1"
    folder.mkdir(parents=True)
    (folder / "validate.py").write_text(source, encoding="utf-8")
    (folder / "ground_truth.csv").write_text(gold, encoding="utf-8")
    return folder


def test_gold_string_is_dabs():
    # common_scaffold/validate/validate.py: non-empty lines, stripped, joined.
    assert dab_grade.gold_string(" a \n\n b\n") == "a\nb"


def test_pass_and_fail(tmp_path):
    folder = query_dir(tmp_path)
    assert dab_grade.grade_answer(folder, "The answer is 42.") == Grade(
        "correct", fields={"overlong": False}
    )
    assert dab_grade.grade_answer(folder, "The answer is 41.") == Grade(
        "incorrect", fields={"overlong": False}
    )


def test_empty_answer_fails_without_running_the_validator(tmp_path):
    folder = query_dir(tmp_path, SLOW)
    assert dab_grade.grade_answer(folder, "", timeout=1).verdict == "incorrect"


def test_overlong_flag_marks_only_passes_far_longer_than_the_gold(tmp_path):
    folder = query_dir(tmp_path)  # gold "42": 2 characters
    at_limit = "42" + " " * 200
    over = "42" + " " * 201
    assert dab_grade.grade_answer(folder, at_limit).fields == {"overlong": False}
    assert dab_grade.grade_answer(folder, over) == Grade(
        "correct", fields={"overlong": True}
    )
    assert dab_grade.grade_answer(folder, "41" + " " * 500).fields == {
        "overlong": False
    }


def test_timeout(tmp_path):
    folder = query_dir(tmp_path, SLOW)
    assert dab_grade.grade_answer(folder, "x", timeout=1) == Grade(
        "incorrect", failure="validator_timeout", fields={"overlong": False}
    )


def test_validator_error(tmp_path):
    folder = query_dir(tmp_path, RAISES)
    grade = dab_grade.grade_answer(folder, "x")
    assert (grade.verdict, grade.failure) == ("incorrect", "validator_error")


def test_answer_reaches_the_validator_byte_for_byte(tmp_path):
    folder = query_dir(tmp_path, ECHO)
    assert dab_grade.grade_answer(folder, "a\r\nb\u00e9").verdict == "correct"


def test_dabs_levenshtein_helper_is_importable(tmp_path):
    helper = tmp_path / "src" / "common_scaffold" / "validate"
    helper.mkdir(parents=True)
    (helper / "levenshtein.py").write_text(
        "def levenshtein(a, b):\n    return 0 if a == b else 1\n", encoding="utf-8"
    )
    folder = query_dir(tmp_path, HELPER)
    grade = dab_grade.grade_answer(folder, "abd", helper_root=tmp_path / "src")
    assert grade.verdict == "correct"


@pytest.mark.parametrize(
    "source",
    [
        "import os\n",
        "import socket\n",
        "from urllib import request\n",
        "from common_scaffold.tools import x\n",
        "import importlib\n",
        "x = __import__('os')\n",
        "open('f')\n",
    ],
)
def test_validators_may_import_only_the_allowed_modules(tmp_path, source):
    folder = query_dir(tmp_path, source + VALIDATE)
    with pytest.raises(dab_grade.ValidatorRefused):
        dab_grade.check_validator(folder / "validate.py")
    with pytest.raises(dab_grade.ValidatorRefused):
        dab_grade.grade_answer(folder, "42")


def test_allowed_validators_pass_the_check(tmp_path):
    dab_grade.check_validator(query_dir(tmp_path, HELPER) / "validate.py")
