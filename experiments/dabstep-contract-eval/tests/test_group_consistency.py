"""The two pure functions behind `analysis/group_consistency.py`.

The script itself reads results and prints a table; these are the parts that
decide what gets flagged, and a silent change in either would move every
number it reports. Imported by path because `analysis/` is a directory of
scripts, not a package.
"""

import importlib.util
from pathlib import Path

_SPEC = importlib.util.spec_from_file_location(
    "group_consistency",
    Path(__file__).parent.parent / "analysis" / "group_consistency.py",
)
assert _SPEC and _SPEC.loader
group_consistency = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(group_consistency)

signature = group_consistency.signature
flag_minorities = group_consistency.flag_minorities


def test_single_letter_and_word_labels_get_different_signatures():
    # The distinction the ACI family turns on: an ACI code letter against the
    # card scheme name the guideline actually asks for.
    assert signature("D:219.36") == "A:N"
    assert signature("TransactPlus:3458.48") == "W:N"
    assert signature("D:219.36") != signature("TransactPlus:3458.48")


def test_markdown_bold_is_stripped_before_shaping():
    assert signature("**D:78.19**") == signature("D:219.36")


def test_precision_does_not_change_the_signature():
    # Full float precision and a rounded value are the SAME shape — this check
    # is about answering a different kind of thing, not about formatting.
    assert signature("42.89798400000003") == signature("42.9") == "N"
    assert signature("-2.46886200000000") == "N"


def test_lists_words_and_empties():
    assert signature("156, 159, 298") == "N, N, N"
    assert signature("Not Applicable") == "W W"
    assert signature("yes") == "W"
    assert signature("") == ""
    assert signature(None) == ""


def test_no_flags_when_a_group_agrees():
    assert flag_minorities(["N", "N", "N"]) == []


def test_minority_signature_is_flagged():
    assert flag_minorities(["W:N", "W:N", "W:N", "A:N"]) == [3]


def test_two_minorities_are_both_flagged():
    assert flag_minorities(["W:N", "W:N", "W:N", "A:N", "N"]) == [3, 4]


def test_a_tie_has_no_minority_and_flags_nothing():
    assert flag_minorities(["N", "N", "W", "W"]) == []


def test_groups_below_the_minimum_size_flag_nothing():
    # Two rows can disagree without either being the odd one out.
    assert flag_minorities(["N", "W"]) == []
