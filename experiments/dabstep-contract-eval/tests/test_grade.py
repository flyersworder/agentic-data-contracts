import json
import re
from pathlib import Path

import pytest
from dce.grade import active_scorer, score

_GOLDS_PATH = Path(__file__).parent.parent / "data" / "golds.json"


def test_exact_and_case_insensitive_match():
    assert score("NL", "NL")
    assert score("nl", "NL")


def test_bracket_and_quote_normalization():
    assert score("['C']", "C")
    assert score('"C"', "C")


def test_float_tolerance_follows_upstream_not_our_own_taste():
    """Upstream compares numerics with `rel_tol=1e-4`/`abs_tol=1e-4` and rounds
    to the lesser decimal precision of the two values — markedly looser than
    the tight tolerance this file used to assert. We adopt it because the
    leaderboard does; grading more strictly than the benchmark is how the
    fallback manufactured a significant result the rules did not support (see
    `tests/test_vendored_scorer.py`).
    """
    assert score("0.120132", "0.1201320000001")
    # Was `not score(...)` under the old fallback. Upstream calls this equal.
    assert score("0.120132", "0.120133")
    # Still discriminating well short of "anything close enough".
    assert not score("0.12", "0.13")


def test_not_applicable_must_be_spelled_the_way_the_task_asks():
    """Adopting the official scorer is not a blanket loosening — here it is
    STRICTER than the fallback was.

    DABStep's own task guidelines say: "If a question does not have a relevant
    or applicable answer for the task, please respond with 'Not Applicable'".
    Upstream honours that literally: `N/A`, `none` and `null` are all wrong
    against a `Not Applicable` gold, where the old fallback accepted them as
    synonyms. 6 of 406 reconstructed golds (1.5%) are `Not Applicable` or
    empty, so this is a real slice of the sweep and it now costs every arm
    equally.
    """
    assert score("Not Applicable", "Not Applicable")
    assert score("not applicable", "Not Applicable")
    assert not score("N/A", "Not Applicable")
    assert not score("none", "Not Applicable")
    # An empty gold still wants an empty answer, and gets one.
    assert score("", "")


def test_comma_lists_compare_order_insensitively():
    assert score("fee_3, fee_1", "fee_1,fee_3")


def test_a_wrong_answer_is_wrong():
    assert not score("BE", "NL")
    assert not score("fee_1", "fee_1, fee_2")


def test_thousands_grouped_gold_is_not_split_into_a_list():
    assert not score("500,10", "10,500")
    assert not score("234,1", "1,234")


def test_thousands_grouped_number_matches_its_plain_form_either_direction():
    assert score("1234.56", "1,234.56")
    assert score("1,234.56", "1234.56")


def test_thousands_grouped_gold_matches_itself():
    assert score("1,234.56", "1,234.56")


def test_genuine_lists_still_compare_order_insensitively():
    assert score("fee_3, fee_1", "fee_1,fee_3")
    assert score(
        "Ecommerce: 97.68, POS: 88.49",
        "[POS: 88.49, Ecommerce: 97.68]",
    )


def test_the_no_space_triple_is_graded_as_a_number_not_a_list():
    """`"709,741,454"` is genuinely ambiguous: three reordered merchant ids,
    or the single number 709741454. No syntactic rule resolves it (see
    `golds.py::_norm`'s docstring, which hit and reverted the opposite guard
    for the same reason).

    Upstream resolves it BEFORE the list branch: `question_scorer` tries
    `is_numeric_with_commas` first, and that pattern matches comma-grouped
    digits, so this reads as one number and the reordering is wrong. That is
    now the benchmark's call rather than ours -- this test pins that we
    inherit it, and that a future local "fix" cannot reopen
    `score("500,10", "10,500") -> True`.
    """
    assert not score("741,454,709", "709,741,454")


# Approximates the comma-grouped-number branch of upstream's
# `is_numeric_with_commas`, to FIND the golds at risk of the number/list
# ambiguity. Deliberately a local copy rather than an import: this test must
# reach `score` only through its public API, and upstream's pattern is a
# vendored file we do not edit. It need not match upstream exactly -- it is a
# net for candidates, and the assertion below is what checks them.
_THOUSANDS_PATTERN = r"^-?\d{1,3}(,\d{3})+(\.\d+)?$"


@pytest.mark.skipif(
    not _GOLDS_PATH.exists(), reason="data/golds.json not present locally"
)
def test_every_numeric_comma_gold_uses_the_reconstruction_separator():
    """Upstream reads a comma-joined gold as one number, not a list, when it
    matches `is_numeric_with_commas`. That boundary is not structurally
    guaranteed by `golds.py` --
    the stored gold is whichever raw submission string won the plurality
    vote, and task 59 proves a vote can drop the space (its gold is the
    no-space "IT,ES,FR"; harmless there only because letters never match the
    thousands pattern). This test checks the boundary that actually matters:
    every gold that *would* match the thousands pattern once you strip its
    spaces -- i.e. every gold at real risk of the number/list ambiguity --
    must still carry its spaces, so it does not accidentally cross into
    "looks like a number" territory. It fails loudly the day a numeric list
    gold's modal rendering drops its spaces too.
    """
    data = json.loads(_GOLDS_PATH.read_text())
    golds = data["golds"]
    thousands = re.compile(_THOUSANDS_PATTERN)
    violations = {
        task_id: gold
        for task_id, gold in golds.items()
        if "," in gold
        and thousands.match(gold.replace(" ", ""))
        and re.search(r",(?!\s)", gold)
    }
    assert not violations, violations


def test_active_scorer_names_the_scorer_actually_in_use():
    """There is no fallback left to name.

    `vendor/dabstep_scorer.py` is a pinned copy of DABStep's own scorer and
    grades every row; an exception from it becomes a visible `scoring_error`
    row rather than a silent regrade under different rules. The field is still
    recorded per row because the choice is made at import time — an
    environment with a real `dabstep_benchmark` installed reports `"official"`
    instead, and one results file must never silently mix provenances."""
    assert active_scorer() in {"official", "official-vendored"}
    assert active_scorer() == "official-vendored"


# ── end-to-end view: the answer in the final paragraph ─────────────────────


def test_final_paragraph_is_the_last_non_empty_block():
    from dce.grade import final_paragraph

    assert final_paragraph("Working: 24 x 17874.16 / 10000.\n\n42.897984") == (
        "42.897984"
    )
    # Trailing blank lines and whitespace-only blocks are not a paragraph.
    assert final_paragraph("Some working.\n\n  42  \n\n \n") == "42"
    # One block is its own final paragraph; nothing is empty-string'd away.
    assert final_paragraph("36, 51, 65") == "36, 51, 65"
    assert final_paragraph("") == ""
    assert final_paragraph(None) == ""


def test_score_final_paragraph_credits_an_answer_given_after_its_working():
    """The measured case. Qwen 3.8 explains, then states the exact value as
    its last paragraph, against a prompt that asked for the answer alone.
    DABStep's scorer grades the whole message and marks it wrong; the
    end-to-end view asks whether the question was answered.
    """
    from dce.grade import score_final_paragraph

    answer = (
        "The fee rule ID=398 already has a rate of 99, so changing the "
        "relative fee to 99 produces no change.\n\n0.00000000000000"
    )
    assert not score(answer, "0.00000000000000")
    assert score_final_paragraph(answer, "0.00000000000000")


def test_score_final_paragraph_grades_only_the_final_paragraph():
    """Extraction, not a search: a value that appears in the working but is
    not what the model finally answered earns nothing. Otherwise any answer
    that lists candidates on the way would be credited with the right one.
    """
    from dce.grade import score_final_paragraph

    answer = "Candidates were Rafa_AI and Crossfit_Hanna.\n\nCrossfit_Hanna"
    assert not score_final_paragraph(answer, "Rafa_AI")


def test_score_final_paragraph_adds_nothing_to_a_single_block_answer():
    """With one paragraph there is nothing to extract, so the official
    verdict on the whole message already is the end-to-end verdict. Returning
    False here, rather than re-scoring the same text, keeps a row with no
    stored answer from being graded against no gold -- `score("", "")` is
    True under DABStep's rules.
    """
    from dce.grade import score_final_paragraph

    assert not score_final_paragraph("", "")
    assert not score_final_paragraph("42", "42")
