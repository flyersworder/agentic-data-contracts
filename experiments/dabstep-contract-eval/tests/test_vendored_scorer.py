"""The vendored official scorer: provenance, adoption, and the cases that
made adopting it load-bearing.

Everything here guards one claim — that this experiment grades by DABStep's
published rules and not by a hand-rolled approximation of them.
"""

import hashlib
import re
from pathlib import Path

import pytest
from dce.grade import active_scorer, score

VENDOR = Path(__file__).resolve().parents[1] / "vendor" / "dabstep_scorer.py"

#: sha256 of the UPSTREAM file, i.e. `vendor/dabstep_scorer.py` with the
#: locally-added header docstring removed. Recorded in `vendor/README.md`
#: alongside the Space revision it came from.
UPSTREAM_SHA256 = "229c04ecaa249ccc5b555dee11c85fe93d073d41eee61b6c3584065ae33c9df3"
UPSTREAM_REVISION = "d4431c2e4a695cbe43c33aab2adaa304a37ae64a"


def _upstream_body() -> bytes:
    """The vendored file with only our added header docstring stripped."""
    text = VENDOR.read_text()
    match = re.match(r'^""".*?"""\n\n', text, re.DOTALL)
    assert match, "vendored file lost its provenance header"
    return text[match.end() :].encode()


def test_vendored_scorer_is_upstream_byte_for_byte():
    """The whole value of vendoring is that it is a COPY, not a
    reimplementation. An edit here — however well-meant — silently forks this
    experiment's grading from the leaderboard's, which is exactly the failure
    that adopting it was meant to end.
    """
    assert hashlib.sha256(_upstream_body()).hexdigest() == UPSTREAM_SHA256


def test_provenance_header_names_the_pinned_revision():
    """A hash with no revision beside it cannot be re-fetched or re-checked."""
    header = VENDOR.read_text()[:1200]
    assert UPSTREAM_REVISION in header
    assert "huggingface.co/spaces/adyen/DABstep" in header


def test_the_official_scorer_is_the_one_actually_in_force():
    """Not merely present — used. `score()` reaching the fallback instead
    would restore the stricter grading with nothing in the results to show it.
    """
    assert active_scorer() in {"official", "official-vendored"}


# ── the two rows that changed a headline ────────────────────────────────────


@pytest.mark.parametrize("answer", ["Yes.", "yes.", "YES", " yes ", "Yes"])
def test_trailing_punctuation_and_case_do_not_fail_a_yes(answer):
    """The exact disagreement found on the 12-task smoke run.

    Both ungoverned arms answered `Yes.` where the gold was `yes`. The old
    fallback normalised case but not trailing punctuation and marked them
    wrong; upstream's `compare_strings` strips all non-word characters and
    marks them right. Those two rows moved `schema_only vs contract` from
    p=0.0156 (significant) to p=0.0703 (not) — a false positive in this
    library's own favour.
    """
    assert score(answer, "yes") is True


def test_a_verbose_prose_answer_is_still_wrong():
    """The complement, and the reason this is a scorer fix rather than a
    blanket loosening: arm C's answer on the same task was a whole sentence,
    and stays incorrect under upstream's rules too. Adopting the official
    scorer corrected two rows AGAINST this library and none for it.
    """
    verbose = "Yes. In 2023, the average was higher for that segment."
    assert score(verbose, "yes") is False


def test_official_numeric_tolerance_is_more_lenient_than_exact_match():
    """Upstream compares numerics with `rel_tol=1e-4` and rounds to the lesser
    decimal precision of the two values. This will matter far more across 450
    tasks than the yes/no case did across 12, and it is why no FINDINGS number
    may be computed with the fallback."""
    assert score("52.3700", "52.37") is True
    assert score("1,234.5", "1234.5") is True
    # Still discriminating, not simply permissive.
    assert score("52.14", "52.37") is False


def test_list_comparison_stays_order_insensitive():
    """Validated live on smoke-run task 2564, where the contract arm returned
    the five correct merchants in a different order from the gold."""
    assert score("b, a, c", "a, b, c") is True
    assert score("a, b", "a, b, c") is False
