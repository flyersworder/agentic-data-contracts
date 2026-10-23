import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "analysis"))

import lsb_kb_clarity as kc  # noqa: E402


def _r(clarity, decisive=True):
    return {"kb_clarity": clarity, "decisive": decisive}


def test_settled_is_clear_or_not_decisive():
    assert kc.settled(_r("clear"))
    assert kc.settled(_r("ambiguous", decisive=False))
    assert not kc.settled(_r("misleading"))


def test_a_task_is_determined_only_when_both_raters_say_so():
    assert kc.consensus(_r("clear"), _r("ambiguous", False)) == "determined"
    assert kc.consensus(_r("absent"), _r("ambiguous")) == "undetermined"
    assert kc.consensus(_r("clear"), _r("ambiguous")) == "split"


def test_kappa():
    assert kc.kappa([True, False, True, False], [True, False, True, False]) == 1.0
    assert kc.kappa([True, True, False, False], [True, False, True, False]) == 0.0
    assert kc.kappa([True, True], [True, True]) == pytest.approx(1.0)
