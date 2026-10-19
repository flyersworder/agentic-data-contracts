"""Experiment 4b's blind raters: structured readings of each gap location,
compared with the reference reading mechanically."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "prep" / "procedures4b"))

import rate  # noqa: E402

GAPS = {
    "g1": {"sentinel": "ANY"},
    "g2": {
        "recover": "v + 1",
        "table": "q",
        "column": "s",
        "json_path": "$.results.aesthetic_score",
        "entry_id": 36,
    },
    "g3_entries": {"33": {}},
}


def test_two_raters_of_different_families():
    assert len(rate.RATERS) == 2
    assert len({m.split("5")[0][:4] for m in rate.RATERS}) == 2


def test_the_items_follow_the_gaps():
    items = rate.items("reverse_logistics", GAPS)
    assert [i["gap"] for i in items] == ["g1", "g2", "g3"]
    assert all("ANY" not in i["ask"] for i in items)
    assert "v + 1" not in items[1]["ask"]
    assert [
        i["gap"] for i in rate.items("x", {"g1": {"sentinel": "*"}, "g2": None})
    ] == ["g1"]


def test_g1_reading_must_name_the_sentinel_and_match_missing_values():
    assert rate.matches_reference(
        "g1", GAPS, {"sentinel": "ANY", "matches_missing": True, "settled": True}
    )
    assert not rate.matches_reference(
        "g1", GAPS, {"sentinel": "ANY", "matches_missing": False, "settled": True}
    )
    assert not rate.matches_reference(
        "g1", GAPS, {"sentinel": "*", "matches_missing": True, "settled": True}
    )
    assert not rate.matches_reference(
        "g1", GAPS, {"sentinel": "ANY", "matches_missing": True, "settled": False}
    )


def test_g2_reading_compares_the_transformation_numerically():
    assert rate.matches_reference("g2", GAPS, {"recover": "1 + v", "settled": True})
    assert not rate.matches_reference("g2", GAPS, {"recover": "v", "settled": True})
    assert not rate.matches_reference(
        "g2", GAPS, {"recover": "len(v)", "settled": True}
    )
    assert not rate.matches_reference("g2", GAPS, {"recover": None, "settled": False})


def test_classification():
    yes = {"sentinel": "ANY", "matches_missing": True, "settled": True}
    no = {"sentinel": None, "matches_missing": None, "settled": False}
    assert rate.classify("g1", GAPS, [yes, yes]) == "data-settled"
    assert rate.classify("g1", GAPS, [yes, no]) == "not settled"
    assert (
        rate.classify(
            "g3",
            GAPS,
            [
                {"convention": "sum_all", "settled": True},
                {"convention": "most_specific", "settled": True},
            ],
        )
        == "convention-only"
    )
    assert (
        rate.classify(
            "g3",
            GAPS,
            [
                {"convention": "sum_all", "settled": False},
                {"convention": "sum_all", "settled": False},
            ],
        )
        == "convention-only"
    )
    assert (
        rate.classify(
            "g3",
            GAPS,
            [
                {"convention": "most_specific", "settled": True},
                {"convention": "most_specific", "settled": True},
            ],
        )
        == "settled (not convention-only)"
    )
