"""`analysis/dab_recovery.py`: experiment 7's recovery, assembled from the
checkers' files, and the offline predictions (43), (43c), (43w)."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "analysis"))

import dab_recovery as dr  # noqa: E402

DRAWS = ("draw-a1", "draw-a2", "draw-a3", "draw-b", "no-data")


def _item(i, cls, category):
    return {"id": i, "cls": cls, "category": category}


def _scores(by_draw: dict[str, list[str]], classes: list[str]) -> dict:
    return {
        "dataset": "d",
        "draws": {
            draw: {
                "items": [
                    _item(f"d-{n}", c, cat)
                    for n, (c, cat) in enumerate(zip(classes, cats), 1)
                ],
                "additions": [],
                "correctable_errors": [],
            }
            for draw, cats in by_draw.items()
        },
    }


def test_structural_errors_are_those_in_sql_or_declared_structure():
    assert dr.structural({"where": "semantic.yml metrics.x.sql_expression"})
    assert dr.structural({"where": "semantic.yml metrics.x.from_sql"})
    assert dr.structural({"where": "contract.yml relationships[0]"})
    assert dr.structural({"where": "contract.yml rules.cast_date_text"})
    assert not dr.structural({"where": "contract.yml domains.packages.description"})
    assert not dr.structural(
        {"where": "contract.yml semantic.domains.support.description"}
    )
    # A relationship's description is prose, though it names a relationship.
    assert not dr.structural(
        {
            "where": "contract.yml domains.repositories.description; "
            "semantic.yml relationships (repos -> languages) description 'subset'"
        }
    )
    # One structural part among prose parts makes the error structural.
    assert dr.structural(
        {
            "where": "semantic.yml metrics.upstream_published_at; "
            "contract.yml rules.timestamp_milliseconds; "
            "contract.yml domains.packages.description"
        }
    )


def test_a_prose_error_becomes_an_unconfirmed_addition():
    s = _scores({d: ["recovered"] for d in DRAWS}, ["data-discoverable"])
    s["draws"]["draw-b"]["correctable_errors"] = [
        {
            "where": "contract.yml domains.p.description",
            "cause": "wrong range",
            "smallest_fix": "say 2020-04-02",
        },
        {
            "where": "semantic.yml metrics.m.sql_expression",
            "cause": "bad regex",
            "smallest_fix": "widen it",
        },
    ]
    out = dr.apply_error_rule(s)
    b = out["draws"]["draw-b"]
    assert [e["cause"] for e in b["correctable_errors"]] == ["bad regex"]
    assert b["additions"][-1]["confirmed"] == "no"
    assert b["additions"][-1]["quote"] == "wrong range"
    assert "prose" in b["additions"][-1]["where"]


def test_counts_by_variant_and_class():
    s = _scores(
        {
            "draw-a1": ["recovered", "partial"],
            "draw-a2": ["recovered", "missed"],
            "draw-a3": ["partial", "missed"],
            "draw-b": ["recovered", "recovered"],
            "no-data": ["missed", "missed"],
        },
        ["data-discoverable", "definition"],
    )
    c = dr.counts([s])
    assert c["draw-a"]["data-discoverable"] == {"recovered": 2, "partial": 1}
    assert c["draw-a1"]["definition"] == {"partial": 1}
    assert c["no-data"]["data-discoverable"] == {"missed": 1}


def test_predictions_read_recovered_only_and_put_described_apart():
    # Two data-discoverable items, one definition, one described.
    classes = ["data-discoverable", "data-discoverable", "definition", "described"]
    s = _scores(
        {
            "draw-a1": ["recovered", "stated, not built", "missed", "recovered"],
            "draw-a2": ["recovered", "recovered", "missed", "recovered"],
            "draw-a3": ["recovered", "partial", "recovered", "recovered"],
            "draw-b": ["recovered", "recovered", "recovered", "recovered"],
            "no-data": ["missed", "missed", "missed", "recovered"],
        },
        classes,
    )
    p = dr.predictions([s])
    # (43): 4 of 6 data-discoverable item-draws recovered.
    assert p["43"]["recovered"] == 4 and p["43"]["of"] == 6
    assert p["43"]["holds"] is True
    # (43c): no-data 0 of 2, against draw A's mean of 4/3 per draw.
    assert p["43c"]["no_data"] == 0 and p["43c"]["holds"] is True
    # (43w): draw A recovers 1 of 3 definition item-draws, 4 of 6 data ones.
    assert p["43w"]["other"] == (1, 3) and p["43w"]["holds"] is True
    # Described items enter no prediction.
    assert p["described"]["draw-a"] == (3, 3)


def test_43_fails_below_half_and_43c_fails_above_a_third():
    s = _scores(
        {
            "draw-a1": ["recovered", "missed", "missed"],
            "draw-a2": ["missed", "missed", "missed"],
            "draw-a3": ["missed", "missed", "missed"],
            "draw-b": ["recovered", "recovered", "recovered"],
            "no-data": ["recovered", "recovered", "missed"],
        },
        ["data-discoverable"] * 3,
    )
    p = dr.predictions([s])
    assert p["43"]["holds"] is False
    assert p["43c"]["holds"] is False


def test_exploratory_share_counts_stated_beside_recovered():
    s = _scores(
        {d: ["stated, not built", "recovered"] for d in DRAWS},
        ["data-discoverable", "data-discoverable"],
    )
    p = dr.predictions([s])
    assert p["43"]["recovered_or_stated"] == 6 and p["43"]["of"] == 6
