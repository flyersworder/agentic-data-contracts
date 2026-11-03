"""Experiment 7's blind hint raters (spec, "Hint items"): two raters of
different families label each hint item described / definition /
data-discoverable / world / guidance from the description, the schemas and
read-only data -- never the questions, golds, validators or the hints file.
A split goes to the later class."""

import json
import sys
from pathlib import Path

import duckdb
import pytest
from dce.benchmarks import dab_fetch
from dce.compile.session import DriverResult

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "prep" / "dataagentbench"))

import rate_hints  # noqa: E402


def test_two_raters_of_different_families():
    assert len(rate_hints.RATERS) == 2
    assert len({m[:4] for m in rate_hints.RATERS}) == 2


def test_the_items_cover_every_phase_one_dataset_once():
    items = rate_hints.load_items()
    assert set(items) == set(dab_fetch.DATASETS)
    ids = [i["id"] for v in items.values() for i in v]
    assert len(ids) == len(set(ids)) == 37
    assert all(i["id"].startswith(ds + "-") for ds, v in items.items() for i in v)


@pytest.mark.parametrize(
    ("a", "b", "want"),
    [
        ("world", "world", "world"),
        ("described", "data-discoverable", "data-discoverable"),
        ("data-discoverable", "described", "data-discoverable"),
        ("data-discoverable", "world", "world"),
        ("definition", "guidance", "guidance"),
    ],
)
def test_a_split_goes_to_the_later_class(a, b, want):
    assert rate_hints.resolve(a, b) == want


def test_a_submission_needs_a_known_class_and_evidence_for_every_item():
    ids = ["x-1", "x-2"]
    good = {
        "x-1": {"class": "world", "evidence": "e"},
        "x-2": {"class": "guidance", "evidence": "e"},
    }
    assert rate_hints.check_ratings(good, ids) == []
    assert rate_hints.check_ratings([], ids)
    problems = rate_hints.check_ratings(
        {
            "x-1": {"class": "obvious", "evidence": "e"},
            "x-3": {"class": "world", "evidence": "e"},
        },
        ids,
    )
    assert any("x-1" in p and "class" in p for p in problems)
    assert any("x-2" in p for p in problems)
    assert any("x-3" in p for p in problems)
    assert rate_hints.check_ratings({**good, "x-1": {"class": "world"}}, ids), (
        "evidence is required"
    )


@pytest.fixture
def data(tmp_path: Path) -> Path:
    src = tmp_path / "src" / dab_fetch.dataset_dir("bookreview")
    (src / "query1").mkdir(parents=True)
    (src / "db_description.txt").write_text(
        "books_database holds books_info.\n", encoding="utf-8"
    )
    (src / "db_description_withhint.txt").write_text(
        "books_database holds books_info.\nHINTS: SECRET-HINT\n", encoding="utf-8"
    )
    (src / "query1" / "query.json").write_text('"SECRET-QUESTION"', encoding="utf-8")
    (src / "query1" / "ground_truth.csv").write_text("SECRET-GOLD", encoding="utf-8")
    (tmp_path / "duckdb").mkdir()
    con = duckdb.connect(str(tmp_path / "duckdb" / "bookreview.duckdb"))
    con.execute("CREATE SCHEMA books_database")
    con.execute("CREATE TABLE books_database.books_info (book_id VARCHAR)")
    con.execute("INSERT INTO books_database.books_info VALUES ('b1')")
    con.close()
    return tmp_path


ITEMS = [
    {"id": "bookreview-1", "text": "book_id is a string."},
    {"id": "bookreview-2", "text": "Some queries need categories."},
]


def test_a_session_sees_items_description_and_schema_and_nothing_else(data):
    seen: dict = {}

    def driver(*, system_prompt, user_prompt, tools):
        seen["prompts"] = system_prompt + user_prompt
        run_sql, submit = tools
        seen["sql"] = run_sql("SELECT count(*) AS n FROM books_database.books_info")
        assert submit({"bookreview-1": {"class": "world"}}).startswith("ERROR")
        assert (
            submit(
                {
                    "bookreview-1": {"class": "data-discoverable", "evidence": "q"},
                    "bookreview-2": {"class": "guidance", "evidence": "g"},
                }
            )
            == "Accepted."
        )
        return DriverResult(messages=[], usage={"usd": 0.01})

    ratings, log = rate_hints.rate_one("bookreview", ITEMS, "m", driver, data)
    prompts = seen["prompts"]
    assert "books_database holds books_info." in prompts
    assert "[bookreview-1] book_id is a string." in prompts
    assert "CREATE TABLE books_database.books_info (" in prompts
    assert "schemas of one DuckDB database" in prompts
    for secret in ("SECRET-HINT", "SECRET-QUESTION", "SECRET-GOLD", "HINTS"):
        assert secret not in prompts
    assert seen["sql"].split() == ["n", "1"]
    assert ratings is not None and ratings["bookreview-2"]["class"] == "guidance"
    assert log["submissions"] == 2 and len(log["queries"]) == 1


def test_a_write_is_refused(data):
    def driver(*, system_prompt, user_prompt, tools):
        out = tools[0]("CREATE TABLE books_database.x (a INT)")
        assert out.startswith("ERROR")
        return DriverResult(messages=[], usage={})

    ratings, _ = rate_hints.rate_one("bookreview", ITEMS, "m", driver, data)
    assert ratings is None


def test_the_report_applies_the_rule_per_item():
    items = {"bookreview": ITEMS}
    ratings = {
        "bookreview|a": {
            "bookreview-1": {"class": "described", "evidence": "x"},
            "bookreview-2": {"class": "guidance", "evidence": "y"},
        },
        "bookreview|b": {
            "bookreview-1": {"class": "data-discoverable", "evidence": "z"},
            "bookreview-2": {"class": "guidance", "evidence": "w"},
        },
    }
    resolved = rate_hints.resolved(items, ratings, raters=("a", "b"))
    assert resolved == {"bookreview-1": "data-discoverable", "bookreview-2": "guidance"}
    text = rate_hints.report(items, ratings, raters=("a", "b"))
    assert (
        "bookreview-1: data-discoverable (a: described; b: data-discoverable)" in text
    )
    assert json.dumps(resolved)  # serialisable for the commit
