"""The clause detectors behind `analysis/clauses.py`.

Every row of the paper's clause table is a detector firing or not, so each one
is pinned here against SQL the agents actually wrote: the spellings it must
accept and the near misses it must reject. Imported by path because
`analysis/` is a directory of scripts, not a package.
"""

import importlib.util
import sys
from pathlib import Path

_DIR = Path(__file__).parent.parent / "analysis"
sys.path.insert(0, str(_DIR))
_SPEC = importlib.util.spec_from_file_location("clauses", _DIR / "clauses.py")
assert _SPEC and _SPEC.loader
clauses = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(clauses)

clauses_in = clauses.clauses_in
names_a_month = clauses.names_a_month


def test_month_boundaries_are_2023s():
    assert len(clauses.MONTH_STARTS) == len(clauses.MONTH_ENDS) == 12
    for start, end, next_start in zip(
        clauses.MONTH_STARTS, clauses.MONTH_ENDS, clauses.MONTH_STARTS[1:] + (366,)
    ):
        assert start <= end == next_start - 1


def test_a_hand_written_month_range_is_a_natural_month():
    # Written by the manual arm; the first detectors saw no month here.
    assert names_a_month("SELECT 1 FROM payments WHERE day_of_year BETWEEN 274 AND 304")
    assert names_a_month("WHERE p.day_of_year >= 60 AND p.day_of_year <= 90")
    assert names_a_month("WHERE day_of_year >= 60 AND day_of_year < 91")


def test_a_range_that_is_not_exactly_one_month_is_not_a_month():
    assert not names_a_month("WHERE day_of_year BETWEEN 1 AND 30")  # rolling window
    assert not names_a_month("WHERE day_of_year BETWEEN 1 AND 59")  # two months
    assert not names_a_month("WHERE day_of_year >= 60 AND day_of_year < 90")
    assert not names_a_month("WHERE day_of_year = 200")


def test_monthly_volume_over_a_month_range_is_a_monthly_aggregate():
    sql = (
        "SELECT SUM(eur_amount) AS total_volume FROM payments "
        "WHERE merchant = 'Crossfit_Hanna' AND year = 2023 "
        "AND day_of_year BETWEEN 182 AND 212"
    )
    assert {"natural_month", "monthly_aggregate"} <= clauses_in([sql])


def test_a_month_range_without_volume_is_not_a_monthly_aggregate():
    sql = (
        "SELECT * FROM payments "
        "WHERE merchant = 'Rafa_AI' AND day_of_year BETWEEN 274 AND 304"
    )
    found = clauses_in([sql])
    assert "natural_month" in found
    assert "monthly_aggregate" not in found


def test_the_contracts_own_spelling_still_counts():
    sql = (
        "SELECT date_trunc('month', make_date(year, 1, 1) + (day_of_year - 1)) m, "
        "SUM(eur_amount) FROM payments GROUP BY 1"
    )
    assert {"natural_month", "monthly_aggregate"} <= clauses_in([sql])


def test_fraud_weighted_by_euro_volume_counts():
    for sql in [
        "SUM(p.eur_amount) FILTER (WHERE p.has_fraudulent_dispute) / SUM(p.eur_amount)",
        "SUM(CASE WHEN has_fraudulent_dispute THEN eur_amount ELSE 0 END)",
        "sum(if(has_fraudulent_dispute, eur_amount, 0))",
        "sum(eur_amount * CAST(has_fraudulent_dispute AS INT))",
        "sum(has_fraudulent_dispute::int * eur_amount)",
        "SELECT SUM(eur_amount) FROM payments "
        "WHERE merchant = 'X' AND has_fraudulent_dispute",
    ]:
        assert "fraud_volume" in clauses_in([sql]), sql


def test_fraud_counted_in_transactions_does_not_count():
    # The wrong rule; the first detector accepted any mention of the flag.
    for sql in [
        "SUM(CASE WHEN has_fraudulent_dispute THEN 1 ELSE 0 END) / COUNT(*)",
        "SELECT SUM(eur_amount) AS vol, "
        "COUNT(*) FILTER (WHERE has_fraudulent_dispute) AS n "
        "FROM payments WHERE merchant = 'X'",
        "SELECT psp_reference, eur_amount, has_fraudulent_dispute FROM payments",
    ]:
        assert "fraud_volume" not in clauses_in([sql]), sql


def test_a_clause_must_be_expressed_within_one_statement():
    # A month in one query and a volume sum in the next is not one aggregate.
    found = clauses_in(
        [
            "SELECT * FROM payments WHERE day_of_year BETWEEN 60 AND 90",
            "SELECT SUM(eur_amount) FROM payments",
        ]
    )
    assert "natural_month" in found
    assert "monthly_aggregate" not in found
