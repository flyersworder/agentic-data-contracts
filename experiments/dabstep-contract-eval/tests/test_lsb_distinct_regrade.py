import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "analysis"))

import lsb_distinct_regrade as rd  # noqa: E402

SQL = "SELECT DISTINCT a IS DISTINCT FROM b, COUNT(DISTINCT c) FROM t"


def test_the_official_stripping_breaks_both_constructs():
    (out,) = rd.OFFICIAL([SQL])
    assert "IS FROM" in out and "COUNT( c)" in out


def test_variant_a_keeps_is_distinct_from_only():
    (out,) = rd.protected(rd.VARIANTS["A"])([SQL])
    assert out.startswith("SELECT a IS DISTINCT FROM b")
    assert "COUNT( c)" in out


def test_variant_b_also_keeps_aggregate_distinct():
    (out,) = rd.protected(rd.VARIANTS["B"])([SQL])
    assert out == "SELECT a IS DISTINCT FROM b, COUNT(DISTINCT c) FROM t"


def test_a_query_without_either_construct_is_untouched():
    assert not rd.affected("SELECT DISTINCT a FROM t")
    assert rd.affected("SELECT x IS NOT DISTINCT FROM y")
