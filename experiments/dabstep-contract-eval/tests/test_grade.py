from dce.grade import score


def test_exact_and_case_insensitive_match():
    assert score("NL", "NL")
    assert score("nl", "NL")


def test_bracket_and_quote_normalization():
    assert score("['C']", "C")
    assert score('"C"', "C")


def test_float_tolerance_is_relative_and_tight():
    assert score("0.120132", "0.1201320000001")
    assert not score("0.120132", "0.120133")


def test_na_equivalence():
    assert score("", "N/A")
    assert score("none", "N/A")
    assert score("null", "N/A")


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
