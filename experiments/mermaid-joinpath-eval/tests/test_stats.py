from mje.stats import aggregate, mcnemar_pairs


def _row(
    item,
    rendering,
    *,
    n_joins=2,
    ambiguous=False,
    f1=1.0,
    exact=True,
    truncated=False,
    pred_edges=(("t.a", "t.b"),),
):
    return {
        "item_id": item,
        "model": "M",
        "rendering": rendering,
        "n_joins": n_joins,
        "ambiguous": ambiguous,
        "f1": f1,
        "exact": exact,
        "truncated": truncated,
        "pred_edges": [list(e) for e in pred_edges],
    }


def _rows():
    return [
        _row("a", "xml", n_joins=2, f1=1.0, exact=True),
        _row("a", "mermaid", n_joins=2, f1=1.0, exact=True),
        _row("b", "xml", n_joins=3, f1=0.0, exact=False),
        _row("b", "mermaid", n_joins=3, f1=1.0, exact=True),
    ]


def test_aggregate_means_by_group():
    agg = aggregate(_rows())
    xml = [
        r
        for r in agg
        if r["model"] == "M" and r["rendering"] == "xml" and r["stratum"] == "all"
    ][0]
    mer = [
        r
        for r in agg
        if r["model"] == "M" and r["rendering"] == "mermaid" and r["stratum"] == "all"
    ][0]
    assert xml["mean_f1"] == 0.5
    assert mer["mean_f1"] == 1.0


def test_mcnemar_returns_pvalue():
    res = mcnemar_pairs(_rows(), model="M")
    pair = [r for r in res if {r["a"], r["b"]} == {"xml", "mermaid"}][0]
    assert "p_value" in pair and 0.0 <= pair["p_value"] <= 1.0


def test_mcnemar_keys_are_self_describing():
    # item b: xml wrong, mermaid right -> the discordant count must be attributed
    # to whichever of a/b is mermaid, not to a fixed positional slot.
    res = mcnemar_pairs(_rows(), model="M")
    pair = [r for r in res if {r["a"], r["b"]} == {"xml", "mermaid"}][0]
    wins = {pair["a"]: pair["a_right_b_wrong"], pair["b"]: pair["b_right_a_wrong"]}
    assert wins["mermaid"] == 1
    assert wins["xml"] == 0


def test_aggregate_reports_parse_failure_and_truncated_rates():
    rows = [
        _row("a", "xml", f1=0.0, exact=False, pred_edges=()),  # parse failure
        _row("b", "xml", f1=1.0, exact=True, truncated=True),  # truncated
    ]
    g = [r for r in aggregate(rows) if r["stratum"] == "all"][0]
    assert g["parse_fail_rate"] == 0.5
    assert g["truncated_rate"] == 0.5


def test_aggregate_stratifies_by_ambiguity():
    rows = [
        _row("a", "xml", ambiguous=True, f1=0.4, exact=False),
        _row("b", "xml", ambiguous=False, f1=0.8, exact=True),
    ]
    by = {r["stratum"]: r for r in aggregate(rows) if r["rendering"] == "xml"}
    assert by["ambig"]["mean_f1"] == 0.4 and by["ambig"]["n"] == 1
    assert by["unambig"]["mean_f1"] == 0.8 and by["unambig"]["n"] == 1
    assert by["all"]["n"] == 2


def test_aggregate_exclude_truncated_changes_mean():
    rows = [
        _row("a", "xml", f1=1.0, exact=True, truncated=False),
        _row("b", "xml", f1=0.0, exact=False, truncated=True),
    ]
    incl = [r for r in aggregate(rows) if r["stratum"] == "all"][0]
    excl = [
        r for r in aggregate(rows, exclude_truncated=True) if r["stratum"] == "all"
    ][0]
    assert incl["mean_f1"] == 0.5 and incl["n"] == 2
    assert excl["mean_f1"] == 1.0 and excl["n"] == 1


def test_mcnemar_unambiguous_only_drops_ambiguous_items():
    rows = [
        # ambiguous discordant pair (xml right, mermaid wrong)
        _row("a", "xml", ambiguous=True, exact=True),
        _row("a", "mermaid", ambiguous=True, exact=False),
        # unambiguous concordant pair (both right)
        _row("b", "xml", ambiguous=False, exact=True),
        _row("b", "mermaid", ambiguous=False, exact=True),
    ]
    full = [
        r for r in mcnemar_pairs(rows, "M") if {r["a"], r["b"]} == {"xml", "mermaid"}
    ][0]
    unamb = [
        r
        for r in mcnemar_pairs(rows, "M", ambiguity="unambig")
        if {r["a"], r["b"]} == {"xml", "mermaid"}
    ][0]
    assert full["a_right_b_wrong"] + full["b_right_a_wrong"] == 1  # the ambig pair
    assert unamb["a_right_b_wrong"] + unamb["b_right_a_wrong"] == 0  # dropped


def test_mcnemar_exclude_truncated_drops_pair():
    rows = [
        _row("a", "xml", exact=True, truncated=False),
        _row("a", "mermaid", exact=False, truncated=True),  # truncated -> drop pair
    ]
    full = [
        r for r in mcnemar_pairs(rows, "M") if {r["a"], r["b"]} == {"xml", "mermaid"}
    ][0]
    clean = [
        r
        for r in mcnemar_pairs(rows, "M", exclude_truncated=True)
        if {r["a"], r["b"]} == {"xml", "mermaid"}
    ][0]
    assert full["a_right_b_wrong"] + full["b_right_a_wrong"] == 1
    assert clean["a_right_b_wrong"] + clean["b_right_a_wrong"] == 0
