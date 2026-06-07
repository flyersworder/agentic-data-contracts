from mje.stats import aggregate, mcnemar_pairs


def _rows():
    return [
        {
            "item_id": "a",
            "model": "M",
            "rendering": "xml",
            "n_joins": 2,
            "ambiguous": False,
            "f1": 1.0,
            "exact": True,
        },
        {
            "item_id": "a",
            "model": "M",
            "rendering": "mermaid",
            "n_joins": 2,
            "ambiguous": False,
            "f1": 1.0,
            "exact": True,
        },
        {
            "item_id": "b",
            "model": "M",
            "rendering": "xml",
            "n_joins": 3,
            "ambiguous": False,
            "f1": 0.0,
            "exact": False,
        },
        {
            "item_id": "b",
            "model": "M",
            "rendering": "mermaid",
            "n_joins": 3,
            "ambiguous": False,
            "f1": 1.0,
            "exact": True,
        },
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
    assert pair["b_a_wrong_b_right"] == 1
