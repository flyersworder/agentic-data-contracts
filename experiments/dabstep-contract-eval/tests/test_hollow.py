"""The hollow contract is a control, so it has exactly two obligations:
hold the scaffolding identical, and remove the knowledge. A control that
fails either one silently produces a number that looks like a measurement.
"""

import re
from pathlib import Path

import yaml
from dce import hollow

from agentic_data_contracts import DataContract

ROOT = Path(__file__).parent.parent
REAL = ROOT / "contract"
HOLLOW = ROOT / "contract_hollow"


def _load(d: Path) -> tuple[dict, dict]:
    return (
        yaml.safe_load((d / "contract.yml").read_text()),
        yaml.safe_load((d / "semantic.yml").read_text()),
    )


def test_generation_is_idempotent(tmp_path: Path):
    """The artifact is regenerated on any checkout, so a second run must not
    drift — otherwise the digest stamped on a result row is not reproducible."""
    first = hollow.build(REAL, tmp_path / "a")
    second = hollow.build(REAL, tmp_path / "b")
    for name in ("contract.yml", "semantic.yml"):
        assert (first / name).read_text() == (second / name).read_text()


def test_committed_artifact_matches_the_generator(tmp_path: Path):
    """`contract_hollow/` is committed so the sweep is reproducible, which
    means it can go stale against the frozen contract it derives from."""
    fresh = hollow.build(REAL, tmp_path / "fresh")
    for name in ("contract.yml", "semantic.yml"):
        assert (fresh / name).read_text() == (HOLLOW / name).read_text(), (
            f"{name} is stale — re-run `python -m dce.hollow`"
        )


# ── obligation 1: the scaffolding is identical ───────────────────────────


def test_same_domains_tables_and_rules():
    real_c, real_s = _load(REAL)
    hol_c, hol_s = _load(HOLLOW)

    def names(c):
        return [d["name"] for d in c["semantic"]["domains"]]

    assert names(hol_c) == names(real_c)
    assert (
        hol_c["semantic"]["forbidden_operations"]
        == (real_c["semantic"]["forbidden_operations"])
    )
    real_tables = [
        (t["schema"], t.get("tables")) for t in real_c["semantic"]["allowed_tables"]
    ]
    hol_tables = [
        (t["schema"], t.get("tables")) for t in hol_c["semantic"]["allowed_tables"]
    ]
    assert hol_tables == real_tables
    assert [m["name"] for m in hol_s["metrics"]] == [
        m["name"] for m in real_s["metrics"]
    ]
    for rt, ht in zip(real_s["tables"], hol_s["tables"], strict=True):
        assert (rt["schema"], rt["table"]) == (ht["schema"], ht["table"])
        assert [(c["name"], c["type"]) for c in rt["columns"]] == [
            (c["name"], c["type"]) for c in ht["columns"]
        ]


def test_the_tool_surface_and_the_hint_are_unchanged():
    """Arm D must call the same nine tools through the same prompt affordance;
    that is the whole point of the control."""
    real = DataContract.from_yaml(REAL / "contract.yml").to_system_prompt()
    hol = DataContract.from_yaml(HOLLOW / "contract.yml").to_system_prompt()
    for marker in ("lookup_domain", "lookup_metric", "no_select_star", "main.payments"):
        assert marker in real and marker in hol


# ── obligation 2: the knowledge is gone ──────────────────────────────────


def test_no_metric_carries_a_formula():
    """`sql_expression` holds the fee formula — the single field whose
    survival would hand the control the rule that drives the headline."""
    _, hol_s = _load(HOLLOW)
    assert [m["name"] for m in hol_s["metrics"]]  # non-empty, so this bites
    for m in hol_s["metrics"]:
        assert not m.get("sql_expression", "").strip(), m["name"]


def test_no_manual_prose_survives_anywhere():
    """The strong form: no distinctive phrase from the source documents may
    appear in the hollow artifact. Checked as word 6-grams over the whole
    file, so it cannot be satisfied by moving prose to another field."""
    manual = ROOT / "data/hf/data/context/manual.md"
    if not manual.exists():  # data/ is gitignored; skip on a fresh checkout
        import pytest

        pytest.skip("data/hf not present locally")
    text = " ".join(
        ((HOLLOW / "contract.yml").read_text() + (HOLLOW / "semantic.yml").read_text())
        .lower()
        .split()
    )
    words = re.findall(r"[a-z]+", manual.read_text().lower())
    grams = {" ".join(words[i : i + 6]) for i in range(len(words) - 6)}
    hits = sorted(g for g in grams if g in text)
    assert not hits, f"manual prose survived into the hollow contract: {hits[:5]}"


def test_the_real_contract_would_fail_that_same_check():
    """A knowledge-absence test that the knowledge-bearing artifact also
    passes is not testing anything."""
    manual = ROOT / "data/hf/data/context/manual.md"
    if not manual.exists():
        import pytest

        pytest.skip("data/hf not present locally")
    text = " ".join(
        ((REAL / "contract.yml").read_text() + (REAL / "semantic.yml").read_text())
        .lower()
        .split()
    )
    words = re.findall(r"[a-z]+", manual.read_text().lower())
    grams = {" ".join(words[i : i + 6]) for i in range(len(words) - 6)}
    assert any(g in text for g in grams), (
        "the real contract shares no 6-gram with the manual — the check is broken"
    )
