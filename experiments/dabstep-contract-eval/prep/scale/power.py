"""Experiment 6's power, simulated before the run commit (design: Predictions,
Power).

Each task's success rate per model is its experiment 4 `contract` rate (k=3,
gpt-6-luna, gpt-6-sol, qwen3.8-27b). A drop of d points is applied two ways:
spread, every run's chance lowered by d; or concentrated, whole tasks failing,
as many as make the mean drop d. Each simulated pair (1x, top) is tested as
(37) is: the one-sided sign test over tasks scored 0-3k pooled, p < 0.05, and
the direction on each model. The minimum detectable drop is the smallest d
with power 0.8. For (40), how often the 95% bootstrap lower bound of
(level - 1x) clears -5 points when nothing changes.

    UV_FROZEN=1 uv run python prep/scale/power.py
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

import numpy as np
from scipy.stats import binomtest

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

from dce.stats import load  # noqa: E402

FILES = {
    "gpt-6-luna": "exp4-luna-r{}.jsonl",
    "gpt-6-sol": "exp4-sol-r{}.jsonl",
    "qwen3.8-27b": "exp4-qwen-r{}.jsonl",
}
SIMS = 2000
SEED = 2026
OUT = ROOT / "contracts-scale" / "power.json"


def rates() -> np.ndarray:
    """tasks x models: experiment 4's contract success rate."""
    per: dict[str, dict[str, list[int]]] = {}
    for m, pattern in FILES.items():
        for i in (1, 2, 3):
            for r in load(ROOT / "results" / "procedures" / pattern.format(i)):
                if r["arm"] == "contract":
                    per.setdefault(str(r["task_id"]), {}).setdefault(m, []).append(
                        r["verdict"] == "correct"
                    )
    tasks = sorted(per)
    return np.array([[np.mean(per[t][m]) for m in FILES] for t in tasks])


def holds(a: np.ndarray, b: np.ndarray) -> bool:
    """a, b: tasks x models x repeats of 0/1; does a beat b as (37) asks?"""
    sa, sb = a.sum(axis=(1, 2)), b.sum(axis=(1, 2))
    up, down = int((sa > sb).sum()), int((sa < sb).sum())
    if up + down == 0:
        return False
    p = binomtest(up, up + down, 0.5, alternative="greater").pvalue
    per_model = (a.mean(axis=(0, 2)) - b.mean(axis=(0, 2))) > 0
    return p < 0.05 and bool(per_model.all())


def simulate(p: np.ndarray, d: float, k: int, shape: str, rng) -> float:
    hits = 0
    for _ in range(SIMS):
        one = rng.random((*p.shape, k)) < p[..., None]
        if shape == "spread":
            q = np.clip(p - d, 0, 1)
        else:
            q = p.copy()
            n = int(round(d * p.size / max(p.mean(), 1e-9)))
            idx = rng.choice(p.size, size=min(n, p.size), replace=False)
            q.flat[idx] = 0
        top = rng.random((*p.shape, k)) < q[..., None]
        hits += holds(one, top)
    return hits / SIMS


def null_bound(p: np.ndarray, k: int, rng, sims: int = 500) -> float:
    """How often the 95% bootstrap lower bound of (level - 1x) is above -5
    points when the level changes nothing."""
    ok = 0
    for _ in range(sims):
        one = (rng.random((*p.shape, k)) < p[..., None]).mean(axis=(1, 2))
        lvl = (rng.random((*p.shape, k)) < p[..., None]).mean(axis=(1, 2))
        diff = lvl - one
        boots = [
            diff[rng.integers(0, len(diff), len(diff))].mean() for _ in range(1000)
        ]
        ok += np.percentile(boots, 2.5) > -0.05
    return ok / sims


def main() -> None:
    p = rates()
    rng = np.random.default_rng(SEED)
    out: dict = {
        "tasks": int(p.shape[0]),
        "base_rate": dict(
            zip(FILES, [round(float(x), 3) for x in p.mean(axis=0)], strict=True)
        ),
    }
    for k in (3, 5):
        for shape in ("spread", "concentrated"):
            curve = {}
            for d in (0.02, 0.04, 0.06, 0.08, 0.10, 0.12, 0.15, 0.20):
                curve[f"{d:.2f}"] = simulate(p, d, k, shape, rng)
            mdd = next((d for d, pw in curve.items() if pw >= 0.8), None)
            out[f"k={k} {shape}"] = {"power": curve, "minimum_detectable_drop": mdd}
            print(f"k={k} {shape}: MDD {mdd}; {curve}")
        out[f"k={k} (40) null bound clears -5"] = null_bound(p, k, rng)
        print(f"k={k} (40): {out[f'k={k} (40) null bound clears -5']}")
    OUT.write_text(json.dumps(out, indent=1) + "\n", encoding="utf-8")


if __name__ == "__main__":
    main()
