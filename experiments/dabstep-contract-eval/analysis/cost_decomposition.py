"""Where does the contract arm's bill go, token class by token class?

Section 6.2 of the paper explains why the contract arm is the cheapest way
to buy a correct answer on three models and not on GPT-5.6: at that
endpoint's ten-to-one price ratio between fresh and cached input, the
contract's knowledge (delivered as tool results, billed fresh the first time
it enters a context) costs more than the manual's (a system prompt that is
the same cached prefix in every request). This script recomputes every
number that section prints from the result rows and the pinned prices, and
`--check` fails if any of them drifts from the paper.

Run:  uv run python analysis/cost_decomposition.py [--check]
"""

from __future__ import annotations

import argparse
import json
import sys
from collections import Counter
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from dce.pricing import MODELS  # noqa: E402

RESULTS = ROOT / "results"
FILES = {
    "gpt-5.6": RESULTS / "sol-full.jsonl",
    "sonnet-5": RESULTS / "sonnet5-full.jsonl",
}
ARMS = ("schema_only", "contract_hollow", "manual_prompt", "contract")

# As printed in the paper (Table 9 and the prose of Section 6.2). Dollar
# figures are rounded to cents, token figures as the text rounds them.
EXPECTED = {
    ("gpt-5.6", "manual_prompt"): {
        "fresh_usd": 3.55,
        "cached_usd": 3.85,
        "output_usd": 7.02,
        "total_usd": 14.42,
        "fresh_per_turn": 582,
        "visible_output_k": 425,
        "reasoning_k": 277,
    },
    ("gpt-5.6", "contract"): {
        "fresh_usd": 8.87,
        "cached_usd": 3.78,
        "output_usd": 8.67,
        "total_usd": 21.32,
        "fresh_per_turn": 1692,
        "visible_output_k": 726,
        "reasoning_k": 141,
        "calls_per_turn": 2.0,
        "lookup_metric": 1690,
        "lookup_domain": 776,
        "inspect_query": 676,
        "run_query": 622,
    },
    ("sonnet-5", "manual_prompt"): {
        "fresh_per_turn": 1037,
        "cached_usd": 14.40,
        "input_per_task_k": 191,
    },
    ("sonnet-5", "contract"): {"fresh_per_turn": 1976},
}


def decompose(path: Path) -> dict[str, dict[str, float]]:
    rows = [json.loads(line) for line in path.open()]
    out: dict[str, dict[str, float]] = {}
    for arm in ARMS:
        arm_rows = [r for r in rows if r["arm"] == arm]
        spec = MODELS[arm_rows[0]["model"]]
        tot = Counter()
        for r in arm_rows:
            for k in (
                "input_tokens",
                "cached_tokens",
                "output_tokens",
                "reasoning_tokens",
                "turns",
                "usd",
            ):
                tot[k] += r.get(k) or 0
            tot["calls"] += len(r.get("tool_calls") or [])
            for name in r.get("tool_calls") or []:
                tot[f"tool:{name}"] += 1
        fresh = tot["input_tokens"] - tot["cached_tokens"]
        d = {
            "fresh_usd": fresh * spec.price_in / 1e6,
            "cached_usd": tot["cached_tokens"] * spec.price_cached / 1e6,
            "output_usd": tot["output_tokens"] * spec.price_out / 1e6,
            "total_usd": tot["usd"],
            "fresh_per_turn": fresh / tot["turns"],
            "visible_output_k": (tot["output_tokens"] - tot["reasoning_tokens"]) / 1e3,
            "reasoning_k": tot["reasoning_tokens"] / 1e3,
            "calls_per_turn": tot["calls"] / tot["turns"],
            "input_per_task_k": tot["input_tokens"] / len(arm_rows) / 1e3,
            "lookup_metric": tot["tool:lookup_metric"],
            "lookup_domain": tot["tool:lookup_domain"],
            "inspect_query": tot["tool:inspect_query"],
            "run_query": tot["tool:run_query"],
        }
        # The three priced components must reproduce the recorded bill.
        recon = d["fresh_usd"] + d["cached_usd"] + d["output_usd"]
        assert abs(recon - d["total_usd"]) < 0.01, (
            path.name,
            arm,
            recon,
            d["total_usd"],
        )
        out[arm] = d
    return out


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument(
        "--check",
        action="store_true",
        help="fail if any printed number differs from EXPECTED",
    )
    args = ap.parse_args()

    drift: list[str] = []
    for model, path in FILES.items():
        table = decompose(path)
        print(f"\n== {model}")
        print(
            f"{'arm':16}{'fresh$':>8}{'cached$':>9}{'output$':>9}{'total$':>8}{'fresh/turn':>11}{'calls/turn':>11}"
        )
        for arm in ARMS:
            d = table[arm]
            print(
                f"{arm:16}{d['fresh_usd']:>8.2f}{d['cached_usd']:>9.2f}{d['output_usd']:>9.2f}"
                f"{d['total_usd']:>8.2f}{d['fresh_per_turn']:>11.0f}{d['calls_per_turn']:>11.2f}"
            )
        for (m, arm), want in EXPECTED.items():
            if m != model:
                continue
            for key, value in want.items():
                got = table[arm][key]
                # Match at the precision the paper prints: cents, whole
                # tokens, thousands of tokens, or one decimal of calls/turn.
                digits = (
                    2 if key.endswith("_usd") else 1 if key == "calls_per_turn" else 0
                )
                if round(got, digits) != round(value, digits):
                    drift.append(
                        f"{model}/{arm}/{key}: rows give {got:.3f}, "
                        f"the paper prints {value}"
                    )

    if drift:
        print("\nPAPER/ROWS MISMATCH:\n  " + "\n  ".join(drift))
        return 1 if args.check else 0
    print("\nevery Section 6.2 number reproduces from the rows")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
