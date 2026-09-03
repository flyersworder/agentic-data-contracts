#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.11"
# dependencies = ["matplotlib>=3.9"]
# ///
"""Build the paper's figures from the raw result rows.

Run: ``uv run make_figures.py`` (or ``make figures`` from docs/paper).

WHY THIS RECOMPUTES RATHER THAN IMPORTS. The scoring logic lives in
``dce.stats`` inside the experiment's own virtualenv, and importing it here
would couple figure rendering to that environment. Instead this script
reimplements the two rules that matter -- deduplicate to the last row per
(arm, task_id), and restrict run B to tasks scoreable in all four arms --
and then ASSERTS every derived number against the value printed in the
paper (see EXPECTED below). A divergence between the results file and the
prose fails the build loudly instead of drawing a wrong chart quietly.

COLOR. One mapping across every figure: color always means ARM, never model
and never rank. The four hues are validated all-pairs for CVD and
normal-vision separation. Model is encoded by marker shape and line style so
the figures survive greyscale printing, which is how a lot of PVLDB actually
gets read.
"""

from __future__ import annotations

import json
import math
from pathlib import Path

import matplotlib
import matplotlib.ticker

matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.lines import Line2D

HERE = Path(__file__).resolve().parent
RESULTS = HERE.parents[2] / "experiments" / "dabstep-contract-eval" / "results"
RUNS = {
    "glm-5.3-flash": RESULTS / "glm-full.jsonl",
    "deepseek-v4-flash": RESULTS / "dsflash-full.jsonl",
    # Order is bare-schema capability, which fig_interaction plots along x.
    "claude-sonnet-5": RESULTS / "sonnet5-full.jsonl",
    "gpt-5.6-sol": RESULTS / "sol-full.jsonl",
}
ARMS = ("schema_only", "manual_prompt", "contract_hollow", "contract")

# Validated all-pairs (light surface): worst normal-vision dE 16.3, worst CVD
# dE 9.2. Assigned in fixed order by arm identity and never cycled.
COLOR = {
    "contract": "#2a78d6",
    "manual_prompt": "#eb6834",
    "contract_hollow": "#1baf7a",
    "schema_only": "#4a3aa7",
}
LABEL = {
    "contract": "contract",
    "manual_prompt": "manual_prompt",
    "contract_hollow": "contract_hollow",
    "schema_only": "schema_only",
}
MARKER = {
    "glm-5.3-flash": "o",
    "deepseek-v4-flash": "s",
    "claude-sonnet-5": "D",
    "gpt-5.6-sol": "^",
}
LINESTYLE = {
    "glm-5.3-flash": "-",
    "deepseek-v4-flash": "--",
    "claude-sonnet-5": "-.",
    "gpt-5.6-sol": ":",
}

INK = "#0b0b0b"
INK_MUTED = "#52514e"
GRID = "#d8d7d2"

# Every number the figures draw, as printed in the paper. Guards against the
# results file and the prose drifting apart.
EXPECTED = {
    ("glm-5.3-flash", "schema_only"): (46, 332),
    ("glm-5.3-flash", "manual_prompt"): (76, 332),
    ("glm-5.3-flash", "contract_hollow"): (64, 332),
    ("glm-5.3-flash", "contract"): (183, 332),
    ("deepseek-v4-flash", "schema_only"): (51, 226),
    ("deepseek-v4-flash", "manual_prompt"): (97, 226),
    ("deepseek-v4-flash", "contract_hollow"): (51, 226),
    ("deepseek-v4-flash", "contract"): (128, 226),
    ("gpt-5.6-sol", "schema_only"): (123, 332),
    ("gpt-5.6-sol", "manual_prompt"): (167, 332),
    ("gpt-5.6-sol", "contract_hollow"): (170, 332),
    ("gpt-5.6-sol", "contract"): (257, 332),
    ("claude-sonnet-5", "schema_only"): (76, 332),
    ("claude-sonnet-5", "manual_prompt"): (79, 332),
    ("claude-sonnet-5", "contract_hollow"): (126, 332),
    ("claude-sonnet-5", "contract"): (227, 332),
}


def wilson(k: int, n: int, z: float = 1.96) -> tuple[float, float]:
    if n == 0:
        return 0.0, 0.0
    p = k / n
    d = 1 + z * z / n
    centre = (p + z * z / (2 * n)) / d
    half = z * math.sqrt(p * (1 - p) / n + z * z / (4 * n * n)) / d
    return max(0.0, centre - half), min(1.0, centre + half)


def load(path: Path) -> dict[tuple[str, str], dict]:
    """Last row wins per (arm, task_id) -- a retried unit leaves several."""
    rows: dict[tuple[str, str], dict] = {}
    for line in path.read_text().splitlines():
        if not line.strip():
            continue
        r = json.loads(line)
        rows[(r["arm"], r["task_id"])] = r
    return rows


def hard_stats(rows: dict) -> tuple[dict[str, tuple[int, int]], dict[str, float]]:
    """Hard-split (correct, n) and total cost per arm, on common support.

    Restricting to tasks scoreable in EVERY arm is what makes the four
    numbers comparable; run B needs it because a provider rate-limit voided
    122 tasks, and run A is unaffected because nothing there failed.
    """
    scoreable = {"correct", "incorrect"}
    tasks = sorted({t for _, t in rows})
    common = [
        t
        for t in tasks
        if all(rows.get((a, t), {}).get("verdict") in scoreable for a in ARMS)
    ]
    hard = [t for t in common if rows[(ARMS[0], t)]["level"] == "hard"]

    acc, cost = {}, {}
    for a in ARMS:
        acc[a] = (sum(rows[(a, t)]["verdict"] == "correct" for t in hard), len(hard))
        cost[a] = sum(rows[(a, t)]["usd"] for t in common)
    return acc, cost


def style_axes(ax) -> None:
    for side in ("top", "right"):
        ax.spines[side].set_visible(False)
    for side in ("left", "bottom"):
        ax.spines[side].set_color(GRID)
    ax.tick_params(colors=INK_MUTED, labelsize=7, length=3, width=0.6)
    ax.set_axisbelow(True)


def fig_ladder(acc: dict, out: Path) -> None:
    """Two steps: tools-and-procedure, then prose. Only the second moves."""
    order = ["schema_only", "contract_hollow", "contract"]
    ys = range(len(order))
    fig, ax = plt.subplots(figsize=(3.3, 2.15))

    for model in RUNS:
        xs = [acc[model][a][0] / acc[model][a][1] * 100 for a in order]
        ax.plot(
            xs,
            list(ys),
            LINESTYLE[model],
            color=GRID,
            lw=1.0,
            zorder=1,
        )
        for y, a in zip(ys, order):
            k, n = acc[model][a]
            lo, hi = wilson(k, n)
            ax.plot(
                [lo * 100, hi * 100],
                [y, y],
                color=COLOR[a],
                lw=1.0,
                alpha=0.45,
                solid_capstyle="butt",
                zorder=2,
            )
            ax.plot(
                k / n * 100,
                y,
                MARKER[model],
                color=COLOR[a],
                ms=6,
                mec="white",
                mew=0.9,
                zorder=3,
            )

    ax.set_yticks(list(ys))
    ax.set_yticklabels(order, fontsize=7.5)
    ax.set_ylim(-0.55, len(order) - 0.35)
    ax.set_xlim(0, 84)
    ax.set_xlabel("hard-task accuracy (%)", fontsize=7.5, color=INK_MUTED)
    ax.grid(axis="x", color=GRID, lw=0.5)
    style_axes(ax)

    # The claim, annotated once per STEP rather than once per model: the
    # figure exists to contrast a flat first step with a large second one,
    # and four separate deltas bury that under arithmetic.
    def pct(model, arm):
        k, n = acc[model][arm]
        return k / n * 100

    # y positions chosen so the text sits right of every connecting line at
    # that height: at y=1.35 the rightmost line (sol's) is at x=60, and at
    # y=0.5 it is at x=44; the text spans roughly x=62 to 83.
    steps = [
        (0.5, "schema_only", "contract_hollow", "+ tools, procedure", INK_MUTED),
        (1.35, "contract_hollow", "contract", "+ contract prose", INK),
    ]
    for y, lo_arm, hi_arm, what, ink in steps:
        # min and max of ALL runs, not deltas[0]/deltas[1]. With two runs
        # those were the same thing; with three, indexing the first two
        # silently dropped the largest delta -- which on the scaffolding step
        # is run C's +14.2, the entire reason this section is a correction.
        deltas = sorted(pct(m, hi_arm) - pct(m, lo_arm) for m in RUNS)
        lo, hi = deltas[0], deltas[-1]
        span = f"{lo:+.1f} pp" if abs(hi - lo) < 0.6 else f"{lo:+.1f} to {hi:+.1f} pp"
        ax.annotate(
            f"{what}\n{span}",
            xy=(83.5, y),
            ha="right",
            va="center",
            fontsize=6.3,
            color=ink,
            linespacing=1.35,
        )

    ax.legend(
        handles=[
            Line2D(
                [],
                [],
                color=INK_MUTED,
                marker=MARKER[m],
                ls=LINESTYLE[m],
                lw=1.0,
                ms=5,
                label=m,
            )
            for m in RUNS
        ],
        loc="upper left",
        fontsize=6.3,
        frameon=False,
        handlelength=2.2,
        bbox_to_anchor=(-0.01, 1.02),
    )
    fig.tight_layout(pad=0.3)
    fig.savefig(out, format="pdf", bbox_inches="tight")
    plt.close(fig)


def fig_cost(acc: dict, cost: dict, out: Path) -> None:
    """Accuracy against spend. The treatment arm is cheapest and best."""
    fig, ax = plt.subplots(figsize=(3.3, 2.3))
    for model in RUNS:
        for a in ARMS:
            k, n = acc[model][a]
            ax.plot(
                cost[model][a],
                k / n * 100,
                MARKER[model],
                color=COLOR[a],
                ms=7,
                mec="white",
                mew=0.9,
                zorder=3,
            )
    ax.set_xlabel("cost of the run (USD)", fontsize=7.5, color=INK_MUTED)
    ax.set_ylabel("hard-task accuracy (%)", fontsize=7.5, color=INK_MUTED)
    # Log scale, and wide enough for runs C and D. The four runs' costs span
    # $0.67 to $46.08 -- a 69x range that a linear axis cannot show without
    # collapsing runs A and B onto the origin. The previous limits (0.4, 2.05)
    # silently placed all four run-C points off-canvas while the legend went
    # on advertising the model; (0.55, 30) would have done the same to run D.
    ax.set_xscale("log")
    ax.set_xlim(0.55, 60)
    ax.set_ylim(5, 85)
    ax.set_xticks([0.6, 1, 2, 5, 10, 20, 50])
    ax.get_xaxis().set_major_formatter(
        matplotlib.ticker.FuncFormatter(lambda v, _: f"${v:g}")
    )
    ax.grid(color=GRID, lw=0.5)
    style_axes(ax)

    handles = [
        Line2D([], [], color=COLOR[a], marker="o", ls="none", ms=5, label=LABEL[a])
        for a in ("contract", "manual_prompt", "contract_hollow", "schema_only")
    ] + [
        Line2D([], [], color=INK_MUTED, marker=MARKER[m], ls="none", ms=5, label=m)
        for m in RUNS
    ]
    # Without a direction cue the reader has to work out which corner is
    # good; with two encodings on the plot that is a real ask.
    ax.annotate(
        "cheaper and more accurate",
        xy=(0.58, 65),
        fontsize=6,
        color=INK_MUTED,
        style="italic",
    )
    ax.annotate(
        "",
        xy=(0.58, 61.5),
        xytext=(0.95, 47),
        arrowprops=dict(arrowstyle="->", color=GRID, lw=0.9),
    )
    # The band between $2 and $10 is the only region with no marks in it:
    # runs A and B sit left of it and runs C and D right of it. Upper-right
    # would now cover sol's and sonnet 5's contract points.
    ax.legend(
        handles=handles,
        loc="upper left",
        fontsize=5.8,
        frameon=False,
        ncol=2,
        columnspacing=0.9,
        handletextpad=0.35,
        labelspacing=0.35,
        bbox_to_anchor=(0.17, 1.03),
    )
    fig.tight_layout(pad=0.3)
    fig.savefig(out, format="pdf", bbox_inches="tight")
    plt.close(fig)


def fig_interaction(acc: dict, out: Path) -> None:
    """Accuracy against base-model capability, one line per arm.

    The x axis is the bare-schema arm's own accuracy: our operational
    definition of what the model can do unaided. Four points per line: the
    third breaks the apparent convergence of the first two, and the fourth
    (claude-sonnet-5, x=22.9, beside deepseek's 22.6) reverses it.
    """
    fig, ax = plt.subplots(figsize=(3.3, 2.3))
    xs = [acc[m]["schema_only"][0] / acc[m]["schema_only"][1] * 100 for m in RUNS]

    # Nudges in POINTS, applied only where two lines end within ~1.5 points
    # of each other. Run C's manual_prompt (50.3) and contract_hollow (51.2)
    # would otherwise print their labels on top of one another.
    label_dy = {"contract": 0.0, "manual_prompt": -5.0, "contract_hollow": 5.0}
    for a in ("contract", "manual_prompt", "contract_hollow"):
        ys = [acc[m][a][0] / acc[m][a][1] * 100 for m in RUNS]
        ax.plot(xs, ys, "-", color=COLOR[a], lw=1.6, zorder=2)
        for x, y, m in zip(xs, ys, RUNS):
            ax.plot(
                x, y, MARKER[m], color=COLOR[a], ms=6, mec="white", mew=0.9, zorder=3
            )
        ax.annotate(
            LABEL[a],
            xy=(xs[-1], ys[-1]),
            xytext=(4, label_dy[a]),
            textcoords="offset points",
            va="center",
            fontsize=6.5,
            color=COLOR[a],
        )

    # y = x is the no-benefit line: an arm on it does no better than the
    # bare schema. Without it the reader has no reference for "flat".
    lim = [10, 80]
    ax.plot(lim, lim, ":", color=GRID, lw=0.9, zorder=1)
    ax.annotate(
        "y = x: no gain over a bare schema",
        xy=(40.5, 36.8),
        fontsize=5.8,
        color=INK_MUTED,
        rotation=27,
        ha="right",
        va="top",
    )

    # Wide enough for run C: its schema_only x is 37.0 and its contract y is
    # 77.4, both outside the previous (11.5, 31) x (10, 66) window, so the
    # third model -- the whole point of this figure's caption -- was drawn
    # off-canvas.
    ax.set_xlim(11.5, 41)
    ax.set_ylim(10, 84)
    ax.set_xlabel(
        "base-model capability:\nschema_only hard accuracy (%)",
        fontsize=7.5,
        color=INK_MUTED,
    )
    ax.set_ylabel("hard-task accuracy (%)", fontsize=7.5, color=INK_MUTED)
    ax.grid(color=GRID, lw=0.5)
    style_axes(ax)
    # deepseek (22.6) and sonnet 5 (22.9) share an x to within 0.3 points, so
    # their labels are pushed apart and placed at different heights.
    label_pos = {
        "deepseek-v4-flash": (-3, 60.0, "right"),
        "claude-sonnet-5": (3, 79.5, "left"),
    }
    for x, m in zip(xs, RUNS):
        dx, y, ha = label_pos.get(m, (0, 63.4, "center"))
        ax.annotate(
            m,
            xy=(x, y),
            xytext=(dx, 0),
            textcoords="offset points",
            fontsize=5.8,
            color=INK_MUTED,
            ha=ha,
            va="bottom",
        )
        ax.axvline(x, color=GRID, lw=0.5, ls=":", zorder=0)
    fig.tight_layout(pad=0.3)
    fig.savefig(out, format="pdf", bbox_inches="tight")
    plt.close(fig)


def main() -> None:
    plt.rcParams.update(
        {
            "font.family": "serif",
            "font.serif": ["Times New Roman", "Nimbus Roman", "DejaVu Serif"],
            "text.color": INK,
            "axes.labelcolor": INK_MUTED,
            "axes.edgecolor": GRID,
            "figure.facecolor": "white",
            "savefig.facecolor": "white",
            "pdf.fonttype": 42,
        }
    )

    acc, cost = {}, {}
    for model, path in RUNS.items():
        if not path.exists():
            raise SystemExit(f"missing results file: {path}")
        acc[model], cost[model] = hard_stats(load(path))

    for (model, arm), want in EXPECTED.items():
        got = acc[model][arm]
        if got != want:
            raise SystemExit(
                f"FIGURE/PAPER MISMATCH for {model}/{arm}: results give {got}, "
                f"the paper states {want}. Reconcile before rendering."
            )

    fig_ladder(acc, HERE / "fig-ablation.pdf")
    fig_cost(acc, cost, HERE / "fig-cost.pdf")
    fig_interaction(acc, HERE / "fig-interaction.pdf")
    print("wrote fig-ablation.pdf, fig-cost.pdf, fig-interaction.pdf")


if __name__ == "__main__":
    main()
