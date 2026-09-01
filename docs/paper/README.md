# Paper 1 — draft

Source for the arXiv preprint and the PVLDB EA&B submission. The plan this
draft executes is [`../paper-plan.md`](../paper-plan.md); the numbers all come
from [`../../experiments/dabstep-contract-eval/FINDINGS.md`](../../experiments/dabstep-contract-eval/FINDINGS.md).

```bash
make        # build main.pdf
make check  # fail on overfull boxes or unresolved refs
```

Currently 12 pages with `article`. Requires only a basic TeX Live install —
`multirow` and `balance` were dropped rather than added as dependencies.

## Switching to the PVLDB template

Replace the `\documentclass` line in `main.tex` with

```latex
\documentclass[sigconf, nonacm]{acmart}
```

and delete the `geometry`, `\title`-formatting and `abstract` blocks it
supersedes. Nothing under `sections/` depends on the class: arm names go
through the `\armS`/`\armM`/`\armC`/`\armH` macros, and every table uses
`booktabs` only.

Six tables are `table*` (full width). Under `acmart`'s narrower columns expect
to promote one or two more; `make check` will say which.

## What is not finished

- **Author affiliation is blank** in `main.tex`. Deliberate — fill before
  submission.
- **`refs.bib` has a `CITE:` note on `motherduck-guides`**: it needs the exact
  post URLs and dates for both the 99.8% run and the 93.2% "views and macros"
  figure. The other three entries need a check that URLs still resolve.
- **The artifact URL in `10-conclusion.tex` is a placeholder.**
- **No pro-sweep results.** Section 5 reports two flash-tier models. The
  pre-registered comparison is unrun, and §10 currently states that as a debt
  rather than reporting it. When the sweep lands, §5.5 (capability
  interaction) gains a third row and §10's future-work paragraph shrinks by
  one item.
- **No figures.** Everything is tables. A single plot of hard accuracy against
  bare-schema baseline, one line per arm, would carry §5.5 better than
  `tab:interaction` does — worth adding once there is a third point to plot.
