# Paper 1 — draft

Source for the arXiv preprint and the PVLDB EA&B submission. The plan this
draft executes is [`../paper-plan.md`](../paper-plan.md); the numbers all come
from [`../../experiments/dabstep-contract-eval/FINDINGS.md`](../../experiments/dabstep-contract-eval/FINDINGS.md).

```bash
make          # rebuild figures if stale, then main.pdf
make figures  # figures only
make check    # fail on overfull boxes or unresolved refs
```

20 pages with `article`, references included. Requires only a basic TeX Live
install; `multirow` and `balance` were dropped rather than added as
dependencies.

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

- **`motherduck-semantic` has no publication date.** The post carries no
  byline or date on the page; it is cited with an access date. Worth one more
  attempt before submission.
- **arXiv abstract field.** `abstract.txt` is the plain-text abstract for
  the submission form (about 1,750 characters, under arXiv's 1,920 cap);
  regenerate it if `sections/00-abstract.tex` changes.

## Figures

`figures/make_figures.py` regenerates all three from the raw result rows —
they are not committed as opaque images. The script **asserts every value it
draws against the number printed in the paper** and fails the build on a
mismatch, so the prose and the plots cannot drift apart.

Colour always means *arm*, never model and never rank; model is carried by
marker shape and line style, so the figures survive greyscale printing. The
four hues are validated all-pairs for colour-vision deficiency (worst ΔE 9.2)
and normal-vision separation (worst ΔE 16.3).
