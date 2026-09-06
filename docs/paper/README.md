# Paper 1 — draft

Source for the arXiv preprint and the PVLDB EA&B submission. The plan this
draft executes is [`../paper-plan.md`](../paper-plan.md); the numbers all come
from [`../../experiments/dabstep-contract-eval/FINDINGS.md`](../../experiments/dabstep-contract-eval/FINDINGS.md).

```bash
make            # rebuild figures if stale, then main.pdf (extended, arXiv)
make pvldb.pdf  # the PVLDB submission: acmart sigconf, no appendix
make figures    # figures only
make check      # both builds: no overfull boxes, no unresolved refs,
                # and the submission's content pages <= 12
```

Two builds share the section files. `main.tex` is the extended version
(stock `article`, appendix included, compiles on a basic TeX Live); it is
21 pages with references. `pvldb.tex` is the submission (acmart `sigconf`,
no appendix, since PVLDB counts appendices toward its 12 content pages);
it is 12 content pages plus references. Appendix cross-references go
through `\appref`, which resolves to `Appendix X` in the extended build
and to a citation of the extended version in the submission.

## Conventions

The rewrite of 2026-09-06 fixed these; keep them.

- **Arms** are \armS{} (`schema_only`), \armM{} (`manual_prompt`),
  \armH{} (`contract_hollow`) and \armC{} (`contract`). Prose and tables
  use the macros; the artifact names appear once, in the arms table.
- **Models, not runs.** There are no run letters. Models are named by
  the \mGLM{}, \mDS{}, \mSON{} and \mGPT{} macros and always appear in
  that order, which is bare-schema hard accuracy (13.9, 22.6, 22.9,
  37.0). Every table and figure uses the same order. "The two flash
  models" and "the two frontier models" are the only tier words.
- **Terms are defined once**, at first use, and then used unqualified:
  content, scaffolding, hollow, compiled contract, derivation gap,
  *macro* and *derived* buckets.
- **Banned**: load-bearing, deflationary, forecloses, licenses (verb),
  headline (as a noun), and any narration of the paper's own revision
  history ("an earlier version", "overturned", "we withdraw"). The
  four-model result is stated directly; the one methodological lesson
  lives in a single paragraph of Threats.
- **Numbers live in tables.** Prose states directions, ratios and the
  few numbers a reader must carry; the repeat runs planned for PVLDB
  will change table cells, not sentences.
- **Main text vs appendix.** `main.tex` builds the extended (arXiv)
  version by default; `make pvldb.pdf` builds the submission without the
  appendix, and `make check` fails if its content pages exceed 12. PVLDB
  counts appendices toward the limit, so the appendix is arXiv-only and
  the submission cites the arXiv version for it.

## acmart without admin rights

`pvldb.tex` needs the `acmart` class, which the basic TeX Live scheme does
not ship. It installs into the user tree without `sudo`:

```bash
tlmgr init-usertree
tlmgr --usermode install acmart xstring environ totpages ncctools comment \
  textcase libertine newtx inconsolata cmap draftwatermark setspace \
  caption float fancyhdr fontaxes mweights xkeyval etoolbox refcount \
  ifmtarg preprint upquote kastrup iftex xcolor trimspaces
```

`hyperxmp` is not relocatable, so `tlmgr --usermode` refuses it; copy
`hyperxmp.sty` out of the TeX Live archive tarball
(`.../tlnet/archive/hyperxmp.tar.xz`) into `~/Library/texmf/tex/latex/hyperxmp/`
and run `mktexlsr ~/Library/texmf`. A full TeX Live has all of this already.

## What is not finished

- **`motherduck-semantic` is dated from page metadata.** The page shows no
  byline or date, but its `datePublished` metadata says 8 June 2026, and the
  bib entry says so.
- **arXiv build.** arXiv does not run BibTeX: upload `main.bbl` alongside the
  sources. `\pdfoutput=1` is on line 1 of `main.tex` so its build picks
  pdflatex for the PDF figures.
- **arXiv abstract field.** `abstract.txt` is the plain-text abstract for
  the submission form (about 1,780 characters, under arXiv's 1,920 cap);
  regenerate it if `sections/00-abstract.tex` changes.

## Figures

`figures/make_figures.py` regenerates both figures from the raw result rows —
they are not committed as opaque images. The script **asserts every value it
draws against the number printed in the paper** and fails the build on a
mismatch, so the prose and the plots cannot drift apart.

Colour always means *arm*, never model and never rank; model is carried by
marker shape and line style, so the figures survive greyscale printing. The
four hues are validated all-pairs for colour-vision deficiency (worst ΔE 9.2)
and normal-vision separation (worst ΔE 16.3).
