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

Two builds share the section files and `preamble.tex` (macros, listings
style, the `\appref` switch). `main.tex` is the extended version
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
  the \mGLM{}, \mDS{}, \mSON{} and \mGPT{} macros and appear in that
  order, which is bare-schema hard accuracy (13.9, 22.6, 22.9, 37.0), in
  every table, figure and enumeration, and wherever prose names two of
  them. "The two flash
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

## Next: the k=3 gateway panel (decided 2026-09-18)

Pick this up from the company laptop. It replaces the ~$47 flash-tier repeat
plan in [`../paper-plan.md`](../paper-plan.md) ("PVLDB timing and what
remains") and moves the target from the 2026-11-01 cycle to **2026-12-01**
(abstract due **2026-11-25**). Volume 20 stays open monthly until 2027-03-01,
so the extra month costs nothing.

**What to run.** Every arm, on three models reached through the company's
self-hosted LiteLLM gateway (the same route as `claudesonnet5`; see
"`claudesonnet5` — the enterprise gateway route" in the
[eval README](../../experiments/dabstep-contract-eval/README.md)). The
repeats exist to measure stability, so k=3 goes where stability is in
question, not on every model:

| Model | k | Why |
|---|---|---|
| Claude Sonnet 5 | 3 | Replicates run D on the claim that most needs it. The scaffolding step (`schema_only` vs `contract_hollow`, +15.1 pp on 78 discordant pairs) is decisive only on sol and Sonnet 5, and 78 is below the ~94-flip noise floor measured on glm. Bedrock rejects any temperature but 1, so this is also the unpinned run that FINDINGS names as needing a flip rate most. Run D, on an older commit, adds a near-replicate. |
| Qwen 3.6 27B | 3 | Same size class as MotherDuck's local Qwen3.8 27B (`motherduck-local`), so our contract can be compared against their semantic layer on nearly the same model, not just by citing their number. It is a different Qwen version, so say so. Also gives stability on an open-weight model. |
| GPT-5.6 luna | 1 | Breadth only: one more model next to sol on the GPT-5.6 tier ladder. Like sol it probably cannot pin temperature; the smoke test will show. |

GPT-5.6 terra is left out: the four existing k=1 models and luna already
cover breadth. 401 tasks x 4 arms x (3 + 3 + 1) runs is about 11.2k agent
runs. If Sonnet 5's cost matters, its k=3 can be cut to the `schema_only`
and `contract_hollow` arms, which carry the claim it replicates.

**Rules for the panel.**

- **One library commit for the whole panel.** The eval installs the library
  as an editable path dependency, so the library under test is whatever is
  checked out. Start from `v0.53.0` (or a later tag) and record the SHA.
  Every row stamps `commit_sha`; all panel rows must carry the same one.
  0.52.0 added the Layer 1 multi-statement block, which changes the
  treatment relative to the earlier runs; that is fine inside the panel,
  because every arm and model runs on the same version.
- **Frozen contract, untouched.** Do not add `sensitivity_checks`
  properties to `contract/` or `contract_hollow/`. Properties written after
  reading the losing traces would be fitted to the test set.
- **Keep the existing runs.** glm, deepseek and sol stay in the paper as
  independent k=1 replication across other model families. Run D stays too;
  since it predates 0.52.0, compare it with the Sonnet 5 repeats as a
  near-replicate (like run E), not as a fourth repeat. The gateway panel becomes the main k=3 result. Dropping glm and
  deepseek would lose the finding that scaffolding's effect depends on the
  model.
- **Report** per-model McNemar per repeat and pooled, and the flip rate as
  a measured quantity instead of the 23.4% upper bound.

**Before spending anything.**

1. **Company sign-off** to use the gateway for a publication, and the
   wording of the acknowledgment. This touches how much of the pilot can be
   disclosed (see Paper 2 in `../paper-plan.md`).
2. **Public model ids.** EA&B requires "all experimental data and related
   software must be available". Confirm luna is a public GPT-5.6 model
   served unmodified, and name every model by its public id. Each
   needs an entry in `dce/pricing.py`, which rejects any id it does not list,
   and `--max-spend` is required even when the gateway bills the company.
3. **Smoke test**: `--n 12` per model through the gateway to check
   throughput, rate limits and parameter handling (temperature, reasoning
   effort). The Sonnet 5 route needed special handling for both, and GPT-5.6
   through LiteLLM will probably need its own. Throughput decides whether
   12-01 is realistic.

**Not in this panel.**

- **Sensitivity checks in the agent's loop.** They find latent defects, and
  most of those are in answers already scored correct, so they cannot close
  an accuracy gap. The after-the-fact audit is already measured: on 1,411
  agent queries the shipped verdicts agreed with ground truth every time, and
  they found latent defects in answers scored correct (glm 20/84, Sonnet 5
  10/80, deepseek 5/43, sol 4/95). That goes into the paper as a paragraph.
  Testing them inside the agent's loop needs questions the frozen contract
  has never seen; see the second-benchmark notes below.
- **DABStep v2.** Not released as of 2026-09-18. The only public sign is a
  leaderboard notice of 2026-07-22 (`dabstep-v2-notice`) that closed the
  validated leaderboard. Call the benchmark "DABStep (v1)" in the paper, and
  note that the validated leaderboard is frozen, which keeps the MotherDuck
  comparison stable.

### Second-benchmark candidates (checked 2026-09-18)

The paper states that relying on one benchmark is a limitation. A second
benchmark only counts if its domain rules are **shared across questions**
(so a contract can be written from them and frozen before any question is
read) and its **gold answers are public** (the EA&B availability rule).

| Candidate | Bib key | Verdict |
|---|---|---|
| EntSQL | `entsql` | **Ruled out.** Checked in the released data: every question carries its own `long_doc` (942 distinct documents for 942 questions outside HR; in HR, 78 documents over 124 questions, 43 with none), written from that question's own evidence. Gold answers are not in the release; `evaluate.py` expects a ground-truth file that isn't included. Use it in related work as the contrast: each question comes with its own rules document, where a contract states the rules once for every question. |
| DI-Bench | `di-bench` | **Best fit, unconfirmed.** Business rules shared across questions that change the computation; 731 tasks; models reach 32% on rule-grounded tasks. Published 2026-09-04; the paper does not say whether the data is released. Future work. |
| DataAgentBench | `dab-berkeley` | Too small: 54 queries over 12 datasets, and the leaderboard top is already 94.7% pass@1. |
| DataSpace (KDD Cup 2026 Phase 2) | `dataspace` | **Ruled out** (checked 2026-09-19). Every task has its own workspace (410 tasks, CSV/JSON/SQLite/Markdown/PDF/video, 15 GB), so no rules are shared across questions. Gold answers are public for 60 tasks; the other 350 are withheld for the official leaderboard. Use it in related work beside EntSQL as the task-local contrast; its failure analysis (over half of Grok's failures are "answer materialization", right values in the wrong columns) separates output-shape errors from the rule errors we measure. |
| KDD Cup 2026 Phase 1 | — (BIRD-derived; `bird`) | **Ruled out** (checked 2026-09-19 on the 50-task demo release). The structure fits: each database has one `knowledge.md`, byte-identical across its tasks (11 guides for 50 tasks). But the questions are BIRD dev questions (one gold header is BIRD's own SQL, `COUNT(DISTINCT T1.ID)`), so BIRD's contamination and annotation errors come with them; the guides are LLM-written and incomplete (task 344 needs a normal white-blood-cell range its guide doesn't state); and only the 50 demo golds are public, the 381 hidden tasks were never released. |
| ACME Insurance | `dbt-benchmark` | 11 questions; too few for any statistics. Already cited. |
| AgenticDataBench | `agenticdatabench` | Found but not assessed. |

## What is not finished

- **`make check` also runs `analysis/cost_decomposition.py --check`** in
  the experiment directory, which recomputes every number in Section 6.2 (efficiency)
  from the result rows and the pinned prices.
- **The self-citation `extended` in `refs.bib` has no arXiv id yet.** v1
  was submitted on 2026-09-06 and the id is pending. The submission build
  cites this entry wherever the extended version's appendices are
  referenced, so fill in the id as soon as it is assigned, run `make check`,
  and commit; the citation must resolve before the PVLDB submission.

- **`motherduck-semantic` is dated from page metadata.** The page shows no
  byline or date, but its `datePublished` metadata says 8 June 2026, and the
  bib entry says so.
- **arXiv build.** arXiv does not run BibTeX: upload `main.bbl` alongside the
  sources. `\pdfoutput=1` is on line 1 of `main.tex` so its build picks
  pdflatex for the PDF figures.
- **arXiv abstract field.** `abstract.txt` is the plain-text abstract for
  the submission form; regenerate it if `sections/00-abstract.tex` changes.
  `make check` fails if it exceeds arXiv's 1,920-character cap.

## Figures

`figures/make_figures.py` regenerates both figures from the raw result rows —
they are not committed as opaque images. The script **asserts every value it
draws against the number printed in the paper** and fails the build on a
mismatch, so the prose and the plots cannot drift apart.

Colour always means *arm*, never model and never rank; model is carried by
marker shape and line style, so the figures survive greyscale printing. The
four hues are validated all-pairs for colour-vision deficiency (worst ΔE 9.2)
and normal-vision separation (worst ΔE 16.3).
