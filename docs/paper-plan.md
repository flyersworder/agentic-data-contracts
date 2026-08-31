# Paper 1 — plan

**Status:** in preparation. Focus is Paper 1 and its arXiv preprint. A second
paper carrying the enterprise-deployment lessons is deferred until quantitative
disclosure is cleared — see [Paper 2](#paper-2-deferred).

## Target

| | |
|---|---|
| **Preprint** | arXiv, primary **cs.DB**, cross-list cs.AI (optionally cs.CL) |
| **Venue** | **PVLDB "Experiment, Analysis & Benchmark" (EA&B)** research track |
| **Fallback** | EDBT Industrial & Applications (2027 Lille, rolling cycles) |
| **Not** | VLDB Industrial — it expects a production system at scale; our warehouse is 138k rows at a venue named for very large databases |

`cs.DB` and not `cs.AI` is deliberate: the primary category decides which
mailing list announces the paper, the audience we want is the data-management
community, and cs.AI is high-volume enough to bury an applied evaluation.

EA&B is the right track because it asks for exactly what we have, and not for
a new method. Its stated criteria: *"fundamentally new insights into the
strengths and weaknesses of existing methods"*, *"new ways to evaluate
existing methods and systems"*, and an Experimental Survey subcategory that
accepts *comparison of existing (including open-source) solutions*. That
dissolves the "no novel method" objection an industrial track would raise.

## The claim

> Holding the tool surface and the procedural instruction fixed,
> contract-delivered domain knowledge raises hard-task accuracy from 19.3% to
> 55.1%. The same knowledge pasted verbatim into the prompt reaches 22.9%.

The contribution is the **decomposition**, not the accuracy: **content 87%,
scaffolding 13%**. Published context-layer results (MotherDuck Guides, +72pp)
report two accuracy numbers, from which no mechanism can be recovered — was it
the content, the fetch loop, or the pre-built views? This is, as far as we
know, the only controlled answer.

Lead with mechanism. Never imply we compete on score: we do not, and a
reviewer who reads it that way is right to reject it.

## Structure, and what exists today

| § | content | status |
|---|---|---|
| 1 Introduction | context layers work; nobody has said *which part* works | to write |
| 2 Background | DABStep; the semantic-layer / context-layer landscape | to write |
| 3 Experimental design | four arms, what is held fixed, the hollow control | **`FINDINGS.md`** |
| 4 Protocol | frozen digest-pinned artifact, official vendored scorer, gold reconstruction + independent verification | **`FINDINGS.md`**, `dce/golds.py` |
| 5 Results | four arms x 401 tasks x 2 models, paired McNemar | glm done; **deepseek-flash running** |
| 6 Mechanism | task 1278 traced end to end; both baselines produce the identical wrong number | **`FINDINGS.md`** |
| 7 Threats to validity | one benchmark / 26 templates, reconstructed golds, k=1, self-attested provenance | **`FINDINGS.md`** |
| 8 Related work | MotherDuck Guides; why their 99.8% and our 55.1% measure different things | **`FINDINGS.md`** |
| 9 Artifacts | harness, frozen contract, 3,208 transcripts, results JSONL | **in repo** |

Most of sections 3–8 already exist in `experiments/dabstep-contract-eval/FINDINGS.md`.
The paper is substantially a rewrite of that document for an academic
audience, not new analysis.

## Gaps to close before submission

1. **k>1.** No variance estimate, and one task (1480) was observed flipping
   verdict between identical runs. A reviewer will ask. ~20 tasks x 3 repeats
   on the contract arm; a few dollars.
2. **Gold reconstruction needs its own subsection**, not a footnote. The
   defence is strong — only answers DABStep's own grader marked correct enter
   the vote, and 59/59 of the largest template family reproduce from the
   database independently — but it has to be argued, since golds are the one
   thing a reader cannot check for themselves.
3. **Single benchmark.** Cannot be fixed with compute. State it plainly.
4. **The `COUNT(*)` validator defect** (fixed in `cde8b20`, after the sweep)
   handicapped both contract arms and neither baseline. Report as a floor, not
   a ceiling — the direction is known, the magnitude is not.

## Sequencing

1. deepseek-flash sweep lands -> two model families, killing "it's a glm quirk"
2. k>1 probe
3. draft; arXiv preprint
4. submit

Do not post the preprint before the second model lands. A v1 with one model
invites precisely the objection a v2 would then look like it was patching.

## Paper 2 (deferred)

An experience report on deploying governed agentic SQL against a dialect no
parser models: the parse/emit asymmetry, template assembly instead of SQL
generation, honest degradation to `unverified_compliance`, and a framework
migration forced by operations.

**It becomes a paper only if quantitative disclosure is cleared** (contracts
in use, tables covered, error-rate change). Without numbers it is an
architecture description and a reviewer will say so — in which case the
material belongs in a practitioner talk or a written case study, which reaches
the adopting audience better anyway and clears far more easily.

Keep the two questions disjoint so neither can be called redundant:
**Paper 1 asks whether it works and which part does the work; Paper 2 asks
what it takes to run it.** Paper 2 cites Paper 1 for the mechanism and spends
its pages on the deployment.
