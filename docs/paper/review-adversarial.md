# Review — PVLDB Experiments, Analysis & Benchmarks
### "Which Part of the Context Layer Does the Work?"
**Reviewer 2. Adversarial pass.**

---

## Verdict

**Reject.**

Single reason: the paper's two load-bearing claims — that the contract and the
prompt arm carry *the same knowledge in different form* (the "delivery, not
content" result), and that the contract *compiles to gold on 176 of 176 tasks*
(the sufficiency result that licenses the derivation-gap headline) — are both
contradicted by the authors' own released artifacts. The frozen contract
contains at least three semantic decisions that appear in **neither** source
document handed to the baseline, and the "compiled contract" is not the compiled
contract: 79 of its 176 tasks are answered by hand-written per-family SQL that
never touches either view, and 59 of them are the same tasks, resolved by the
same rule, that were earlier used to *validate the gold key itself*. The paper
is unusually candid about its other weaknesses — genuinely, admirably so — which
makes these two failures more damaging, not less: they are the two places where
the candour stops exactly where the headline begins.

This is a fixable paper. It is not fixable by revision at this venue in one
cycle, because both repairs change numbers rather than sentences.

---

## Kill shots

### K1. The treatment arm was given knowledge the baseline's source documents do not contain. The paper's central mechanism claim is therefore false as stated.

**Claim.** `sections/06-mechanism.tex:57–58`:

> "The finding is not that the contract arm was given more information. Both
> arms had the information. Only one of them used it."

and `sections/06-mechanism.tex:126–127`:

> "\armM{} holds the same knowledge, as prose, in its system prompt."

and the provenance guarantee at `sections/04-protocol.tex:9–10`:

> "The contract's header records that every fact in it came from
> \texttt{manual.md} and the dataset README, and nothing else."

**Why it fails.** The frozen artifact carries **seven** clauses the authors
themselves flag as going beyond the sources — `contract/semantic.yml` lines 55,
131, 165, 189, 237, 272, 287, each opening `INTERPRETATION:`. The file's own
header (`contract/semantic.yml:10–13`) says these are "the places where arm C's
encoding is an act of reading rather than transcription." At least three of them
are not readings of an ambiguity but facts absent from both sources:

1. **Empty list means wildcard.** `contract/semantic.yml:55–58, 62–68` adds
   `len(f.aci) = 0`, `len(f.account_type) = 0`, `len(f.merchant_category_code) = 0`
   as wildcard disjuncts, justified by inspecting the annexed `fees` data. I
   grepped both source documents: `data/hf/data/context/manual.md` and
   `payments-readme.md` contain **zero** occurrences of "empty" and zero
   occurrences of `[]`. The manual states only the NULL convention
   (`manual.md:95`). Arm M cannot know this rule because it was never told it.
2. **The capture-delay band mapping.** `contract/semantic.yml:69–75` hands arm C
   the exact `CASE` translating `merchant_data.capture_delay` (a day count) into
   the `fees.capture_delay` band labels. `manual.md:81` documents the band
   *labels* only; `payments-readme.md` does not mention `capture_delay` at all.
   The join between the two representations is supplied to arm C and to no one
   else.
3. **Band-boundary inclusivity.** `contract/contract.yml:100–115` and
   `contract/semantic.yml:93–113` resolve "between A and B" as closed at both
   ends, with the explicit consequence that a rule on a shared boundary matches
   both bands. The contract's own prose concedes "The manual does not resolve
   that tie."

Two of these three are, by the authors' own construction, **load-bearing clauses
in the §6.3 clause analysis** (`analysis/clauses.py:55–62`:
`emptylist_wildcard`, `capture_delay_band`). So the flagship behavioural result
— arm C writes all six clauses 39/65/98% of the time against arm M's 3/9/4%
(`sections/06-mechanism.tex:117–121`) — is measuring, in part, that arm C was
told something arm M was not. An arm cannot write a clause encoding a rule it
was never given. The paper's gloss on that table ("\armM{} holds the same
knowledge, as prose") is not defensible.

The frozen artifact states the standard against which this should be judged, and
it is the authors' own:

> `contract/contract.yml:14–16`: "Arm B receives the same two documents verbatim
> in its system prompt; **if this contract encoded anything they do not contain,
> the arms would no longer carry identical knowledge in different form and the
> comparison would be void.**"

By that criterion the C-vs-M comparison is void. I do not think it is *fully*
void — the task-1278 walkthrough (`06-mechanism.tex:9–45`) turns on the NULL
convention, which **is** at `manual.md:95`, so that specific vignette survives —
but the generalisation from it does not.

**What would repair it.** Either (a) a fifth arm: `manual_prompt` plus the seven
INTERPRETATION sentences appended verbatim as prose, which is the only clean test
of "delivery vs possession"; or (b) a task-level partition of the C-vs-M
comparison into tasks whose correct answer requires an INTERPRETATION and tasks
whose does not, with the delivery claim restricted to the latter. (b) is
tractable from the released data and I would expect it to preserve a real but
much smaller effect. Either way, §6 must stop asserting information parity.

---

### K2. The "176 of 176" sufficiency result is misdescribed, partly circular, and defined on an author-chosen denominator. It is the load-bearing premise of the derivation-gap headline.

**Claim.** `sections/05-results.tex:311–322`:

> "We transcribed those fields into two DuckDB views… **The only work the
> transcription adds is \emph{composition}** … **The compiled contract
> reproduces the benchmark's gold answers exactly on every task it covers: 176
> of 176, graded by the official scorer.**"

and `sections/00-abstract.tex:24–27`, `sections/01-intro.tex:89–92`, which repeat
it verbatim as an abstract-level claim.

**Three independent problems.**

**(a) 79 of the 176 are not answered by the two views.** `analysis/coverage.py`
splits its eleven families into `cat="macro"` (7 families) and `cat="rules"` (4
families), and `classify()` (`coverage.py:276–295`) lumps all eleven into the
`macro` bucket. But the four `rules` families —
`avg_fee_credit`/`avg_fee_account`/`avg_fee_account_mcc` (n=59) and
`fee_ids_by_at_aci` (n=20) — are answered by `AVG_FEE`
(`coverage.py:99`), a hand-written aggregate over the raw `fees` table with a
hand-picked WHERE clause, and never touch `transaction_fee_matches` or
`merchant_month`. `coverage.py:167–173` says so in its own words: "The other four
families resolve straight off `fees` / `merchant_category_codes`, so any analysis
that lesions or inspects the macro's own predicates MUST restrict itself to
these." 45% of the sufficiency evidence is not evidence about the compiled views
at all. `FINDINGS.md:308` is honest about this ("it counts the families we
actually implemented"); the paper is not.

**(b) 59 of the 176 are circular.** `sections/04-protocol.tex:68–71` validates the
reconstructed gold key like this:

> "The largest template family --- the fee-average questions --- was recomputed
> directly from DuckDB using the matching rule documented in \texttt{manual.md}.
> **All 59 reconstructed golds reproduce exactly.**"

Those are the same 59 `avg_fee_*` tasks (`FINDINGS.md:285`, `FINDINGS.md:664`),
recomputed with the same wildcard/matching logic the contract encodes. So "the
golds agree with our rule" and "our compiled rule reproduces the golds" are one
measurement reported twice, once as gold validation and once as contract
sufficiency. Neither can corroborate the other.

**(c) The denominator is chosen, and chosen after seeing gold.** `FAMILIES`
(`coverage.py:101–165`) is a hand-curated list of eleven regexes. The 176 is
"tasks matching a regex somebody wrote a handler for." `coverage.py:297–320`
imports the gold key and calls `score(got, gold)` on every answer, printing a
`misses` list. That is a fitting loop, whether or not it was used as one: the
authors could see, per family, whether their transcription matched gold. And
crucially, **`analysis/macro.sql` and `analysis/coverage.py` are not covered by
the freeze discipline the paper leans on**. Only `contract/` is digest-pinned
(`sections/04-protocol.tex:13–18`); the compiled layer is post-hoc analysis code
written with the key in hand. `macro.sql:4–5` asserts "nothing was chosen by
consulting the benchmark's answers," which is exactly the kind of self-attested
claim the paper elsewhere (correctly) refuses to accept from itself
(`04-protocol.tex:30–33`).

**And the argument it is used to make runs backwards.** `05-results.tex:324–326`:

> "First, it forecloses an objection we cannot otherwise answer: that \armC{}
> wins because the contract is subtly wrong in a benchmark-favouring way. It is
> not wrong; it compiles to gold."

Exact reproduction of the answer key on 176 tasks is equally consistent with the
contract having been *tuned toward* the answer key. Combined with K1 — three
undocumented interpretation calls, each of which happens to land on the value
that reproduces gold — this sentence strengthens the leakage worry rather than
foreclosing it. I want to be fair: I read `contract/semantic.yml` closely and I
believe the interpretations are the *natural* readings, not obviously reverse-
engineered, and I could not find a benchmark-shaped artifact in the contract (no
merchant names, no task phrasings, no answer formats). But "plausible" is not
"demonstrated", and the demonstration the paper offers is the one that cannot
work.

**What would repair it.** Report the 176 split by family category, with the
`rules` families labelled as hand-written question-to-SQL rather than compiled
views. Drop the 59 avg-fee tasks from *either* the gold validation or the
sufficiency claim, not both. Freeze and digest-pin `macro.sql` before running it
against gold, or have a third party transcribe the `sql_expression` fields
blind. Then re-derive every derivation-gap number on what survives.

---

### K3. The "derivation gap" is, arithmetically, the macro-bucket error rate. The headline finding restates that a stronger model is more accurate.

**Claim.** `sections/05-results.tex:374–377`:

> "\textbf{The derivation gap collapses with capability.} \armC{} on
> \emph{macro} scores 60.8\%, 62.7\% and 94.9\%, a shortfall of 39.2, 37.3 and
> \textbf{5.1} points below a ceiling we have proven reachable."

and the abstract, `sections/00-abstract.tex:29–31`: "the derivation gap is not a
fixed cost of the declarative approach but one that collapses with model
capability, a prediction we recorded publicly before the third run executed."

**Why it fails.** Because the proven ceiling is exactly 100%, the "derivation
gap" is *identically* $100 - \text{accuracy}$: $100-60.8=39.2$, $100-62.7=37.3$,
$100-94.9=5.1$. `FINDINGS.md:348–350` tabulates it that way explicitly. The
quantity is the error rate on a bucket, relabelled. "The derivation gap collapses
with capability" therefore contains no information beyond "arm C's error rate on
the macro bucket falls from 39% to 5% across three models" — i.e. a stronger
model gets more questions right, which the same table shows for every arm.

The paper's rhetorical move is to treat this as a *pre-registered prediction*
confirmed. But the prediction, decoded, is "arm C's accuracy on the easiest,
most templated third of the benchmark will be higher on a frontier model than on
two flash models." Arm C's overall hard accuracy already moves 55.1 → 56.6 →
77.4 (`05-results.tex:122–124`); a bucket-level rise was near-certain, and the
"harness reading" the prediction was contrasted against (that it would stay at
61–63%) was never a live hypothesis for a model 40 points better on the bare
schema.

The framing also creates a ceiling artifact the paper does not flag. The
`derived` bucket has **no proven ceiling**, so the contrast at
`05-results.tex:391–395` ("macro 60.8→62.7→94.9 against derived
45.9→46.5→56.1") compares a bounded quantity near its bound against an unbounded
one. At 94.9%, arm C on `macro` is 9 wrong answers out of 176; a single
run-to-run flip (and the paper concedes at `08-threats.tex:62–63` that verdicts
do flip between identical runs) moves the "gap" by 0.6 points on a scale where
the whole headline is 5.1.

**What would repair it.** State the identity plainly, drop "derivation gap" as a
new construct, and report the comparison on a scale that is not compressed at
the bound — error-rate ratio, log-odds, or accuracy with Wilson intervals — with
the `derived` bucket's own ceiling estimated rather than assumed absent. If the
claim is about *economics* (declarative vs pre-computed), it needs a cost axis,
not an accuracy relabelling.

---

### K4. Contamination is never mentioned, and the paper's own method proves the answer key is public.

**The contradiction.** `sections/02-background.tex:22–24`:

> "\textbf{Golds are withheld and grading is centralised}, so the benchmark
> resists the overfitting that makes many text-to-SQL results hard to trust"

Against `sections/04-protocol.tex:43–46`:

> "Every submission to the DABStep leaderboard publishes a per-task file
> containing, for each task, the submitted answer and whether the official
> grader marked it correct. At the time of reconstruction the corpus held 2{,}199
> such submissions."

The paper recovers 401 of 450 golds from public artifacts, at a median plurality
share of 0.981. That is a demonstration that the answer key **is** public, in
machine-readable JSONL, on Hugging Face, with per-task correctness labels — I
can see 2,199 such files under `data/hf/data/task_scores/`, dating from January
2025 onward. §2's claim that withheld golds protect the benchmark from
overfitting is refuted by §4.2 two pages later.

**Why this is a kill shot rather than a nitpick.** I searched the entire paper
(`sections/*.tex`, `refs.bib`) for *contaminat\**, *memoris\**, *memoriz\**,
*training data*, *cutoff*, *pretrain*. **Zero hits.** For an EA&B submission
whose three models are all 2026 releases evaluated on a benchmark whose tasks
and answers have been public since early 2025, the absence of any contamination
analysis is disqualifying on its own.

It also confounds the paper's single most-promoted trend. The capability ordering
(glm-5.3-flash → deepseek-v4-flash → gpt-5.6-sol) is perfectly collinear with
recency and with training-corpus size. `sections/08-threats.tex:93–94` concedes
"any run-C contrast confounds capability with both [provider and model family]"
but omits the third and most damaging confound: run C's model had the most
opportunity to have ingested DABStep's questions *and* the leaderboard's graded
answers. Every claim of the form "X collapses/rises with capability" — K3, the
`+27.1` non-monotonicity (`05-results.tex:277–278`), the delivery-asymmetry claim
at `06-mechanism.tex:129–134` — is unidentified against memorisation.

**What would repair it.** A contamination section with at least: (i) an explicit
statement of each model's training cutoff against DABStep's release and against
the leaderboard corpus's growth; (ii) a canary/perturbation probe — re-run a
sample of tasks with merchants, months and thresholds re-parameterised off-
distribution (the benchmark is built by exactly this parameterisation,
`02-background.tex:30–33`, so the machinery exists) and show the contract effect
survives; (iii) a memorisation probe: ask each model for the fee-matching rule
and the `manual.md` conventions with no context supplied. (iii) is cheap and
would also directly test the delivery-vs-possession claim.

---

### K5. The governance result is not ablated, by the paper's own standard.

**Claim.** `sections/07-governance.tex:38–40` and `:94–97`:

> "\textbf{144 mutating statements across 24 tasks in the ungoverned arms; zero
> in either governed arm}" … "The 144-to-0 contrast measures the
> \emph{declarative} mechanism: rules the model reads and complies with"

**Why it fails.** The entire methodological contribution of this paper is the
insight that a two-condition comparison cannot attribute an effect when the
conditions differ along several axes at once — that is what
`sections/03-design.tex:50–53` says, and it is why `\armH{}` exists. The
governance section then makes exactly the error §3.2 was built to prevent. Both
governed arms differ from both ungoverned arms in *three* ways simultaneously:
the declared `forbidden_operations` in the prompt, the table allow-list, **and**
an entirely different tool surface (`dce/arms.py:437–460`: nine governed tools,
no raw `execute_sql`, with tool descriptions that coach procedure). The paper
attributes the 144-to-0 to the first of the three, with no arm that varies it
alone.

The alternative explanation is concrete and the paper's own §7.3 supplies it: the
escalation happened because a model needed to hold an intermediate result and
`CREATE TEMP TABLE` did not survive the connection lifecycle
(`07-governance.tex:133–145`). A governed arm has `preview_table`,
`inspect_query`, `run_query` and a session object — a tool surface that reduces
the need to materialise scratch state, and whose `run_query` description
(`dce/arms.py:78–79`) tells the model to "Prefer this tool over any other SQL or
data-access path." "The model did not need a temp table" and "the model was
deterred by a stated rule" both produce zero, and this design cannot separate
them. §7.2's "Nor can our data distinguish a model that will not attempt from one
that would have been caught" concedes a *different*, smaller version of this
problem and stops short of the one that matters.

The evidential base is also thinner than the headline. Run C contributes nothing
(`07-governance.tex:42–45`); run B alone is $p=0.25$ (`:39`); so 93 of the 144
statements and the entire significant result come from **one model, one run,
$k=1$**, on a hazard the paper itself says is partly an artifact of its own
connection lifecycle (`:143–145`).

**What would repair it.** A fifth arm — governed tools with
`forbidden_operations` and the allow-list emptied — which is a two-line change to
`dce/hollow.py` (it currently, deliberately, preserves exactly those fields;
`dce/hollow.py:24–30`). That arm is the governance analogue of `\armH{}` and its
absence is conspicuous in a paper whose whole argument is that such an arm is
mandatory.

---

## Substantive concerns, ranked

### S1. Arm C's resident context is understated by ~3,000 characters, and the harness says so in a comment the paper does not carry forward.

`sections/01-intro.tex:81–82` frames the mechanism as "a 2{,}475-character prompt
plus one targeted lookup, versus a 24{,}177-character wall of text"; repeated at
`03-design.tex:41–43` and `06-mechanism.tex:53–55`.

`dce/arms.py:75–85` states:

> "DISCLOSED ASYMMETRY, NOT FIXED: arm `contract`'s nine library-supplied tools
> carry 3,042 characters of tool descriptions against 167 for arms A/B's three,
> and several of arm C's coach procedure rather than merely describe function —
> e.g. `inspect_query` says a model \"MUST call lookup_metric first\" … That is
> procedural instruction reaching only arm C. … it means \"the arms differ only
> in context\" is true of *content* and not of *coaching*, **and that distinction
> should travel with any result this experiment produces.**"

It did not travel. Arm C's true resident context is ~5.5k characters, not 2,475
— the contrast is 4.4×, not 9.8×. The direction of the claim survives; the number
does not, and a paper this scrupulous about disclosing asymmetries (the 31
`preview_table` calls, `03-design.tex:103–112`; the inert digest stamp,
`04-protocol.tex:19–28`) should not have dropped the one its own code flagged as
mandatory. Note in fairness: `\armH{}` *does* control for this, since both
governed arms get identical tool descriptions (`dce/arms.py:474–488`), so the
scaffolding-step measurement is unaffected. It is the C-vs-M framing that is
affected.

### S2. Effect sizes and $p$-values in the same table are computed on different populations.

The ablation table at `sections/05-results.tex:136–144` pairs a hard-split effect
($\Delta$ hard $= +5.4$, $+0.0$, $+14.2$ pp) with an all-401 McNemar
($p = 0.058$, $0.61$, $4\times10^{-7}$) and an all-401 discordant count (55, 34,
87). The paper admits the mismatch two lines later — "On run~A the hard slice
alone is already $p{=}0.015$ (16/34); the 0.058 above is the all-401 test"
(`:152–153`) — which shows the authors know, but the table still ships as if the
$p$ tests the $\Delta$. The abstract inherits it: `00-abstract.tex:19–20` reads
"worth $+14.2$ points ($p{=}4{\times}10^{-7}$)", pairing a hard-only effect with
an all-task test. Report both quantities on the same task set, or label the
columns.

### S3. Run B's missingness is by run order, not at random, and the three-point derivation curve puts one point on a different task set.

`sections/05-results.tex:19–29` argues the truncation costs power not validity,
and the argument is decent as far as it goes — near-uniform per-arm loss, 114 of
122 tasks losing all four arms together. But the mechanism is temporal
(`:10–15`: 0.2% error over the first 1,000 rows, 100% thereafter) and the runner
iterates in "task-major order" with no shuffle (`dce/runner.py:694–695`), so the
surviving 279 are the *first ~70% of the task list*. DABStep task ids cluster by
template family. The paper checks only hard/easy enrichment (`:32–34`); it never
reports the family composition of the 122 lost tasks. The bucket table gives
indirect reassurance (118/176 macro = 67%, 101/148 derived = 68%,
`FINDINGS.md:326`) but that check belongs in the paper, at family granularity.

Downstream: the derivation-gap curve 39.2 → 37.3 → 5.1 has its middle point
computed on $n=118$ and its outer points on $n=176$
(`sections/05-results.tex:359–361` caption). The "fixed ≈38-point tax,
model-invariant to within two points" observation (`:376–378`) that the paper
says run C overturned was never a comparison on a common task set.

### S4. No confidence intervals on the headline accuracy table; scored-vs-strict promised but not delivered.

`sections/04-protocol.tex:101–107` promises: "We report accuracy two ways for
every arm… Reporting one alone hides which is in play." Table 1
(`05-results.tex:38–69`) reports **one** number per cell and never says which,
and no other table in the paper carries the second. Wilson intervals are promised
at `04-protocol.tex:97–98` and appear only in Figure 2's error bars. For an EA&B
paper, the headline table needs $n$, the accuracy definition, and an interval per
cell. As it stands a reader cannot tell whether run A's `\armS{}` 13.9% is over
332 or over 332 minus errors, which matters because that arm had 41 forced
answers (`05-results.tex:204`).

### S5. Novelty: `motherduck-semantic` already reports a decomposition, on this benchmark, at higher fidelity.

`sections/09-related.tex:40–47` describes the very citation that most threatens
the contribution: vector-retrieved fragments "capped out around 88%", baked-in
schema comments/macros/derived tables reached 93%, a hierarchical semantic layer
reached 100%. That *is* a decomposition of a context layer by kind of component,
measured on DABStep, spanning a wider range than this paper's two steps and
landing 23 points above its best number. The paper's §9.5 distinctiveness table
(`:131–144`) claims four rows; two of them ("Artifact frozen first",
"Transcripts released") are artifact-hygiene rather than scientific contribution,
and one ("four arms") is a design property, not a result. What is genuinely
novel reduces to a single row — the hollow placebo isolating scaffolding from
semantics — which is a real contribution and, in my judgement, one row's worth.
Compounding this: the *motivating* claim of the entire paper ("72 percentage
points", `01-intro.tex:13–14`) and the anchor of §9 both rest on vendor blog
posts, one of which `refs.bib:21–28` concedes "carries no publication date or
byline". A PVLDB paper cannot have its problem statement rest on undated
marketing.

### S6. $k{=}1$, and the paper's three strongest claims all rest on the one run with unpinned temperature.

`sections/08-threats.tex:60–69` is exemplary in stating this, and I will not
pretend it is hidden. But the consequence deserves more weight than it gets: run
C uniquely supplies (i) the scaffolding-step reversal ($p=4\times10^{-7}$), (ii)
the non-monotone $+27.1$ margin, (iii) the 94.9% derivation-gap collapse — and
run C is the run that "does not accept a temperature parameter and therefore ran
at the provider's default rather than at 0" (`:66–69`). Every headline correction
in this paper comes from a single unreplicated sample at unknown sampling
temperature. The paper's own methodological moral — "A null across two adjacent
models is weak evidence of absence" (`00-abstract.tex:20–21`) — applies with
equal force to a *positive* result from one model at $k=1$, and the paper does
not draw that symmetric conclusion.

### S7. The abstract reads as if the experiment was replicated.

`sections/00-abstract.tex:9`: "a controlled four-arm ablation on DABStep, **run
three times** across three model families". §3.4 is unambiguous that "The three
runs are separate experiments rather than replicates" (`03-design.tex:121–122`),
but the abstract never says $k=1$ or "single run", and "run three times" is the
natural phrase for replication. Given that $k=1$ is the paper's own
self-identified largest gap (`08-threats.tex:66`), it belongs in the abstract.

### S8. Eighteen uncorrected tests is an undercount.

`sections/08-threats.tex:71–78` counts the 18 McNemar comparisons in Table 2 and
declines to correct. But the paper also reports: 9 Fisher tests on the clause
analysis (`06-mechanism.tex:125–126`, three arms × three runs), 8 within-arm
Fisher tests (`:137–138`, "every Fisher $p{\ge}0.14$"), 3 governance Fisher tests
(`07-governance.tex:31–34`), plus per-slice comparisons in §8.1. The realistic
count is 40+. The three vs-`\armC{}` contrasts survive anything; the
`\armS{}`-vs-`\armH{}` row and the clause-analysis Fishers do not have that
margin uniformly, and the paper should either report BH-adjusted $q$-values or
declare a small pre-specified confirmatory family and label the rest exploratory.

---

## Minor

- `sections/04-protocol.tex:111` and `07-governance.tex:7, 83, 105` all say
  "4{,}812 transcripts / rows". $401\times4\times3 = 4{,}812$ is the *attempted*
  count; run B's 466 terminal rate-limit rows never produced an agent transcript
  (`05-results.tex:10–15`). "In 4{,}812 rows the enforcement layer never had to
  fire" (`07-governance.tex:83–84`) counts ~466 rows in which nothing ran.
- `sections/04-protocol.tex:62–65` attributes the five excluded golds to "manual
  verification" by the authors. `dce/golds.py:112–118` credits MotherDuck Labs,
  who publish the same exclusions independently. **That independent
  corroboration is worth more than the current phrasing gives it** — say so.
- `sections/06-mechanism.tex:104–107` says the comparison is restricted to "the
  97 \emph{tasks} that require all six" clauses. `analysis/clauses.py:102–117`
  restricts to hard, complete-case tasks in `PAYMENTS_FAMILIES` — a family-level
  filter. "Tasks in the families whose answers join payments to fees" is the
  accurate description; "require all six" is an inference, not a per-task test.
- `sections/03-design.tex:73–76` describes the hollow-arm $n$-gram test as
  checking "no 6-gram of \texttt{manual.md} survives into the hollow contract".
  Given K1, the more informative test is the *converse*: what fraction of the
  real contract's content-bearing $n$-grams appears in `manual.md`. The contract
  is 60,181 characters against a 22,127-character manual; the paper invites the
  reader to verify provenance by reading both (`04-protocol.tex:11–12`) but never
  measures the overlap it could have measured in ten lines of Python.
- Table 3 (`05-results.tex:193–223`) mixes units across runs — "Reasoning tokens"
  for A and C, "Reasoning / task" for B — making the rows non-comparable at a
  glance.
- `sections/08-threats.tex:48–51`: the 30-task MCC sub-bucket where every arm
  scores 1–2/30 on runs A and B, then `\armS{}` leads with 8/30 on run C, is
  reported without comment. An arm with *no* context beating the contract arm 2:1
  on a slice is the single most contamination-suggestive datum in the paper.

---

## What holds up under audit (stated explicitly, because it matters)

I checked these against the code and they survive:

- **The hollow arm is a genuine one-variable control.** `dce/hollow.py:54–88`
  empties descriptions, summaries, `sql_expression` and column/relationship prose
  and touches nothing else; `dce/arms.py:474–488` builds `contract` and
  `contract_hollow` through the *same* code path with the same procedural
  sentence written once and shared. Tool descriptions, allow-list,
  forbidden-operations and the nine-tool surface are identical between them. The
  scaffolding-step measurement is clean, and the deliberate refusal to pad the
  hollow prompt to equal length (`03-design.tex:82–86`, `hollow.py:32–38`) is the
  right call, argued correctly.
- **The harness's arm symmetry is unusually well policed.** `dce/agent.py:46–230`
  documents four separate occasions where a nominally uniform cap turned out to
  bind one arm harder than another — the 4,000→16,000→64,000 output cap, the
  dollar-derived token budget, `GROWTH=4`, the 1,000→50 row cap — and each was
  fixed in the direction that *removes* an advantage the treatment would
  otherwise have had. `dce/arms.py:55–73` identifies the pydantic-ai
  `ModelRetry` budget asymmetry (which would have penalised arm C) and requires
  callers to neutralise it; `dce/agent.py:1046` sets `retries=max_tool_calls`.
  This is better instrumentation hygiene than most EA&B submissions.
- **The forced-answer policy is the conservative choice.**
  `08-threats.tex:143–153`: forcing an answer rather than discarding budget-
  exhausted runs eliminated 41/18/0/0 (run A) and 55/42/15/0 (run B) unscoreable
  rows, all concentrated in the *baselines*. Dropping them would have inflated
  the baselines' accuracy; the authors chose the option that hurts their own
  hypothesis.
- **The row-limit asymmetry disclosure is real and correctly signed.**
  `03-design.tex:103–112` and `dce/arms.py:448–458` agree: 31 `preview_table`
  calls exceeded 50 rows, in the treatment's favour, and it is disclosed rather
  than buried. Same for the inert digest stamp on `\armH{}`'s rows
  (`04-protocol.tex:19–28`, `dce/agent.py:237–248`).
- **The gold reconstruction is defensible.** Admission gated on DABStep's own
  grader verdict, 0.75 plurality with median realised share 0.981, official
  scorer vendored at a pinned revision, five exclusions independently published
  by a third party. `04-protocol.tex:84–89`'s disclosure that the authors' own
  stricter fallback normaliser had manufactured a favourable result on a pilot
  ($p=0.0156$ vs $0.0703$) is the kind of thing most papers delete.
- **The within-arm clause confound is caught and reported, not exploited.**
  `06-mechanism.tex:143–151` and `analysis/clauses.py:15–21` both flag that the
  tempting pooled 90%-vs-23% number is a task-mix artifact and that it vanishes
  under the correct restriction. Reporting the confound you resisted is rare.
- **Two prior claims are retracted in the paper itself** (the scaffolding null,
  the shrinking margin), with the superseded reasoning left visible. Whatever
  else is wrong here, this is not a paper that hides its corrections.

The construct-validity worry the prompt anticipated — that "contract" just means
"more relevant tokens" — is, I think, **answered**. `\armH{}` has the scaffolding
without the tokens and `\armM{}` has ~10× the tokens without the structure, and
`\armC{}` beats both. The confound that remains is not token count. It is K1:
`\armC{}` has *different* content, not merely better-delivered content. That is a
sharper problem than the one the hollow arm was built to solve, and the hollow
arm cannot touch it.

---

## What I would need to see to raise my score

Ordered by cost to the authors, cheapest first.

1. **Fix the descriptions, not the numbers (days).** Say that 79 of the 176 are
   answered by hand-written per-family SQL rather than the two compiled views;
   say that the 59 avg-fee tasks appear in both the gold validation and the
   sufficiency check and count for one, not two; state that the derivation gap is
   $100-\text{accuracy}$ on the macro bucket; carry the 3,042-character tool-
   description asymmetry into §6; put $k=1$ in the abstract; align every effect
   size with the population its $p$-value was computed on; add Wilson intervals
   and the scored/strict pair to Table 1. This alone moves me from reject to
   weak reject, because it removes the claims I cannot defend to the PC.
2. **Partition the C-vs-M comparison by INTERPRETATION dependence (a week).**
   Label each of the seven interpretation clauses, tag every task whose gold
   answer depends on one, and re-report the delivery-vs-possession result on the
   complement. If the effect survives on tasks where `manual.md` is genuinely
   sufficient, §6 becomes the strongest section in the paper instead of the most
   vulnerable. This is the single highest-value experiment you can run without
   spending a dollar of API budget.
3. **A contamination section (a week, ~$50).** Training cutoffs against DABStep's
   and the leaderboard corpus's timelines; a closed-book memorisation probe on
   the fee-matching conventions for all three models; a re-parameterised
   perturbation set (new merchants/months/thresholds off the published
   distribution) run on `\armS{}` and `\armC{}` for the frontier model. Without
   this I do not believe any capability claim in the paper, and I would say so in
   the PC discussion.
4. **The fifth arm (a run, ~run-C cost).** Governed tools with
   `forbidden_operations` and the allow-list emptied. It converts §7 from an
   unablated two-condition comparison into the same clean design as §5, using the
   machinery you already have.
5. **Repeats on the frontier run ($k\ge5$; ~\$100).** `\armS{}` and `\armC{}` on
   run C's model, five seeds, hard split only. That is enough to put a
   within-condition band under 94.9% and 77.4% and to tell me whether 5.1 points
   is a finding or a sample. `08-threats.tex:62–63` already reports a verdict
   flipping between identical runs; the paper needs to know how often.
6. **The sixth arm that settles construct validity (a run).** "Paste the semantic
   layer as text": `contract/semantic.yml` rendered to prose into a raw-SQL arm's
   system prompt, no governed tools, no retrieval. This is the baseline a
   practitioner will ask about and the paper does not have it. If contract-as-
   retrieval still beats contract-as-prose, the delivery claim is established on
   matched content and K1 dissolves entirely. If it does not, that is a finding
   too, and a more interesting one than the current §6.

With 1–3 I would move to borderline. With 1–5 I would argue for acceptance: the
hollow-placebo design is a genuine methodological contribution, the artifact
discipline is better than the field's norm, and a paper that retracts two of its
own claims in print deserves a venue. It does not yet deserve this one.
