# Readability review — *Which Part of the Context Layer Does the Work?*

Cover-to-cover read of `main.tex` + `sections/00`–`10` (11,621 words of prose,
18 pages, 8 numbered floats, 6 unnumbered in-text tables). Reviewing for flow,
not correctness. All counts below were re-derived from the source, not taken
from the previous review.

---

## The read

**The arc as I experienced it.** The paper opens beautifully: an agent writes
clean SQL and returns a confidently wrong number, the industry sells you a
"context layer" to fix it, and nobody has ever asked which part of the layer is
doing the fixing. A fourth arm — the contract with all its prose surgically
emptied but its tools, rules and allow-list byte-identical — is a genuinely
elegant instrument, and by the end of §3 I wanted to see the number. Then §4
spends a page and a half on protocol hygiene, and §5 arrives and does *six*
things: confesses run B's truncation, drops the headline, runs the ablation,
reports cost, reports the capability interaction, and — sixth, on page 8, with
its table stranded two pages downstream on page 10 — delivers what is
plainly the paper's best result, the compiled contract that reproduces gold
176/176 and turns every arm's accuracy into efficiency against a proven
ceiling. §6 (mechanism) and §7 (governance) are each strong, self-contained,
and read like separate short papers bolted on. §8 restates §4 and §5. §9 is the
most intellectually generous related-work section I have read in a while and is
also where the paper's real competitor (the 100% semantic layer) finally gets
named. §10 recaps four of the paper's seven results.

**Where that diverges from the promise.** Three gaps, in order of how much they
cost.

1. **The title asks question A; the abstract's loudest claim answers question
   B.** "Which Part of the Context Layer Does the Work?" is answered in §5.3:
   the content, by 1.8×–6.6× over the scaffolding. But the abstract bolds the
   *derivation gap* twice and the intro calls it "the finding with the most
   consequence." The derivation-gap result is not a decomposition of the
   context layer at all — it is a claim about how the declarative approach ages.
   Both are good results. The paper never decides which one it is.

2. **The delivered arc is a correction narrative, and it is never
   consolidated.** Run C overturns the scaffolding null (§5.3), overturns the
   shrinking-margin finding (§5.5), forces withdrawal of the cost claim (§5.4),
   and neutralises the governance result (§7.1). Four self-corrections, plus a
   fifth in §8.5. Each is individually admirable. Cumulatively — scattered
   across five subsections in two sections, each with its own paragraph of
   apologia — the reader stops being able to tell which claims are current. By
   §5.5 I was reading each new bold sentence and mentally asking "is this one
   still true?" That is a real cost to a device that is meant to buy trust.

3. **The abstract, §1 and §10 never mention that run B lost 29% of its rows.**
   §5.1 opens "We state this before the numbers, because every run B figure is
   conditioned on it" — and then the three entry points a busy reviewer
   actually reads state the run B figures with no condition attached. The
   abstract's "22.6% to 56.6%" is on 279 tasks, not 401, and run B is
   underpowered in every comparison the paper draws from it (its own §8.3 says
   so twice). A reviewer who reaches page 6 and discovers this feels ambushed
   by a paper that is otherwise scrupulous.

One structural orphan: **§7 Governance (1,236 words, ~2 pages) does not appear
in §10 at all.** A skim-reader would not learn the paper has a governance
result.

---

## Top 10 changes, ranked by reading improvement per unit of effort

### 1. Add a "What run C changed" paragraph at the head of §5, and delete four scattered apologias
`sections/05-results.tex:36` (before `\subsection{Headline}`)

Four self-corrections currently live at `05:147–153`, `05:164–167`,
`05:234–240`, `05:279–281`, plus a fifth at `08:80–88`. Consolidate the
*framing* once; leave each result's own correction to a single sentence in
place. Insert:

> \subsection{What the third model changed}
>
> Runs~A and~B were run and analysed first, and three of this paper's claims
> were written against them. Run~C overturned two and forced the withdrawal of
> a third. We state the reversals here, once, so that no later section has to
> re-open the question: the scaffolding step is not zero (§\ref{sec:ablation});
> the contract's margin over a hand-written prompt does not shrink with
> capability (§\ref{sec:interaction}); the contract is not the cheapest arm
> (§\ref{sec:efficiency}). Each is reported below at the number that overturned
> it. Everything else in this section held across all three models.

Then cut `05:164–167` entirely (its methodological lesson is already the
abstract's closer and §10's closer — three statements of one idea), and cut
`05:279–281` to its last clause.

**Effort:** 20 minutes. **Payoff:** the largest single improvement available.
The reader stops re-litigating trust every two pages.

### 2. Fix the Figure 1 caption's number ordering
`sections/05-results.tex:104–108`

Current: *"moves hard accuracy by $+0.0$, $+5.4$ and $+14.2$ points. Restoring
the prose moves it by a further $+34.0$, $+35.8$ and $+26.2$."*

Every other triple in the paper is run A, B, C. These two are **sorted
ascending** (they are B, A, C — `make_figures.py:203` does `sorted(...)`
because the figure annotates a range, not a per-model list). A reader will map
$+5.4$ to run B and $+0.0$ to run A. Replace:

> \caption{The two steps of the ablation, on all three models. Adding nine
> governed tools, the retrieval instruction, the table allow-list and the
> operation rules --- with the prose emptied --- moves hard accuracy by $+5.4$,
> $+0.0$ and $+14.2$ points on runs~A, B and~C. Restoring the prose moves it by
> a further $+35.8$, $+34.0$ and $+26.2$. Bars are Wilson 95\% intervals; the
> figure annotates each step's range across models rather than each model
> separately.}

**Effort:** 2 minutes. **Payoff:** removes an outright misreading trap in the
figure that carries the title's answer.

### 3. Fix Table 2's row order, which flips between run blocks
`sections/05-results.tex:44–61`

Run A and run C list `contract, manual_prompt, contract_hollow, schema_only`.
Run B lists `contract, manual_prompt, schema_only, contract_hollow`. The rows
are sorted by accuracy within each block, so the reader cannot scan a column
without re-reading the labels three times. Use one fixed order everywhere —
`contract, manual_prompt, contract_hollow, schema_only` — which also puts the
ablation's two arms adjacent in every block, which is what the paper is about.

**Effort:** 2 minutes. **Payoff:** the paper's central table becomes scannable.

### 4. Move §5.7 (leaderboard validation) up, to immediately after §5.2
`sections/05-results.tex:397–409` → insert after `05:95`

"The harness measures what the leaderboard measures" is the check that
*licenses* every number in §5. It currently sits at the very end of §5, after
six subsections of findings. A reviewer wants to know the instrument is
trustworthy before they are asked to believe anything, not after. Moving it
costs nothing structurally — it references only `\armM{}` and Table 2 — and it
converts §5.2's three-sentence "Headline" stub into a real subsection.

**Effort:** cut and paste. **Payoff:** removes the "why should I believe this?"
overhang that runs under §5.3–§5.6.

### 5. Give §7 one sentence in §10
`sections/10-conclusion.tex:30` (after the corrections paragraph)

Two pages of governance results — 144 mutation attempts to zero, the CaMeL/RuLES
framing, the Gaussian-elimination escalation — vanish from the conclusion.
Insert:

> The same transcripts carry a second, narrower result. Declared rules that the
> model merely reads stopped every mutating statement it would otherwise have
> submitted --- 144 to zero, on the two models that reached for the idiom at
> all --- while the enforcement layer standing behind those rules never had to
> fire. On the third model no arm attempted a write, so the hazard disappeared
> before the mechanism could be tested on it.

**Effort:** 5 minutes. **Payoff:** a whole section stops being invisible to the
skim path.

### 6. Put run B's truncation into the abstract
`sections/00-abstract.tex:9–10`

Current: *"We report a controlled four-arm ablation on DABStep, run three times
across three model families spanning a wide capability range."*

Replace with:

> We report a controlled four-arm ablation on DABStep, run three times across
> three model families spanning a wide capability range. One run lost 29\% of
> its rows to provider rate-limiting, near-uniformly across arms; it is reported
> on the 279 tasks scoreable in all four, and it is underpowered throughout.

**Effort:** 5 minutes. **Payoff:** the paper's most conspicuous omission from
its own entry points, in a paper whose whole register is "we tell you the bad
parts."

### 7. Rewrite the digest-defect paragraph
`sections/04-protocol.tex:19–28`

The subject ("One defect") is severed from its colon by twelve words of
self-description, and a 34-word em-dash aside sits inside the sentence that
tells you whether it matters. It is the hardest paragraph in the setup half.

> The machinery has one defect. The digest is stamped unconditionally, so
> \armH{}'s rows carry the \emph{real} contract's hash instead of the hollow
> artifact's (\texttt{sha256:c46a767d\ldots}) --- pinned to a file that arm
> never loaded. No result changes. Which artifact each arm loaded is fixed by
> the harness, and the hollow arm's empty-placeholder responses and the
> $n$-gram tests of Section~\ref{sec:hollow} evidence it independently. What is
> missing is the per-row tamper-evidence we claim for \armH{}. The harness is
> fixed; the released rows are left as they ran.

Saves 18 words and one meta-tic.

### 8. Rewrite the row-limit asymmetry paragraph
`sections/03-design.tex:103–112`

A 46-word sentence with two nested relatives, then a meta-clause.

> They were also not perfectly symmetric here. The 50-row cap reached the
> governed arms' \texttt{run\_query} but not their \texttt{preview\_table},
> which the library clamps at 100 rows of its own accord. A governed arm could
> therefore see 100 rows from a preview where an ungoverned arm saw 50 from
> \texttt{execute\_sql}. That happened on \textbf{31 preview calls} across all
> three runs, against many thousands of tool calls --- small, but in the
> treatment's favour. The harness is fixed for future runs; the reported runs
> are as they ran.

Saves 24 words.

### 9. Delete the duplicated sentence in §9.3
`sections/09-related.tex:95–98`

The paper says the same thing twice in consecutive sentences:

> The generous reading --- that these are complementary layers rather than
> competitors --- is also the correct one. These are complementary layers, not
> competitors: macros optimise...

Delete the first sentence. Keep: *"These are complementary layers, not
competitors: macros optimise the head of the question distribution, a contract
covers the tail, and in a system with both, the contract is what tells the
agent which macro exists and when to call it."* — which is one of the best
sentences in the paper and is currently blunted by the sentence in front of it.

### 10. Rewrite the closing paragraph so the paper ends on a claim
`sections/10-conclusion.tex:37–45`

Three verbless fragments in a row; the last runs 55 words and ends on a feature
that does not exist yet. The last thing a reviewer reads should be what the
paper found.

> \paragraph{Future work.} Three things, in order of what they would settle.
> Repeat runs would put a variance estimate under every number here; that is
> the largest gap, and largest for the frontier run, whose temperature could
> not be pinned. A second frontier model would test every capability claim in
> this paper, all of which now rest on one --- the exact condition under which
> a claim here has already broken once. And an executable metric layer, built
> \emph{after} this measurement rather than before it, would separate the
> declarative contribution from the executable one the way this paper separates
> content from scaffolding.
>
> What we would defend today is narrower than what two models supported: the
> content in a context layer does most of the work, the scaffolding does some,
> and the cost of making an agent derive rather than look up is falling fast
> enough that it should not decide the design.

---

## Section-by-section notes

**§1 Introduction** (929 w). Opens well, closes well, and does the hardest
job in the paper — motivating a decomposition — in four paragraphs. The
"Contribution" subsection is a clean four-findings-and-a-non-finding structure.

*Problems.* `01:72–73` is grammatically broken: *"the claim two models
supported, and which an earlier version of this paper made"* coordinates a
zero-relative with a `which`-relative. And `01:74–76` buries a 12-word
subject: *"a null with a pooled $p{=}0.29$ that a third model overturns at
$p{=}4{\times}10^{-7}$ is a caution..."* Replace both:

> ...which is what the fourth arm was built to test. But the stronger claim ---
> that the scaffolding contributes nothing --- is false. Two models supported
> it, and an earlier version of this paper made it. The third overturns it at
> $p{=}4{\times}10^{-7}$, against a pooled null of $p{=}0.29$. That is a
> caution about how far two models generalise.

Also `01:84–86`: *"the contract arm writes every one of them 39\%, 65\% and
98\% of the time"* → *"the contract arm writes all six on 39\%, 65\% and 98\%
of tasks, against the prompt arm's 3\%, 9\% and 4\%."*

**§2 Background** (797 w). Justifies the benchmark choice properly, and the
"a third is a limitation" framing at `02:11` is a good move — it puts the
threat in front of the reader before they can raise it.

*Problems.* `02:53–67` is the densest paragraph in the paper: 161 words, one
paragraph, three definitions (semantic layers / context layers / data
contracts), six citations, and two lineage asides (datasheets, Croissant,
TAG). It has no topic sentence and makes four points. Break it into three:

> Three families of approach exist and are routinely conflated.
>
> \emph{Semantic layers} in the classical sense --- dbt metrics, Cube, LookML
> --- define metrics as executable artifacts, so the agent calls a metric
> rather than deriving it and correctness is inherited from the definition.
> \emph{Context layers} store natural-language documentation next to the data
> and retrieve it at query time; MotherDuck Guides~\cite{motherduck-guides} is
> the best-documented instance. The agent still writes the SQL, but writes it
> knowing what the columns mean. \emph{Data contracts} originate in governance
> rather than assistance: a declarative artifact stating what a dataset
> contains, who owns it and what may be done with it, with the Open Data
> Contract Standard~\cite{odcs} the emerging specification.
>
> The third family inherits a longer line of dataset documentation ---
> datasheets~\cite{datasheets} and, closer to our use, Croissant's shift from
> prose a human reads to metadata a tool loads~\cite{croissant}. The database
> community's framing of the wider problem is TAG~\cite{tag}: natural-language
> questions over a warehouse need more than translation to SQL.

§2.3 "The system under test" ends on the paper's best short paragraph
(`02:95–97`). Keep untouched.

**§3 Experimental Design** (926 w). The strongest section in the paper. §3.2
"The hollow control" is where a reviewer decides whether to keep reading, and
it earns that. The `min-demonstrations` citation at `03:57` is well placed —
it tells the reader this is a known move, not an invention, which is exactly
the right register.

*One flow note.* §3.3 "What is held fixed" ends on two paragraphs of defect
disclosure (row limit history, then the preview asymmetry). The section
therefore closes on the experiment's flaws rather than on its design. Consider
moving `03:98–112` to §8, where the other defects live, or at minimum add a
one-line close after `03:112` returning to the design.

**§4 Protocol** (824 w). Necessary, and the "three safeguards, of decreasing
strength" framing at `04:7` is a nice piece of honest scaffolding. §4.2 (gold
reconstruction) is well argued and the "consensus is an argument, not a
measurement" line at `04:67` is genuinely good.

*Cut candidate:* §4.5 "Artifacts" (`04:109–115`) is a verbatim duplicate of the
"Artifact Availability" section at `10:47–53`. Delete §4.5, keep §10's.
**Saves ~60 words and a subsection heading.**

**§5 Results** (2,690 words — 23% of the paper). This is four results sections
wearing one trenchcoat. §5.1 truncation, §5.2 headline, §5.3 ablation, §5.4
efficiency, §5.5 interaction, §5.6 derivation, §5.7 harness validation. The
reader has no map: nothing at `05:1` tells them seven things are coming or in
what order.

*Add a one-sentence roadmap at `05:1`:*

> This section reports the ablation (\S\ref{sec:ablation}), the cost of each
> arm (\S\ref{sec:efficiency}), how the effect moves with model capability
> (\S\ref{sec:interaction}), and a measurement of how much of the contract's
> own content each arm recovers (\S\ref{sec:derivation}). It opens with a
> failure in one run that conditions every number drawn from it.

*§5.2 "Headline"* is three sentences and two full-width tables. It is not a
subsection; it is a caption. Merge it into §5.3, or absorb the relocated §5.7
into it (change 4).

*§5.3* presents the same decomposition **three times on one page**: Figure 1
(with Wilson intervals), the mini-table at `05:115–127` (same three numbers ×
three models), and the delta table at `05:133–145` (whose $p$-values duplicate
the last row of Table 3). **Cut the mini-table at `05:115–127`** — the figure
already carries it and the figure is better. **Saves ~13 lines of float.**

*§5.4 Efficiency* opens on a methodological justification
(`05:225–227`, "Reporting accuracy without cost yields needlessly expensive
agents...") rather than a finding. Swap the order: lead with `05:229` ("\armC{}
is not thinking harder; it has less to search for"), and demote the citation
to the end of that paragraph.

*§5.6 Derivation* has the paper's best opening paragraph after §1's
(`05:305–308` — "Every result so far compares arms to each other. None of them
says how much of the contract's own content an agent recovers"). It is also
buried sixth and its table lands on page 10. **Recommend promoting §5.6 to
directly after §5.3.** The ablation answers the title; the derivation gap is
the paper's most consequential result; the cost and interaction subsections are
supporting material and should follow, not precede.

*§5.6's classification paragraph* (`05:329–339`) is a 55-word sentence with an
em-dash aside and an embedded three-item list, followed by two further points
in the same paragraph. Split:

> Second, it converts accuracy into \emph{efficiency against a proven ceiling}.
> We classified all 401 tasks by question shape alone --- never by gold, never
> by any arm's output. 176 tasks the compiled contract answers outright, all of
> them hard; 148 need those semantics \emph{plus} a counterfactual or an
> optimisation it does not encode; 77 have no fee semantics at all. A
> pre-computed macro layer \emph{of this kind} therefore reaches 53\% of the
> hard set on this benchmark --- not a bound on pre-computation in general
> (Section~\ref{sec:related} reports a hierarchical semantic layer reaching
> 100\% here), only on a layer compiled from a domain contract's declared
> expressions.
>
> On the 176 where the information is provably sufficient, whatever an agent
> fails to get is a failure to derive (Table~\ref{tab:buckets}).

Also demote `05:152–153` (*"On run~A the hard slice alone is already
$p{=}0.015$ (16/34); the 0.058 above is the all-401 test, diluted by the easy
tasks"*) to a footnote. It is a defensive clarification sitting in the middle
of the paper's main claim.

**§6 Mechanism** (940 w). Opens with a proper why-you're-here sentence
(`06:4–5`) and the task-1278 walkthrough is the most persuasive two paragraphs
in the paper — two arms, the same rule, one number, and the SQL that produces
each. Keep exactly as is.

*Problem.* `06:47–50` drops two citations (`lost-in-the-middle`,
`tool-documentation`) between the example and its interpretation, ending on
*"Both bear on what follows"* — a promissory note that interrupts the story at
its climax. Move it to after `06:61`:

> Two published results frame this. Retrieval quality falls for material buried
> mid-prompt~\cite{lost-in-the-middle}, and what a tool's documentation says
> changes what an agent does with it~\cite{tool-documentation}. \armM{}'s copy
> of the rule was buried mid-prompt; \armC{}'s was a tool's documentation.

*Cut candidate:* `06:143–151`, the task-mix confound paragraph (89 words). It
describes at length a number the paper is *not* reporting, and the preceding
paragraph already told us the comparison was restricted to the 97 tasks
requiring all six clauses. Compress to one sentence:

> The uncontrolled version of this number is tempting and wrong: pooled across
> families, \armC{} attempts writing the \texttt{NULL}-wildcard clause were
> correct 90\% of the time against 23\% for those that did not, but that
> 67-point effect is task mix, and restricting to a single required-clause set
> removes it entirely.

**Saves ~55 words.**

**§7 Governance** (1,236 w). Reads as a strong standalone short paper. §7.1's
"Run C produces no events at all, and that is a limitation rather than a
confirmation" is exactly the right move at exactly the right moment. The
Gaussian-elimination escalation in §7.3 is the most memorable thing in the
paper — a competent agent routing around a session boundary, narrating itself
into corrupting a database.

*Problem.* §7.3 tells that story and then **de-escalates it three times in a
row**: `07:128–131` (nothing was harmed), `07:133–145` (the harness made it
worse), `07:147–152` (one event is an anecdote). 222 words of qualification for
a 200-word anecdote. Keep `07:133–145` (it is a real and non-obvious mechanism
finding) and fold the other two into one closing sentence:

> The run used a disposable per-worker copy, so nothing real was harmed, and
> one corruption event is an anecdote rather than a rate --- the defensible
> number remains the 144 attempts, and the defensible claim is narrow: on
> models that reach for the mutating idiom, declared rules stopped them from
> trying.

**Saves ~85 words.**

*Also:* `07:49–50` — *"Run~C uses common table expressions on 59\% and 55\% of
its ungoverned traces, against 32\%/30\% on run~A and 26\%/27\% on run~B"* —
six unlabelled percentages. The reader cannot tell which arm each belongs to.
Add *"(\armS{} and \armM{} respectively)"* after the first pair.

**§8 Threats** (1,215 w). §8.1 is genuinely excellent and correctly placed
first — leading a threats section with the sharpest limit rather than the
easiest is the right instinct and reviewers notice.

*Problem.* §8.6, §8.7's first paragraph, and §8.9 are pure cross-references to
§4.2, §5.1 and §4.1 respectively — 111 words that restate what the reader
already read. Collapse to one paragraph:

> \subsection{Limitations stated where they arise}
>
> Three are argued in full elsewhere and listed here only so the threats
> section is complete. Golds are reconstructed for 401 of 450 tasks by
> plurality over officially-graded-correct submissions, with one template
> family independently reproduced from the database
> (Section~\ref{sec:golds}); the reader cannot check this without the external
> validation we plan there. Run~B lost 29\% of its rows to provider
> rate-limiting, near-uniformly across arms
> (Section~\ref{sec:truncation}); it widens run~B's intervals by roughly 20\%.
> And nobody outside the project can verify that no benchmark question was
> consulted while the contract was authored (Section~\ref{sec:protocol}).

Keep §8.7's *second* paragraph (`08:111–118`, the pinned-endpoint lesson) — it
generalises beyond this paper and is one of the few places the paper teaches
something to a reader who does not care about contracts. Promote it to its own
subsection.

*Misfiled:* §8.10 "Harness failures are reported, not hidden" (`08:143–154`) is
a **result** — forced-answer counts by arm, and the finding that \armC{} is the
only arm that never exhausted its budget on any run — dressed as a threat. That
finding is repeated at `05:231–232`. Delete §8.10 and keep the §5.4 statement.
**Saves ~130 words and ends §8 on a limitation rather than a defence.**

**§9 Related Work** (1,238 w). Unusually good. §9.2's when-is-the-metric-
compiled table is the single clearest thing in the paper, and §9.3's admission
that "our hard-split figures are a measurement of a harder task, not a worse
attempt at the same one" is the correct and generous framing. §9.5's "two rows
we would once have claimed here belong to others" is the sort of thing that
buys a reviewer's trust cheaply.

*Problem.* §9 starts at page 14 of 18. A reviewer who is out of time has
already stopped. §9.1–§9.3 contain the paper's honest positioning against its
strongest competitor (the 100% semantic layer), and that positioning is
load-bearing for how the reader reads §5's 77.4%. **Consider moving §9.1–§9.3
to just after §2** — the comparison is background, not aftermath, and §5.6
already forward-references it twice (`05:335`, `06:89`).

*Also:* `09:77–78` restates the 113-shapes/27-cover-three-quarters/294-of-332
statistics for the **third** time (after `02:34–36` and `08:55–56`). Cut to a
cross-reference: *"DABStep's question space is narrow (Section~\ref{sec:background}), and pre-built views are maximally effective when..."*

**§10 Conclusion** (474 w). Recaps §5.3, §6, §5.6 and the corrections. Omits §7
entirely (change 5) and §9's positioning. The corrections paragraph
(`10:25–30`) is the strongest in the section — *"A null across two adjacent
models is weak evidence of absence, which is the methodological result of this
paper"* — and it is then buried under two more paragraphs. Move the "Three
limits" paragraph *before* it, so the section runs: results → limits →
corrections → future work → closing claim.

---

## Tic counts

All counts re-derived from `sections/*.tex`.

| Device | Count | Earned | Target | Cut these first |
|---|---|---|---|---|
| Em-dash `---` | **85** | ~40 | **45** | Densest in `05` (19), `07` (11), `09` (10). Cut where the aside is a full clause the sentence could just make: `04:24–27` (34-word aside inside the result-impact sentence), `05:330–331`, `06:47–49`, `07:51–53`, `09:96–97`, `10:38–39`. Rule of thumb: keep em-dashes that carry a *number* or a *quote*; convert the ones carrying a *clause* to a full stop. |
| `rather than` | **29** | ~12 | **14** | Heaviest in `03` (5) and `09` (5). Twelve are semantically load-bearing (the paper's whole argument is A-instead-of-B). The reflex ones: `03:58`, `03:110`, `04:20`, `04:77`, `05:281`, `08:34`, `09:164`, `10:42`. Most become `instead of` or vanish: *"reported rather than quietly repaired"* → delete; *"we state it rather than leave it to be found"* → delete; *"built \emph{after} this measurement rather than before it"* → keep (load-bearing). |
| `, not ` tail | **30** | ~14 | **15** | The construction is the paper's signature and half of them land. The ones that don't, because they follow another within two sentences: `03:85` + `03:86` (back to back), `03:101`, `05:17` + `05:240` + `05:322`, `06:107` + `06:141`, `08:66` + `08:108` + `08:118` (three in one subsection), `09:152` + `09:155` (back to back). Fix by never letting two appear in adjacent sentences. |
| `\textbf{}` prose lead-in | **36** (of 131 total `\textbf`) | ~20 | **22** | `05` has 8 lead-ins, `06` has 5, `07` has 4. The device is doing real work in §1 (four findings) and §2 (three properties) — keep those. Demote to plain topic sentences: `05:129`, `05:320`, `05:368`, `05:391`, `06:129`, `06:136`, `07:38`, `08:37`, `09:49`. Also: `08:34` and `09:82` bold *whole sentences* rather than a lead-in phrase, which reads as shouting. |
| Meta-honesty | **15** | ~5 | **5** | `04:20` ("reported rather than quietly repaired"), `03:110` ("state it rather than leave it to be found"), `05:281` ("we say so here rather than quietly dropping it"), `06:143` ("We record one confound because the uncontrolled version is tempting to report"), `08:34` ("The honest statement of the result is"), `08:143` ("reported, not hidden"), `09:91` ("We are careful not to convert that into"). Delete all seven — the disclosure itself is the evidence of honesty; narrating it undercuts it. Keep `04:30–33` ("we cannot prove intent"), `05:147` ("We report this as a correction"), `09:94`, and the abstract's closer. |
| `\emph{}` | 87 | ~55 | **60** | Not flagged previously but worth a pass: `05` and `09` italicise for emphasis where the sentence structure already supplies it. |

**The monotony problem, concretely.** On page 6 alone the reader meets: two
em-dash asides, two `, not` tails, three bold lead-ins and one "rather than."
The devices stop marking emphasis and start reading as a verbal tic. The fix is
not to hit a global number but to enforce *one emphasis device per paragraph* —
if a paragraph has a bold lead-in, it does not also get an em-dash aside and an
A-not-B tail.

---

## Skim test

I reconstructed what a reviewer gets from abstract + §1 + the three figure
captions + §10, before weighing it against the full read.

**What the skim-reader concludes:**

- Four-arm ablation on DABStep, three models, a hollow control that keeps the
  scaffolding and removes the prose.
- The contract raises hard accuracy 13.9→55.1, 22.6→56.6, 37.0→77.4, beating a
  hand-written prompt on all three.
- Content dominates scaffolding everywhere; scaffolding is real on one model
  (+14.2), null on two.
- The contract compiles to views that reproduce gold 176/176. The contract arm
  recovers 60.8%, 62.7%, 94.9% of that ceiling — the derivation gap collapses
  with capability, so declarative-vs-macro is a transient cost.
- Governance: 144 mutating statements in ungoverned arms, zero in governed.
- Non-finding: no advantage outside the fee domain.
- Methodological lesson: a null across two adjacent models is weak evidence of
  absence.

**Where the skim misleads:**

1. **Run B's 29% row loss is invisible.** The skimmer reads "22.6% to 56.6%"
   as one of three equal-weight results. It is on 279 tasks, and §8.3 says run
   B "is underpowered here as elsewhere." *Fix: change 6.*

2. **"176 of 176 covered tasks" hides the word doing all the work.** The
   compiled contract answers **53% of the hard split** and *nothing* outside
   it — a number that appears only at `05:333`, and nowhere in the abstract,
   intro, or conclusion. A skimmer reasonably concludes the contract compiles
   to a layer that solves the benchmark. *Fix: add "— 53\% of the hard split —"
   to the abstract's sentence at `00:25–27`.*

3. **Run A's governed arms ran with a validator defect** that falsely rejected
   161 queries across 124 of 401 tasks (§8.8). It runs *in the paper's
   disfavour* (55.1% is a floor), so it is not a credibility problem — but a
   reviewer meeting it on page 15 after a confident abstract will wonder what
   else is down there. One clause in §1 would defuse it entirely.

4. **The governance result is much narrower than the skim suggests.** §1 does
   flag that run C contributes nothing and that the number measures deterrence
   rather than blocking. But the skim path drops (a) that the enforcement layer
   *never fired at all* in 4,812 rows, and (b) that §7.3 attributes part of the
   hazard to the harness's per-call connection lifecycle. §10 mentions
   governance not at all, so the skimmer's last impression is whatever §1 left.

5. **The skimmer cannot tell which claims are current.** §1 announces four
   findings; §5 withdraws one of them (cost) and §5.5 refutes a fifth that §1
   does not mention. *Fix: change 1.*

6. **Three of the six in-text tables are invisible to a skim** because they
   have no caption, number, or label — including the clause table at
   `06:110–123`, which carries the 98%-vs-4% number quoted in both §1 and §10.
   Promote it to a numbered `table` with a self-contained caption:

   > \caption{Fraction of the 97 tasks requiring all six of the contract's
   > load-bearing fee clauses on which the agent's submitted SQL expresses all
   > six (69 tasks on run~B). Detectors are permissive: they test whether the
   > agent expressed the idea, not whether it copied the contract's phrasing.
   > \armM{} holds the same knowledge as prose in its system prompt.}

**Verdict:** the skim gets the paper's shape right and its confidence level
wrong. Every correction above is a two-sentence fix.

---

## Tables and figures

**Float placement.** Verified against `main.aux`. Five floats appear two pages
after their first reference, all in §5:

| Float | Appears | First referenced | Drift |
|---|---|---|---|
| Table 4 `tab:efficiency` | p8 | `05:113` (§5.3, p6) | 2 pages |
| Figure 2 `fig:cost` | p9 | Table 2's caption (p7) | 2 pages |
| Figure 3 `fig:interaction` | p9 | `05:256` (§5.5, p7) | 2 pages |
| Table 6 `tab:buckets` | **p10** | `05:339` (§5.6, p8) | 2 pages — lands at the top of §6 |
| Table 8 `tab:feebucket` | p14 | `08:11` (§8.1, p13) | 1 page — lands on the Related Work page |

Cause: seven full-width `table*`/`figure*` floats compete for top-of-page slots
in a four-page stretch. Cutting the §5.3 mini-table and promoting §5.6 (above)
relieves most of it. Table 6 in particular reads as if it belongs to §6.

**Self-contained captions.** Table 2, Table 3, Table 6 and Figure 1 pass —
each states what the numbers are and what the reader should notice. Table 4's
caption (*"The governed arm wins while doing less work on every model. It is not
the cheapest arm on run~C."*) does not define "Turns / task" or explain why run
B reports "Reasoning / task" while runs A and C report "Reasoning tokens" — the
row label changes between blocks and the units change with it. Fix by using one
unit throughout.

**Uncaptioned in-text tables (6).** `05:115–127`, `05:133–145`, `06:12–25`,
`06:29–38`, `06:110–123`, `09:26–38`, `09:131–144`. Three are fine as inline
displays (§6.1's two little answer tables genuinely read better unnumbered,
inline with the story). Three should be promoted: the clause table
(`06:110–123`, carries a headline claim), the compiled-vs-declarative table
(`09:26–38`, the clearest object in the paper), and the distinctiveness table
(`09:131–144`, which a reviewer will want to cite).

**Numbers in prose that want a table.** §7.1's CTE/`CREATE TEMP` idiom
statistics (`07:49–53`) are eight numbers in two sentences across three runs
and two arms. That is a 3×2 table.

**A table that should be a sentence.** The §5.3 mini-table at `05:115–127` —
already carried by Figure 1. Cut it (change in §5 notes).

---

## Cut list

Ordered by words saved per unit of risk. Total ≈ **740 words**, roughly 1.2
columns — enough to absorb every addition above and still come in shorter.

| # | Location | Passage | Words | Why |
|---|---|---|---|---|
| 1 | `05:164–167` | *"The correction carries a methodological lesson worth more than the claim it replaced..."* | 55 | Third statement of the same point on one page (`05:147–153`, `05:155–162`). The abstract and §10 both close on it. |
| 2 | `08:143–154` | §8.10 "Harness failures are reported, not hidden" | 130 | A result, not a threat; duplicates `05:231–232`. |
| 3 | `07:128–131` + `07:147–152` | The second and third de-escalations of the escalation anecdote | 85 | Replace with the single closing sentence proposed in §7 notes. |
| 4 | `06:143–151` | The task-mix confound paragraph | 55 | Compress to one sentence; the preceding paragraph already establishes the control. |
| 5 | `08:96–102` + `08:104–109` + `08:138–141` | §8.6, §8.7¶1, §8.9 | 111 → 75 | Collapse three cross-reference subsections into one paragraph. |
| 6 | `04:109–115` | §4.5 "Artifacts" | 60 | Verbatim duplicate of `10:47–53`. |
| 7 | `05:115–127` | The §5.3 mini-table | ~13 lines of float | Figure 1 carries the same three numbers with intervals. |
| 8 | `09:77–78` | Third restatement of 113-shapes / 294-of-332 | 30 | Already at `02:34–36` and `08:55–56`. Cross-reference instead. |
| 9 | `09:95–97` | *"The generous reading --- that these are complementary layers rather than competitors --- is also the correct one."* | 22 | Immediately restated in the next sentence. |
| 10 | `05:283–300` | The two candidate readings of the 55.1/56.6 coincidence | 133 → ~60 | The reader is walked through two hypotheses and told both are wrong. Compress: *"Two readings of that coincidence were available — that structured context substitutes for capability, so the margin keeps shrinking; or that \armC{} was pinned near a benchmark ceiling at ${\approx}56\%$. Run~C reached 77.4\%, so there is no ceiling at 56\%; and the $+27.1$ refutes the shrinking margin. Both simple stories are wrong, and three points cannot support a third."* |
| 11 | `03:98–101` + `03:103–112` | Row-limit history and asymmetry | 24 (rewrite) | Rewrite per change 8; consider relocating both to §8. |
| 12 | `04:19–28` | Digest-defect paragraph | 18 (rewrite) | Rewrite per change 7. |

**Where the paper explains something twice in different words:**

- The three-part economics (content once / scaffolding once / macros per
  metric): `01:34–38`, `05:383–389`, `10:3–6`. Three full statements. Keep §1's
  and §10's — the §5.6 one can shrink to *"which is the economics of
  Section~\ref{sec:intro}: a gap fixed at 38 points would be a standing cost of
  the declarative approach; a gap that falls to 5 is a transient one."*
- The hollow arm's construction: `00:10–13`, `01:55–63`, `03:47–69`, `10:8–9`.
  Four times. §1's and §3's are both needed; the abstract's and §10's are
  correctly compressed. No action.
- The 24,177-vs-2,475 character contrast: `01:80–82`, `03:36`, `03:41`,
  `06:53–55`. Four times, and it lands hardest at `06:53–55` where it explains
  a specific failure. Drop it from `01:80–82`, which is a summary.
- Run C's non-monotone margin: `05:275–281`, `fig:interaction` caption,
  `08:82–88`. §8.5's is the one that generalises (it warns about the derivation
  gap); §5.5's can lose its final sentence.
- The scaffolding-null correction: abstract, `01:72–76`, `05:147–153`,
  `05:164–167`, `08:86`, `10:25–30`. Six times. Change 1 addresses this.

---

## Opening and closing

**Opening** — `sections/01-intro.tex:4–9`:

> An analytics agent pointed at a warehouse it does not understand fails in a
> characteristic way. It reads the schema, writes plausible SQL, and returns a
> number that is wrong for a reason no column name records: a code whose values
> mean something specific to the business, a join whose cardinality depends on
> a convention, a `NULL` that means *applies to everything* rather than
> *unknown*. The query runs. The answer is confidently wrong.

**Assessment: this is the best paragraph in the paper and I would not touch a
word of it.** It states the failure mode in one sentence, gives three concrete
instances rising in specificity — the third of which is *the exact rule
DABStep turns on*, so the paragraph quietly plants the example §6 will pay off
— and then lands on two short sentences that do more rhetorical work than the
preceding forty. "The query runs. The answer is confidently wrong." is the
sentence a reviewer will still remember on the plane home. It also contains the
paper's first `rather than` and first em-dash-free contrast, which is worth
noting: the opening is clean precisely because it is not yet carrying the tics
that accumulate from §3 onward. **No change.**

**Closing** — `sections/10-conclusion.tex:37–45`:

> **Future work.** Repeat runs, to put a variance estimate under every number
> here — the largest gap, and largest for the frontier run, whose temperature
> could not be pinned. A second frontier model, since every capability claim
> now rests on one and a trend has already broken once under exactly that
> condition. And an executable metric layer, built *after* this measurement
> rather than before it, so that the declarative and executable contributions
> can be told apart the way content and scaffolding are told apart here — a
> direction the derivation-gap result makes more interesting, since it bounds
> how much such a layer could still buy.

**Assessment: this underperforms badly, and it is the second-most-read
paragraph in the paper.** Three problems. (i) Three verbless fragments in a row
— "Repeat runs, to…", "A second frontier model, since…", "And an executable
metric layer, built…" — which reads as a bulleted list that lost its bullets.
(ii) The final sentence runs 55 words and ends on a subordinate clause about a
feature that does not exist. (iii) The paper's last words are therefore about
its own to-do list, which is the weakest possible note for a paper whose actual
contribution is a decomposition nobody else has run. The strong material —
`10:25–30`, *"A null across two adjacent models is weak evidence of absence,
which is the methodological result of this paper and the one we would most want
a reader to carry away"* — sits two paragraphs earlier and is buried by what
follows it.

**Proposed replacement:** see change 10. Restructure the section as results →
limits → corrections → future work → one-sentence closing claim, and end on:

> What we would defend today is narrower than what two models supported: the
> content in a context layer does most of the work, the scaffolding does some,
> and the cost of making an agent derive rather than look up is falling fast
> enough that it should not decide the design.

That sentence does three things the current ending does not: it states the
paper's claim, it states it at the confidence the evidence actually supports,
and it tells a practitioner what to do on Monday.
