# Cross-family blind ratings: plan

Written and committed on 2026-10-23, before any rating by these raters existed. The commit that adds this file precedes every rating file it describes.

## Why

The paper uses three subsets, and each rests on blind ratings:
- the 199 DABStep rule-matching tasks, from the procedural-load ratings;
- the held-out set, which is the rule-matching tasks whose transcripts were never read;
- the LiveSQLBench determined/undetermined split, from the determinacy ratings.

Every one of those ratings came from Opus 5.5 sessions. Two instances of one model agreeing can share its blind spots. This plan adds one rater from each of two other model families, with the same inputs, and fixes in advance what is computed from their labels.

These ratings check robustness. **The paper's subsets stay as registered**: the Opus consensus, as published in `ratings/`.

## Raters

- **Models:** `gpt-6.1-sol` (OpenAI) and `gemini-4-argon` (Google).
- **Harness:** GitHub Copilot CLI 1.0.103, `--reasoning-effort high`, `--autopilot`.
  - Copilot exposes no temperature setting.
  - Each session's model and CLI version are recorded with its output.
- **One rater per family.** Each rates every item once.

## Inputs

The chunks and packets are the ones the Opus raters had. There are 16 sessions per model:
- **Procedural load:** `procedural-load/packets/c01.md` to `c10.md`, 703 items (401 DABStep, 302 LiveSQLBench), with `RUBRIC.md` and `manual.md`.
- **Determinacy, 302 tasks:** the dossiers in `failure-audit/dossiers-blind302/`, split into the five chunks of the original `blind302-r1-c*` files.
- **Determinacy, 55 tasks:** the experiment 2 tasks in `failure-audit/dossiers-blind/`, as one chunk.

The rubrics' text is unchanged except for three things:
- file locations;
- the read-only data command, which is now `./q <db> "<sql>"`;
- the output file, which is `out.jsonl` in the working folder.

The session prompts are the Opus raters' prompts with the same path changes. They are in `prompts/`.

## Blindness, by construction

Each session runs in its own user and mount namespace (`sandbox.sh`). Its home directory is an empty tmpfs, and only three things are mounted into it:
- the Copilot install;
- a fresh Copilot state folder holding only the login, so no earlier session is readable;
- a work folder holding that chunk's rubric, packet or dossiers, and nothing else.

Everything else is hidden: `/tmp` is empty, `/mnt` is hidden apart from WSL's resolver, and other processes are not visible. In particular the rater cannot see:
- the repository;
- the private rating folders;
- the results;
- other raters' files.

Tools are limited:
- **Load sessions** may read and write in their folder.
- **Determinacy sessions** may also run `./q`. That client talks to a query server outside the sandbox, which opens the LiveSQLBench DuckDB files read-only with external file access disabled, the same connection the grader uses.

The determinacy dossiers hold LiveSQLBench's private gold SQL, as they did for the Opus raters. They reach the model providers through Copilot and nowhere else. Outputs stay in `~/data/dce-held-results/cross-family/`. Only labels are exported to `ratings/`, the same cut as the Opus labels.

## Validity checks and failures

A session's output is valid when:
- it has exactly one JSON line per item in the chunk;
- every id appears once;
- every field has the rubric's type and allowed values.

An invalid or incomplete output is rerun once, in a fresh session, with the same prompt. Items still missing are reported as missing and never imputed. There is no other filtering. Gemini made up details in an earlier review of this paper, so its outputs get no extra trust and no extra filtering.

## Analyses, fixed now

All analyses are computed by `analysis/cross_family.py`. Every number below is reported whatever it shows.

### A. Agreement

For each pair of raters among {Opus r1, Opus r2, GPT, Gemini}:
- **Load:** quadratic-weighted kappa on `load`, and Cohen's kappa on `rule_matching`, separately for DABStep and LiveSQLBench.
- **Determinacy:**
  - Cohen's kappa on *settled* (`kb_clarity` is clear, or `decisive` is false);
  - percent agreement on the four `kb_clarity` values;
  - for the 302 tasks and the 55 tasks separately.

### B. Alternative subsets

Each alternative uses the same rule as the paper.
- **Rule-matching** means both raters say `rule_matching`.
- **Determined** means both raters say settled; **undetermined** means both say not; otherwise **split**.

That gives three alternatives:
- **GPT alone:** both raters are the GPT rating.
- **Gemini alone:** both raters are the Gemini rating.
- **Cross-family:** the GPT and Gemini ratings together.

### C. The results recomputed on each alternative

Each result is computed exactly as in the paper, with the same arms, models, repeats and interval method.

1. **Representation step on rule-matching tasks** (experiment 5c): `contract_text_noted` against `manual_summed`, end-to-end.
   - Models: gpt-6-luna and Qwen 3.8.
   - Report: the gap, the task sign test, and the shape-cluster interval from `clustered.py`.
   - The paper's values: luna +3.7 [+0.6, +7.9]; Qwen +5.5 [−0.4, +10.4].
2. **The second fact on rule-matching tasks** (experiment 5): `manual_summed` against `manual_resolved`, and `contract_text` against `manual_summed`, as `contract_text.py` and `clustered.py` report them.
3. **Held out:** the same two comparisons on the rule-matching tasks whose transcripts were never read (`heldout_note.py`). The inspected set stays the same 34 tasks; the held-out set is the alternative rule-matching set minus those 34.
4. **LiveSQLBench determinacy:**
   - the knowledge arms' accuracy on determined against undetermined tasks, and the gain over `schema_only` by group, as `lsb_kb_clarity.py --all` and `clustered.py` report them;
   - the 55 tasks of experiment 2, as `lsb_kb_clarity.py` reports them.

### D. What counts as robust

A result is **robust** on an alternative when two things hold:
- its estimate has the same sign;
- if the paper's interval excluded zero, or its test had p < 0.05, the recomputed one does too.

Otherwise it is **not robust** on that alternative, and the paper says so where the result is stated. A result that was not significant in the paper is reported, with no robustness claim either way.

### E. What enters the paper

- The agreement table (A) and the robustness table (B to D) go in the extended version.
- One sentence in the threats section states the cross-family agreement and how many results were robust.
- The rater identities are stated as above.
- Nothing else in the paper changes on the strength of these ratings.
