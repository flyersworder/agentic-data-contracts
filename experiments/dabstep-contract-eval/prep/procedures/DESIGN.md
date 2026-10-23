# Experiment 4: a controlled test of knowledge type

The procedural-load test (FINDINGS, *Is the gain where the knowledge carries
procedure?*) found the contract's delivery gain only on knowledge that
applies a table of rules record by record, and on these benchmarks that
knowledge is one procedure: DABStep's fee matching. This experiment holds
the database, the domain, the author and the model fixed and varies only the
type of knowledge, with a procedure that is not DABStep's.

## Materials, per database

Three LiveSQLBench databases from three domains: `reverse_logistics`,
`labor_certification_applications`, `solar_panel`. Each gets two new pieces
of knowledge, written as LiveSQLBench KB entries appended to its KB (so the
manual is the whole KB, as in every LiveSQLBench arm):

- **R, a rule-matching procedure** (top level of the procedural-load
  rubric). A new table of rules, generated with a fixed seed, that a record
  must be matched against on several fields at once (a NULL criterion
  matches any value), at least one criterion a band of an aggregate of the
  record's entity over the calendar month, and a per-record amount computed
  from the matched rule(s), then aggregated. Every choice is stated: how
  criteria match, how bands are bounded, what happens when several rules or
  no rule applies, the formula, units and rounding.
- **C, a composite** (level 2 of the rubric). Over existing columns: two or
  three dependent formulas, a banded classification, and a computation at
  one grain aggregated at another. No rule table. Every choice is stated.

R and C are written to comparable length, and the prose of both is complete
and correct: nothing is left to the data or to judgement (the knowledge is
determined), so the only difference between them is the kind of knowledge.
R is a generic business rule of the database's own domain, not a payment
fee schedule.

## Questions

Ten per piece of knowledge per database (60 in all), in matched shapes, so R
and C questions ask the same kinds of thing:

1. one entity, one month (a single value)
2. one entity, one year (a single value)
3. an average per record for a category over a period
4. the top five entities in a period, with their values
5. a count of records past a threshold of the computed value
6. which rule applies to a described record (R) / which band a described
   entity falls in (C)
7. the change if one parameter changes (a rule's rate; a weight)
8. a share of a total, as a percentage
9. a monthly series for one entity
10. a comparison of two groups

Each question names the concept it needs, as LiveSQLBench questions do,
states its output columns and their order, and its rounding. Gold is the
result of reference SQL written from the knowledge and run on the database;
it is non-empty, non-null and not constant. A question's author sees the
knowledge, the database and the reference implementation; the contract's
author sees none of the questions.

## Contracts

`dce.author` writes each database's contract from the extended KB (as in
experiment 2), never seeing a question. Each contract is then checked
against the reference implementation of R and C on every record (not on
the questions); a definition that disagrees is corrected to agree, by a
reviewer who sees no questions, and the correction is recorded. The arms
must know the same things.

## Arms, models, measure

`schema_only`, `manual_prompt`, `manual_compiled` (the manual plus the
contract's definitions), `contract`, on `gpt-6-luna` and `gpt-6-sol`, k=3,
run together in each repeat. Graded as LiveSQLBench is (result sets, Soft-EX
tolerance). Predictions are fixed in the run commit.

## The writing factor (added after the smoke, before the run commit)

The smoke (12 R questions, shapes 1-4, gpt-6-luna) put every knowledge arm
at 12 of 12: R and C are written as concise, explicit KB entries, and a
procedure written that way gets through prose. Condition 2 is about
knowledge that is easy to get wrong *from prose*, which depends on how the
prose is written as much as on what it says. DABStep's manual states its
procedure the way manuals do. So the prose arm gets a second version.

- **`manual_styled`**: `manual_prompt` with R's and C's entries replaced by
  the same knowledge written as a business manual writes it -- an overview
  addressed to the reader, the rule table's fields described one by one,
  the matching rule where the narrative reaches it, precedence, defaults and
  edge cases in later notes, bands and formulas in words with a worked
  example -- modelled on DABStep's manual. Every fact of the clean version
  is present, nothing contradicts it, nothing is left open: a checker
  confirms the facts one by one, and a blind solver with the styled KB
  must still answer all 60 questions. Read from `LSB_DATA/styled/kb/`.
- The contract is unchanged (it is the verified definition, whichever
  prose sits beside it), as is `manual_compiled` (clean KB plus the
  contract's SQL).
