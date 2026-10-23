# Experiment 4b: recovery rubric

Fixed before the first author draw (design: "Authoring"). Applied to every
draw: three with data per database (`contracts-procedures4b/draw-{1,2,3}/`)
and one without (`contracts-procedures4b/no-data/`). Draw 1 feeds the agent
arms; that was fixed before any draw ran.

## Who checks, and what they see

A checker who sees no question, gold, task file or result: the draw's
`contract.yml`, `semantic.yml` and `kb_coverage` (in its log), `gaps.json`,
the 4b database (`PROC4B_DATA/duckdb/<db>.duckdb`), experiment 4's database
and its `reference.sql` (the meaning every gap must preserve), and the
documentation. The checker runs the contract's SQL on the 4b database and
the reference's named queries on experiment 4's, and joins them on the
record key (return, case or snapshot; rule id for rule-level outputs).
Numbers agree within 1e-6 (relative or absolute); text and ids exactly.

## Comparisons per gap

**G1 (wildcard sentinel; every database).** From the contract, wherever it
defines them (its `kb_coverage` names where):

1. the (record, rule) pairs that match;
2. each rule's specificity;
3. the applying or governing rule per record (`r_record_amounts`);
4. the amount per record (RHC, CRH or S).

**G2 (silent encoding; reverse_logistics, solar_panel).** The per-record
quantity that reads the encoded field: reverse_logistics' IRV and RVL
(`c_record_values`), solar's potential yield and loss (`c_record_values`).

**G3 (reverse_logistics).** The amount per return (RHC), against three
versions of the reference computed on experiment 4's database: most
specific wins (the reference), every matching rule summed, the lowest
matching `rule_id`. Where G1 disagreements make the per-record comparison
unreadable, the convention is read from the contract's SQL and text, and
that is said.

## Categories, per gap and draw

Two readings are recorded separately: the **text** (does the contract say
what the stored form means, with a quote) and the **SQL** (what it
computes).

- **recovered**: the SQL agrees with the reference on every record the gap
  touches.
- **partial**: the SQL agrees on some records or criteria and not others,
  for example the sentinel read as no restriction except for records whose
  own value is missing, or the encoded field decoded in one metric and not
  another that reads it. Which part is right is recorded.
- **stated, not built**: the text gives the reference reading and the SQL
  does not.
- **missed**: the literal reading (the sentinel compared as a value; the
  field read as documented) or another wrong one, in text and SQL.

For G3 the category is replaced by the convention chosen: most_specific,
sum, lowest_id or other (described).

## Correctable errors

A disagreement whose cause lies outside the gaps is an author error, not a
measurement: a wrong JSON path or join for another field, a wrong band
bound, a wrong month, a formula part other than the encoded field. The
checker names the cause; in draw 1 only, a reviewer who sees no question
makes the smallest edit that removes it and records it in
`contracts-procedures4b/draw-1/CORRECTIONS.md`, and the fix reaches both
`contract_gap` and the notes. Anything a gap decides (how the sentinel is
read, whether a missing value matches it, how the encoded field is
decoded, how matching rules combine), including a slip in the SQL that
implements it, is never corrected.

## Recorded per draw

`contracts-procedures4b/RECOVERY.md` and `recovery.json`: per database,
draw and gap, the category (or convention), the text reading with its
quote, the comparison counts, and any correctable error with its cause.
(32), (32g), (32c) and (33) are computed from `recovery.json` alone.
