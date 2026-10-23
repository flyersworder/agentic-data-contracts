# Procedural-load rubric (blind raters)

Fixed before any rating was made; used as written by every rater. The
analysis that reads the ratings is `analysis/procedural_load.py`.

You rate tasks from two question-answering benchmarks over databases. Each
task is a question an analyst answers by querying a database, using domain
knowledge written in prose. You are given the question and the prose
knowledge; you are not given any answer, any query, or how anyone did on the
task. Do not look for them, and do not open any file other than the ones
your instructions name.

For each task, rate **how much procedure the domain knowledge the question
needs carries**: how much of it an analyst must turn from prose into query
logic of their own to answer this question.

- Rate the knowledge **as this question uses it**. If the question needs
  only part of a long entry or section, rate that part.
- Rate what the **knowledge** demands, not the wording of the question, and
  not query work the knowledge does not cause (a join only to reach a
  column, sorting, rounding, output format).
- Rate from the text. You do not need the data.

Two kinds of task:

- **Manual tasks** come with one shared domain manual (a separate file). First
  find which parts of the manual the question needs, then rate them. If it
  needs none, say so and rate 0.
- **Entry tasks** come with the knowledge entries the benchmark lists for the
  question, with the entries they build on. The list can be incomplete: if
  the question plainly needs knowledge no listed entry gives, note it, and
  rate what the question needs as far as the text lets you.

## Fields

- `steps` (integer): the number of derived quantities that must be computed
  in sequence from the knowledge before the answer can be read off -- each a
  value the knowledge defines in terms of columns or of other derived values.
  0 when the knowledge only names a column, a filter value or a category.
- `rule_matching` (true/false): must each record be matched against a table
  of rules or criteria on several fields at once (a rule applies when all its
  fields agree), rather than filtered by one condition or computed by one
  formula?
- `multi_grain` (true/false): must a quantity be computed at one grain and
  then aggregated or compared at another (per record then per period; per
  entity then averaged; a band chosen from an aggregate and applied back to
  records)?
- `branching` (true/false): does the knowledge contain cases, bands or
  thresholds with several branches that decide how a value is computed or
  classified?
- `direct` (true/false): could a competent analyst write the needed knowledge
  as query logic straight from the text, with no design decision of their
  own (how to represent a rule, at what grain to compute, in what order to
  combine)?
- `load` (0-3), the overall rating:
  - **0 -- none or a label.** No domain knowledge beyond the schema, or only
    a name for a column, a filter value or a category.
  - **1 -- one formula.** A single expression (a ratio, a difference, a
    weighted sum, a unit conversion) applied at one grain, written straight
    from the text.
  - **2 -- a composite.** Several dependent formulas; or one formula with
    branches, bands or thresholds; or a computation at one grain then
    aggregated at another.
  - **3 -- a procedure.** Several of the above together: for example records
    matched against a table of rules on several criteria, a quantity computed
    per record from the matched rule, then aggregated; or a chain of three or
    more dependent quantities with branching.
  Choose the lowest level whose description fits.

## Output

One JSON line per task, every task in your batch, to the file your
instructions name:

```json
{"id": "P001", "needed": ["short name of each knowledge unit used"],
 "steps": 2, "rule_matching": false, "multi_grain": true, "branching": false,
 "direct": true, "load": 2, "notes": "one sentence"}
```
