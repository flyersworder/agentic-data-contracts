# labor_certification_applications: contract check against reference.sql

The contract's definitions were run on
`PROC_DATA/duckdb/labor_certification_applications.duckdb` under
`connect_readonly`. Two pieces were checked: R (Case Review Effort Schedule,
KB 61-67) and C (Wage Assurance, KB 68-72). The results were compared, row by
row, with the named queries in
`prep/procedures/labor_certification_applications/reference.sql`. Numbers had
to agree within 1e-6, relative or absolute. Text and ids had to agree exactly.
The employer is the exact `homefirm` text, and the month is the Start Month
from `beginday`, written 'YYYY-MM'.

## Where the contract holds R and C

The mapping comes from the author's `kb_coverage`.

| KB | piece | place in the contract |
|----|-------|-----------------------|
| 61 Case Review Effort Schedule | R | domain `review_effort` (text) |
| 62 EMCV | R | metric `employer_monthly_case_volume` |
| 63 Monthly Volume Band | R | metric `monthly_volume_band` |
| 64 Review Rule Matching, 65 Governing Review Rule | R | metric `governing_review_rule_id` |
| 66 CRH | R | metric `case_review_hours` |
| 67 ERL | R | metrics `employer_review_load`, `average_case_review_hours` |
| 68 AOW, APW | C | metrics `annualized_offered_wage`, `annualized_prevailing_wage` |
| 69 WHR | C | metrics `wage_headroom_ratio`, `headroom_points` |
| 70 CWAS | C | metric `case_wage_assurance_score` |
| 71 EWAS | C | metric `employer_wage_assurance_score` |
| 72 Wage Assurance Tier | C | metric `wage_assurance_tier` |

The domains `review_effort` and `wage_assurance` also carry KB 61-72 in full,
each entry tagged with its KB id.

## What was compared (before any edit)

| comparison | contract definition | reference | agree |
|---|---|---|---|
| EMCV per employer-month | `employer_monthly_case_volume` grouped by homefirm and Start Month | `r_employer_month.emcv` | 643 / 643 |
| volume band per case | `monthly_volume_band` | `r_record_amounts.volume_band` | 981 / 981 |
| governing rule per case (NULL = default) | `governing_review_rule_id` | `r_record_amounts.governing_rule_id` | 981 / 981 |
| CRH per case | `case_review_hours` | `r_record_amounts.review_hours` | 981 / 981 |
| ERL per employer-month | `employer_review_load` grouped by homefirm and Start Month | `r_employer_month.employer_review_load` | 643 / 643 |
| ERL per employer-year | `employer_review_load` grouped by homefirm and Start Month year | sum of `r_record_amounts.review_hours` | 569 / 569 |
| average CRH per employer-month | `average_case_review_hours` | ERL / cases | 643 / 643 |
| AOW, APW and WHR per case | the three metrics | `c_record_scores` | 981 / 981 each |
| Headroom Points per case | `headroom_points` | `c_record_scores.headroom_points` | 981 / 981 |
| CWAS per case | `case_wage_assurance_score` | `c_record_scores.cwas` | 981 / 981 |
| EWAS and tier per employer, all cases | `employer_wage_assurance_score`, `wage_assurance_tier` by homefirm | `c_employer_scores` | 546 / 546 each |
| EWAS and tier per employer-month | the same metrics, by homefirm and Start Month | `c_employer_month_scores` | 643 / 643 each |
| EWAS and tier per employer-year | the same metrics, by homefirm and Start Month year | weighted from `c_record_scores` | 569 / 569 each |

Every employer and Start Month present was evaluated: 13 months, from 2023-12
to 2024-12. The tier comparisons include the 13 employers (21 employer-months)
whose EWAS is exactly 30.0. The contract puts all of them in 'Adequate', as the
reference does.

## Disagreements

None.

## Edits

None. The SQL agrees with the reference on every case, employer, employer-month
and employer-year. The domain text restates KB 61-72 without changing any rule.
The data facts the author added were checked and are true:
- 912 of 981 cases match a rule.
- The rule criteria are true NULLs, never empty strings.
- Every case has exactly one `case_worksite` row.

## Where the contract is silent

- There is no stand-alone metric for rule matching (KB 64) or for specificity.
  Both sit inside the from_sql of `governing_review_rule_id`, and the domain
  text states them.
- The rule that a case's NULL wage level or h1bDep matches only a NULL
  criterion is likewise carried in the domain text. The SQL behaves that way
  because NULL never compares equal.

## Final agreement

All 18 comparisons agree on every row, before and after the check. No edits
were made. The contract loads (`DataContract.from_yaml`, `contract_digest`
sha256:3847aa5b...), and all 52 metrics run.
