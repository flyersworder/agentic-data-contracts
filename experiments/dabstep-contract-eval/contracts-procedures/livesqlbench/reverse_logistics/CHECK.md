# reverse_logistics: contract check against reference.sql

The contract's definitions of R (Return Handling Charge, KB 30-34) and C
(Customer Leakage Ratio, KB 35-39) were run on
`PROC_DATA/duckdb/reverse_logistics.duckdb` under `connect_readonly`. They were
compared, row by row, with the named queries in
`prep/procedures/reverse_logistics/reference.sql`. Numbers had to agree within
1e-6 (relative or absolute), and text and ids had to agree exactly.

## Where the contract holds R and C

The mapping comes from the author's `kb_coverage`.

| KB | piece | place in the contract |
|----|-------|-----------------------|
| 30 Return Handling Charge Schedule | R | domain `return_charges` (text) |
| 31 CMRC | R | metric `customer_monthly_return_count` |
| 32 Rule Matching | R | metric `return_charge_rule_matches` |
| 33 Rule Precedence | R | metric `applying_charge_rule_id` (with `return_charge_rule_specificity`) |
| 34 RHC | R | metrics `return_handling_charge`, `total_return_handling_charge`, `average_return_handling_charge` |
| 35 Customer Return Month | C | domain `value_leakage` (text) |
| 36 IRV | C | metric `inspected_resale_value` |
| 37 RVL | C | metrics `return_value_leakage`, `total_return_value_leakage` |
| 38 CLR | C | metric `customer_leakage_ratio` |
| 39 Leakage Class | C | metric `leakage_class` |

The domains `return_charges` and `value_leakage` also carry the full text of
KB 30-39, nearly verbatim.

## What was compared (before any edit)

| comparison | contract definition | reference | agree |
|---|---|---|---|
| CMRC per return | `customer_monthly_return_count` | `r_record_amounts.cmrc` | 1500 / 1500 |
| matching (return, rule) pairs | `return_charge_rule_matches` over its from_sql | `matches` CTE of `r_record_amounts` | 4493 / 4493 |
| specificity per rule | `return_charge_rule_specificity` | reference expression on all 400 rules | 400 / 400 |
| applying rule per return (NULL = standard) | `applying_charge_rule_id` | `r_record_amounts.applying_rule_id` | 1500 / 1500 |
| RHC per return | `return_handling_charge` | `r_record_amounts.rhc` | 1500 / 1500 |
| total RHC per customer-month | `total_return_handling_charge` grouped by buyerlink and month | `r_customer_month.total_rhc` | 397 / 397 |
| average RHC per customer-month | `average_return_handling_charge` grouped the same way | `total_rhc / cmrc` | 397 / 397 |
| total RHC per customer, all months | `total_return_handling_charge` by buyerlink | sum of `r_record_amounts.rhc` | 206 / 206 |
| IRV per return | `inspected_resale_value` | `c_record_values.irv` | 1500 / 1500 |
| RVL per return | `return_value_leakage` | `c_record_values.rvl` | 1500 / 1500 |
| total RVL per customer-month | `total_return_value_leakage` | `c_customer_month.total_rvl` | 397 / 397 |
| CLR per customer-month | `customer_leakage_ratio` | `c_customer_month.clr` | 397 / 397 |
| Leakage Class per customer-month | `leakage_class` | `c_customer_month.leakage_class` | 397 / 397 |
| returns and CLR in `leakage_class`'s from_sql | `cm.n_returns`, `cm.clr` | `n_returns`, `clr` | 397 / 397 each |
| CLR per customer, all months | `customer_leakage_ratio` by buyerlink | sum RVL / sum V | 206 / 206 |

There are only two months, 2025-01 and 2025-02. The customer-month rows
therefore cover every month, and the per-customer rows cover the whole period.

## Disagreements

None.

## Edits

None. The contract's SQL agrees with the reference on every record and every
customer-month. Its text for KB 30-39 restates the KB without changing any
rule. The data facts the author added were checked and are true:
- 93 customers have a NULL segment.
- The maximum CMRC is 13.
- 1409 of 1500 returns match a rule.
- Total RHC is 108,713.19.
- Every return has exactly one quality_assessment row.
- revdate runs from 2025-01-20 to 2025-02-19.

## Where the contract is silent

- There is no metric for a group's average RVL. The `total_return_value_leakage`
  description gives it as the sum divided by COUNT(*), and the domain text
  carries KB 38's definition.
- The Customer Return Month (KB 35) is not a metric of its own. The domain text
  defines it, and `leakage_class` and the `month_from_revdate` rule fix the
  grain.

## Final agreement

All 16 comparisons agree on every row, before and after the check. No edits
were made. The contract loads (`DataContract.from_yaml`, `contract_digest`
sha256:22a4433b...), and all 32 metrics run.
