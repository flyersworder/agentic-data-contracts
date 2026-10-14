# reverse_logistics: R and C

**R, Return Handling Charge (KB 30-34).** This is a rule-matching procedure. Each return is matched against
`return_charge_rules` (400 rules, seed 20261014) on six criteria at once: customer segment,
return channel, item category, primary reason (JSON), item condition (JSON, from
`quality_assessment`), and a band of the customer's number of returns in the calendar month of
`revdate` (CMRC, inclusive bounds). A NULL criterion matches anything. The most specific matching
rule wins, and ties go to the lowest `rule_id`. A return that matches no rule gets the Standard
Handling Charge (15.00 + 5.0%). `RHC = flat_charge + charge_pct/100 * unit_value`, summed per
customer, month or group. It is a rule-matching procedure under DESIGN.md because it has a new
rule table, matching on several fields with wildcards, a monthly-aggregate band, precedence and a
default.

**C, Customer Leakage Ratio (KB 35-39).** This is a composite over existing columns. It chains
three formulas: `IRV = V * aesthetic/10 * K(completeness)`, then `RVL = V - IRV + shipping fee`
at the return grain, then `CLR = sum RVL / sum V` at the customer-month grain. A banded class
follows: Contained < 0.55 <= Elevated < 0.70 <= Severe. There is no rule table.

**Word counts** (knowledge + description + definition): R 468 and C 407, so R/C = 1.15.

## Sanity numbers (PROC_DATA build)

- Records: all 1,500 returns get an RHC. 1,409 are charged under a rule and 91 (6.1%) under the
  standard charge. 325 of the 400 rules actually win.
- Matching: 1,213 returns match more than one rule (3.2 matches on average, 9 at most).
  - For 589 returns, the most specific rule is not the lowest-id match.
  - For 485 returns, the tie-break decides the rule.
  - Specificity of the winning rule: 2 (148), 3 (531), 4 (583), 5 (147).
- Bands: 1,029 winning rules carry a band. Ignoring the band would change the rule for 1,045
  returns and the amount for 950. The 397 customer-months split by CMRC as 1-2: 123, 3-4: 159,
  5-7: 82 and 8+: 33 (returns 178 / 553 / 469 / 300). Average RHC rises with the band, from
  57.57 to 89.30.
- RHC: total 108,713.19, mean 72.48, median 58.30, max 509.86. 199 returns have RHC = 0, because
  rules on 'Wrong Item' charge nothing.
- C classes over the 397 customer-months: Contained 126, Elevated 150, Severe 121. No CLR lies
  within 1e-4 of a boundary.
- A plain-Python reimplementation written from the KB text agrees with `reference.sql` on every
  return (rule id and RHC), on every RVL, and on every customer-month CLR and class.

## For the reviewer

- `revdate` spans only **2025-01 and 2025-02**, so there are two months.
  - A "one year" question covers both months, and a monthly series has two points.
  - `logtime` spans 2024-02..2025-06, but it comes in six formats and 254 rows give only a year,
    so it was rejected as the month source.
- 93 customers (655 returns) have NULL `seg_category`. They match only rules whose segment is
  NULL, and the text says so. This is the one place a NULL record value meets the matching.
- Rounding: 12 RHC values sit within 1e-4 of a half cent. The KB forbids per-record rounding, but
  a solver who rounds each record could be off by a cent on totals.
- `unit_value` is REAL (float32), so V carries float noise. Some `aesthetic_score` (172) and
  `shipping.fee` (15) values are JSON integers, and casting them to DOUBLE handles both.
- C's completeness factor is a three-value mapping inside the IRV formula, in the KB's own FFS
  style. Please check that it does not read as a rule table.
- KB 31 (R) and KB 35 (C) both define the customer and month on purpose, so that each piece stands
  alone.
- Not used: `return_processing` (26 rows, no key to `returns`) and `financial_management` (995 of
  1,500 returns).
- The DuckDB file's bytes differ between builds, but its content does not: the rules hash is
  stable. The three text outputs are byte-identical across runs. New entries use
  `children_knowledge: -1` where the original KB uses `[]`.

## Review

An independent reviewer wrote R and C again from the KB text, the schema and the column meanings
alone, before reading `reference.sql`. That version agrees with every named query, record by
record: `r_record_amounts` 1,500/1,500 (customer, month, CMRC, applying rule, RHC),
`r_customer_month` 397/397, `c_record_values` 1,500/1,500 (IRV, RVL) and `c_customer_month`
397/397 (CLR, class).

**Changes:** none. No ambiguity in the text, and no bug in the reference, was found.

**Checked, with nothing open:**
- Every join resolves for all 1,500 returns.
- Every JSON path is present.
- Every `revdate` is ISO.
- Category values are exact, with no case or space variants.
- The 655 NULL-segment returns are covered by the text.
- The integer-stored JSON numbers are covered by "read as a decimal". This matters under
  `integer_division`.
- No CLR lies within 4e-4 of a band edge.

**Notes for the experiment (not changed):**
- KB 20, a child of KB 30, lists the channels Store, Courier, Mail, Locker and Pickup. The data
  and the rule table use Collection Point and have no Locker or Pickup. Matching is exact text, so
  results do not change, but question authors should not describe a return by a KB-20-only
  channel.
- C's completeness factor K is a three-value lookup on one field. It is not a multi-field rule
  table, so C still has no rule table.
- The column meanings of the new columns describe content only. `charge_pct` "as a percentage of
  the product's unit value" is the nearest to the formula, and the other two databases are at the
  same level.
