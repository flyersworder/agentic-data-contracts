# labor_certification_applications: R and C

**R, Case Review Effort Schedule (KB 61-67).** This is a rule-matching procedure. The new table `review_schedule` (496 rules, seed 11) gives the reviewer-hours budgeted per case. Each case is matched on six criteria at once: visa class, prevailing wage level (JSON), H-1B dependency, SOC major group, worksite state, and the Monthly Volume Band. That band is a band of EMCV, the employer's number of cases in the calendar month of `beginday`: `1`, `2-4`, `5-14`, `15+`, inclusive at both ends. A NULL criterion is a wildcard. The most specific matching rule wins, and a tie goes to the lowest `rule_id`. A case with no match gets the default of 3.50 + 0.50 per position. The amounts are CRH = base_hours + position_hours x headCt, summed to the Employer Review Load per employer and month or year. This is the top level of the rubric: the knowledge is a rules table applied record by record, with a monthly-aggregate band and a precedence rule.

**C, Wage Assurance (KB 68-72).** This is a composite over existing columns, with no rule table. It parses and annualizes the offered and prevailing wages, computes WHR = (AOW - APW) / APW, caps WHR into Headroom Points, and forms CWAS = 0.7 x Headroom + 0.3 x Level Points per case. CWAS is then aggregated to EWAS, a headcount-weighted average per employer and period, and banded into Thin (<30), Adequate [30, 45), Strong [45, 60) and Exemplary (>=60). That makes it level 2: dependent formulas, a band, and a case-to-employer change of grain.

**Word counts** (knowledge, description and definition): R has 7 entries and 591 words (480 of them in definitions). C has 5 entries and 517 words (438 in definitions). R/C = 1.14.

**Sanity numbers.** All counts come from `reference.sql` on the built DB, and an independent Python reimplementation agrees on every record, every employer and every employer-month.
- Coverage: 981 cases, 546 employers, 643 employer-months, and 13 Start Months from 2023-12 to 2024-12.
- 912 cases use a non-default rule. The other 69 get the default: 13 in band `1`, 30 in `2-4`, 26 in `5-14`.
- 112 distinct rules win, out of 496.
- Specificity of the governing rule: 42 cases at 1 criterion, 97 at 2, 75 at 3, 349 at 4, 336 at 5, 13 at 6.
- 230 cases have a tie at the top specificity that the lowest `rule_id` resolves. In 320 cases the lowest-id matching rule is not the governing one.
- Bands: 366 rules carry a band criterion, and 804 of the 912 non-default cases are governed by one.
- Employer-months by band: `1` = 540 (540 cases), `2-4` = 80 (192 cases), `5-14` = 19 (154 cases), `15+` = 4 (95 cases). Mean CRH by band is 4.78, 4.18, 3.39 and 2.97. Total CRH is 4,188.00 hours.
- C tiers over all of an employer's cases: Thin 236, Adequate 104, Strong 90, Exemplary 116 employers. Per employer-month: 273, 124, 97, 149.
- CWAS runs from 7.5 to 100, with a median of 34.6.

**For a reviewer to double-check**
- `recvday` (2023/12/21) and `decisionday` (2023-12-29) are constant in this DB, so the month for both pieces is the employment start month (`beginday`, DD/MM/YYYY). The months are lumpy: Dec-23, Jan-24 and Jun-24 hold 78% of cases, Jul-Dec 2024 hold 1-10 cases each, and the year 2023 is only December.
- The employer is the exact `homefirm` text. Case variants such as `Amazon.com Services LLC` and `AMAZON.COM SERVICES LLC` are separate employers. The KB says so.
- 13 employers have EWAS exactly 30.0: level IV with offered = prevailing, so CWAS = 0.3 x 100. Inclusive lower bounds put them in Adequate, and float and exact arithmetic agree. A boundary slip would show up here.
- `headct` is 1 for 965 of 981 cases, so `position_hours` mostly acts as a per-case constant. A "change one rule's rate" question should vary `base_hours`, or pick a rule that governs a multi-position case.
- The `15+` band holds only 4 employer-months. 273 cases (28%) have a NULL wage level, so they score 50 Level Points in C and can match only wage-level-wildcard rules in R.
- The seed and the per-k rule counts were chosen among four seeds and three plans so that the default share is a few percent and spread over bands. With broader rules, every case matched something.
- The KB already has loosely defined neighbours: WDR (11, a percent), Wage Competitiveness Tiers (48), and Employer Size Classification (46). C uses new names (WHR is a fraction, not a percent). Check that a contract does not conflate them.
- `reference.sql` runs under `integer_division = true`, and every division in it has a DOUBLE operand.

## Review

An independent reviewer wrote R and C again from the KB text, the schema and the column meanings
alone, before reading `reference.sql`. That version agrees with every named query, record by
record:
- `r_record_amounts` 981/981 (employer, Start Month, EMCV, band, governing rule, CRH);
- `r_employer_month` 643/643;
- `c_record_scores` 981/981 (AOW, APW, WHR, CWAS);
- `c_employer_scores` 546/546 (EWAS and tier);
- `c_employer_month_scores` 643/643 (EWAS and tier).

**Changes:** none. No ambiguity in the text, and no bug in the reference, was found.

**Checked, with nothing open:**
- Every case has exactly one `case_worksite` row and one wage record.
- Every `beginday` is DD/MM/YYYY.
- Every wage text parses.
- APW is never 0.
- On exact `homefirm` text: 7 case or space variant groups exist, and the KB settles them. No
  firm has more than one zip.
- 21 employer-months sit exactly on EWAS = 30, all of them level IV with offered = prevailing.
  Every float order of `0.7*HP + 0.3*LP` that was tried (DuckDB, and Python with
  `0.3*100` and `(1-0.7)*100`) gives a value >= 30, so they are Adequate whichever way the sum is
  computed.

**Notes for the experiment (not changed):**
- C chains more steps than DESIGN.md's "two or three dependent formulas":
  - annualised wages (AOW and APW),
  - WHR,
  - Headroom Points,
  - CWAS,
  - EWAS,
  - and two value lookups: the unit factor and the Level Points.

  R/C word balance is fine (1.14), but C may be harder than the C of the other two databases.
  Changing this would be a redesign, so it is left as is.
- The `review_schedule` column meanings describe content. `soc_major_group` "(the first two
  digits of an SOC code)" and `base_hours`/`position_hours` ("once per case", "per worker position
  requested") are the closest to the procedure. They give no matching, precedence or default
  rule, and they match the level of the other two databases.
