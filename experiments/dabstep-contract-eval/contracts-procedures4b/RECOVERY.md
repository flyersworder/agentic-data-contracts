# Experiment 4b: recovery

Scored per `prep/procedures4b/RUBRIC.md` after all twelve author sessions
(three draws with data and one without, per database; `dce.author`,
claudeopus5.5, run commit 67ac853), before any agent run. One checker per
database, who saw no question, gold, task file or result, ran each draw's SQL
on the 4b database and experiment 4's `reference.sql` on experiment 4's,
joined on the record key. Machine-readable: `recovery.json`. The offline
predictions are computed from it by `analysis/procedures_gap.py`.

## Summary

| database | gap (label) | draw-1 | draw-2 | draw-3 | no-data |
|---|---|---|---|---|---|
| labor_certification_applications | G1 `*` (split) | recovered | recovered | recovered | missed |
| reverse_logistics | G1 `ANY` (split) | recovered | recovered | recovered | missed |
| reverse_logistics | G2 score +1 (split) | missed | missed | missed | missed |
| reverse_logistics | G3 precedence (convention-only) | lowest_id | lowest_id | lowest_id | lowest_id |
| solar_panel | G1 `ALL` (settled) | recovered | recovered | recovered | missed |
| solar_panel | G2 fraction (settled) | recovered | recovered | recovered | missed |

- **(32) holds:** 12 of the 15 data gap-draws were recovered.
- **(32g) holds:** recovery is 100% on settled gaps, 67% on split and 0% on convention-only.
- **(32c) holds:** the no-data control recovered 0 of 5.
- **(33) holds:** no draw chose most specific wins. All four chose the lowest
  matching `rule_id` and each said it was its own tie-break. None summed, so
  the account's expectation of summing did not hold.

Recovery is not a function of noticing:
- **reverse_logistics G2.** Every data draw saw the stored scores run from 2
  to 11 against a documented 1 to 10, said so, and chose to "use as stored".
  None decoded the shift.
- **solar G2.** Draw 1 hard-codes the fraction. Draws 2 and 3 multiply a
  value at or below 1 by 100. That agrees with the reference on every stored
  value, but it hedges between the scales rather than settling one.

No draw had a correctable error, so draw 1 is unchanged
(`draw-1/CORRECTIONS.md`).

**Ruling (reverse_logistics G1).** No reverse_logistics draw computes rule
specificity: each resolves several matches by the lowest `rule_id`, so whether
`ANY` counts toward specificity is never built. Draws 1-3 are scored
*recovered* because:
- every matching pair agrees (4,493 of 4,493), including the 1,349 pairs of
  returns with a NULL segment;
- every applying-rule and RHC disagreement is G3's convention: RHC equals the
  lowest-id variant of the reference on all 1,500 returns.

The rubric reads G3's convention from SQL and text where G1 makes the records
unreadable, and this is the same reading the other way round. Scored *partial*
instead, (32) would fail (9 of 15, below two thirds), while (32g) would still
hold (100%, 33%, 0%).

## labor_certification_applications

Gap: G1 only. The rule table `review_schedule` stores the wildcard as the text `'*'` (496 rules; 130 to 222 `'*'` per criterion; no SQL NULL criteria), while the documentation still says "A NULL criterion is a wildcard". Reference reading (gaps.json `full_note`): `'*'` is the NULL criterion, matches every value including a missing one, and is not counted as a specified criterion. 286 of 981 cases have a NULL wage level or h1bDep (273 and 26); 233 of them match at least one rule in the reference.

Method: contract SQL run on the 4b database, `r_record_amounts` (and its `matches` CTE and specificity expression) on experiment 4's, joined on `filekey` (rule-level outputs on `rule_id`). Matching pairs agree for a case when its set of matching rule ids is identical. As a control, every draw's G1 SQL was also run on experiment 4's database (NULL wildcards): all four draws agree there on 981/981 cases and 496/496 rules, so every 4b disagreement below comes from the gap. Outside G1, the C quantity (AOW, APW, WHR, Headroom Points, Level Points where defined, CWAS per case; EWAS and tier per employer and per employer-month) and ERL per employer-month were checked as well. They agree in every draw (981/981, 546/546, 643/643). no-data's ERL is the exception: 26/643 on 4b, but 643/643 on experiment 4's database, so that disagreement is G1's.

### draw-1: recovered

| Comparison | Contract | Agree / total |
|---|---|---|
| matching pairs | `review_rule_matches_case` | 981/981 (2788 pairs both sides) |
| specificity | `review_rule_specificity` | 496/496 |
| governing rule | `governing_review_rule_id` | 981/981 |
| CRH per record | `case_review_hours` | 981/981 |

- Text: gives the reference reading. `'*'` is the stored wildcard with no restriction, and a NULL case value matches only a wildcard. Quote: "DATA FINDING: no criterion is stored as SQL NULL; the wildcard is stored as the text '*' (e.g. 222 of 496 rules have visa_class '*'). Treat NULL or '*' as "no restriction"." (domain `review_effort`). From `review_rule_matches_case`: "A NULL case value only matches a wildcard criterion (equality with NULL is not true)."
- SQL: `(r.x IS NULL OR r.x = '*' OR r.x = cvb.x)`, so `'*'` matches every case, NULL values included. Specificity counts a criterion only when it is `IS NOT NULL AND <> '*'`. The SQL agrees on all 286 NULL-value cases and all 695 others.
- Correctable errors: none.

### draw-2: recovered

| Comparison | Contract | Agree / total |
|---|---|---|
| matching pairs | `review_rule_match` | 981/981 (2788 pairs both sides) |
| specificity | ORDER BY in `governing_review_rule_id`'s from_sql (no metric of its own) | 496/496 |
| governing rule | `governing_review_rule_id` | 981/981 |
| CRH per record | `case_review_hours` | 981/981 |

- Text: gives the reference reading. Quote: "DATA CHECK: the stored table has NO NULL criteria; the wildcard is written as the literal '*'. Treat both NULL and '*' as "places no restriction" (matching and specificity)." (domain `review_effort`). The same domain also says "When the case's value is NULL (wage level or h1bDep) only a wildcard criterion can match."
- SQL: `(r.x IS NULL OR r.x = '*' OR r.x = m.x)`. The specificity term adds 0 when `r.x IS NULL OR r.x = '*'`. The SQL agrees on all NULL-value cases.
- Correctable errors: none.

### draw-3: recovered

| Comparison | Contract | Agree / total |
|---|---|---|
| matching pairs | `review_rule_matches` | 981/981 (2788 pairs both sides) |
| specificity | ORDER BY in `governing_review_rule_id`'s from_sql (no metric of its own) | 496/496 |
| governing rule | `governing_review_rule_id` | 981/981 |
| CRH per record | `case_review_hours` | 981/981 |

- Text: gives the reference reading. Quote: "DATA FINDING: the table stores wildcards as the literal '*' (no criterion is actually NULL); treat '*' exactly like NULL — a wildcard placing no restriction and not counting toward specificity." (domain `review_effort`). The same domain also says "When the case's value is NULL (wage level or h1bDep) only a wildcard criterion matches."
- SQL: `(r.x IS NULL OR r.x = '*' OR r.x = cb.x)`. Specificity is `IS NOT NULL AND <> '*'`. The SQL agrees on all NULL-value cases.
- Correctable errors: none.

### no-data: missed

| Comparison | Contract | Agree / total |
|---|---|---|
| matching pairs | LEFT JOIN condition in `governing_review_rule`'s from_sql (no metric of its own; kb_coverage maps 64 and 65 to `governing_review_rule`) | 69/981 (13 pairs vs 2788) |
| specificity | ORDER BY in `governing_review_rule`'s from_sql | 30/496 |
| governing rule | `governing_review_rule` | 82/981 |
| CRH per record | `case_review_hours` | 92/981 |

- Text: the literal reading. The text names only NULL as the wildcard and never mentions `'*'`. Quote: "A NULL criterion is a wildcard." (domain `review_effort`). The same domain also says "If the case value is NULL, only a rule with a NULL criterion for that attribute matches." and "GOVERNING RULE: the matching rule with the most non-NULL criteria".
- SQL: `(r.x IS NULL OR r.x = k.x)` compares `'*'` as a value, and specificity counts `'*'` as a restriction. Only the 30 rules with no `'*'` can match, and every case with a NULL wage level or h1bDep matches nothing. The 69 cases that agree on matching are exactly the reference's no-match cases. Governing rule adds 13 cases governed by a `'*'`-free rule. CRH adds 10 cases where the default amount happens to equal the reference rule's amount.
- Correctable errors: none. The same SQL agrees with the reference 981/981 on experiment 4's database, so every disagreement is the gap.

### Corrections (draw 1)

None. Draw 1 agrees with the reference on all four comparisons, and its C and ERL agree too. `check` prints "OK: the contract passes" on the unedited contract.

## reverse_logistics

Gaps: G1, the wildcard `ANY` in `return_charge_rules` (388 of 400 rules carry it); G2, the aesthetic score at `quality_assessment.assessment_summary` `$.results.aesthetic_score`, stored one point higher (A = stored - 1), which feeds IRV and RVL; G3, how several matching rules combine (entry 33 now gives only the Standard Handling Charge).

How the comparisons were run: each draw's SQL ran on the 4b database and the reference's named queries ran on experiment 4's, joined on `casenum`, or on (`casenum`, `rule_id`) for pairs. 1,500 returns; the reference finds 4,493 matching pairs. 655 returns have a NULL `seg_category`, and the reference matches them in 1,349 pairs. No draw computes rule specificity: all four resolve several matches by the lowest `rule_id`. G3 variants were built from the reference's own `matches` CTE: most_specific is the reference itself; sum adds every matching rule's F + P/100 x V; lowest_id applies the lowest matching `rule_id`. A return with no matching rule takes 15.00 / 5.0 in all three.

### draw-1

| Comparison | Contract | Agree / total |
|---|---|---|
| G1 matching (return, rule) pairs | `return_charge_rule_matches` | 4493 / 4493 |
| G1 rule specificity | not defined | n/a |
| G1 applying rule per return | `return_handling_charge` (`rh.rule_id`) | 911 / 1500 |
| G1 RHC per return | `return_handling_charge` | 955 / 1500 |
| G2 IRV per return | `inspected_resale_value` | 0 / 1500 |
| G2 RVL per return | `return_value_leakage` | 0 / 1500 |
| G3 RHC vs most_specific / sum / lowest_id | `return_handling_charge` | 955 / 352 / **1500** of 1500 |

- **G1: recovered.** Text and SQL both treat `ANY` as no restriction. A NULL record value matches it, because the `= 'ANY'` test never reads the record value. Text: "the literal 'ANY' is used instead on all five text columns. Treat both NULL and 'ANY' as the wildcard" and "A criterion that places no restriction matches every return, including one whose value is missing". Every pair agrees. Every applying-rule and RHC disagreement comes from the G3 convention: RHC equals the lowest-id variant on all 1,500 returns.
- **G2: missed**, in both text and SQL. The author noticed the range breach and kept the stored value: "read as a decimal (documented as 1-10; observed range 2 to 11 - use as stored, do not clip)". Four metrics read the score (IRV, RVL, CLR, Leakage Class), and none decodes it. With stored - 1 substituted, as a diagnostic only, IRV and RVL agree 1500/1500.
- **G3: lowest_id**, read from the records. Text: "the contract uses the matching rule with the LOWEST rule_id (first rule in the schedule). This tie-break is the contract's, not the documentation's." SQL: `MIN(x.rule_id)` and `arg_min(..., x.rule_id)`.
- Correctable errors: none.

### draw-2

| Comparison | Contract | Agree / total |
|---|---|---|
| G1 matching (return, rule) pairs | LATERAL subquery of `return_handling_charge`, without `ORDER BY ... LIMIT 1` | 4493 / 4493 |
| G1 rule specificity | not defined | n/a |
| G1 applying rule per return | `return_handling_charge` (`k.rule_id`) | 911 / 1500 |
| G1 RHC per return | `return_handling_charge`, grouped by `r.casenum` | 955 / 1500 |
| G2 IRV per return | `inspected_resale_value` | 0 / 1500 |
| G2 RVL per return | `return_value_leakage` | 0 / 1500 |
| G3 RHC vs most_specific / sum / lowest_id | `return_handling_charge` | 955 / 352 / **1500** of 1500 |

- **G1: recovered.** Text: "'no restriction' is stored as the literal 'ANY' (e.g. 170 of 400 customer_segment values), so 'ANY' must be treated exactly like NULL", and "a criterion placing no restriction matches every return, including one whose value is missing". The SQL `k0.col = 'ANY'` matches NULL record values. Every pair agrees, and every RHC disagreement comes from G3.
- **G2: missed**, in both text and SQL. Text: "read as a decimal (documented 1 to 10; observed 2.0 to 11.0, used as stored, not clipped)". None of the four metrics that read the score decodes it. The diagnostic decode gives 1500/1500.
- **G3: lowest_id**, read from the records. Text: "The metric applies the matching rule with the LOWEST rule_id (first rule in the schedule)." SQL: `ORDER BY k0.rule_id LIMIT 1`.
- Correctable errors: none.

### draw-3

| Comparison | Contract | Agree / total |
|---|---|---|
| G1 matching (return, rule) pairs | LATERAL subquery of `return_handling_charge`, without `ORDER BY ... LIMIT 1` | 4493 / 4493 |
| G1 rule specificity | not defined | n/a |
| G1 applying rule per return | `return_handling_charge` (`rule.rule_id`) | 911 / 1500 |
| G1 RHC per return | `return_handling_charge` | 955 / 1500 |
| G2 IRV per return | `inspected_resale_value` | 0 / 1500 |
| G2 RVL per return | `return_value_leakage` | 0 / 1500 |
| G3 RHC vs most_specific / sum / lowest_id | `return_handling_charge` | 955 / 352 / **1500** of 1500 |

- **G1: recovered.** Text: "the wildcard is stored as the literal text 'ANY', so treat NULL and 'ANY' alike", and "the 93 customers with NULL seg_category match only segment-'ANY' rules". The SQL agrees on every pair, and every RHC disagreement comes from G3.
- **G2: missed**, in both text and SQL. Text: "read as a decimal (documented 1-10; the data holds 2 to 11, 147 rows above 10 - use as stored, no clamping)". None of the four metrics that read the score decodes it. The diagnostic decode gives 1500/1500.
- **G3: lowest_id**, read from the records. Text: "The metric return_handling_charge applies the matching rule with the LOWEST rule_id." SQL: `ORDER BY k.rule_id LIMIT 1`.
- Correctable errors: none.

### no-data

| Comparison | Contract | Agree / total |
|---|---|---|
| G1 matching (return, rule) pairs | `return_charge_rule_matches` | 12 / 4493 |
| G1 rule specificity | not defined | n/a |
| G1 applying rule per return | `return_handling_charge` (`mr.rule_id`) | 102 / 1500 |
| G1 RHC per return | `return_handling_charge` | 103 / 1500 |
| G2 IRV per return | `inspected_resale_value` | 0 / 1500 |
| G2 RVL per return | `return_value_leakage` | 0 / 1500 |
| G3 RHC vs most_specific / sum / lowest_id | `return_handling_charge` | 103 / 91 / 95 of 1500 |

- **G1: missed**, in both text and SQL. Only NULL is a wildcard, and `ANY` is compared as a value: "A text criterion matches when it is NULL or equals the return's value exactly; ... a non-NULL criterion never matches a return whose value is NULL". The contract finds only the 12 pairs of the 12 rules that have no `ANY` criterion. The other 1,488 returns fall to the Standard Handling Charge. The applying rule agrees on the 91 returns that truly match no rule, plus 11 of the 12 returns the contract does match.
- **G2: missed**, in both text and SQL. Text: "A = JSON results.aesthetic_score (1-10, read as decimal)". None of the four metrics that read the score decodes it. The diagnostic decode gives 1500/1500.
- **G3: lowest_id**, read from SQL and text: the per-return comparison is unreadable because `ANY` never matches. Text, in the `return_handling_charge` description: "INTERPRETATION: if several rules match, the lowest rule_id is used." SQL: `MIN(rr.rule_id)`. A diagnostic run that reads `ANY` as a wildcard reproduces the pairs (4493/4493) and the lowest-id RHC (1500/1500). That run also shows no RHC error outside G1 and G3.
- Correctable errors: none.

### Corrections (draw 1)

None. Every disagreement in draw 1 traces to a gap: the IRV and RVL differences are G2's undecoded score, and the applying-rule and RHC differences are G3's lowest-id convention. Matching, joins, JSON paths, CMRC, the completeness factor, the shipping fee and the standard charge all agree with the reference.

## solar_panel

Gaps: G1, the wildcard `ALL` in `warranty_settle_rules`; G2, `energy_yield.availability_pct` stored as a fraction (A = stored x 100). There is no G3.

How the comparisons were run: each contract's SQL ran on the 4b database, and the reference's named queries (`r_record_matches`, `r_record_amounts`, `c_record_values`) ran on experiment 4's. Results were joined on `snapkey`, or on `rule_id` for specificity. None of the draws defines matching pairs or specificity as a metric of its own, so both were taken from the draw's governing-rule SQL. For matching pairs, the top-1 filter was removed and a snapshot counts as agreeing when its set of matched rules equals the reference's. For specificity, the draw's expression was applied to all 520 rules.

Records the gaps touch:
- G1: 865 of 998 snapshots have a NULL manufacturer, alert state or warranty state. 793 of them match a rule only through an unrestricted criterion on that field (2,532 of the reference's 3,750 pairs). 72 snapshots match no rule.
- G2: all 998 snapshots. The stored values run from 0.95 to 0.9999.

As a separate check for author errors, each draw's SQL was also run on experiment 4's database, where rules use NULL and availability is a percentage. Draw 2, draw 3 and no-data agree there on every record in every comparison. Draw 1 agrees on G1 and gives 0/998 on G2, which is expected because it reads availability as a fraction without condition. So every disagreement on the 4b database comes from a gap.

### Draw 1

| Comparison | Contract source | Agree / total | After correction |
|---|---|---|---|
| G1 matching (snapshot, rule) pairs | `governing_settlement_rule_id` (subquery `st`, top-1 removed) | 998/998 (3,750 pairs) | 998/998 |
| G1 rule specificity | same, spec expression | 520/520 | 520/520 |
| G1 governing rule | `governing_settlement_rule_id` | 998/998 | 998/998 |
| G1 settlement amount S | `warranty_settlement_amount` | 998/998 | 998/998 |
| G2 potential snapshot yield | `potential_snapshot_yield` (subquery `ep`) | 998/998 | 998/998 |
| G2 recoverable energy loss | `recoverable_energy_loss` | 998/998 | 998/998 |

**G1: recovered.**
- Text: the contract states that `ALL` is the stored form of "no restriction", and that only a restricting criterion fails on a NULL snapshot value. That `ALL` matches a NULL value follows from these two statements; the contract does not say it in so many words. Quote: "DATA FINDING: in warranty_settle_rules no criterion is NULL; "no restriction" is stored as the literal 'ALL' (all 6 columns), so 'ALL' and NULL are both wildcards. A restricting criterion never matches a NULL snapshot value" (contract.yml, domain `warranty_settlement`; also rule `settlement_wildcard_all`).
- SQL: `w.c IS NULL OR w.c = 'ALL' OR w.c = s.value`, so `ALL` matches a NULL value. Specificity excludes both NULL and `ALL`.

**G2: recovered.**
- Text: "the documentation says A is on a 0-100 scale, but availability_pct is stored as a FRACTION (0.95-0.9999 in all 998 rows), so A/100 is the stored value itself" (contract.yml, domain `energy_loss`; also rule `availability_is_fraction`).
- SQL: E / NULLIF(avail x (1 - soil/100), 0). The stored value is used directly as A/100. All five metrics that read availability share subquery `ep`, so all five are decoded.

**Correctable errors:** none.

### Draw 2

| Comparison | Contract source | Agree / total |
|---|---|---|
| G1 matching (snapshot, rule) pairs | `governing_settlement_rule` (x-froms `ws`, inner `g`, rn = 1 removed) | 998/998 (3,750 pairs) |
| G1 rule specificity | same, spec expression | 520/520 |
| G1 governing rule | `governing_settlement_rule` | 998/998 |
| G1 settlement amount S | `warranty_settlement_amount` | 998/998 |
| G2 potential snapshot yield | `potential_snapshot_yield` (x-froms `el`) | 998/998 |
| G2 recoverable energy loss | `recoverable_energy_loss` | 998/998 |

**G1: recovered.**
- Text: says explicitly that an unrestricted criterion matches a missing value. Quote: "a criterion with no restriction matches anything including a missing value; a restricted criterion ... never matches a NULL snapshot value ... DATA: the rules table stores "no restriction" as the literal 'ALL' (it contains no NULLs); treat NULL and 'ALL' alike as unrestricted" (contract.yml, domain `warranty_settlement`).
- SQL: `COALESCE(w.c,'ALL') = 'ALL' OR w.c = s.value`, and specificity counts `COALESCE(w.c,'ALL') <> 'ALL'`.

**G2: recovered.**
- Text: "A must be on a 0-100 scale: the stored availability_pct is a fraction (0.95-0.9999), so it is scaled x100 first (values <= 1 multiplied by 100)" (contract.yml, domain `energy_loss`).
- SQL: A = CASE WHEN avail <= 1 THEN 100 x avail ELSE avail END, then A/100. The rescale is conditional, but every stored value is at most 1, so it equals the reference on every record. All five metrics that read availability share `el`.

**Correctable errors:** none.

### Draw 3

| Comparison | Contract source | Agree / total |
|---|---|---|
| G1 matching (snapshot, rule) pairs | `governing_settlement_rule` (subquery `g`, rule_rank = 1 removed) | 998/998 (3,750 pairs) |
| G1 rule specificity | same, spec expression | 520/520 |
| G1 governing rule | `governing_settlement_rule` | 998/998 |
| G1 settlement amount S | `warranty_settlement_amount` | 998/998 |
| G2 potential snapshot yield | `potential_snapshot_yield` (subquery `y`) | 998/998 |
| G2 recoverable energy loss | `recoverable_energy_loss` | 998/998 |

**G1: recovered.**
- Text: says `ALL` matches a NULL value by implication: a NULL value "only satisfies unrestricted criteria", and `ALL` is unrestricted. Quote: "a restricting criterion never matches a NULL snapshot value (so 'Unlisted' or no alert/warranty state only satisfies unrestricted criteria) ... DATA FINDING: in warranty_settle_rules no criterion is SQL NULL; "no restriction" is stored as the literal string 'ALL' ... Treat both NULL and 'ALL' as unrestricted." (contract.yml, domain `warranty_settlement`).
- SQL: `COALESCE(w.c, 'ALL') = 'ALL' OR w.c = s.value`, and specificity counts `COALESCE(w.c, 'ALL') <> 'ALL'`.

**G2: recovered.**
- Text: "the documentation says A is on a 0-100 scale but every stored availability_pct is a fraction (0.95-0.9999); the metric rescales values <= 1 by x100 before applying A/100" (contract.yml, domain `energy_loss`).
- SQL: the same conditional x100 as draw 2. All five metrics that read availability share `y`.

**Correctable errors:** none.

### No-data

| Comparison | Contract source | Agree / total |
|---|---|---|
| G1 matching (snapshot, rule) pairs | `warranty_governing_rule` (lateral, arg_min removed) | 72/998 (7 pairs vs 3,750) |
| G1 rule specificity | same, spec expression | 72/520 |
| G1 governing rule | `warranty_governing_rule` | 79/998 |
| G1 settlement amount S | `warranty_settlement_amount` | 79/998 |
| G2 potential snapshot yield | `potential_snapshot_yield` | 0/998 |
| G2 recoverable energy loss | `recoverable_energy_loss` | 0/998 |

The agreeing records are ones the sentinel does not decide:
- Matching pairs: the 72 snapshots that match no rule under either reading.
- Specificity: the 72 rules that contain no `ALL`.
- Governing rule and S: those 72 snapshots plus 7 governed by a rule of specificity 6.

**G1: missed.**
- Text: gives only the documented reading, in which NULL is the wildcard. `ALL` is never mentioned, so it is compared as a value and does not match a NULL record value. Quote: "a snapshot satisfies a criterion of warranty_settle_rules when the criterion is NULL (matches anything, including missing) or equals the snapshot value exactly ... Specificity = number of restricting (non-NULL) criteria 0-6" (contract.yml, domain `warranty_settlement`).
- SQL: `r.c IS NULL OR r.c = s.value`, and specificity counts IS NOT NULL. As a result, a rule with any `ALL` never matches, and `ALL` counts as a specified criterion.

**G2: missed.**
- Text: reads the field as documented. Quote: "E_pot = E / ((A/100) x (1 - L_soil/100)), E = energy_yield.energy_yield_kwh, A = energy_yield.availability_pct" (contract.yml, domain `energy_loss`).
- SQL: `avail / 100.0` applied to the stored fraction, which makes E_pot about 100 times too large. All five metrics that read availability do the same.

**Correctable errors:** none. On experiment 4's database the same SQL agrees on every record in all six comparisons.

### Corrections (draw 1)

None.

