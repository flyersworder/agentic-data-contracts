# labor_certification_applications: styled facts check

Each result-relevant fact of the clean entries (`knowledge.json`, KB 61-72) is listed below,
together with the styled entry (`knowledge_styled.json`, KB 73-78) and the sentence that states it.

Styled entries:
- 73 Case Review Effort Schedule (R guide)
- 74 Case Review Effort Schedule: Rule Fields (R field reference)
- 75 Case Review Effort Schedule: Notes on Overlapping Rules, Defaults and Edge Cases (R notes)
- 76 Wage Assurance Scores (C guide)
- 77 Wage Assurance Scores: Wage Record Fields (C field reference)
- 78 Wage Assurance Scores: Notes on Missing Levels, Tiers and Rounding (C notes)

## R: Case Review Effort Schedule (clean 61-67)

1. **The schedule is the `review_schedule` table, one rule per row, identified by `rule_id`** (61).
   73: "The Case Review Effort Schedule, the review_schedule table, is the table of rules that budgets reviewer-hours for each application"; 74: "Each row of review_schedule is one rule." and "**rule_id**: integer. Identifier of the rule within the schedule."
2. **There are six matching criteria: visa_class, wage_level, h1b_dependent, soc_major_group, worksite_state, monthly_volume_band** (61).
   74: "Six of its fields are matching criteria and two are amounts.", followed by one bullet per criterion.
3. **`base_hours` is reviewer-hours charged once per case** (61).
   74: "**base_hours**: decimal. Reviewer-hours the rule charges once per case."
4. **`position_hours` is reviewer-hours charged per worker position requested** (61).
   74: "**position_hours**: decimal. Reviewer-hours the rule charges per worker position requested (cases.headCt)."
5. **A NULL criterion is a wildcard that matches any case value** (61, 64).
   74: "In every criterion, a NULL value means the rule applies to all values of that attribute."; 73 step 3: "A criterion left NULL in the rule places no restriction and matches any value."
6. **EMCV is the number of the employer's cases whose Start Month is the given calendar month** (62).
   73 step 2: "Count the employer's cases whose Start Month is the case's Start Month: this count is the Employer Monthly Case Volume (EMCV)."
7. **Every row of `cases` counts once in EMCV, whatever its status or headCt** (62).
   75: "In the EMCV, every row of cases counts once, whatever its status or headCt."
8. **The employer is the exact text of `cases.homeFirm`, with no case folding or trimming (the Amazon example)** (62).
   73 step 2: "The employer is the exact text of cases.homeFirm."; 75: "The employer is the exact text of cases.homeFirm, with no case folding or trimming: 'Amazon.com Services LLC' and 'AMAZON.COM SERVICES LLC' are different employers."
9. **The Start Month is the calendar year and month of `cases.beginDay`, which is text in DD/MM/YYYY format ('01/04/2024' is 1 April 2024)** (62).
   73 step 1: "The Start Month of a case is the calendar year and month of cases.beginDay ... stored as text in DD/MM/YYYY format, day first: '01/04/2024' is 1 April 2024, so its Start Month is April 2024."
10. **The band is set by the EMCV of the case's employer in the case's Start Month, and is written as in `review_schedule.monthly_volume_band`** (63).
    73 step 2: "The EMCV places the case in one of four Monthly Volume Bands, written exactly as in the schedule's monthly_volume_band field"; 74: "Compared with the case's Monthly Volume Band, which the employer's EMCV in the case's Start Month determines".
11. **Band '1' when EMCV = 1** (63).
    73: "'1' when EMCV is 1;"
12. **Band '2-4' when 2 <= EMCV <= 4** (63).
    73: "'2-4' when EMCV is from 2 to 4;"
13. **Band '5-14' when 5 <= EMCV <= 14** (63).
    73: "'5-14' when EMCV is from 5 to 14;"
14. **Band '15+' when EMCV >= 15** (63).
    73: "'15+' when EMCV is 15 or more."
15. **Both ends of each range are inclusive** (63).
    73: "Both ends of each range are inclusive, so an EMCV of 4 is '2-4' and an EMCV of 5 is '5-14'."
16. **A case matches a rule when every non-NULL criterion equals the case's value, compared as exact text** (64).
    73 step 3: "A case matches a rule when every non-NULL criterion of the rule equals the case's own value, compared as exact text."
17. **`visa_class` is compared with `cases.visaCls`** (64).
    74: "**visa_class**: text, such as 'H-1B'. Compared with cases.visaCls."
18. **`wage_level` is compared with the 'level' field of the 'prevailing_wage' object in `prevailing_wage.wage_details` ('I' to 'IV')** (64).
    74: "**wage_level**: text, 'I' to 'IV'. Compared with the 'level' field of the 'prevailing_wage' object in prevailing_wage.wage_details."
19. **The wage record is reached through the case's single `case_worksite` row (`dockKey = fileKey`) and `wageTrack = trackNo`** (64).
    74: "A case reaches its wage record through its single case_worksite row (case_worksite.dockKey = cases.fileKey) and then case_worksite.wageTrack = prevailing_wage.trackNo."
20. **`h1b_dependent` is compared with `cases.h1bDep`** (64).
    74: "**h1b_dependent**: text, 'Yes' or 'No'. Compared with cases.h1bDep."
21. **`soc_major_group` is compared with the first two characters of `cases.SocCd` ('15' for '15-1253.00')** (64).
    74: "Compared with the first two characters of cases.SocCd: a case with SocCd '15-1253.00' is in major group '15'."
22. **`worksite_state` is compared with `case_worksite.wsState`** (64).
    74: "Compared with case_worksite.wsState of the case's single case_worksite row."
23. **`monthly_volume_band` is compared with the case's Monthly Volume Band** (64).
    74: "**monthly_volume_band**: ... Compared with the case's Monthly Volume Band".
24. **When the case's own value (wage level or h1bDep) is NULL, only a rule with a NULL criterion for that attribute can match** (64).
    75: "When the case's own value is NULL (its wage level or its h1bDep), only a rule whose criterion for that attribute is NULL can match it."
25. **The governing rule is the most specific matching rule: the most non-NULL criteria among its six** (65).
    75: "The governing rule is the most specific matching rule: the one with the most non-NULL criteria among its six."
26. **A tie on that count goes to the lowest `rule_id`** (65).
    75: "If several matching rules tie on that count, the one with the lowest rule_id governs." (worked in the rule 145/210 example)
27. **Only the governing rule's amounts are used; other matching rules are never added or averaged** (65).
    75: "Only the governing rule's base_hours and position_hours are used. The other matching rules are ignored; their amounts are never added or averaged."
28. **No match: the Default Review Schedule applies, with base_hours 3.50 and position_hours 0.50** (65).
    75: "A case that matches no rule is charged by the Default Review Schedule: base_hours 3.50 and position_hours 0.50."
29. **A defaulted case has no governing rule_id (NULL)** (65).
    75: "Such a case has no governing rule_id (it is NULL)."
30. **CRH = base_hours + position_hours x headCt, using the governing rule's amounts (or the default's)** (66).
    73 step 4: "CRH = base_hours + position_hours * headCt"; 75 applies the default amounts in the same formula ("CRH = 3.50 + 0.50 * 2 = 4.50").
31. **headCt is `cases.headCt`, the worker positions requested** (66).
    73 step 4: "its position_hours for each worker position requested (cases.headCt)".
32. **CRH is in reviewer-hours** (66).
    73 step 4: "CRH is in reviewer-hours."
33. **CRH is exact to two decimal places and is not rounded before it is aggregated** (66).
    75: "CRH is exact to two decimal places and is not rounded before it is aggregated."
34. **ERL is the sum of CRH over the employer's cases whose Start Month lies in the period** (67).
    73: "The Employer Review Load (ERL) of an employer for a period is the sum of the CRH of the employer's cases whose Start Month lies in that period."
35. **A period is a calendar month, or a calendar year meaning all twelve Start Months of that year** (67).
    73: "A period is a calendar month, or a calendar year, meaning all twelve Start Months of that year."
36. **An average CRH is the sum of CRH divided by the number of cases** (67).
    73: "The average CRH of a set of cases is their total CRH divided by the number of cases."
37. **Totals and averages are rounded only when reported** (67).
    75: "Totals and averages are rounded only when reported."; 73's example reports 3.5333... "as 3.53 when rounded to two decimals".

## C: Wage Assurance (clean 68-72)

38. **The wage record is reached through the case's single `case_worksite` row (`dockKey = fileKey`) and `wageTrack = trackNo`** (68).
    77: "Each case has a single case_worksite row (case_worksite.dockKey = cases.fileKey), and that row leads to the case's wage record through case_worksite.wageTrack = prevailing_wage.trackNo."
39. **The offered amount is the 'from' field of the 'offered_wage' object in `wage_details`; remove '$' and commas and read the rest as a number** (68).
    77: "**offered_wage.from** (the 'from' field of the 'offered_wage' object): text. The offered amount, such as '$130000.00'. Remove '$' and any commas and read the rest as a number."
40. **'to' is ignored** (68).
    77: "**offered_wage.to**: not used."
41. **The prevailing amount is the 'value' field of the 'prevailing_wage' object; remove 'USD ' and commas** (68).
    77: "**prevailing_wage.value** (the 'value' field of the 'prevailing_wage' object): text. The prevailing amount, such as 'USD 116,979.00'. Remove 'USD ' and any commas and read the rest as a number."
42. **Each amount is multiplied by the factor for the 'unit' field of its own object** (68).
    76 step 1: "multiplying each by the factor for its own pay unit"; 77: "Each amount uses the unit of its own object, so the two units need not be the same."
43. **The factors are Hour 2080, Week 52, Month 12, Year 1** (68).
    76 step 1: "Hour 2080, Week 52, Month 12, Year 1"; 77: "The unit factors, to US dollars per year, are Hour 2080, Week 52, Month 12 and Year 1."
44. **AOW and APW are in US dollars per year ('$41.00' hourly gives 85,280)** (68).
    76 step 1: "This gives the Annualized Offered Wage (AOW) and the Annualized Prevailing Wage (APW), both in US dollars per year. For example, an hourly offer of '$41.00' gives AOW = 41.00 * 2080 = 85,280."
45. **WHR = (AOW - APW) / APW** (69).
    76 step 2: "WHR = (AOW - APW) / APW"
46. **WHR is a fraction (0.15 means 15% above the prevailing wage)** (69).
    76 step 2: "the offer's margin over the prevailing wage, as a fraction" and "A WHR of 0.15 means the offer is 15% above the prevailing wage."
47. **WHR is computed per case without rounding** (69).
    78: "WHR is computed per case without rounding".
48. **Headroom Points = 100 x min(max(WHR, 0), 0.40) / 0.40** (69).
    76 step 3: "As one formula, Headroom Points = 100 * min(max(WHR, 0), 0.40) / 0.40."
49. **A WHR at or below 0 earns 0, a WHR of 0.40 or more earns 100, values in between scale linearly (0.10 earns 25)** (69).
    76 step 3: "A WHR at or below 0 earns 0 points, a WHR of 0.40 or more earns 100 points, and values in between scale linearly. ... A WHR of 0.10 earns 25 points."; 78 Capping.
50. **CWAS = 0.7 x Headroom Points + 0.3 x Level Points** (70).
    76 step 5: "CWAS = 0.7 * Headroom Points + 0.3 * Level Points"
51. **Level Points come from the 'level' field of the 'prevailing_wage' object: I = 25, II = 50, III = 75, IV = 100** (70).
    76 step 4: "'I' = 25, 'II' = 50, 'III' = 75, 'IV' = 100."; 77: "**prevailing_wage.level**: ... it sets the Level Points."
52. **Level Points are 50 when the level is NULL** (70).
    78: "When the 'level' field of the 'prevailing_wage' object is NULL, the case scores 50 Level Points."
53. **The weights 0.7 and 0.3 sum to 1** (70).
    76 step 5: "weighting headroom at 0.7 and level at 0.3 (the two weights sum to 1)".
54. **CWAS is not rounded (level II with WHR 0.10 scores 32.5)** (70).
    78: "CWAS is not rounded"; 76 step 5: "a level 'II' case with a WHR of 0.10 scores 0.7 * 25 + 0.3 * 50 = 17.5 + 15 = 32.5."
55. **CWAS is on a 0-100 scale** (70, description).
    76: "Each case gets a Case Wage Assurance Score (CWAS) on a 0-100 scale".
56. **EWAS = sum(CWAS x headCt) / sum(headCt) over the employer's cases whose Start Month lies in the period** (71).
    76: "The Employer Wage Assurance Score (EWAS) is the headcount-weighted average CWAS of the employer's cases whose Start Month lies in the period: EWAS = sum(CWAS * headCt) / sum(headCt)".
57. **headCt comes from `cases.headCt`; a headCt 3 case counts three times as much as a headCt 1 case** (71).
    76: "where headCt is cases.headCt, the worker positions requested."; 78: "EWAS weights each case by its headCt: a case with headCt 3 counts three times as much as a case with headCt 1."
58. **The employer is the exact text of `cases.homeFirm`, with no case folding or trimming** (71).
    78: "The employer is the exact text of cases.homeFirm, with no case folding or trimming: 'Amazon.com Services LLC' and 'AMAZON.COM SERVICES LLC' are different employers."
59. **The Start Month is the calendar year and month of `cases.beginDay`, DD/MM/YYYY text ('01/04/2024' is April 2024)** (71).
    76: "The Start Month of a case is the calendar year and month of cases.beginDay, stored as text in DD/MM/YYYY format ('01/04/2024' is April 2024)."
60. **A period is a calendar month, or a calendar year meaning all twelve Start Months of that year** (71).
    76: "A period is a calendar month, or a calendar year, meaning all twelve Start Months of that year".
61. **With no period stated, EWAS covers all of the employer's cases** (71).
    76: "when no period is stated, EWAS covers all of the employer's cases."
62. **EWAS uses unrounded CWAS values** (71).
    78: "EWAS uses the unrounded CWAS values."
63. **The tier follows the unrounded EWAS** (72).
    78: "The tier follows the unrounded EWAS."
64. **Thin when EWAS < 30** (72).
    76: "**Thin**: EWAS below 30;"
65. **Adequate when 30 <= EWAS < 45** (72).
    76: "**Adequate**: EWAS from 30 up to but not including 45;"
66. **Strong when 45 <= EWAS < 60** (72).
    76: "**Strong**: EWAS from 45 up to but not including 60;"
67. **Exemplary when EWAS >= 60** (72).
    76: "**Exemplary**: EWAS of 60 or more."
68. **Lower bounds are inclusive and upper bounds exclusive** (72).
    78: "Lower bounds are inclusive and upper bounds exclusive: an EWAS of exactly 30 is Adequate, exactly 45 is Strong and exactly 60 is Exemplary."
69. **A tier exists only for an employer with at least one case in the period** (72).
    78: "A tier exists only for an employer with at least one case in the period."
70. **Scores and averages are rounded only when reported** (72).
    78: "Scores and averages are rounded only when reported."

## Worked examples (made-up numbers), checked

- 73: CRH = 2.75 + 0.40 * 3 = 3.95; ERL = 3.95 + 4.15 + 2.50 = 10.60; average = 10.60 / 3 = 3.5333 (3.53 to two decimals).
- 73: EMCV 1, 6 and 15 give bands '1', '5-14' and '15+'.
- 75: rules 145 and 210 both have 3 criteria and rule 388 has 1; rule 145 governs as the lower id; CRH = 3.25 + 0.30 * 2 = 3.85. Default: 3.50 + 0.50 * 2 = 4.50.
- 76: 41.00 * 2080 = 85,280; 0.7 * 25 + 0.3 * 50 = 32.5; 1,725 * 52 = 89,700; 11,700 / 78,000 = 0.15; 100 * 0.15 / 0.40 = 37.5; 0.7 * 37.5 + 0.3 * 75 = 48.75.
- 76: EWAS = (48.75 + 60.00 + 70.00) / 5 = 35.75, which is Adequate.
- 78: a WHR of 0.55 gives 100 points (capped); a WHR of -0.05 gives 0.

Result: all 70 facts are placed. None is missing, and none is contradicted.
