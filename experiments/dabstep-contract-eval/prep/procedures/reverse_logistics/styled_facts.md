# reverse_logistics: facts of the clean R and C, placed in the styled version

The clean entries are KB 30-34 (R) and 35-39 (C) in `knowledge.json`. The styled entries in
`knowledge_styled.json` replace them:

- R: 40 Return Handling Charge (RHC) Guide, 41 Return Handling Charge Schedule, 42 Return
  Handling Charge Notes.
- C: 43 Customer Leakage Ratio (CLR) Guide, 44 Customer Leakage Ratio Data Reference, 45
  Customer Leakage Ratio Notes.

Each line gives a fact of the clean version and its source entry, then the styled entry and the
sentence that states it.

## R, Return Handling Charge

1. **Rule table.** `return_charge_rules` holds one rule per row, and `rule_id` identifies it (30).
   - 41: "The Return Handling Charge Schedule is the table return_charge_rules; each row is one rule."
   - 41: "**rule_id**: integer. Identifier of the rule".
2. **Segment criterion.** `customer_segment` is compared with `customers.seg_category` of the
   return's customer (30).
   - 41, customer_segment bullet: "Compared with customers.seg_category of the return's customer."
3. **Channel criterion.** `return_channel` is compared with `returns.return_channel` (30).
   - 41, return_channel bullet: "Compared with returns.return_channel."
4. **Category criterion.** `item_category` is compared with `products.itemcategory`, joined by
   `returns.itemlink = products.itemcode` (30).
   - 41, item_category bullet: "Compared with products.itemcategory of the returned item
     (returns.itemlink = products.itemcode)."
5. **Reason criterion.** `primary_reason` is compared with JSON path `reasoning.primary_reason` in
   `returns.return_details` (30).
   - 41, primary_reason bullet: "Compared with the text at JSON path reasoning.primary_reason in
     returns.return_details."
6. **Condition criterion.** `item_condition` is compared with JSON path `condition.item_condition`
   in `quality_assessment.assessment_summary`, joined by `quality_assessment.inspectref =
   returns.casenum` (30).
   - 41, item_condition bullet: "Compared with the text at JSON path condition.item_condition in
     quality_assessment.assessment_summary (quality_assessment.inspectref = returns.casenum)."
7. **Band criterion.** The band from `min_monthly_returns` to `max_monthly_returns` is compared with
   the return's CMRC (30).
   - 41: "**min_monthly_returns**, **max_monthly_returns**: integers. The lowest and the highest
     Customer Monthly Return Count the rule covers".
   - 40: "the return's CMRC must fall within the rule's monthly band, from min_monthly_returns to
     max_monthly_returns".
8. **Charge terms.** The charge terms are `flat_charge` and `charge_pct` (30).
   - 41, flat_charge and charge_pct bullets.
9. **Customer.** The customer of a return is `orders.buyerlink`, joined by `returns.srctxn =
   orders.txnnum` (31).
   - 40, CMRC: "The customer of a return is orders.buyerlink of its order (returns.srctxn =
     orders.txnnum)."
10. **Month.** The month is the calendar month of `returns.revdate`, a 'YYYY-MM-DD' text whose first
    7 characters give 'YYYY-MM' (31).
    - 40, CMRC: "The month of a return is the calendar month of returns.revdate, a 'YYYY-MM-DD' text
      whose first seven characters give 'YYYY-MM'."
11. **CMRC count.** CMRC counts the rows of `returns` with the same customer and month, the return
    itself included (31).
    - 40: "Count every row of returns with the same customer and month, the return itself included".
12. **CMRC properties.** CMRC is at least 1, and it is the same for every return of that
    customer-month (31).
    - 40: "so the CMRC is at least 1 and is the same for all of a customer's returns in a month."
    - The example of dates 01-03, 01-17, 01-30 and 02-02 gives CMRC 3 for each January return and
      1 for the February one, which is correct.
13. **All criteria.** A rule matches when all of its criteria match (32).
    - 40: "A rule matches a return when all of its criteria match."
14. **NULL criterion.** A NULL text criterion matches anything (32).
    - 42: "A NULL text criterion (customer_segment, return_channel, item_category, primary_reason or
      item_condition) matches any value, an empty one included."
15. **Exact match.** A non-NULL text criterion must equal the return's value exactly (32).
    - 40: "Each of the rule's five text criteria must equal, exactly, the return's corresponding
      value". 40 also points to the Notes for empty criteria.
    - 42: "A non-NULL text criterion matches only a return whose value equals it exactly".
16. **NULL on the record.** A non-NULL criterion never matches a NULL record value, for example a
    NULL `seg_category` (32).
    - 42: "and never a return whose value is NULL: a customer whose seg_category is NULL is matched
      only by rules whose customer_segment is NULL."
17. **Band bounds.** The band matches when min <= CMRC <= max, both bounds inclusive (32).
    - 40: "both ends included: a band of 3 to 4 matches a CMRC of 3 or 4 and no other."
    - 41: "both included: 5 and 7 cover a customer with 5, 6 or 7 returns in the month."
18. **NULL bound.** A NULL bound sets no limit on its side (32).
    - 42: "A NULL min_monthly_returns sets no lower limit and a NULL max_monthly_returns no upper
      limit, so with both NULL the band matches any CMRC."
19. **One rule.** At most one rule applies (33).
    - 42: "At most one rule applies to a return".
    - 40: "At most one rule sets the charge, even when several match".
20. **Precedence.** The highest specificity wins (33).
    - 42: "of the rules that match it, the one with the highest specificity".
21. **Tie-break.** Ties go to the lowest `rule_id` (33).
    - 42: "ties broken by the lowest rule_id."
    - The example has rules 140 and 155 tied at specificity 3, ahead of rule 112 at 2, and 140
      applies. This is correct.
22. **Specificity, text part.** Specificity counts each non-NULL text criterion as 1 (33).
    - 42: "Specificity counts 1 for each non-NULL text criterion".
23. **Specificity, band part.** The band adds 1 if either bound is non-NULL, so specificity runs
    from 0 to 6 (33).
    - 42: "plus 1 for the band if min_monthly_returns or max_monthly_returns is non-NULL, so it runs
      from 0 to 6."
24. **Default.** A return that matches no rule takes the Standard Handling Charge, with
    `flat_charge` 15.00 and `charge_pct` 5.0 (33).
    - 42: "The return pays the Standard Handling Charge, a flat charge of 15.00 and a charge
      percentage of 5.0 in the same formula".
    - The example is 15.00 + 4.62 = 19.62, which is correct.
25. **Default rule id.** A default return has no applying rule id (33).
    - 42: "and has no applying rule id."
26. **Formula.** RHC = F + P/100 * V (34).
    - 40: "RHC = flat_charge + charge_pct * unit_value / 100", and in words: "the applying rule's
      flat charge plus its charge percentage of the returned item's unit value".
    - The example is 6.50 + 11.088 = 17.588, which is correct.
27. **F and P.** F and P come from the applying rule, or from the Standard Handling Charge (34).
    - 40: "the applying rule's flat charge plus its charge percentage".
    - 42: "in the same formula".
28. **V.** V is `products.unit_value` of the returned item (34).
    - 40: "where unit_value is products.unit_value of the returned item".
29. **Unit.** RHC is in the currency units of `unit_value` (34).
    - 40: "the charge is in the currency units of unit_value."
30. **Grain.** Every row of `returns` has an RHC (34).
    - 40: "Every row of returns has an RHC."
31. **Period total.** A customer's or group's RHC over a period is the sum over its returns whose
    `revdate` falls in the period (34).
    - 40: "A customer's or group's RHC over a period is the sum over its returns whose revdate falls
      in the period".
32. **Period average.** The average is that sum divided by the number of those returns (34).
    - 40: "and its average is that sum divided by the number of those returns."
33. **Rounding.** There is no intermediate rounding, and only the reported result is rounded, to 2
    decimals (34).
    - 40: "Round only the reported result, to 2 decimals, never an intermediate figure".
    - The example is 29.924 -> 29.92 against 29.93 when each return is rounded first, and an
      average of 14.962 -> 14.96. Both are correct.

## C, Customer Leakage Ratio

34. **Customer.** The customer is `orders.buyerlink`, joined by `returns.srctxn = orders.txnnum`
    (35).
    - 43, Customer Return Month: "The customer of a return is orders.buyerlink of its order
      (returns.srctxn = orders.txnnum)."
35. **Month.** The month is the calendar month of `revdate`, whose first 7 characters give
    'YYYY-MM' (35).
    - 43: "The month of a return is the calendar month of returns.revdate, a 'YYYY-MM-DD' text whose
      first seven characters give 'YYYY-MM'."
36. **Customer Return Month.** It is the set of rows with one customer and one month, and it exists
    only for months that have at least one return (35).
    - 43: "A Customer Return Month is the set of rows in returns with one customer and one month; a
      customer has one only for the months in which it has at least one return."
37. **IRV formula.** IRV = V * A/10 * K (36).
    - 43, IRV bullet: "Take the unit value, scale it by the aesthetic score out of 10, then by the
      completeness factor".
    - 44: "IRV = V * A / 10 * K".
38. **V.** V is `products.unit_value`, joined by `returns.itemlink = products.itemcode` (36).
    - 44: "**Unit value (V)**: products.unit_value of the returned item (returns.itemlink =
      products.itemcode)."
39. **A, source.** A is JSON path `results.aesthetic_score` in
    `quality_assessment.assessment_summary`, joined by `inspectref = casenum` (36).
    - 44: "the number at JSON path results.aesthetic_score in quality_assessment.assessment_summary
      (quality_assessment.inspectref = returns.casenum)".
40. **A, range and type.** A runs from 1 to 10 and is read as a decimal (36).
    - 44: "from 1 to 10, read as a decimal."
41. **K, source.** K comes from JSON path `condition.completeness` in the same assessment_summary
    (36).
    - 44: "the text at JSON path condition.completeness in the same assessment_summary, which sets
      the completeness factor (K)".
42. **K values.** K is 1.00 for 'Complete', 0.85 for 'Accessories Missing' and 0.60 for 'Missing
    Parts' (36).
    - 43, IRV bullet: "1.00 for 'Complete', 0.85 for 'Accessories Missing', 0.60 for 'Missing
      Parts'."
    - 44 gives the same values as a table.
43. **IRV unit.** IRV is in the currency units of `unit_value` (36).
    - 44: "All amounts are in the currency units of products.unit_value."
44. **RVL formula.** RVL = V - IRV + S_f (37).
    - 43, RVL bullet: "(unit value minus IRV) plus the shipping fee".
    - 44: "RVL = V - IRV + S_f".
45. **S_f.** S_f is JSON path `shipping.fee` in `returns.return_details`, read as a decimal (37).
    - 44: "**Shipping fee (S_f)**: the amount at JSON path shipping.fee in returns.return_details
      (see Shipping Fee), read as a decimal."
46. **RVL grain and unit.** RVL is computed for every row of `returns`, in the currency units of
    `unit_value` (37).
    - 44: "Every row of returns has an IRV and an RVL."
    - 44: "All amounts are in the currency units of products.unit_value."
47. **CLR formula.** CLR = sum RVL / sum V over the same returns (38).
    - 43, CLR bullet: "add up the RVL of its returns and divide by the sum of the unit values of
      the same returns."
    - 45: "The CLR divides the sum of RVL by the sum of V, both over the same returns".
48. **Ratio of sums.** CLR is a ratio of sums, not an average of ratios (38).
    - 45: "a ratio of sums, not an average of the returns' own ratios."
    - 43, example: "Averaging the two returns' own ratios (0.26 and 0.82) would give 0.54, which is
      not the CLR."
    - The example has IRV 160 and 18, RVL 52 and 41, and CLR 93/250 = 0.372. All are correct.
49. **CLR grain.** CLR is computed per Customer Return Month (38).
    - 45: "The CLR is computed per Customer Return Month."
    - 43: "For a Customer Return Month, add up..."
50. **CLR over another period.** A customer's or group's CLR over another period takes both sums
    over its returns whose `revdate` falls in the period (38).
    - 45: "A customer's or group's CLR over another period, such as a year, takes both sums over its
      returns whose revdate falls in that period."
51. **RVL over a period.** RVL over a period is the sum, and its average is the sum divided by the
    number of returns (38).
    - 45: "Its RVL over a period is the sum of the RVL of those returns, and its average RVL is that
      sum divided by their number."
52. **Rounding.** There is no intermediate rounding, and only the reported result is rounded, to 2
    decimals (38).
    - 45, Rounding: "Do not round the IRV, the RVL or any other intermediate figure; round only the
      reported result, to 2 decimals."
53. **Class input.** The class is graded on the unrounded CLR (39).
    - 45: "The class is decided on the unrounded CLR".
    - The 0.6996 example is reported as 0.70 and is still 'Elevated'.
54. **Contained.** 'Contained' means CLR < 0.55 (39).
    - 45: "'Contained' when CLR < 0.55". 43 says "'Contained' below 0.55".
55. **Elevated.** 'Elevated' means 0.55 <= CLR < 0.70 (39).
    - 45: "'Elevated' when 0.55 <= CLR < 0.70". 43 says "from 0.55 up to 0.70" and points to the
      Notes for the edges.
56. **Severe.** 'Severe' means CLR >= 0.70 (39).
    - 45: "'Severe' when CLR >= 0.70". 43 says "at 0.70 and above".
57. **Edge inclusivity.** Lower bounds are inclusive and upper bounds exclusive (39).
    - 45: "Each band includes its lower edge and excludes its upper edge: a CLR of exactly 0.55 is
      'Elevated', one of exactly 0.70 is 'Severe'".
58. **Class grain.** The class belongs to the customer-month, so one customer can hold different
    classes in different months (39).
    - 45: "The class belongs to the Customer Return Month, not to the customer, so one customer can
      hold different classes in different months."

## Named concepts kept

Every name below still appears and is defined:

- Return Handling Charge (RHC): 40.
- Return Handling Charge Schedule: 41.
- Customer Monthly Return Count (CMRC): 40.
- Return Charge Rule Matching: 40.
- Return Charge Rule Precedence and specificity: 42.
- Standard Handling Charge: 42.
- Customer Return Month: 43.
- Inspected Resale Value (IRV): 43 and 44.
- completeness factor: 43 and 44.
- Return Value Leakage (RVL): 43 and 44.
- Customer Leakage Ratio (CLR): 43.
- Leakage Class: 43 and 45.

## Material in the styled text that is not in the clean KB

None of it changes a result.

- **Value lists in 41.** They give the segment, channel, category and reason values, and point to
  KB 22 for the conditions. The lists are copied from the column meanings of `return_charge_rules`,
  which every arm already sees, and they agree with the data. The channel list follows the rule
  table (Collection Point, Courier, Mail, Store), not KB 20's list.
- **Field types.** The types (integer, text, decimal) come from the schema.
- **Overview sentences.** They say why the charge or measure exists.
- **Worked examples.** They use made-up numbers. All of their arithmetic was checked above.

## Check

A SQL implementation written from the styled text alone
(`scratchpad/style4-rl/check.py`) reproduces the sanity numbers in NOTES.md:

| Measure | Value |
|---|---|
| Returns | 1,500 |
| Charged under a rule | 1,409 (so 91 take the standard charge) |
| Total RHC | 108,713.19 |
| Max RHC | 509.86 |
| Returns with RHC = 0 | 199 |
| Leakage classes | Contained 126, Elevated 150, Severe 121 |
