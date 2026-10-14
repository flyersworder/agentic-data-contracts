# solar_panel: facts of the clean R and C, and where the styled text states them

Clean entries: KB 50-55 (R) and 56-61 (C) in `knowledge.json`. Styled entries: KB 62-64 (R) and
65-67 (C) in `knowledge_styled.json`. Each fact gives the clean entry it comes from, then the
styled entry and the sentence that states it (quoted verbatim).

## R: module warranty settlement

1. Grain: a snapshot is one row of plant_record, and each is settled by its panel manufacturer (clean 55 description, 53). Styled 62: "Every performance snapshot (one row of plant_record) is settled by the manufacturer of its plant's panels"
2. Settlement Capacity Band source: plants.cap_mw, in MW, of the snapshot's plant (clean 50). Styled 62: "Groups the plant by its installed capacity, plants.cap_mw (in MW) of the plant the snapshot belongs to"
3. Capacity band join: plant_record.sitetie = plants.sitekey (clean 50). Styled 62: "(plant_record.sitetie = plants.sitekey): under 100 MW is 'Small'"
4. 'Small' if cap_mw < 100 (clean 50). Styled 62: "under 100 MW is 'Small'"
5. 'Mid' if 100 <= cap_mw < 300 (clean 50). Styled 62: "100 MW up to but not including 300 MW is 'Mid'"
6. 'Large' if cap_mw >= 300 (clean 50). Styled 62: "300 MW or more is 'Large'"
7. Capacity band inclusivity: lower inclusive, upper exclusive; exactly 100 is Mid, exactly 300 is Large (clean 50). Styled 64: "Settlement Capacity Band: lower bounds are inclusive and upper bounds exclusive, so exactly 100 MW is 'Mid' and exactly 300 MW is 'Large'."
8. Manufacturer group source: panel_models.makertag of the panel model recorded on the snapshot's plant (clean 51). Styled 62: "its manufacturer and technology, panel_models.makertag and panel_models.pnlkind of the panel model recorded on its plant" and "The manufacturer group is the makertag reached by the join above"
9. Join path to the panel model: plant_record.sitetie = plants.sitekey, plants.modhook = panel_models.modkey (clean 51, 53). Styled 62: "join plant_record.sitetie = plants.sitekey, then plants.modhook = panel_models.modkey"
10. plant_panel_model is not used (clean 51). Styled 62: "the plant_panel_model table is not used"
11. NULL makertag forms the group 'Unlisted' (clean 51). Styled 62: "a snapshot whose model has a NULL makertag is in the group 'Unlisted'"
12. Calendar month = year and month of plant_record.snapts (clean 51). Styled 62: "in the same calendar month (year and month of plant_record.snapts)"
13. Manufacturer Monthly Claim Volume V_s = number of plant_record rows with the same calendar month and same manufacturer group as s (clean 51). Styled 62: "for a snapshot s, V_s is the number of rows of plant_record in the same calendar month (year and month of plant_record.snapts) and the same manufacturer group as s"
14. s itself is counted, V_s >= 1 (clean 51). Styled 62: "s itself included, so V_s is at least 1"
15. The counted entity is the manufacturer group, not the plant (clean 51: the count is over the manufacturer group). Styled 62: "The count covers the group's snapshots at all its plants, not one plant's."
16. Claim Volume Tier 'Single' if V_s = 1 (clean 52). Styled 62: "The tier is 'Single' when V_s = 1"
17. 'Moderate' if V_s is 2 or 3 (clean 52). Styled 62: "'Moderate' when V_s is 2 or 3"
18. 'Bulk' if V_s >= 4 (clean 52). Styled 62: "'Bulk' when V_s is 4 or more"
19. All snapshots of one manufacturer group in one calendar month share a tier (clean 52). Styled 62: "so all snapshots of one group in one calendar month share a tier"
20. Each row of warranty_settle_rules is a rule with six criteria (clean 53). Styled 62: "Each rule has six criteria, one per characteristic." and 63: "The schedule warranty_settle_rules holds one module warranty settlement rule per row. Six fields are criteria"
21. A criterion is satisfied when it is NULL or equals the snapshot's value exactly (clean 53). Styled 62: "A snapshot satisfies a criterion when the criterion is NULL or equals the snapshot's value exactly"
22. A NULL criterion matches any value, including a missing one (clean 53). Styled 64: "A NULL criterion matches any value, including a missing one." and 63: "a NULL criterion means the rule is not restricted on that characteristic and applies to any value"
23. maker_tag is compared with panel_models.makertag (clean 53). Styled 63: "**maker_tag**: text. Panel manufacturer, compared with panel_models.makertag."
24. pnl_kind is compared with panel_models.pnlkind, through plants.modhook (clean 53). Styled 63: "**pnl_kind**: text. Panel technology, compared with panel_models.pnlkind." with the path in 62 (fact 9)
25. alrt_state is compared with alert.alrtstate, alert.snapalrt = plant_record.snapkey (clean 53). Styled 63: "Snapshot alert status, compared with alert.alrtstate." and 62: "its alert state, alert.alrtstate (alert.snapalrt = plant_record.snapkey)"
26. warr_state is compared with plants.warrstate (clean 53). Styled 63: "Plant equipment warranty status, compared with plants.warrstate."
27. cap_band is compared with the Settlement Capacity Band (clean 53). Styled 63: "**cap_band**: text. Settlement Capacity Band of the plant."
28. vol_tier is compared with the Claim Volume Tier (clean 53). Styled 63: "**vol_tier**: text. Claim Volume Tier of the manufacturer group in the snapshot's calendar month."
29. A non-NULL criterion is never satisfied by a NULL snapshot value (clean 53). Styled 64: "A non-NULL criterion is never satisfied by a NULL snapshot value."
30. An 'Unlisted' snapshot, or one with no alert or warranty state, satisfies only a NULL criterion there (clean 53). Styled 64: "An 'Unlisted' snapshot (NULL makertag) therefore satisfies only a NULL maker_tag, and a snapshot with no alert state or no warranty state satisfies only a NULL alrt_state or warr_state."
31. A rule matches when all six criteria are satisfied (clean 53). Styled 62: "a rule matches when all six are satisfied"
32. Specificity = number of non-NULL criteria, 0 to 6 (clean 53). Styled 64: "A rule's specificity is its number of non-NULL criteria, from 0 to 6."
33. Precedence: of the matching rules, the highest specificity governs (clean 53). Styled 64: "Of the rules a snapshot matches, the one with the highest specificity governs."
34. Tie-break: lowest rule_id (clean 53). Styled 64: "When several matching rules share the highest specificity, the lowest rule_id governs."
35. Default: no matching rule means no governing rule, settled on the Standard Settlement Terms (clean 53, 54). Styled 64: "A snapshot that matches no rule has no governing rule and is settled on the Standard Settlement Terms"
36. Standard Settlement Terms: 300.00 USD fixed fee and 5.00% coverage, in place of fixed_fee_usd and cover_pct (clean 54). Styled 64: "a fixed handling fee of 300.00 USD and a coverage percentage of 5.00%, used in place of fixed_fee_usd and cover_pct"
37. Under the Standard Settlement Terms the rule is reported as none (NULL) (clean 54). Styled 64: "Its rule is reported as none (NULL)."
38. Formula S = F + c/100 * C_repl (clean 55). Styled 62: "`S = F + c / 100 * C_repl`"
39. F = fixed_fee_usd and c = cover_pct of the governing rule, or the Standard Settlement Terms when there is none (clean 55). Styled 62: "F is fixed_fee_usd and c is cover_pct of the snapshot's governing rule (the Standard Settlement Terms when it has none)"
40. C_repl = operational_metrics.replcost, joined by operational_metrics.snapops = plant_record.snapkey (clean 55). Styled 62: "C_repl is operational_metrics.replcost (operational_metrics.snapops = plant_record.snapkey)"
41. Units: C_repl, F and S are in USD; cover_pct is a percentage divided by 100 (clean 55). Styled 62: "All amounts are in USD." and 63: "Fixed handling fee per settled snapshot, in US dollars (F)." and "(c); 8.00 means 8%."
42. NULL replcost counts as 0 (clean 55). Styled 64: "A NULL replacement cost (operational_metrics.replcost) counts as 0."
43. S is not rounded per snapshot (clean 55). Styled 64: "S is not rounded per snapshot."
44. Every row of plant_record is settled (clean 55). Styled 64: "Every row of plant_record is settled."
45. Aggregation: a total over several snapshots is the sum of the unrounded per-snapshot amounts (clean 55). Styled 64: "is the sum of the unrounded per-snapshot amounts"
46. Grain of the monthly total: manufacturer group, 'Unlisted' included, and calendar month of snapts (clean 55). Styled 64: "such as a manufacturer's monthly settlement (by manufacturer group, 'Unlisted' included, and calendar month of snapts)"
47. Rounding: round only the reported result, to 2 decimal places (clean 55). Styled 64: "Round only the reported result, to 2 decimal places."

## C: technology energy loss

48. Potential Snapshot Yield formula E_pot = E / ((A/100) * (1 - L_soil/100)) (clean 56). Styled 65: "`E_pot = E / ((A / 100) * (1 - L_soil / 100))`"
49. E = energy_yield.energy_yield_kwh (clean 56). Styled 66: "**energy_yield.energy_yield_kwh** (E): the energy the snapshot actually yielded, in kWh."
50. A = energy_yield.availability_pct (clean 56). Styled 66: "**energy_yield.availability_pct** (A): availability, a percentage on a 0 to 100 scale."
51. L_soil = efficiency.soil_loss_pct (clean 56). Styled 66: "**efficiency.soil_loss_pct** (L_soil): soiling loss, a percentage on a 0 to 100 scale."
52. All three from the JSON column electrical_performance.elec_perf_snapshot, electrical_performance.snaplink = plant_record.snapkey (clean 56). Styled 66: "**electrical_performance.elec_perf_snapshot**: JSON. The snapshot's electrical performance record (electrical_performance.snaplink = plant_record.snapkey). Three of its fields are used:"
53. Read each as text (->>) and cast to double precision before dividing (clean 56). Styled 66: "Some of these JSON numbers are stored as integers, so read each as text (->>) and cast it to double precision before dividing."
54. Units: E and E_pot in kWh; A and L_soil percentages on a 0 to 100 scale (clean 56). Styled 65: "E is the snapshot's energy yield in kWh, A its availability percentage and L_soil its soiling loss percentage (see Soiling Loss Index). E_pot is in kWh." and 66 (facts 50, 51)
55. Any of the three NULL means no potential yield (clean 56). Styled 67: "A snapshot with any of E, A or L_soil NULL has no Potential Snapshot Yield."
56. E_pot and E_loss are not rounded (clean 56, 57). Styled 67: "The Potential Snapshot Yield and the Recoverable Energy Loss are not rounded."
57. Recoverable Energy Loss E_loss = E_pot - E of the same snapshot, in kWh (clean 57). Styled 65: "`E_loss = E_pot - E`, the Potential Snapshot Yield less the actual energy yield of the same snapshot, both in kWh"
58. E_loss is defined only where E_pot is (clean 57). Styled 67: "Its Recoverable Energy Loss, defined only where the Potential Snapshot Yield is, is missing too"
59. Grouping by panel technology = panel_models.pnlkind of the plant's model, via plant_record.sitetie = plants.sitekey, plants.modhook = panel_models.modkey, plant_panel_model not used (clean 58, 60). Styled 66: "**panel_models.pnlkind**: text. The panel technology of the panel model recorded on the snapshot's plant: join plant_record.sitetie = plants.sitekey, then plants.modhook = panel_models.modkey. The plant_panel_model table is not used."
60. Grouping by calendar month = year and month of plant_record.snapts (clean 58). Styled 66: "Its year and month give the calendar month" and 65: "Snapshots are grouped by panel technology and calendar month."
61. Ratio R = 100 * sum(E_loss) / sum(E_pot) per technology-month (clean 58). Styled 65: "`R = 100 * sum(E_loss) / sum(E_pot)`"
62. Sums are over the group's snapshots that have a Potential Snapshot Yield (clean 58). Styled 65: "both sums over the group's snapshots that have a Potential Snapshot Yield" and 67: "the snapshot is left out of both sums of the ratio"
63. Ratio of sums, not an average of per-snapshot ratios; snapshots with more potential weigh more (clean 58). Styled 65: "It is a ratio of sums, not an average of per-snapshot ratios, so snapshots with more potential energy weigh more."
64. R is a percentage (clean 58). Styled 65: "R is a percentage."
65. R is reported rounded to 2 decimal places but classified unrounded (clean 58, 59). Styled 67: "The Technology Monthly Energy Loss Ratio is reported rounded to 2 decimal places but classified unrounded."
66. A technology with no snapshot in a month has no ratio, not zero (clean 58). Styled 67: "A technology with no snapshot in a month has no ratio for that month, not zero."
67. Energy Loss Band 'Low' if R < 6 (clean 59). Styled 65: "'Low' under 6"
68. 'Moderate' if 6 <= R < 10 (clean 59). Styled 65: "'Moderate' from 6 up to but not including 10"
69. 'Elevated' if 10 <= R < 14 (clean 59). Styled 65: "'Elevated' from 10 up to but not including 14"
70. 'Severe' if R >= 14 (clean 59). Styled 65: "'Severe' at 14 or more"
71. Band inclusivity: lower inclusive, upper exclusive (clean 59). Styled 67: "Energy Loss Band lower bounds are inclusive and upper bounds exclusive: exactly 6 is 'Moderate', 10 'Elevated', 14 'Severe'."
72. The band belongs to the technology-month, and every snapshot in that group carries it (clean 59). Styled 65: "The band belongs to the technology-month, and every snapshot in that group carries it."
73. Technology Recoverable Energy E_rec = sum of E_loss over a technology's snapshots whose snapts falls in the period, in kWh (clean 60). Styled 65: "the sum of E_loss over its snapshots whose plant_record.snapts falls in the period, in kWh"
74. E_rec is rounded to 2 decimal places only after summing (clean 60). Styled 67: "The Technology Recoverable Energy is rounded to 2 decimal places only after summing."
75. Over one calendar month E_rec is the ratio's numerator (clean 60). Styled 65: "Over one calendar month it is the numerator of that technology's Technology Monthly Energy Loss Ratio."
76. Energy Loss Ratio Interpretation: 12% means 12 of every 100 potential kWh lost and 88 delivered (clean 61). Styled 67: "A ratio of 12% means that of every 100 kWh the technology's snapshots in that month could have yielded with full availability and clean panels, 12 kWh were lost and 88 kWh delivered."
77. Interpretation of the thresholds: below 6% means a well-maintained, clean fleet; 14% or more means systematic soiling or downtime (clean 61). Styled 67: "Ratios below 6% indicate a well-maintained, clean fleet; ratios of 14% or more point to systematic soiling or downtime that cleaning or repair should address."
78. Meaning of the concepts: E_pot is the yield with full availability and perfectly clean panels; E_loss is energy lost to downtime and soiling that repair and cleaning could recover (clean 56, 57 descriptions). Styled 65: "The Potential Snapshot Yield is the energy a snapshot would have yielded had the system been fully available and the panels perfectly clean." and "The Recoverable Energy Loss is the energy a snapshot lost to downtime and soiling, which repair and cleaning could recover"

## Worked examples (arithmetic checked)

- 62: rule 17 specificity 1, rules 40 and 85 specificity 2, rule 130 fails on alrt_state; rule 40 governs by the lower rule_id. S = 150.00 + 0.08 * 12,500.00 = 1,150.00.
- 64: Standard Settlement Terms S = 300.00 + 0.05 * 4,200.00 = 510.00. Monthly total 1,150.00 + 642.377 + 87.127 = 1,879.504, reported 1,879.50; rounding each first gives 1,150.00 + 642.38 + 87.13 = 1,879.51.
- 65: E_pot = 450 / 0.864 = 520.833 and 1,140 / 0.9025 = 1,263.158; E_loss = 70.833 and 123.158; R = 100 * 193.991 / 1,783.991 = 10.874 ('Elevated'); E_rec = 193.99; mean of per-snapshot ratios (13.600 + 9.750) / 2 = 11.675, shown as 11.68.
- 67: 9.996 is reported as 10.00 and classified 'Moderate' (9.996 < 10).
