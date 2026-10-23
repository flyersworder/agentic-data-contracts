# Experiment 4b: gap checks

Each database's menu in order; the first candidate that passes is kept.

## labor_certification_applications

- G1[0] `*`: bite 100% of R (0 loud). PASS
- G2[0] prevailing_wage.wage_details $.offered_wage.from `v*2` (reversed relationship to the recorded maximum / doubled numeric scale): bite 0% of 10 C questions reading it (0 loud). no bound stated in the documentation for this quantity FAIL: no bound stated in the documentation for this quantity; bites 0% of the C questions reading it
- G2[1] prevailing_wage.wage_details $.offered_wage.from `v+1` (additive offset that reverses near-equal wage-range bounds): bite 0% of 10 C questions reading it (0 loud). no bound stated in the documentation for this quantity FAIL: no bound stated in the documentation for this quantity; bites 0% of the C questions reading it
- G2[2] prevailing_wage.wage_details $.prevailing_wage.value `v*2` (doubled numeric scale, breaking corroboration by wage-range endpoints): bite 0% of 10 C questions reading it (0 loud).  FAIL: a question text names it
- G2[3] prevailing_wage.wage_details $.prevailing_wage.value `v+1` (additive offset relative to independently recorded wage-range minimum): bite 0% of 10 C questions reading it (0 loud).  FAIL: a question text names it
- G2: menu exhausted; no G2 for this database.

## reverse_logistics

- G1[0] `ANY`: bite 100% of R (1 loud). PASS
- G2[0] quality_assessment.assessment_summary $.results.aesthetic_score `v-1` (zero-based ordinal offset): bite 0% of 10 C questions reading it (0 loud).  FAIL: blind raters split (round 1): gpt-6-sol said the data cannot tell, claudeopus5.5 read v + 1
- G2[1] case_management.satisfscore  `v-1` (zero-based ordinal offset): bite 0% of 0 C questions reading it (0 loud).  FAIL: no C question reads it
- G2[2] quality_assessment.assessment_summary $.results.aesthetic_score `v+1` (one-step positive ordinal offset): bite 100% of 10 C questions reading it (0 loud). stored 2..11 breaks KB 36: 'a number from 1 to 10' PASS
- G3 `sum`: differs from the gold on 100% of the 9 G3 questions; degenerate answers: 0
- G3 `lowest_id`: differs from the gold on 100% of the 9 G3 questions; degenerate answers: 0

## solar_panel

- G1[0] `*`: bite 0% of R (0 loud). FAIL: already used by another database
- G1[1] `ANY`: bite 0% of R (0 loud). FAIL: already used by another database
- G1[2] `ALL`: bite 100% of R (0 loud). PASS
- G2[0] electrical_performance.elec_perf_snapshot $.energy_yield.availability_pct `v / 100` (percentage-to-fraction scale): bite 100% of 10 C questions reading it (0 loud). stored 0.95..0.9999, all within [0, 1], against KB 56: 'a percentage on a 0 to 100 scale' PASS

