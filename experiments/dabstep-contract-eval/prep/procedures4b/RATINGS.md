# Experiment 4b: blind ratings

- labor_certification_applications g1: not settled; gpt-6-sol: {"sentinel": "*", "matches_missing": null, "settled": false}; claudeopus5.5: {"sentinel": "*", "matches_missing": true, "settled": true}
- reverse_logistics g1: not settled; gpt-6-sol: {"sentinel": "ANY", "matches_missing": null, "settled": false}; claudeopus5.5: {"sentinel": "ANY", "matches_missing": true, "settled": true}
- reverse_logistics g2: not settled; gpt-6-sol: {"recover": null, "settled": false}; claudeopus5.5: {"recover": "v - 1", "settled": true}
- reverse_logistics g3: convention-only; gpt-6-sol: {"convention": "other", "detail": "No tie-break or aggregation rule is specified; cannot determine which matching rule(s) set the charge when several match.", "settled": false}; claudeopus5.5: {"convention": "other", "detail": "Not documented. Docs [30]-[34] only say F and P come from 'a rule ... that matches'. None of them says how to choose between several matching rules, and rule order or specificity is never mentioned.", "settled": false}
- solar_panel g1: data-settled; gpt-6-sol: {"sentinel": "ALL", "matches_missing": true, "settled": true}; claudeopus5.5: {"sentinel": "ALL", "matches_missing": true, "settled": true}
- solar_panel g2: data-settled; gpt-6-sol: {"recover": "v * 100", "settled": true}; claudeopus5.5: {"recover": "v * 100", "settled": true}
