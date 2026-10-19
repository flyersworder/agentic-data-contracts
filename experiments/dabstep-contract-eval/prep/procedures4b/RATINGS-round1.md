# Experiment 4b: blind ratings

- labor_certification_applications g1: not settled; gpt-6-sol: {"sentinel": "*", "matches_missing": null, "settled": false}; claudeopus5.5: {"sentinel": "*", "matches_missing": true, "settled": true}
- reverse_logistics g1: not settled; gpt-6-sol: {"sentinel": "ANY", "matches_missing": null, "settled": false}; claudeopus5.5: {"sentinel": "ANY", "matches_missing": true, "settled": true}
- reverse_logistics g2: not settled; gpt-6-sol: {"recover": null, "settled": false}; claudeopus5.5: {"recover": "v + 1", "settled": true}
- reverse_logistics g3: convention-only; gpt-6-sol: {"convention": "other", "detail": "The applicable charge-selection rule for multiple matches is not specified; cannot determine whether one rule wins (or which one), or whether charges combine.", "settled": false}; claudeopus5.5: {"convention": "most_specific", "detail": "The documentation gives no tie-break. Doc [34] only says F and P come from 'a rule of the schedule that matches the return'. Choosing the most specific matching rule (fewest ANY / NULL-bound criteria) is a guess, not something the docs or data establish.", "settled": false}
- solar_panel g1: not settled; gpt-6-sol: {"sentinel": "ALL", "matches_missing": null, "settled": false}; claudeopus5.5: {"sentinel": "ALL", "matches_missing": true, "settled": true}
- solar_panel g2: data-settled; gpt-6-sol: {"recover": "100 * v", "settled": true}; claudeopus5.5: {"recover": "v*100", "settled": true}
