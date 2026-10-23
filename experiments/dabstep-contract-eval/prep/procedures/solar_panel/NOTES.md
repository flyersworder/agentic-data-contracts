# solar_panel: R and C

**R, module warranty settlement (KB 50-55).** Every snapshot (`plant_record` row)
is settled by its panel manufacturer under `warranty_settle_rules`: 520 seeded
rules over six criteria, where a NULL criterion matches any value. The criteria
are manufacturer, technology, alert state, warranty state, capacity band, and the
claim volume tier: a band of the manufacturer group's snapshot count in that
calendar month. The most specific matching rule governs, and a tie goes to the
lowest `rule_id`. A snapshot that matches no rule falls back to the Standard Terms
(300.00 USD + 5.00%). The amount is `fixed_fee_usd + cover_pct/100 * replcost`,
rounded to cents per snapshot, then summed. Under DESIGN.md this is a
rule-matching procedure: a new rules table, multi-field matching with wildcards,
a monthly entity aggregate as a criterion, stated precedence and default, and a
per-record amount that is then aggregated.

**C, technology energy loss (KB 56-61).** C uses existing JSON fields only, and has
no rules table:
- potential yield `E/(A/100 * (1 - soil/100))`;
- the loss, potential minus actual (both per snapshot);
- the ratio of sums `100 * sum(loss)/sum(potential)` per panel technology and
  calendar month;
- a four-level band on that ratio (<6, 6-10, 10-14, >=14).

C has dependent formulas, a band, and a computation at one grain aggregated at
another. That makes it a composite.

**Word counts** (knowledge + description + definition): R 554, C 512 (C is 7.6%
shorter).

**Sanity numbers** (`reference.sql` on the built DB; an independent Python
reimplementation agrees on every record's rule and amount, and on every C group):
- R covers all 998 snapshots. 926 have a governing rule and 72 (7.2%) use the
  Standard Terms. 238 of the 520 rules ever govern.
- Matches per snapshot: 0: 72, 1: 175, 2: 170, 3: 145, 4: 124, 5+: 312. The
  governing rule's specificity is 2: 221, 3: 396, 4: 245, 5: 57, 6: 7.
- 336 snapshots have a tie at the top specificity. For 431 snapshots the governing
  rule is not the lowest matching id.
- Claim volume tiers over the 498 manufacturer-months: Single 226 (226 snapshots),
  Moderate 209 (482), Bulk 63 (290). Capacity bands: Small 179, Mid 406,
  Large 413 snapshots.
- If the tier is taken from the plant's own monthly count (always 1, so 'Single'),
  693 of 998 snapshots get a different governing rule.
- Settlement per snapshot: min 3.82, median 1825.27, max 9189.93 USD.
- C covers 998 snapshots in 493 technology-months. Bands: Low 89 (149 snapshots),
  Moderate 182 (406), Elevated 146 (319), Severe 76 (124). An average of
  per-snapshot ratios instead of the ratio of sums changes the band of 82
  technology-months.

**Reviewer, please double-check**
- **Why the entity is the manufacturer.** No plant has more than one snapshot in a
  calendar month (939 plants, 998 snapshots), so a per-plant monthly aggregate
  would always be 1. R's band entity is therefore the manufacturer group
  (`COALESCE(makertag, 'Unlisted')`; Unlisted is the largest group, with 378
  snapshots). C's entity is the panel technology.
- **"Top five" questions are weak.** There are only 6 or 5 groups, so a top-five
  question covers nearly all of them; ranking plants is a better choice. A
  per-plant month or year usually holds one snapshot.
- **`plant_panel_model` is excluded.** The knowledge says to use
  `plants.modhook`. Joining through `plant_panel_model` gives 1130 rows, not 998.
- **Half-cent ties.** Two snapshots land exactly on a half cent: PV300123 (773.445)
  and PV393267 (519.275). The KB says a half rounds up, and DuckDB `ROUND` agrees;
  Python's `round` gives .44 and .27.
- **Unit conflict in the original column meaning.** It calls
  `energy_yield_kwh` "watt-hours", while the field name and C say kWh.
- **Float32 replacement cost.** `replcost` is FLOAT (float32), and the reference
  casts it to DOUBLE. The effect is below a cent, except at the ties above.

## Review

An independent reviewer wrote R and C again from the KB text, the schema and the column meanings
alone, before reading `reference.sql`.

**Change 1: KB 55 and `reference.sql`, per-snapshot rounding dropped.** KB 55 said S is "rounded
to 2 decimal places per snapshot (a half cent rounds up)" and that totals sum the rounded amounts.
That left a result-changing choice open:
- `replcost` is float32, so whether a snapshot sits on a half cent depends on how V is read.
- Read as the stored binary value widened to double (the reference, and plain SQL on the column),
  the reviewer's text-only version matched.
- Read as its displayed decimal (for example `CAST(replcost AS DECIMAL)`, 25430.7), 5 snapshots
  land exactly on a half cent and round up a cent higher: PV807195, PV361569, PV841116,
  PV357027 and PV371336.
- Python `round` disagrees on a sixth (PV393267).
- The grader strips `ROUND` from both gold and answer SQL, so the per-snapshot rule could not be
  graded anyway. It only let a non-`ROUND` rounding drift from gold.

KB 55 now reads: "S is in USD and is not rounded per snapshot. ... is the sum of the unrounded
per-snapshot amounts; round only the reported result, to 2 decimal places." This matches the other
two databases ("no intermediate rounding"). In `reference.sql`, the per-snapshot `ROUND(..., 2)`
was removed from `r_record_amounts` and `r_entity_month`, which keeps its final
`ROUND(SUM(...), 2)`, and the comments were updated. The two bullets above on half-cent ties and
float32 replacement cost, and "rounded to cents per snapshot" in the R summary, are superseded.

**Change 2: `build.py` column meanings.** The meanings of `warranty_settle_rules.fixed_fee_usd`
and `cover_pct` began "REAL." although the columns are DOUBLE (the schema text says double
precision). They now say "DOUBLE."

`build.py` was re-run after both changes. The KB file and the column meanings carry the new text.
With R computed unrounded per snapshot, the text-only version agrees with every named query,
record by record:
- `r_record_features`/`r_record_amounts` 998/998 (group, month, claim volume, tier, capacity band,
  governing rule, S);
- `r_record_matches` 3,750/3,750 (snapshot, rule, specificity);
- `r_entity_month` 498/498;
- `c_record_values` 998/998;
- `c_entity_month` 493/493 (ratio, recoverable kWh, band).

**Checked, with nothing open:**
- All joins resolve for the 998 snapshots.
- No JSON input is NULL. Availability is 95 to 99.99 and soiling 0 to 15, so no division by zero.
- The alert and warranty NULLs are true NULLs, not empty strings.
- No `cap_mw` sits at 100 or 300.
- The nearest loss ratio to a band edge is 9e-4.

**Notes for the experiment (not changed):**
- The original column meaning calls `energy_yield_kwh` watt-hours, while KB 56 states kWh. The KB
  is explicit, so a careful reader follows it.
- KB 61 is a value illustration that adds words to C but no procedure. Without it, C is shorter
  than R by more than the 1.08 word ratio suggests.
