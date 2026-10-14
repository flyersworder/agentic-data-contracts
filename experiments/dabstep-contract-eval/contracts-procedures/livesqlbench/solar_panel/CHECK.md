# solar_panel: contract check against reference.sql

The contract's definitions were run on `PROC_DATA/duckdb/solar_panel.duckdb`
under `connect_readonly`. Two pieces were checked: R (module warranty
settlement, KB 50-55) and C (technology energy loss, KB 56-61). The results
were compared, row by row, with the named queries in
`prep/procedures/solar_panel/reference.sql`, which is the version after its
review, with no per-snapshot rounding. Numbers had to agree within 1e-6,
relative or absolute, or exactly where the reference rounds to cents. Text and
ids had to agree exactly. The month is the calendar month of `snapts`, written
'YYYY-MM'. The R entity is the manufacturer group
(`COALESCE(makertag, 'Unlisted')`), and the C entity is the technology
(`pnlkind`).

## Where the contract holds R and C

The mapping comes from the author's `kb_coverage`.

| KB | piece | place in the contract |
|----|-------|-----------------------|
| 50 Settlement Capacity Band | R | metric `settlement_capacity_band` |
| 51 Manufacturer Monthly Claim Volume | R | metric `manufacturer_monthly_claim_volume` |
| 52 Claim Volume Tier | R | metric `claim_volume_tier` |
| 53 Warranty Settlement Rule Match | R | metric `governing_rule_id` |
| 54 Standard Settlement Terms | R | domain `warranty_settlement` (text); also the COALESCE in the `ws` from_sql |
| 55 Warranty Settlement Amount | R | metric `warranty_settlement_amount` |
| 56 Potential Snapshot Yield | C | metric `potential_snapshot_yield` |
| 57 Recoverable Energy Loss | C | metric `recoverable_energy_loss` |
| 58 Technology Monthly Energy Loss Ratio | C | metric `technology_monthly_energy_loss_ratio` |
| 59 Energy Loss Band | C | metric `energy_loss_band` |
| 60 Technology Recoverable Energy | C | metric `technology_recoverable_energy` |
| 61 Energy Loss Ratio Interpretation | C | domain `energy_loss` (text) |

The domains `warranty_settlement` and `energy_loss` also carry KB 50-61 in
full. They follow the current KB 55: S is not rounded per snapshot, totals
sum unrounded amounts, and only the result is rounded.

## What was compared (before any edit)

| comparison | contract definition | reference | agree |
|---|---|---|---|
| capacity band per snapshot | `settlement_capacity_band` | `r_record_features.cap_band` | 998 / 998 |
| claim volume per snapshot | `manufacturer_monthly_claim_volume` | `r_record_features.claim_volume` | 998 / 998 |
| claim volume tier per snapshot | `claim_volume_tier` | `r_record_features.vol_tier` | 998 / 998 |
| matching (snapshot, rule) pairs and which one governs | `ranked` CTE inside the `ws` from_sql | `r_record_matches` | 3750 / 3750 |
| governing rule per snapshot (NULL = standard terms) | `governing_rule_id` | `r_record_amounts.rule_id` | 998 / 998 |
| settlement per snapshot | `warranty_settlement_amount` | `r_record_amounts.settlement_usd` | 998 / 998 |
| monthly settlement per group-month | `ROUND(SUM(warranty_settlement_amount), 2)` grouped by `ws.maker_group` and month, as the metric's description says | `r_entity_month.settlement_usd` | 498 / 498 |
| snapshots per group-month | COUNT(*) over the same grouping | `r_entity_month.snapshots` | 498 / 498 |
| settlement per group-year | SUM of `warranty_settlement_amount` grouped by group and year | sum of `r_record_amounts` | 66 / 66 |
| potential yield per snapshot | `potential_snapshot_yield` | `c_record_values.potential_kwh` | 998 / 998 |
| recoverable loss per snapshot | `recoverable_energy_loss` | `c_record_values.loss_kwh` | 998 / 998 |
| loss ratio per technology-month | `technology_monthly_energy_loss_ratio` grouped by pnlkind and month | `c_entity_month.loss_ratio` | 493 / 493 |
| loss band per technology-month | `energy_loss_band` | `c_entity_month.loss_band` | 493 / 493 |
| recoverable energy per technology-month | `ROUND(technology_recoverable_energy, 2)` grouped by pnlkind and month | `c_entity_month.recoverable_kwh` | 493 / 493 |
| recoverable energy per technology-year | `technology_recoverable_energy` grouped by pnlkind and year | sum of `c_record_values.loss_kwh` | 55 / 55 |

Every manufacturer group, technology, month and year in the data was
evaluated. Snapshots run from 2015-03 to 2025-02.

## Disagreements

None.

## Edits

None. The SQL agrees with the reference on every snapshot, every match, every
group-month and every technology-month. The domain text restates KB 50-61
without changing any rule. The contract already reflects the post-review
KB 55, with no per-snapshot rounding. The data facts the author added were
checked and are true:
- There are 520 rules.
- The rule criteria are true NULLs, never empty strings.
- All 998 snapshots appear in `ws`.

## Where the contract is silent

- There is no stand-alone metric for rule specificity or for the match set.
  Both sit inside the `ws` from_sql, which the check above compared directly,
  and the domain text states them.
- `potential_snapshot_yield` and the loss metrics wrap the denominator in
  NULLIF(..., 0). The KB does not mention this, and it has no effect: on this
  data availability is 95 to 99.99 and soiling is 0 to 15, so no denominator
  is zero.
- A technology-month whose E_pot were all NULL would get a row with a NULL
  ratio and band in `energy_loss_band`, where the KB says it has no ratio. No
  JSON input is NULL in this data, so no such row exists. This was left as is.

## Final agreement

All 15 comparisons agree on every row, before and after the check. No edits
were made. The contract loads (`DataContract.from_yaml`, `contract_digest`
sha256:4d56a5ac...), and all 50 metrics run.
