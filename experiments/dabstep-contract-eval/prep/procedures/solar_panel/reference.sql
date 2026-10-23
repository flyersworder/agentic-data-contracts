-- Reference implementation of the solar_panel procedure pieces (KB ids 50-60).
-- Runs on PROC_DATA/duckdb/solar_panel.duckdb under connect_readonly
-- (PostgreSQL-compatible settings, integer_division = true, so every division
-- below is on doubles). Each named query is self-contained; a query ends at
-- the next "-- name:" line. month is the calendar month of plant_record.snapts
-- as 'YYYY-MM'.

-- name: r_record_features
-- R inputs per snapshot: manufacturer group, capacity band, claim volume and tier.
WITH rec AS (
    SELECT r.snapkey, r.sitetie AS sitekey, strftime(r.snapts, '%Y-%m') AS month,
           m.makertag, COALESCE(m.makertag, 'Unlisted') AS maker_group, m.pnlkind,
           a.alrtstate, p.warrstate, p.cap_mw, CAST(o.replcost AS DOUBLE) AS replcost
    FROM plant_record r
    JOIN plants p ON p.sitekey = r.sitetie
    JOIN panel_models m ON m.modkey = p.modhook
    LEFT JOIN alert a ON a.snapalrt = r.snapkey
    LEFT JOIN operational_metrics o ON o.snapops = r.snapkey
), vol AS (
    SELECT *, COUNT(*) OVER (PARTITION BY maker_group, month) AS claim_volume
    FROM rec
)
SELECT snapkey, sitekey, month, maker_group, makertag, pnlkind, alrtstate, warrstate, cap_mw,
       CASE WHEN cap_mw < 100 THEN 'Small' WHEN cap_mw < 300 THEN 'Mid' ELSE 'Large' END AS cap_band,
       claim_volume,
       CASE WHEN claim_volume = 1 THEN 'Single' WHEN claim_volume <= 3 THEN 'Moderate' ELSE 'Bulk' END AS vol_tier,
       replcost
FROM vol
ORDER BY snapkey;

-- name: r_record_matches
-- Every rule each snapshot matches, with its specificity and whether it governs.
WITH rec AS (
    SELECT r.snapkey, r.sitetie AS sitekey, strftime(r.snapts, '%Y-%m') AS month,
           m.makertag, COALESCE(m.makertag, 'Unlisted') AS maker_group, m.pnlkind,
           a.alrtstate, p.warrstate, p.cap_mw
    FROM plant_record r
    JOIN plants p ON p.sitekey = r.sitetie
    JOIN panel_models m ON m.modkey = p.modhook
    LEFT JOIN alert a ON a.snapalrt = r.snapkey
), feat AS (
    SELECT *,
           CASE WHEN cap_mw < 100 THEN 'Small' WHEN cap_mw < 300 THEN 'Mid' ELSE 'Large' END AS cap_band,
           CASE WHEN COUNT(*) OVER (PARTITION BY maker_group, month) = 1 THEN 'Single'
                WHEN COUNT(*) OVER (PARTITION BY maker_group, month) <= 3 THEN 'Moderate'
                ELSE 'Bulk' END AS vol_tier
    FROM rec
), matched AS (
    SELECT f.snapkey, w.rule_id,
           (w.maker_tag IS NOT NULL)::INT + (w.pnl_kind IS NOT NULL)::INT + (w.alrt_state IS NOT NULL)::INT
           + (w.warr_state IS NOT NULL)::INT + (w.cap_band IS NOT NULL)::INT + (w.vol_tier IS NOT NULL)::INT
           AS specificity
    FROM feat f
    JOIN warranty_settle_rules w
      ON (w.maker_tag IS NULL OR w.maker_tag = f.makertag)
     AND (w.pnl_kind IS NULL OR w.pnl_kind = f.pnlkind)
     AND (w.alrt_state IS NULL OR w.alrt_state = f.alrtstate)
     AND (w.warr_state IS NULL OR w.warr_state = f.warrstate)
     AND (w.cap_band IS NULL OR w.cap_band = f.cap_band)
     AND (w.vol_tier IS NULL OR w.vol_tier = f.vol_tier)
)
SELECT snapkey, rule_id, specificity,
       ROW_NUMBER() OVER (PARTITION BY snapkey ORDER BY specificity DESC, rule_id) = 1 AS governs
FROM matched
ORDER BY snapkey, specificity DESC, rule_id;

-- name: r_record_amounts
-- R per snapshot: the governing rule (NULL = Standard Settlement Terms) and
-- the Warranty Settlement Amount in USD, unrounded.
WITH rec AS (
    SELECT r.snapkey, r.sitetie AS sitekey, strftime(r.snapts, '%Y-%m') AS month,
           m.makertag, COALESCE(m.makertag, 'Unlisted') AS maker_group, m.pnlkind,
           a.alrtstate, p.warrstate, p.cap_mw, CAST(o.replcost AS DOUBLE) AS replcost
    FROM plant_record r
    JOIN plants p ON p.sitekey = r.sitetie
    JOIN panel_models m ON m.modkey = p.modhook
    LEFT JOIN alert a ON a.snapalrt = r.snapkey
    LEFT JOIN operational_metrics o ON o.snapops = r.snapkey
), feat AS (
    SELECT *,
           CASE WHEN cap_mw < 100 THEN 'Small' WHEN cap_mw < 300 THEN 'Mid' ELSE 'Large' END AS cap_band,
           COUNT(*) OVER (PARTITION BY maker_group, month) AS claim_volume
    FROM rec
), feat2 AS (
    SELECT *,
           CASE WHEN claim_volume = 1 THEN 'Single' WHEN claim_volume <= 3 THEN 'Moderate' ELSE 'Bulk' END AS vol_tier
    FROM feat
), matched AS (
    SELECT f.snapkey, w.rule_id, w.fixed_fee_usd, w.cover_pct,
           (w.maker_tag IS NOT NULL)::INT + (w.pnl_kind IS NOT NULL)::INT + (w.alrt_state IS NOT NULL)::INT
           + (w.warr_state IS NOT NULL)::INT + (w.cap_band IS NOT NULL)::INT + (w.vol_tier IS NOT NULL)::INT
           AS specificity
    FROM feat2 f
    JOIN warranty_settle_rules w
      ON (w.maker_tag IS NULL OR w.maker_tag = f.makertag)
     AND (w.pnl_kind IS NULL OR w.pnl_kind = f.pnlkind)
     AND (w.alrt_state IS NULL OR w.alrt_state = f.alrtstate)
     AND (w.warr_state IS NULL OR w.warr_state = f.warrstate)
     AND (w.cap_band IS NULL OR w.cap_band = f.cap_band)
     AND (w.vol_tier IS NULL OR w.vol_tier = f.vol_tier)
), governing AS (
    SELECT * FROM matched
    QUALIFY ROW_NUMBER() OVER (PARTITION BY snapkey ORDER BY specificity DESC, rule_id) = 1
)
SELECT f.snapkey, f.sitekey, f.maker_group, f.month, f.pnlkind, f.alrtstate, f.warrstate,
       f.cap_band, f.claim_volume, f.vol_tier,
       g.rule_id, g.specificity,
       COALESCE(g.fixed_fee_usd, 300.00) AS fixed_fee_usd,
       COALESCE(g.cover_pct, 5.00) AS cover_pct,
       f.replcost,
       COALESCE(g.fixed_fee_usd, 300.00)
         + COALESCE(g.cover_pct, 5.00) / 100.0 * COALESCE(f.replcost, 0.0) AS settlement_usd
FROM feat2 f
LEFT JOIN governing g ON g.snapkey = f.snapkey
ORDER BY f.snapkey;

-- name: r_entity_month
-- R per manufacturer group and calendar month: the monthly settlement, the
-- sum of the unrounded per-snapshot amounts, rounded to cents.
WITH rec AS (
    SELECT r.snapkey, strftime(r.snapts, '%Y-%m') AS month,
           m.makertag, COALESCE(m.makertag, 'Unlisted') AS maker_group, m.pnlkind,
           a.alrtstate, p.warrstate, p.cap_mw, CAST(o.replcost AS DOUBLE) AS replcost
    FROM plant_record r
    JOIN plants p ON p.sitekey = r.sitetie
    JOIN panel_models m ON m.modkey = p.modhook
    LEFT JOIN alert a ON a.snapalrt = r.snapkey
    LEFT JOIN operational_metrics o ON o.snapops = r.snapkey
), feat AS (
    SELECT *,
           CASE WHEN cap_mw < 100 THEN 'Small' WHEN cap_mw < 300 THEN 'Mid' ELSE 'Large' END AS cap_band,
           COUNT(*) OVER (PARTITION BY maker_group, month) AS claim_volume
    FROM rec
), feat2 AS (
    SELECT *,
           CASE WHEN claim_volume = 1 THEN 'Single' WHEN claim_volume <= 3 THEN 'Moderate' ELSE 'Bulk' END AS vol_tier
    FROM feat
), matched AS (
    SELECT f.snapkey, w.rule_id, w.fixed_fee_usd, w.cover_pct,
           (w.maker_tag IS NOT NULL)::INT + (w.pnl_kind IS NOT NULL)::INT + (w.alrt_state IS NOT NULL)::INT
           + (w.warr_state IS NOT NULL)::INT + (w.cap_band IS NOT NULL)::INT + (w.vol_tier IS NOT NULL)::INT
           AS specificity
    FROM feat2 f
    JOIN warranty_settle_rules w
      ON (w.maker_tag IS NULL OR w.maker_tag = f.makertag)
     AND (w.pnl_kind IS NULL OR w.pnl_kind = f.pnlkind)
     AND (w.alrt_state IS NULL OR w.alrt_state = f.alrtstate)
     AND (w.warr_state IS NULL OR w.warr_state = f.warrstate)
     AND (w.cap_band IS NULL OR w.cap_band = f.cap_band)
     AND (w.vol_tier IS NULL OR w.vol_tier = f.vol_tier)
), governing AS (
    SELECT * FROM matched
    QUALIFY ROW_NUMBER() OVER (PARTITION BY snapkey ORDER BY specificity DESC, rule_id) = 1
), amounts AS (
    SELECT f.maker_group, f.month, f.vol_tier,
           COALESCE(g.fixed_fee_usd, 300.00)
             + COALESCE(g.cover_pct, 5.00) / 100.0 * COALESCE(f.replcost, 0.0) AS settlement_usd
    FROM feat2 f
    LEFT JOIN governing g ON g.snapkey = f.snapkey
)
SELECT maker_group, month, ANY_VALUE(vol_tier) AS vol_tier, COUNT(*) AS snapshots,
       ROUND(SUM(settlement_usd), 2) AS settlement_usd
FROM amounts
GROUP BY maker_group, month
ORDER BY maker_group, month;

-- name: c_record_values
-- C per snapshot: Potential Snapshot Yield and Recoverable Energy Loss (kWh, unrounded).
WITH rec AS (
    SELECT r.snapkey, r.sitetie AS sitekey, strftime(r.snapts, '%Y-%m') AS month, m.pnlkind,
           CAST(e.elec_perf_snapshot ->> '$.energy_yield.energy_yield_kwh' AS DOUBLE) AS energy_kwh,
           CAST(e.elec_perf_snapshot ->> '$.energy_yield.availability_pct' AS DOUBLE) AS availability_pct,
           CAST(e.elec_perf_snapshot ->> '$.efficiency.soil_loss_pct' AS DOUBLE) AS soil_loss_pct
    FROM plant_record r
    JOIN plants p ON p.sitekey = r.sitetie
    JOIN panel_models m ON m.modkey = p.modhook
    LEFT JOIN electrical_performance e ON e.snaplink = r.snapkey
), pot AS (
    SELECT *, energy_kwh / ((availability_pct / 100.0) * (1.0 - soil_loss_pct / 100.0)) AS potential_kwh
    FROM rec
)
SELECT snapkey, sitekey, pnlkind, month, energy_kwh, availability_pct, soil_loss_pct,
       potential_kwh, potential_kwh - energy_kwh AS loss_kwh
FROM pot
ORDER BY snapkey;

-- name: c_entity_month
-- C per panel technology and calendar month: Technology Monthly Energy Loss
-- Ratio (ratio of sums), its Energy Loss Band (from the unrounded ratio) and
-- Technology Recoverable Energy for the month.
WITH rec AS (
    SELECT r.snapkey, strftime(r.snapts, '%Y-%m') AS month, m.pnlkind,
           CAST(e.elec_perf_snapshot ->> '$.energy_yield.energy_yield_kwh' AS DOUBLE) AS energy_kwh,
           CAST(e.elec_perf_snapshot ->> '$.energy_yield.availability_pct' AS DOUBLE) AS availability_pct,
           CAST(e.elec_perf_snapshot ->> '$.efficiency.soil_loss_pct' AS DOUBLE) AS soil_loss_pct
    FROM plant_record r
    JOIN plants p ON p.sitekey = r.sitetie
    JOIN panel_models m ON m.modkey = p.modhook
    LEFT JOIN electrical_performance e ON e.snaplink = r.snapkey
), pot AS (
    SELECT *, energy_kwh / ((availability_pct / 100.0) * (1.0 - soil_loss_pct / 100.0)) AS potential_kwh
    FROM rec
), grp AS (
    SELECT pnlkind, month, COUNT(*) AS snapshots,
           SUM(potential_kwh) AS potential_kwh, SUM(potential_kwh - energy_kwh) AS loss_kwh,
           100.0 * SUM(potential_kwh - energy_kwh) / SUM(potential_kwh) AS loss_ratio
    FROM pot
    WHERE potential_kwh IS NOT NULL
    GROUP BY pnlkind, month
)
SELECT pnlkind, month, snapshots, potential_kwh, ROUND(loss_kwh, 2) AS recoverable_kwh,
       loss_ratio, ROUND(loss_ratio, 2) AS loss_ratio_pct,
       CASE WHEN loss_ratio < 6 THEN 'Low' WHEN loss_ratio < 10 THEN 'Moderate'
            WHEN loss_ratio < 14 THEN 'Elevated' ELSE 'Severe' END AS loss_band
FROM grp
ORDER BY pnlkind, month;
