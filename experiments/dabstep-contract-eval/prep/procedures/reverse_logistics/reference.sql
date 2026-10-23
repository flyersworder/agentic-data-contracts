-- Reference implementation of R (Return Handling Charge, KB 30-34) and
-- C (Return Value Leakage / Customer Leakage Ratio / Leakage Class, KB 35-39)
-- for reverse_logistics. Runs on PROC_DATA/duckdb/reverse_logistics.duckdb
-- under dce.benchmarks.lsb_grade.connect_readonly (PostgreSQL NULL order and
-- integer division on). Each named query is self-contained; nothing here is
-- stored in the database. Amounts are left unrounded.

-- name: r_record_amounts
-- One row per return: its customer and month, CMRC, the applying rule
-- (NULL = Standard Handling Charge), that rule's specificity and terms, and RHC.
WITH rec AS (
    SELECT
        r.casenum,
        o.buyerlink AS customer,
        substr(r.revdate, 1, 7) AS month,
        c.seg_category,
        r.return_channel,
        p.itemcategory,
        r.return_details ->> '$.reasoning.primary_reason' AS primary_reason,
        q.assessment_summary ->> '$.condition.item_condition' AS item_condition,
        CAST(p.unit_value AS DOUBLE) AS unit_value
    FROM returns r
    JOIN orders o ON o.txnnum = r.srctxn
    LEFT JOIN customers c ON c.profilenum = o.buyerlink
    JOIN products p ON p.itemcode = r.itemlink
    LEFT JOIN quality_assessment q ON q.inspectref = r.casenum
),
rec_cmrc AS (
    SELECT rec.*, COUNT(*) OVER (PARTITION BY customer, month) AS cmrc
    FROM rec
),
matches AS (
    SELECT
        x.casenum,
        k.rule_id,
        k.flat_charge,
        k.charge_pct,
        CAST(k.customer_segment IS NOT NULL AS INTEGER)
          + CAST(k.return_channel IS NOT NULL AS INTEGER)
          + CAST(k.item_category IS NOT NULL AS INTEGER)
          + CAST(k.primary_reason IS NOT NULL AS INTEGER)
          + CAST(k.item_condition IS NOT NULL AS INTEGER)
          + CAST(k.min_monthly_returns IS NOT NULL OR k.max_monthly_returns IS NOT NULL AS INTEGER)
          AS specificity
    FROM rec_cmrc x
    JOIN return_charge_rules k
      ON (k.customer_segment IS NULL OR k.customer_segment = x.seg_category)
     AND (k.return_channel IS NULL OR k.return_channel = x.return_channel)
     AND (k.item_category IS NULL OR k.item_category = x.itemcategory)
     AND (k.primary_reason IS NULL OR k.primary_reason = x.primary_reason)
     AND (k.item_condition IS NULL OR k.item_condition = x.item_condition)
     AND (k.min_monthly_returns IS NULL OR x.cmrc >= k.min_monthly_returns)
     AND (k.max_monthly_returns IS NULL OR x.cmrc <= k.max_monthly_returns)
),
best AS (
    SELECT *
    FROM matches
    QUALIFY ROW_NUMBER() OVER (PARTITION BY casenum ORDER BY specificity DESC, rule_id ASC) = 1
)
SELECT
    x.casenum,
    x.customer,
    x.month,
    x.seg_category,
    x.return_channel,
    x.itemcategory,
    x.primary_reason,
    x.item_condition,
    x.cmrc,
    b.rule_id AS applying_rule_id,
    b.specificity,
    COALESCE(b.flat_charge, 15.00) AS flat_charge,
    COALESCE(b.charge_pct, 5.0) AS charge_pct,
    x.unit_value,
    COALESCE(b.flat_charge, 15.00) + COALESCE(b.charge_pct, 5.0) / 100.0 * x.unit_value AS rhc
FROM rec_cmrc x
LEFT JOIN best b ON b.casenum = x.casenum
ORDER BY x.casenum;

-- name: r_customer_month
-- One row per customer-month: CMRC, the number of returns charged under a
-- rule and under the standard charge, and total RHC.
WITH rec AS (
    SELECT
        r.casenum,
        o.buyerlink AS customer,
        substr(r.revdate, 1, 7) AS month,
        c.seg_category,
        r.return_channel,
        p.itemcategory,
        r.return_details ->> '$.reasoning.primary_reason' AS primary_reason,
        q.assessment_summary ->> '$.condition.item_condition' AS item_condition,
        CAST(p.unit_value AS DOUBLE) AS unit_value
    FROM returns r
    JOIN orders o ON o.txnnum = r.srctxn
    LEFT JOIN customers c ON c.profilenum = o.buyerlink
    JOIN products p ON p.itemcode = r.itemlink
    LEFT JOIN quality_assessment q ON q.inspectref = r.casenum
),
rec_cmrc AS (
    SELECT rec.*, COUNT(*) OVER (PARTITION BY customer, month) AS cmrc
    FROM rec
),
matches AS (
    SELECT
        x.casenum,
        k.rule_id,
        k.flat_charge,
        k.charge_pct,
        CAST(k.customer_segment IS NOT NULL AS INTEGER)
          + CAST(k.return_channel IS NOT NULL AS INTEGER)
          + CAST(k.item_category IS NOT NULL AS INTEGER)
          + CAST(k.primary_reason IS NOT NULL AS INTEGER)
          + CAST(k.item_condition IS NOT NULL AS INTEGER)
          + CAST(k.min_monthly_returns IS NOT NULL OR k.max_monthly_returns IS NOT NULL AS INTEGER)
          AS specificity
    FROM rec_cmrc x
    JOIN return_charge_rules k
      ON (k.customer_segment IS NULL OR k.customer_segment = x.seg_category)
     AND (k.return_channel IS NULL OR k.return_channel = x.return_channel)
     AND (k.item_category IS NULL OR k.item_category = x.itemcategory)
     AND (k.primary_reason IS NULL OR k.primary_reason = x.primary_reason)
     AND (k.item_condition IS NULL OR k.item_condition = x.item_condition)
     AND (k.min_monthly_returns IS NULL OR x.cmrc >= k.min_monthly_returns)
     AND (k.max_monthly_returns IS NULL OR x.cmrc <= k.max_monthly_returns)
),
best AS (
    SELECT *
    FROM matches
    QUALIFY ROW_NUMBER() OVER (PARTITION BY casenum ORDER BY specificity DESC, rule_id ASC) = 1
),
per_record AS (
    SELECT
        x.casenum,
        x.customer,
        x.month,
        x.cmrc,
        b.rule_id AS applying_rule_id,
        COALESCE(b.flat_charge, 15.00) + COALESCE(b.charge_pct, 5.0) / 100.0 * x.unit_value AS rhc
    FROM rec_cmrc x
    LEFT JOIN best b ON b.casenum = x.casenum
)
SELECT
    customer,
    month,
    MAX(cmrc) AS cmrc,
    COUNT(applying_rule_id) AS n_rule_charged,
    COUNT(*) - COUNT(applying_rule_id) AS n_standard_charged,
    SUM(rhc) AS total_rhc
FROM per_record
GROUP BY customer, month
ORDER BY customer, month;

-- name: c_record_values
-- One row per return: its customer and month, the inputs, IRV and RVL.
SELECT
    r.casenum,
    o.buyerlink AS customer,
    substr(r.revdate, 1, 7) AS month,
    CAST(p.unit_value AS DOUBLE) AS unit_value,
    CAST(q.assessment_summary ->> '$.results.aesthetic_score' AS DOUBLE) AS aesthetic_score,
    q.assessment_summary ->> '$.condition.completeness' AS completeness,
    CASE q.assessment_summary ->> '$.condition.completeness'
        WHEN 'Complete' THEN 1.00
        WHEN 'Accessories Missing' THEN 0.85
        WHEN 'Missing Parts' THEN 0.60
    END AS completeness_factor,
    CAST(r.return_details ->> '$.shipping.fee' AS DOUBLE) AS shipping_fee,
    CAST(p.unit_value AS DOUBLE)
      * CAST(q.assessment_summary ->> '$.results.aesthetic_score' AS DOUBLE) / 10.0
      * CASE q.assessment_summary ->> '$.condition.completeness'
            WHEN 'Complete' THEN 1.00
            WHEN 'Accessories Missing' THEN 0.85
            WHEN 'Missing Parts' THEN 0.60
        END AS irv,
    CAST(p.unit_value AS DOUBLE)
      - CAST(p.unit_value AS DOUBLE)
        * CAST(q.assessment_summary ->> '$.results.aesthetic_score' AS DOUBLE) / 10.0
        * CASE q.assessment_summary ->> '$.condition.completeness'
              WHEN 'Complete' THEN 1.00
              WHEN 'Accessories Missing' THEN 0.85
              WHEN 'Missing Parts' THEN 0.60
          END
      + CAST(r.return_details ->> '$.shipping.fee' AS DOUBLE) AS rvl
FROM returns r
JOIN orders o ON o.txnnum = r.srctxn
JOIN products p ON p.itemcode = r.itemlink
JOIN quality_assessment q ON q.inspectref = r.casenum
ORDER BY r.casenum;

-- name: c_customer_month
-- One row per Customer Return Month: returns, summed V and RVL, CLR and the
-- Leakage Class (graded on the unrounded CLR).
WITH per_record AS (
    SELECT
        o.buyerlink AS customer,
        substr(r.revdate, 1, 7) AS month,
        CAST(p.unit_value AS DOUBLE) AS v,
        CAST(p.unit_value AS DOUBLE)
          - CAST(p.unit_value AS DOUBLE)
            * CAST(q.assessment_summary ->> '$.results.aesthetic_score' AS DOUBLE) / 10.0
            * CASE q.assessment_summary ->> '$.condition.completeness'
                  WHEN 'Complete' THEN 1.00
                  WHEN 'Accessories Missing' THEN 0.85
                  WHEN 'Missing Parts' THEN 0.60
              END
          + CAST(r.return_details ->> '$.shipping.fee' AS DOUBLE) AS rvl
    FROM returns r
    JOIN orders o ON o.txnnum = r.srctxn
    JOIN products p ON p.itemcode = r.itemlink
    JOIN quality_assessment q ON q.inspectref = r.casenum
),
cm AS (
    SELECT customer, month, COUNT(*) AS n_returns, SUM(v) AS total_v, SUM(rvl) AS total_rvl,
           SUM(rvl) / SUM(v) AS clr
    FROM per_record
    GROUP BY customer, month
)
SELECT
    customer,
    month,
    n_returns,
    total_v,
    total_rvl,
    clr,
    CASE
        WHEN clr < 0.55 THEN 'Contained'
        WHEN clr < 0.70 THEN 'Elevated'
        ELSE 'Severe'
    END AS leakage_class
FROM cm
ORDER BY customer, month;
