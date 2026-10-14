-- Reference implementation of knowledge pieces R and C for
-- labor_certification_applications (KB ids 61-67 are R, 68-72 are C).
-- Runs on PROC_DATA/duckdb/labor_certification_applications.duckdb under
-- dce.benchmarks.lsb_grade.connect_readonly (integer_division = true, so every
-- division below has a non-integer operand). Each named query is
-- self-contained and ends with a semicolon.
--
-- Entity: the employer, the exact text of cases.homefirm.
-- Month: the Start Month, the year-month of cases.beginday (DD/MM/YYYY),
-- written 'YYYY-MM'.

-- name: r_record_amounts
-- R per case: attributes, EMCV and band, governing rule, review hours (CRH).
WITH base AS (
    SELECT c.filekey,
           c.homefirm AS employer,
           strftime(strptime(c.beginday, '%d/%m/%Y'), '%Y-%m') AS start_month,
           CAST(c.visacls AS VARCHAR) AS visa_class,
           p.wage_details->'prevailing_wage'->>'level' AS wage_level,
           CAST(c.h1bdep AS VARCHAR) AS h1b_dependent,
           substr(c.soccd, 1, 2) AS soc_major_group,
           cw.wsstate AS worksite_state,
           c.headct
    FROM cases c
    JOIN case_worksite cw ON cw.dockkey = c.filekey
    JOIN prevailing_wage p ON p.trackno = cw.wagetrack
), vol AS (
    SELECT *, count(*) OVER (PARTITION BY employer, start_month) AS emcv
    FROM base
), banded AS (
    SELECT *,
           CASE WHEN emcv = 1 THEN '1'
                WHEN emcv BETWEEN 2 AND 4 THEN '2-4'
                WHEN emcv BETWEEN 5 AND 14 THEN '5-14'
                ELSE '15+' END AS volume_band
    FROM vol
), matches AS (
    SELECT b.filekey, r.rule_id, r.base_hours, r.position_hours,
           (CASE WHEN r.visa_class IS NOT NULL THEN 1 ELSE 0 END
            + CASE WHEN r.wage_level IS NOT NULL THEN 1 ELSE 0 END
            + CASE WHEN r.h1b_dependent IS NOT NULL THEN 1 ELSE 0 END
            + CASE WHEN r.soc_major_group IS NOT NULL THEN 1 ELSE 0 END
            + CASE WHEN r.worksite_state IS NOT NULL THEN 1 ELSE 0 END
            + CASE WHEN r.monthly_volume_band IS NOT NULL THEN 1 ELSE 0 END)
               AS specificity
    FROM banded b
    JOIN review_schedule r
      ON (r.visa_class IS NULL OR r.visa_class = b.visa_class)
     AND (r.wage_level IS NULL OR r.wage_level = b.wage_level)
     AND (r.h1b_dependent IS NULL OR r.h1b_dependent = b.h1b_dependent)
     AND (r.soc_major_group IS NULL OR r.soc_major_group = b.soc_major_group)
     AND (r.worksite_state IS NULL OR r.worksite_state = b.worksite_state)
     AND (r.monthly_volume_band IS NULL OR r.monthly_volume_band = b.volume_band)
), ranked AS (
    SELECT *,
           count(*) OVER (PARTITION BY filekey) AS matched_rules,
           row_number() OVER (PARTITION BY filekey
                              ORDER BY specificity DESC, rule_id ASC) AS rn
    FROM matches
)
SELECT b.filekey, b.employer, b.start_month, b.visa_class, b.wage_level,
       b.h1b_dependent, b.soc_major_group, b.worksite_state, b.emcv,
       b.volume_band,
       coalesce(g.matched_rules, 0) AS matched_rules,
       g.rule_id AS governing_rule_id,
       g.specificity,
       coalesce(g.base_hours, 3.50) AS base_hours,
       coalesce(g.position_hours, 0.50) AS position_hours,
       b.headct,
       CAST(coalesce(g.base_hours, 3.50)
            + coalesce(g.position_hours, 0.50) * b.headct AS DECIMAL(10, 2))
           AS review_hours
FROM banded b
LEFT JOIN ranked g ON g.filekey = b.filekey AND g.rn = 1
ORDER BY b.filekey;

-- name: r_employer_month
-- R per employer and Start Month: case count (EMCV), band, Employer Review Load.
WITH base AS (
    SELECT c.filekey,
           c.homefirm AS employer,
           strftime(strptime(c.beginday, '%d/%m/%Y'), '%Y-%m') AS start_month,
           CAST(c.visacls AS VARCHAR) AS visa_class,
           p.wage_details->'prevailing_wage'->>'level' AS wage_level,
           CAST(c.h1bdep AS VARCHAR) AS h1b_dependent,
           substr(c.soccd, 1, 2) AS soc_major_group,
           cw.wsstate AS worksite_state,
           c.headct
    FROM cases c
    JOIN case_worksite cw ON cw.dockkey = c.filekey
    JOIN prevailing_wage p ON p.trackno = cw.wagetrack
), vol AS (
    SELECT *, count(*) OVER (PARTITION BY employer, start_month) AS emcv
    FROM base
), banded AS (
    SELECT *,
           CASE WHEN emcv = 1 THEN '1'
                WHEN emcv BETWEEN 2 AND 4 THEN '2-4'
                WHEN emcv BETWEEN 5 AND 14 THEN '5-14'
                ELSE '15+' END AS volume_band
    FROM vol
), matches AS (
    SELECT b.filekey, r.rule_id, r.base_hours, r.position_hours,
           (CASE WHEN r.visa_class IS NOT NULL THEN 1 ELSE 0 END
            + CASE WHEN r.wage_level IS NOT NULL THEN 1 ELSE 0 END
            + CASE WHEN r.h1b_dependent IS NOT NULL THEN 1 ELSE 0 END
            + CASE WHEN r.soc_major_group IS NOT NULL THEN 1 ELSE 0 END
            + CASE WHEN r.worksite_state IS NOT NULL THEN 1 ELSE 0 END
            + CASE WHEN r.monthly_volume_band IS NOT NULL THEN 1 ELSE 0 END)
               AS specificity
    FROM banded b
    JOIN review_schedule r
      ON (r.visa_class IS NULL OR r.visa_class = b.visa_class)
     AND (r.wage_level IS NULL OR r.wage_level = b.wage_level)
     AND (r.h1b_dependent IS NULL OR r.h1b_dependent = b.h1b_dependent)
     AND (r.soc_major_group IS NULL OR r.soc_major_group = b.soc_major_group)
     AND (r.worksite_state IS NULL OR r.worksite_state = b.worksite_state)
     AND (r.monthly_volume_band IS NULL OR r.monthly_volume_band = b.volume_band)
), ranked AS (
    SELECT *,
           row_number() OVER (PARTITION BY filekey
                              ORDER BY specificity DESC, rule_id ASC) AS rn
    FROM matches
), amounts AS (
    SELECT b.employer, b.start_month, b.emcv, b.volume_band,
           CAST(coalesce(g.base_hours, 3.50)
                + coalesce(g.position_hours, 0.50) * b.headct AS DECIMAL(10, 2))
               AS review_hours
    FROM banded b
    LEFT JOIN ranked g ON g.filekey = b.filekey AND g.rn = 1
)
SELECT employer, start_month, emcv, volume_band,
       count(*) AS cases,
       sum(review_hours) AS employer_review_load
FROM amounts
GROUP BY employer, start_month, emcv, volume_band
ORDER BY employer, start_month;

-- name: c_record_scores
-- C per case: AOW, APW, WHR, Headroom Points, Level Points, CWAS.
WITH wages AS (
    SELECT c.filekey,
           c.homefirm AS employer,
           strftime(strptime(c.beginday, '%d/%m/%Y'), '%Y-%m') AS start_month,
           c.headct,
           p.wage_details->'prevailing_wage'->>'level' AS wage_level,
           CAST(replace(replace(p.wage_details->'offered_wage'->>'from', '$', ''), ',', '')
                AS DOUBLE)
             * CASE p.wage_details->'offered_wage'->>'unit'
                 WHEN 'Hour' THEN 2080 WHEN 'Week' THEN 52
                 WHEN 'Month' THEN 12 WHEN 'Year' THEN 1 END AS aow,
           CAST(replace(replace(p.wage_details->'prevailing_wage'->>'value', 'USD ', ''), ',', '')
                AS DOUBLE)
             * CASE p.wage_details->'prevailing_wage'->>'unit'
                 WHEN 'Hour' THEN 2080 WHEN 'Week' THEN 52
                 WHEN 'Month' THEN 12 WHEN 'Year' THEN 1 END AS apw
    FROM cases c
    JOIN case_worksite cw ON cw.dockkey = c.filekey
    JOIN prevailing_wage p ON p.trackno = cw.wagetrack
), ratio AS (
    SELECT *, (aow - apw) / apw AS whr FROM wages
), points AS (
    SELECT *,
           100.0 * least(greatest(whr, 0.0), 0.40) / 0.40 AS headroom_points,
           CASE wage_level WHEN 'I' THEN 25.0 WHEN 'II' THEN 50.0
                           WHEN 'III' THEN 75.0 WHEN 'IV' THEN 100.0
                           ELSE 50.0 END AS level_points
    FROM ratio
)
SELECT filekey, employer, start_month, headct, wage_level, aow, apw, whr,
       headroom_points, level_points,
       0.7 * headroom_points + 0.3 * level_points AS cwas
FROM points
ORDER BY filekey;

-- name: c_employer_scores
-- C per employer over all of its cases: EWAS and Wage Assurance Tier.
WITH wages AS (
    SELECT c.homefirm AS employer,
           c.headct,
           p.wage_details->'prevailing_wage'->>'level' AS wage_level,
           CAST(replace(replace(p.wage_details->'offered_wage'->>'from', '$', ''), ',', '')
                AS DOUBLE)
             * CASE p.wage_details->'offered_wage'->>'unit'
                 WHEN 'Hour' THEN 2080 WHEN 'Week' THEN 52
                 WHEN 'Month' THEN 12 WHEN 'Year' THEN 1 END AS aow,
           CAST(replace(replace(p.wage_details->'prevailing_wage'->>'value', 'USD ', ''), ',', '')
                AS DOUBLE)
             * CASE p.wage_details->'prevailing_wage'->>'unit'
                 WHEN 'Hour' THEN 2080 WHEN 'Week' THEN 52
                 WHEN 'Month' THEN 12 WHEN 'Year' THEN 1 END AS apw
    FROM cases c
    JOIN case_worksite cw ON cw.dockkey = c.filekey
    JOIN prevailing_wage p ON p.trackno = cw.wagetrack
), scored AS (
    SELECT employer, headct,
           0.7 * (100.0 * least(greatest((aow - apw) / apw, 0.0), 0.40) / 0.40)
           + 0.3 * CASE wage_level WHEN 'I' THEN 25.0 WHEN 'II' THEN 50.0
                                   WHEN 'III' THEN 75.0 WHEN 'IV' THEN 100.0
                                   ELSE 50.0 END AS cwas
    FROM wages
), employer AS (
    SELECT employer, count(*) AS cases, sum(headct) AS positions,
           sum(cwas * headct) / sum(headct) AS ewas
    FROM scored
    GROUP BY employer
)
SELECT employer, cases, positions, ewas,
       CASE WHEN ewas < 30 THEN 'Thin'
            WHEN ewas < 45 THEN 'Adequate'
            WHEN ewas < 60 THEN 'Strong'
            ELSE 'Exemplary' END AS wage_assurance_tier
FROM employer
ORDER BY employer;

-- name: c_employer_month_scores
-- C per employer and Start Month: EWAS and Wage Assurance Tier.
WITH wages AS (
    SELECT c.homefirm AS employer,
           strftime(strptime(c.beginday, '%d/%m/%Y'), '%Y-%m') AS start_month,
           c.headct,
           p.wage_details->'prevailing_wage'->>'level' AS wage_level,
           CAST(replace(replace(p.wage_details->'offered_wage'->>'from', '$', ''), ',', '')
                AS DOUBLE)
             * CASE p.wage_details->'offered_wage'->>'unit'
                 WHEN 'Hour' THEN 2080 WHEN 'Week' THEN 52
                 WHEN 'Month' THEN 12 WHEN 'Year' THEN 1 END AS aow,
           CAST(replace(replace(p.wage_details->'prevailing_wage'->>'value', 'USD ', ''), ',', '')
                AS DOUBLE)
             * CASE p.wage_details->'prevailing_wage'->>'unit'
                 WHEN 'Hour' THEN 2080 WHEN 'Week' THEN 52
                 WHEN 'Month' THEN 12 WHEN 'Year' THEN 1 END AS apw
    FROM cases c
    JOIN case_worksite cw ON cw.dockkey = c.filekey
    JOIN prevailing_wage p ON p.trackno = cw.wagetrack
), scored AS (
    SELECT employer, start_month, headct,
           0.7 * (100.0 * least(greatest((aow - apw) / apw, 0.0), 0.40) / 0.40)
           + 0.3 * CASE wage_level WHEN 'I' THEN 25.0 WHEN 'II' THEN 50.0
                                   WHEN 'III' THEN 75.0 WHEN 'IV' THEN 100.0
                                   ELSE 50.0 END AS cwas
    FROM wages
), employer_month AS (
    SELECT employer, start_month, count(*) AS cases, sum(headct) AS positions,
           sum(cwas * headct) / sum(headct) AS ewas
    FROM scored
    GROUP BY employer, start_month
)
SELECT employer, start_month, cases, positions, ewas,
       CASE WHEN ewas < 30 THEN 'Thin'
            WHEN ewas < 45 THEN 'Adequate'
            WHEN ewas < 60 THEN 'Strong'
            ELSE 'Exemplary' END AS wage_assurance_tier
FROM employer_month
ORDER BY employer, start_month;
