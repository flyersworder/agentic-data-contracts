-- Pre-computed fee macros for DABStep, COMPILED FROM THE FROZEN CONTRACT.
--
-- Every predicate below is a verbatim transcription of a `sql_expression`
-- field in `contract/semantic.yml`; nothing here is authored independently of
-- the contract, and nothing was chosen by consulting the benchmark's answers.
-- The provenance, metric by metric:
--
--   merchant_month.month        <- transaction_natural_month
--   merchant_month.volume       <- merchant_monthly_volume
--   merchant_month.fraud_ratio  <- merchant_monthly_fraud_level
--   the JOIN predicate on fees  <- fee_rule_matches_transaction
--                                + fee_rule_matches_merchant_month
--   the `fee` column            <- transaction_fee
--   summing over matched pairs  <- total_transaction_fees ("sum only over
--                                  matched (transaction, rule) pairs")
--
-- The one thing this file adds is COMPOSITION: `fee_rule_matches_merchant_month`
-- is marked NOT RUNNABLE AS WRITTEN in the contract because it carries
-- `:volume` and `:fraud_ratio` placeholders. Correlating those to a
-- per-merchant natural-month aggregate is the whole of the work here.
--
-- This is an ANALYSIS INSTRUMENT, never an experimental arm: no agent is ever
-- given these views. See FINDINGS.md, "The contract compiles."

-- 1. Per-merchant natural-month aggregates: what the two month-scoped fee
--    dimensions (monthly_volume, monthly_fraud_level) are measured against.
CREATE OR REPLACE VIEW merchant_month AS
SELECT p.merchant,
       date_trunc('month', make_date(p.year, 1, 1)
                  + CAST(p.day_of_year - 1 AS INTEGER))           AS month,
       SUM(p.eur_amount)                                          AS volume,
       SUM(p.eur_amount) FILTER (WHERE p.has_fraudulent_dispute)
         / NULLIF(SUM(p.eur_amount), 0)                           AS fraud_ratio
FROM payments p
GROUP BY 1, 2;

-- 2. Every (transaction, applicable fee rule) pair, with the fee that pair
--    incurs. A transaction may match zero, one, or many rules; the contract's
--    `total_transaction_fees` sums over all matched pairs.
CREATE OR REPLACE VIEW transaction_fee_matches AS
SELECT p.psp_reference,
       p.merchant,
       p.year,
       p.day_of_year,
       date_trunc('month', make_date(p.year, 1, 1)
                  + CAST(p.day_of_year - 1 AS INTEGER))           AS month,
       f.ID                                                       AS fee_id,
       f.fixed_amount + f.rate * p.eur_amount / 10000             AS fee
FROM payments p
JOIN merchant_data  m  ON m.merchant = p.merchant
JOIN merchant_month mm ON mm.merchant = p.merchant
                      AND mm.month = date_trunc('month', make_date(p.year, 1, 1)
                                     + CAST(p.day_of_year - 1 AS INTEGER))
JOIN fees f ON
      (f.card_scheme IS NULL OR f.card_scheme = p.card_scheme)
  AND (f.is_credit   IS NULL OR f.is_credit   = p.is_credit)
  AND (f.aci IS NULL OR len(f.aci) = 0 OR list_contains(f.aci, p.aci))
  AND (f.intracountry IS NULL
       OR (f.intracountry = 1) = (p.issuing_country = p.acquirer_country))
  AND (f.account_type IS NULL OR len(f.account_type) = 0
       OR list_contains(f.account_type, m.account_type))
  AND (f.merchant_category_code IS NULL OR len(f.merchant_category_code) = 0
       OR list_contains(f.merchant_category_code, m.merchant_category_code))
  AND (f.capture_delay IS NULL
       OR f.capture_delay = CASE
            WHEN m.capture_delay IN ('immediate', 'manual') THEN m.capture_delay
            WHEN TRY_CAST(m.capture_delay AS INTEGER) < 3 THEN '<3'
            WHEN TRY_CAST(m.capture_delay AS INTEGER) BETWEEN 3 AND 5 THEN '3-5'
            WHEN TRY_CAST(m.capture_delay AS INTEGER) > 5 THEN '>5'
          END)
  AND (f.monthly_volume IS NULL OR CASE f.monthly_volume
         WHEN '<100k'   THEN mm.volume <  100000
         WHEN '100k-1m' THEN mm.volume >= 100000  AND mm.volume <= 1000000
         WHEN '1m-5m'   THEN mm.volume >= 1000000 AND mm.volume <= 5000000
         WHEN '>5m'     THEN mm.volume >  5000000
       END)
  AND (f.monthly_fraud_level IS NULL OR CASE f.monthly_fraud_level
         WHEN '<7.2%'     THEN 100 * mm.fraud_ratio <  7.2
         WHEN '7.2%-7.7%' THEN 100 * mm.fraud_ratio >= 7.2 AND 100 * mm.fraud_ratio <= 7.7
         WHEN '7.7%-8.3%' THEN 100 * mm.fraud_ratio >= 7.7 AND 100 * mm.fraud_ratio <= 8.3
         WHEN '>8.3%'     THEN 100 * mm.fraud_ratio >  8.3
       END);
