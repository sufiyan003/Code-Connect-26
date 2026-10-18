-- 04_analytics.sql : RFM segments (dim_customer, SCD1 refresh) + seller quality score (dim_seller, SCD1 refresh)
-- Runs after FactSales. Idempotent: attributes are recomputed from scratch on every run.
CREATE SCHEMA IF NOT EXISTS mart;

ALTER TABLE dw.dim_seller ADD COLUMN IF NOT EXISTS quality_score numeric(5,1);
ALTER TABLE dw.dim_seller ADD COLUMN IF NOT EXISTS quality_rank int;
ALTER TABLE dw.dim_seller ADD COLUMN IF NOT EXISTS quality_band text;

-- ----------------------------------------------------------------- RFM
-- Reference date = 2025-07-01 (day after the last valid order date). Customers = non-cancelled orders only.
-- Recency = days since last order; Frequency = distinct orders; Monetary = NMV minus valid refunds.
UPDATE dw.dim_customer SET segment = 'No Purchases', rfm_recency_days = NULL, rfm_frequency = NULL, rfm_monetary = NULL,
  r_score = NULL, f_score = NULL, m_score = NULL, first_order_date = NULL, last_order_date = NULL
WHERE customer_key <> -1;

WITH base AS (
  SELECT f.customer_key, MIN(d.full_date) AS first_order, MAX(d.full_date) AS last_order,
         COUNT(DISTINCT f.order_id) AS freq, SUM(f.net_amount - f.returned_amount) AS monetary
  FROM dw.fact_sales f JOIN dw.dim_date d USING (date_key)
  WHERE NOT f.is_cancelled AND f.customer_key <> -1
  GROUP BY f.customer_key),
scored AS (
  SELECT b.*, (DATE '2025-07-01' - b.last_order) AS recency_days,
         NTILE(5) OVER (ORDER BY (DATE '2025-07-01' - b.last_order) DESC, b.customer_key) AS r,
         NTILE(5) OVER (ORDER BY b.freq, b.monetary, b.customer_key) AS f,
         NTILE(5) OVER (ORDER BY b.monetary, b.customer_key) AS m
  FROM base b)
UPDATE dw.dim_customer c SET
  rfm_recency_days = s.recency_days, rfm_frequency = s.freq, rfm_monetary = s.monetary,
  r_score = s.r, f_score = s.f, m_score = s.m, first_order_date = s.first_order, last_order_date = s.last_order,
  segment = CASE
    WHEN s.r >= 4 AND s.f >= 4 THEN 'Champions'
    WHEN s.r >= 3 AND s.f >= 4 THEN 'Loyal Customers'
    WHEN s.r >= 4 AND s.f >= 2 THEN 'Potential Loyalists'
    WHEN s.r = 5 AND s.f = 1 THEN 'New Customers'
    WHEN s.r >= 3 AND s.f = 1 THEN 'Promising'
    WHEN s.r = 3 THEN 'Need Attention'
    WHEN s.r <= 2 AND s.f >= 3 THEN 'At Risk'
    ELSE 'Hibernating' END
FROM scored s WHERE c.customer_key = s.customer_key;

-- ----------------------------------------------------------------- seller quality score
-- On-time = delivered within 5 days of shipping (median 3 days; return rate steps up from ~4.8% to ~6.9% beyond 5 days).
-- Weights: revenue 30% (scale), return rate 30% (quality + direct refund cost), on-time 25% (delivery reliability),
-- cancellation 15% (partly driven by payment method, so less seller-controlled). Components are percentile ranks (0-1).
-- Sellers with fewer than 30 order lines are scored but not ranked (too little data). Sellers with no sales get no score.
CREATE OR REPLACE VIEW mart.seller_scorecard AS
WITH s AS (
  SELECT ds.seller_key, ds.seller_id, ds.seller_name, ds.seller_tier, ds.province, ds.region,
         COUNT(*) AS lines, COUNT(DISTINCT f.order_id) AS orders,
         SUM(f.net_amount) FILTER (WHERE NOT f.is_cancelled) AS nmv,
         SUM(f.net_amount - f.returned_amount) FILTER (WHERE NOT f.is_cancelled) AS net_revenue,
         COUNT(*) FILTER (WHERE f.is_returned AND f.is_delivered)::numeric / NULLIF(COUNT(*) FILTER (WHERE f.is_delivered), 0) AS return_rate,
         COUNT(DISTINCT f.order_id) FILTER (WHERE f.is_cancelled)::numeric / NULLIF(COUNT(DISTINCT f.order_id), 0) AS cancellation_rate,
         COUNT(*) FILTER (WHERE f.is_delivered AND f.shipping_days <= 5)::numeric
           / NULLIF(COUNT(*) FILTER (WHERE f.is_delivered AND f.shipping_days IS NOT NULL), 0) AS on_time_rate
  FROM dw.fact_sales f JOIN dw.dim_seller ds USING (seller_key)
  WHERE ds.seller_key <> -1
  GROUP BY ds.seller_key, ds.seller_id, ds.seller_name, ds.seller_tier, ds.province, ds.region),
r AS (
  SELECT s.*,
         PERCENT_RANK() OVER (ORDER BY COALESCE(net_revenue, 0)) AS revenue_pct,
         1 - PERCENT_RANK() OVER (ORDER BY COALESCE(return_rate, 1)) AS return_pct,
         PERCENT_RANK() OVER (ORDER BY COALESCE(on_time_rate, 0)) AS ontime_pct,
         1 - PERCENT_RANK() OVER (ORDER BY COALESCE(cancellation_rate, 1)) AS cancel_pct
  FROM s)
SELECT r.*,
       ROUND((100 * (0.30 * revenue_pct + 0.30 * return_pct + 0.25 * ontime_pct + 0.15 * cancel_pct))::numeric, 1) AS quality_score,
       CASE WHEN lines >= 30 THEN RANK() OVER (PARTITION BY (lines >= 30)
            ORDER BY (0.30 * revenue_pct + 0.30 * return_pct + 0.25 * ontime_pct + 0.15 * cancel_pct) DESC, seller_key) END AS quality_rank,
       (lines >= 30) AS rank_eligible
FROM r;

UPDATE dw.dim_seller SET quality_score = NULL, quality_rank = NULL, quality_band = NULL WHERE seller_key <> -1;
UPDATE dw.dim_seller d SET quality_score = v.quality_score, quality_rank = v.quality_rank,
  quality_band = CASE WHEN NOT v.rank_eligible THEN 'Low volume (<30 lines)' WHEN v.quality_score >= 70 THEN 'A (70+)' WHEN v.quality_score >= 55 THEN 'B (55-70)'
                      WHEN v.quality_score >= 40 THEN 'C (40-55)' ELSE 'D (<40)' END
FROM mart.seller_scorecard v WHERE d.seller_key = v.seller_key;
