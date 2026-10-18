-- 03_fact.sql : FactSales, grain = one row per order line item (full rebuild, idempotent)
ALTER TABLE dw.fact_sales ADD COLUMN IF NOT EXISTS on_time_status text;
ALTER TABLE dw.fact_sales ADD COLUMN IF NOT EXISTS delivery_band text;
ALTER TABLE dw.fact_sales ADD COLUMN IF NOT EXISTS discount_band text;
ALTER TABLE dw.fact_sales ADD COLUMN IF NOT EXISTS is_first_order boolean;

TRUNCATE dw.fact_sales RESTART IDENTITY;

WITH cost_ratio AS (
  SELECT percentile_cont(0.5) WITHIN GROUP (ORDER BY cost_price / NULLIF(list_price, 0)) AS r
  FROM dw.dim_product WHERE product_key <> -1 AND list_price > 0 AND cost_price > 0),
ret AS (   -- valid (non-Rejected) returns aggregated to one row per order line
  SELECT order_item_id, SUM(refund_amount) AS refund_amount, MIN(return_date) AS return_date
  FROM stg.returns WHERE return_status <> 'Rejected' GROUP BY order_item_id),
ret_first AS (
  SELECT DISTINCT ON (order_item_id) order_item_id, return_reason, return_status
  FROM stg.returns WHERE return_status <> 'Rejected'
  ORDER BY order_item_id, return_date NULLS LAST, return_id),
pay AS (   -- primary payment = largest amount (split payments exist for ~3% of orders)
  SELECT DISTINCT ON (order_id) order_id, payment_method
  FROM stg.payments ORDER BY order_id, amount DESC NULLS LAST, payment_id),
first_orders AS (  -- each customer's first non-cancelled order (new vs returning analysis)
  SELECT DISTINCT ON (customer_id) customer_id, order_id
  FROM stg.orders WHERE order_status <> 'Cancelled' AND customer_id <> 'UNKNOWN'
  ORDER BY customer_id, order_date, order_id),
line AS (
  SELECT i.*, SUM(i.gross) OVER (PARTITION BY i.order_id) AS order_gross, COUNT(*) OVER (PARTITION BY i.order_id) AS n_lines
  FROM stg.order_items i)
INSERT INTO dw.fact_sales (date_key, customer_key, product_key, seller_key, location_key, order_id, order_item_id, quantity, unit_price,
  gross_amount, discount_amount, net_amount, cost_amount, cost_estimated, shipping_fee, shipping_days, is_cancelled, is_delivered,
  is_kept, is_returned, returned_amount, return_date_key, return_reason, return_status, order_status, payment_method, channel,
  discount_pct, courier, delivery_status, on_time_status, delivery_band, discount_band, is_first_order)
SELECT to_char(o.order_date, 'YYYYMMDD')::int,
       COALESCE(dc.customer_key, -1), COALESCE(dp.product_key, -1), COALESCE(ds.seller_key, -1), COALESCE(dl.location_key, -1),
       i.order_id, i.order_item_id, i.quantity, i.unit_price, i.gross, i.discount, i.line_net,
       CASE WHEN dp.product_key IS NULL OR dp.product_key = -1 THEN ROUND((i.gross * (SELECT r FROM cost_ratio))::numeric, 2)
            ELSE dp.cost_price * i.quantity END,
       (dp.product_key IS NULL OR dp.product_key = -1 OR COALESCE(dp.cost_imputed, false)),
       CASE WHEN s.shipping_fee IS NULL THEN 0
            WHEN i.order_gross > 0 THEN s.shipping_fee * i.gross / i.order_gross
            ELSE s.shipping_fee / i.n_lines END,
       s.shipping_days::int,
       (o.order_status = 'Cancelled'),
       (o.order_status <> 'Cancelled' AND o.order_status <> 'In Transit'),
       (o.order_status <> 'Cancelled' AND r.order_item_id IS NULL),
       (r.order_item_id IS NOT NULL), COALESCE(r.refund_amount, 0),
       COALESCE(to_char(r.return_date, 'YYYYMMDD')::int, -1), rf.return_reason, rf.return_status,
       o.order_status, COALESCE(pay.payment_method, 'Unknown'), o.channel, o.discount_pct, s.courier, s.delivery_status,
       -- on-time = delivered within 5 days of shipping (see 04_analytics.sql for the justification)
       CASE WHEN o.order_status IN ('Cancelled', 'In Transit') OR s.shipping_days IS NULL THEN 'Unknown'
            WHEN s.shipping_days <= 5 THEN 'On Time' ELSE 'Late' END,
       CASE WHEN s.shipping_days IS NULL THEN 'Not delivered / unknown' WHEN s.shipping_days <= 3 THEN '0-3 days'
            WHEN s.shipping_days <= 5 THEN '4-5 days' WHEN s.shipping_days <= 7 THEN '6-7 days' ELSE '8+ days' END,
       CASE WHEN o.discount_pct = 0 THEN 'No discount' WHEN o.discount_pct <= 10 THEN '1-10%' WHEN o.discount_pct <= 20 THEN '11-20%'
            WHEN o.discount_pct <= 30 THEN '21-30%' ELSE '31-50%' END,
       (fo.order_id IS NOT NULL)
FROM line i
JOIN stg.orders o ON o.order_id = i.order_id
LEFT JOIN stg.shipping s ON s.order_id = i.order_id
LEFT JOIN pay ON pay.order_id = i.order_id
LEFT JOIN first_orders fo ON fo.order_id = i.order_id
LEFT JOIN ret r ON r.order_item_id = i.order_item_id
LEFT JOIN ret_first rf ON rf.order_item_id = i.order_item_id
LEFT JOIN dw.dim_customer dc ON dc.customer_id = o.customer_id
LEFT JOIN dw.dim_product dp ON dp.product_id = i.product_id
LEFT JOIN dw.dim_seller ds ON ds.seller_id = i.seller_id
LEFT JOIN dw.dim_location dl ON dl.city_id = o.ship_city_id;

ANALYZE dw.fact_sales;
