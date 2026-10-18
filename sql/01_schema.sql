-- 01_schema.sql : schemas, staging tables, ops tables, star schema (idempotent)
CREATE SCHEMA IF NOT EXISTS stg;
CREATE SCHEMA IF NOT EXISTS dw;
CREATE SCHEMA IF NOT EXISTS ops;

-- ---------------------------------------------------------------- staging (cleaned data from Python)
CREATE TABLE IF NOT EXISTS stg.locations (city_id int PRIMARY KEY, city text, province text, region text,
  latitude numeric(9,6), longitude numeric(9,6), population_tier text, province_imputed boolean);
CREATE TABLE IF NOT EXISTS stg.sellers (seller_id text PRIMARY KEY, seller_name text, join_date date, seller_tier text,
  city_id int, rating numeric(3,1), rating_imputed boolean);
CREATE TABLE IF NOT EXISTS stg.customers (customer_id text PRIMARY KEY, full_name text, email text, phone text, gender text,
  registration_date date, city_id int, birth_year numeric(6,1), reg_repaired boolean);
CREATE TABLE IF NOT EXISTS stg.products (product_id text PRIMARY KEY, product_name text, category text, sub_category text,
  brand text, list_price numeric(14,2), cost_price numeric(14,2), weight_kg numeric(10,3), seller_id text,
  list_price_repaired boolean, cost_imputed boolean, weight_imputed boolean);
CREATE TABLE IF NOT EXISTS stg.orders (order_id text PRIMARY KEY, customer_id text, customer_id_raw text, order_date date,
  order_status text, ship_city_id int, ship_city_repaired boolean, order_total numeric(14,2), discount_code text,
  discount_pct numeric(5,2), channel text, date_repair text);
CREATE TABLE IF NOT EXISTS stg.order_items (order_item_id text PRIMARY KEY, order_id text, product_id text, product_raw text,
  seller_id text, quantity int, unit_price numeric(14,2), discount numeric(14,2), gross numeric(14,2), line_net numeric(14,2),
  price_repaired boolean, qty_repaired boolean, discount_repaired boolean);
CREATE TABLE IF NOT EXISTS stg.payments (payment_id text PRIMARY KEY, order_id text, payment_method text, payment_status text,
  amount numeric(14,2), payment_date date);
CREATE TABLE IF NOT EXISTS stg.shipping (shipment_id text PRIMARY KEY, order_id text, courier text, shipped_date date,
  delivered_date date, shipping_fee numeric(14,2), delivery_status text, shipping_days numeric(6,1),
  shipped_imputed boolean, fee_imputed boolean);
CREATE TABLE IF NOT EXISTS stg.returns (return_id text PRIMARY KEY, order_item_id text, return_date date, return_reason text,
  return_status text, refund_amount numeric(14,2), refund_capped boolean);
CREATE TABLE IF NOT EXISTS stg.customer_map (raw_customer_id text PRIMARY KEY, master_customer_id text, merged boolean, match_rule text);
CREATE TABLE IF NOT EXISTS stg.product_map (raw_product_id text PRIMARY KEY, master_product_id text, merged boolean);

-- ---------------------------------------------------------------- ops (logging, rejects, data quality)
CREATE TABLE IF NOT EXISTS ops.rejects (table_name text, record_key text, reason_code text, action text, raw_detail text);
CREATE TABLE IF NOT EXISTS ops.dq_rules_log (table_name text, rule_id text, column_name text, issue text, action text, records_affected bigint);
CREATE TABLE IF NOT EXISTS ops.dq_table_counts (table_name text, rows_raw bigint, exact_duplicates_removed bigint, rows_rejected bigint, rows_clean bigint);
CREATE TABLE IF NOT EXISTS ops.pipeline_run_log (id bigserial PRIMARY KEY, run_id text, task text, status text,
  records_read bigint, records_rejected bigint, records_loaded bigint, message text, logged_at timestamptz DEFAULT now());
CREATE TABLE IF NOT EXISTS ops.quality_check_results (id bigserial PRIMARY KEY, run_id text, check_name text, severity text,
  expected text, actual text, passed boolean, checked_at timestamptz DEFAULT now());

-- ---------------------------------------------------------------- star schema (dimensions are NOT linked to each other)
CREATE TABLE IF NOT EXISTS dw.dim_date (
  date_key int PRIMARY KEY, full_date date, day_of_month int, day_name text, day_of_week int, week_of_year int,
  week_start_date date, month_num int, month_name text, month_start_date date, year_month text, quarter int, year int,
  is_weekend boolean, is_ramadan boolean, is_eid_ul_fitr boolean, is_eid_ul_adha boolean, is_independence_day boolean,
  is_11_11 boolean, is_black_friday boolean, is_holiday boolean, festival text);

CREATE TABLE IF NOT EXISTS dw.dim_customer (
  customer_key serial PRIMARY KEY, customer_id text NOT NULL UNIQUE, full_name text, email text, phone text, gender text,
  registration_date date, birth_year int, city_id int, city text, province text, region text, population_tier text,
  segment text DEFAULT 'Unassigned', rfm_recency_days int, rfm_frequency int, rfm_monetary numeric(14,2),
  r_score int, f_score int, m_score int, first_order_date date, last_order_date date);

CREATE TABLE IF NOT EXISTS dw.dim_product (
  product_key serial PRIMARY KEY, product_id text NOT NULL UNIQUE, product_name text, category text, sub_category text,
  brand text, list_price numeric(14,2), cost_price numeric(14,2), weight_kg numeric(10,3), cost_imputed boolean);

CREATE TABLE IF NOT EXISTS dw.dim_seller (
  seller_key serial PRIMARY KEY, seller_id text NOT NULL UNIQUE, seller_name text, join_date date, seller_tier text,
  city_id int, city text, province text, region text, rating numeric(3,1));

CREATE TABLE IF NOT EXISTS dw.dim_location (
  location_key serial PRIMARY KEY, city_id int NOT NULL UNIQUE, city text, province text, region text,
  latitude numeric(9,6), longitude numeric(9,6), population_tier text);

CREATE TABLE IF NOT EXISTS dw.fact_sales (
  sales_key bigserial PRIMARY KEY,
  date_key int NOT NULL REFERENCES dw.dim_date(date_key),
  customer_key int NOT NULL REFERENCES dw.dim_customer(customer_key),
  product_key int NOT NULL REFERENCES dw.dim_product(product_key),
  seller_key int NOT NULL REFERENCES dw.dim_seller(seller_key),
  location_key int NOT NULL REFERENCES dw.dim_location(location_key),
  order_id text NOT NULL, order_item_id text NOT NULL UNIQUE,
  quantity int, unit_price numeric(14,2), gross_amount numeric(14,2), discount_amount numeric(14,2), net_amount numeric(14,2),
  cost_amount numeric(14,2), cost_estimated boolean, shipping_fee numeric(14,4), shipping_days int,
  is_cancelled boolean, is_delivered boolean, is_kept boolean, is_returned boolean, returned_amount numeric(14,2),
  return_date_key int REFERENCES dw.dim_date(date_key), return_reason text, return_status text,
  order_status text, payment_method text, channel text, discount_pct numeric(5,2), courier text, delivery_status text,
  on_time_status text, delivery_band text, discount_band text, is_first_order boolean);
CREATE INDEX IF NOT EXISTS ix_fact_date ON dw.fact_sales(date_key);
CREATE INDEX IF NOT EXISTS ix_fact_customer ON dw.fact_sales(customer_key);
CREATE INDEX IF NOT EXISTS ix_fact_product ON dw.fact_sales(product_key);
CREATE INDEX IF NOT EXISTS ix_fact_seller ON dw.fact_sales(seller_key);
CREATE INDEX IF NOT EXISTS ix_fact_location ON dw.fact_sales(location_key);
CREATE INDEX IF NOT EXISTS ix_fact_order ON dw.fact_sales(order_id);
