-- 02_dimensions.sql : build/refresh dimensions (idempotent upserts; SCD Type 1 on dim_customer)

-- Unknown members
-- Unknown date member uses 1900-01-01 so dim_date can be marked as a date table in Power BI (needs unique, non-blank dates)
INSERT INTO dw.dim_date (date_key, full_date, year, day_name, month_name, year_month, festival)
  VALUES (-1, DATE '1900-01-01', 1900, 'Unknown', 'Unknown', 'Unknown', 'Unknown')
  ON CONFLICT (date_key) DO UPDATE SET full_date = DATE '1900-01-01', year = 1900;
INSERT INTO dw.dim_customer (customer_key, customer_id, full_name, gender, city_id, city, province, region, population_tier, segment)
  VALUES (-1, 'UNKNOWN', 'Unknown', 'Unknown', -1, 'Unknown', 'Unknown', 'Unknown', 'Unknown', 'Unknown') ON CONFLICT (customer_id) DO NOTHING;
INSERT INTO dw.dim_product (product_key, product_id, product_name, category, sub_category, brand)
  VALUES (-1, 'UNKNOWN', 'Unknown', 'Unknown', 'Unknown', 'Unknown') ON CONFLICT (product_id) DO NOTHING;
INSERT INTO dw.dim_seller (seller_key, seller_id, seller_name, seller_tier, city_id, city, province, region)
  VALUES (-1, 'UNKNOWN', 'Unknown', 'Unknown', -1, 'Unknown', 'Unknown', 'Unknown') ON CONFLICT (seller_id) DO NOTHING;
INSERT INTO dw.dim_location (location_key, city_id, city, province, region, population_tier)
  VALUES (-1, -1, 'Unknown', 'Unknown', 'Unknown', 'Unknown') ON CONFLICT (city_id) DO NOTHING;

-- dim_date : static calendar 2023-2025 with festival flags (permitted static holiday calendar)
INSERT INTO dw.dim_date
SELECT to_char(d, 'YYYYMMDD')::int, d, EXTRACT(day FROM d)::int, trim(to_char(d, 'Day')), EXTRACT(isodow FROM d)::int,
       EXTRACT(week FROM d)::int, date_trunc('week', d)::date, EXTRACT(month FROM d)::int, trim(to_char(d, 'Month')),
       date_trunc('month', d)::date, to_char(d, 'YYYY-MM'), EXTRACT(quarter FROM d)::int, EXTRACT(year FROM d)::int,
       EXTRACT(isodow FROM d) IN (6, 7),
       (d BETWEEN '2024-03-11' AND '2024-04-09') OR (d BETWEEN '2025-03-01' AND '2025-03-30'),
       (d BETWEEN '2024-04-10' AND '2024-04-12') OR (d BETWEEN '2025-03-31' AND '2025-04-02'),
       (d BETWEEN '2024-06-17' AND '2024-06-19') OR (d BETWEEN '2025-06-07' AND '2025-06-09'),
       (EXTRACT(month FROM d) = 8 AND EXTRACT(day FROM d) = 14),
       (EXTRACT(month FROM d) = 11 AND EXTRACT(day FROM d) = 11),
       d = '2024-11-29'::date,
       ((d BETWEEN '2024-04-10' AND '2024-04-12') OR (d BETWEEN '2025-03-31' AND '2025-04-02')
        OR (d BETWEEN '2024-06-17' AND '2024-06-19') OR (d BETWEEN '2025-06-07' AND '2025-06-09')
        OR (EXTRACT(month FROM d) = 8 AND EXTRACT(day FROM d) = 14)),
       CASE
         WHEN (d BETWEEN '2024-04-10' AND '2024-04-12') OR (d BETWEEN '2025-03-31' AND '2025-04-02') THEN 'Eid-ul-Fitr'
         WHEN (d BETWEEN '2024-06-17' AND '2024-06-19') OR (d BETWEEN '2025-06-07' AND '2025-06-09') THEN 'Eid-ul-Adha'
         WHEN (d BETWEEN '2024-03-11' AND '2024-04-09') OR (d BETWEEN '2025-03-01' AND '2025-03-30') THEN 'Ramadan'
         WHEN d = '2024-11-29'::date THEN 'Black Friday'
         WHEN EXTRACT(month FROM d) = 11 AND EXTRACT(day FROM d) = 11 THEN '11.11'
         WHEN EXTRACT(month FROM d) = 8 AND EXTRACT(day FROM d) = 14 THEN '14 August'
         ELSE 'Regular Day' END
FROM generate_series('2023-01-01'::date, '2025-12-31'::date, interval '1 day') AS g(d)
ON CONFLICT (date_key) DO NOTHING;

-- dim_location
INSERT INTO dw.dim_location (city_id, city, province, region, latitude, longitude, population_tier)
SELECT city_id, city, province, region, latitude, longitude, population_tier FROM stg.locations
ON CONFLICT (city_id) DO UPDATE SET city = EXCLUDED.city, province = EXCLUDED.province, region = EXCLUDED.region,
  latitude = EXCLUDED.latitude, longitude = EXCLUDED.longitude, population_tier = EXCLUDED.population_tier;

-- dim_seller
INSERT INTO dw.dim_seller (seller_id, seller_name, join_date, seller_tier, city_id, city, province, region, rating)
SELECT s.seller_id, s.seller_name, s.join_date, s.seller_tier, s.city_id, COALESCE(l.city, 'Unknown'),
       COALESCE(l.province, 'Unknown'), COALESCE(l.region, 'Unknown'), s.rating
FROM stg.sellers s LEFT JOIN stg.locations l ON l.city_id = s.city_id
ON CONFLICT (seller_id) DO UPDATE SET seller_name = EXCLUDED.seller_name, join_date = EXCLUDED.join_date,
  seller_tier = EXCLUDED.seller_tier, city_id = EXCLUDED.city_id, city = EXCLUDED.city, province = EXCLUDED.province,
  region = EXCLUDED.region, rating = EXCLUDED.rating;

-- dim_product
INSERT INTO dw.dim_product (product_id, product_name, category, sub_category, brand, list_price, cost_price, weight_kg, cost_imputed)
SELECT product_id, product_name, category, sub_category, brand, list_price, cost_price, weight_kg, cost_imputed FROM stg.products
ON CONFLICT (product_id) DO UPDATE SET product_name = EXCLUDED.product_name, category = EXCLUDED.category,
  sub_category = EXCLUDED.sub_category, brand = EXCLUDED.brand, list_price = EXCLUDED.list_price,
  cost_price = EXCLUDED.cost_price, weight_kg = EXCLUDED.weight_kg, cost_imputed = EXCLUDED.cost_imputed;

-- dim_customer (SCD Type 1: attributes are overwritten on the business key; segment columns are kept)
INSERT INTO dw.dim_customer (customer_id, full_name, email, phone, gender, registration_date, birth_year, city_id, city, province, region, population_tier)
SELECT c.customer_id, c.full_name, c.email, c.phone, c.gender, c.registration_date, c.birth_year::int, c.city_id,
       COALESCE(l.city, 'Unknown'), COALESCE(l.province, 'Unknown'), COALESCE(l.region, 'Unknown'), COALESCE(l.population_tier, 'Unknown')
FROM stg.customers c LEFT JOIN stg.locations l ON l.city_id = c.city_id
ON CONFLICT (customer_id) DO UPDATE SET full_name = EXCLUDED.full_name, email = EXCLUDED.email, phone = EXCLUDED.phone,
  gender = EXCLUDED.gender, registration_date = EXCLUDED.registration_date, birth_year = EXCLUDED.birth_year,
  city_id = EXCLUDED.city_id, city = EXCLUDED.city, province = EXCLUDED.province, region = EXCLUDED.region,
  population_tier = EXCLUDED.population_tier;
