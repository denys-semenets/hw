CREATE SCHEMA IF NOT EXISTS prom_raw;

CREATE TABLE IF NOT EXISTS prom_raw.raw_orders AS
WITH base AS (
  SELECT
    -- прості текстові ID, щоб не паритись із INT/UUID, можна потім відредачити як схочете
    CAST(1487 + i AS STRING) AS order_id,
    CAST(1 + MOD(i, 67) AS STRING) AS user_id,
    CAST(1 + MOD(i, 200) AS STRING) AS product_id,

    -- замовлення за останні 30 днів
    TIMESTAMP_SUB(
      CURRENT_TIMESTAMP(),
      INTERVAL CAST(FLOOR(RAND() * 30) AS INT64) DAY
    ) AS order_timestamp,

    -- кількість товару в замовленні
    1 + MOD(i, 5) AS quantity,

    -- базова ціна за одиницю (просто рандом у діапазоні 10–39)
    10 + MOD(i, 30) AS item_price,

    -- знижка іноді є, іноді нє
    IF(RAND() < 0.25, 2, 0) AS discount_amount,

    -- джерело трафіку
    ['seo', 'cpc', 'email', 'direct'][
      OFFSET(CAST(FLOOR(RAND() * 4) AS INT64))
    ] AS traffic_source,

    -- промокод
    IF(RAND() < 0.3, 'PROMO10', NULL) AS promo_code,

    -- статус замовлення
    ['CREATEd','paid','shipped','delivered'][
      OFFSET(CAST(FLOOR(RAND() * 4) AS INT64))
    ] AS order_status,

    -- адреса агрегована як хеш + окремі поля міста/району/регіону
    MD5(CONCAT('addr_', CAST(i AS STRING))) AS address_hash,
    ['Kyiv','Lviv','Odesa'][
      OFFSET(CAST(FLOOR(RAND() * 3) AS INT64))
    ] AS city_name,
    ['Shevchenkivskyi','Sykhivskyi','Prymorskyi'][
      OFFSET(CAST(FLOOR(RAND() * 3) AS INT64))
    ] AS district_name,
    ['Kyivska','Lvivska','Odeska'][
      OFFSET(CAST(FLOOR(RAND() * 3) AS INT64))
    ] AS region
  FROM UNNEST(GENERATE_ARRAY(1, 670)) AS i   -- 670 рядків
)
SELECT * FROM base;


-- payments як імітація платіжного сервісу
CREATE TABLE IF NOT EXISTS prom_raw.raw_payments AS
WITH base AS (
  SELECT
    CAST(5000 + i AS STRING) AS payment_id,
    o.order_id,
    o.user_id,

    -- платіж через 0–3 години після часу замовлення
    TIMESTAMP_ADD(
      o.order_timestamp,
      INTERVAL CAST(FLOOR(RAND()*3) AS INT64) HOUR
    ) AS payment_ts,

    -- сума платежу
    o.quantity * o.item_price - o.discount_amount AS payment_amount,

    ['card', 'google_pay', 'apple_pay', 'cash_on_delivery'][
      OFFSET(CAST(FLOOR(RAND()*4) AS INT64))
    ] AS payment_method,

    ['paid','refunded'][
      OFFSET(CAST(FLOOR(RAND()*2) AS INT64))
    ] AS payment_status,

    o.discount_amount AS discount_amount
  FROM prom_raw.raw_orders AS o
  JOIN UNNEST(GENERATE_ARRAY(1, 400)) AS i ON TRUE
  -- Робим реалізм. Не кожне замовлення обов'язково візьметься, потім для цього фільтри будуть
  WHERE RAND() < 0.8
)
SELECT * FROM base;


-- сесії користувачів (web / app аналітика)
CREATE TABLE IF NOT EXISTS prom_raw.raw_sessions AS
WITH ids AS (
  SELECT
    CONCAT('sess_', CAST(i AS STRING)) AS session_id,
    CAST(1 + MOD(i, 69) AS STRING) AS user_id,
    TIMESTAMP_SUB(
      CURRENT_TIMESTAMP(),
      INTERVAL CAST(FLOOR(RAND() * 30) AS INT64) DAY
    ) AS session_start_ts
  FROM UNNEST(GENERATE_ARRAY(1, 400)) AS i
),
calc AS (
  SELECT
    session_id,
    user_id,
    session_start_ts,
    TIMESTAMP_ADD(
      session_start_ts,
      INTERVAL CAST(60 + RAND()*900 AS INT64) SECOND
    ) AS session_end_ts,
    CAST(1 + RAND()*10 AS INT64) AS product_views,
    CAST(RAND() * 3 AS INT64) AS add_to_carts,
    ['desktop','mobile','tablet'][
      OFFSET(CAST(FLOOR(RAND() * 3) AS INT64))
    ] AS device_type
  FROM ids
)
SELECT * FROM calc;


-- customers на основі замовлень
CREATE TABLE IF NOT EXISTS  prom_raw.raw_customers AS
WITH distinct_users AS (
  SELECT DISTINCT user_id FROM prom_raw.raw_orders
),
users_enriched AS (
  SELECT
    user_id,
    -- реєстрація 60–365 днів тому
    DATE_SUB(
      CURRENT_DATE(),
      INTERVAL CAST(60 + RAND()*300 AS INT64) DAY
    ) AS registration_date,
    ['Kyiv','Lviv','Odesa'][
      OFFSET(CAST(FLOOR(RAND() * 3) AS INT64))
    ] AS city,
    ['active','inactive','banned'][
      OFFSET(CAST(FLOOR(RAND() * 3) AS INT64))
    ] AS account_status
  FROM distinct_users
)
SELECT * FROM users_enriched;


-- products на основі замовлень
CREATE TABLE IF NOT EXISTS  prom_raw.raw_products AS
WITH distinct_products AS (
  SELECT DISTINCT product_id FROM prom_raw.raw_orders
),
products_enriched AS (
  SELECT
    product_id,
    CONCAT('m_', CAST(1 + MOD(ABS(FARM_FINGERPRINT(product_id)), 30) AS STRING)) AS merchant_id,
    CONCAT('Product ', product_id) AS product_name,
    ['BrandA','BrandB','BrandC'][
      OFFSET(CAST(FLOOR(RAND() * 3) AS INT64))
    ] AS brand,
    -- поточна ціна, яка може трохи відрізнятись від item_price в замовленнях
    10 + MOD(ABS(FARM_FINGERPRINT(product_id)), 0) AS current_price
  FROM distinct_products
)
SELECT * FROM products_enriched;


-- -- -- -- -- -- --


CREATE SCHEMA IF NOT EXISTS prom_stage;

-- фільтр під оплачені та доставлені замовлення
CREATE TABLE IF NOT EXISTS prom_stage.stage_orders AS
SELECT
  order_id,
  user_id,
  product_id,
  order_timestamp,
  quantity,
  item_price,
  discount_amount,
  traffic_source,
  promo_code,
  order_status,
  address_hash,
  city_name,
  district_name,
  region
FROM prom_raw.raw_orders
WHERE order_status IN ('paid', 'delivered');

-- фільтр адекватно проведених платежів
CREATE TABLE IF NOT EXISTS prom_stage.stage_payments AS
SELECT
  payment_id,
  order_id,
  user_id,
  payment_ts,
  payment_amount,
  payment_method,
  payment_status,
  discount_amount
FROM prom_raw.raw_payments
WHERE payment_status = 'paid';

-- фільтр битих сессій
CREATE TABLE IF NOT EXISTS prom_stage.stage_sessions AS
SELECT
  session_id,
  user_id,
  session_start_ts,
  session_end_ts,
  TIMESTAMP_DIFF(session_end_ts, session_start_ts, SECOND) AS session_duration_sec,
  product_views,
  add_to_carts,
  device_type
FROM prom_raw.raw_sessions
WHERE session_end_ts > session_start_ts;

-- структура під DIM_CUSTOMER
CREATE TABLE IF NOT EXISTS prom_stage.stage_customers AS
SELECT
  user_id,
  registration_date,
  city AS customer_city,
  account_status
FROM prom_raw.raw_customers;

-- структура під DIM_PRODUCT
CREATE TABLE IF NOT EXISTS prom_stage.stage_products AS
SELECT
  product_id,
  merchant_id,
  product_name,
  brand,
  current_price
FROM prom_raw.raw_products;


CREATE SCHEMA IF NOT EXISTS prom_dim;

CREATE TABLE IF NOT EXISTS prom_dim.DIM_PRODUCT AS
SELECT
  ROW_NUMBER() OVER(ORDER BY product_id) AS product_key,
  product_id AS product_id_NK,
  merchant_id,
  product_name,
  brand,
  CAST(current_price AS NUMERIC) AS current_price
FROM prom_stage.stage_products;


CREATE TABLE IF NOT EXISTS prom_dim.DIM_LOCATION AS
SELECT
  ROW_NUMBER() OVER(ORDER BY address_hash) AS location_key, 
  address_hash AS address_hash_NK,
  city_name,
  district_name,
  region
FROM (
  SELECT DISTINCT
    address_hash,
    city_name,
    district_name,
    region
  FROM prom_stage.stage_orders
  WHERE address_hash IS NOT NULL
);


CREATE TABLE IF NOT EXISTS prom_dim.DIM_CUSTOMER AS --SCD2 на доп бали
SELECT
  ROW_NUMBER() OVER(ORDER BY user_id) AS customer_key, 
  user_id AS user_id_NK,
  registration_date,
  customer_city,
  ['Standard', 'Gold', 'Diamond', 'VIP'][
    OFFSET(CAST(FLOOR(RAND() * 4) AS INT64))
  ] AS customer_tier, 
  CAST(0 AS NUMERIC) AS lifetime_spend, 
  account_status,
  registration_date AS date_valid_from, 
  CAST(NULL AS DATE) AS date_valid_to     
FROM prom_stage.stage_customers;

CREATE TABLE IF NOT EXISTS prom_dim.DIM_DATETIME AS 
WITH dates AS ( 
    SELECT
      MIN(DATE(order_timestamp)) AS final_min_date,
      MAX(DATE(order_timestamp)) AS final_max_date
    FROM prom_stage.stage_orders
    WHERE order_timestamp IS NOT NULL
),
gen_dates AS (
    SELECT d
    FROM
      dates, 
      UNNEST(
        GENERATE_DATE_ARRAY(
          COALESCE(final_min_date, CURRENT_DATE()), 
          COALESCE(final_max_date, CURRENT_DATE()),
          INTERVAL 1 DAY
        )
      ) AS d
)
SELECT
  CAST(FORMAT_DATE('%Y%m%d', d) AS INT64) AS date_key,
  d AS full_date,
  EXTRACT(YEAR FROM d) AS year_number,
  EXTRACT(MONTH FROM d) AS month_number,
  EXTRACT(DAY FROM d) AS day_of_month,
  FORMAT_DATE('%A', d) AS day_of_week 
FROM _generated_dates;

CREATE TABLE IF NOT EXISTS prom_dim.FACT_SALES AS
SELECT
  ROW_NUMBER() OVER() AS sales_key,
  do.order_key AS order_key_FK,
  dc.customer_key AS customer_key_FK,
  dp.product_key AS product_key_FK,
  dd.date_key AS date_key_FK,
  dl.location_key AS location_key_FK,
  so.quantity AS quantity_sold,
  CAST(so.item_price AS NUMERIC) AS item_price,
  CAST(so.discount_amount AS NUMERIC) AS discount_amount
FROM
  prom_stage.stage_orders so
LEFT JOIN prom_dwh.DIM_ORDER do
  ON so.order_id = do.order_id_NK
LEFT JOIN prom_dwh.DIM_CUSTOMER dc
  ON so.user_id = dc.user_id_NK
  AND DATE(so.order_timestamp) BETWEEN dc.date_valid_from AND COALESCE(dc.date_valid_to, CURRENT_DATE())
LEFT JOIN prom_dwh.DIM_PRODUCT dp
  ON so.product_id = dp.product_id_NK
LEFT JOIN prom_dwh.DIM_DATETIME dd
  ON DATE(so.order_timestamp) = dd.full_date
LEFT JOIN prom_dwh.DIM_LOCATION dl
  ON so.address_hash = dl.address_hash_NK;



CREATE TABLE IF NOT EXISTS prom_dim.FACT_PAYMENTS AS
SELECT
  ROW_NUMBER() OVER() AS payment_key,
  do.order_key AS order_key_FK,
  dc.customer_key AS customer_key_FK,
  dd.date_key AS date_key_FK,
  CAST(sp.payment_amount AS NUMERIC) AS payment_amount,
  CAST(sp.discount_amount AS NUMERIC) AS discount_amount,
  sp.payment_method
FROM
  prom_stage.stage_payments sp
LEFT JOIN prom_dwh.DIM_ORDER do
  ON sp.order_id = do.order_id_NK
LEFT JOIN prom_dwh.DIM_CUSTOMER dc
  ON sp.user_id = dc.user_id_NK
  AND DATE(sp.payment_ts) BETWEEN dc.date_valid_from AND COALESCE(dc.date_valid_to, CURRENT_DATE())
LEFT JOIN prom_dwh.DIM_DATETIME dd
  ON DATE(sp.payment_ts) = dd.full_date;

CREATE TABLE IF NOT EXISTS prom_dim.FACT_USER_SESSIONS AS
SELECT
  ROW_NUMBER() OVER() AS session_key,
  ss.session_id AS session_id_NK,
  dc.customer_key AS customer_key_FK,
  dd.date_key AS date_key_FK,
  ss.product_views,
  ss.add_to_carts,
  ss.session_duration_sec,
  ss.device_type
FROM
  prom_stage.stage_sessions ss
LEFT JOIN prom_dwh.DIM_CUSTOMER dc
  ON ss.user_id = dc.user_id_NK
  AND DATE(ss.session_start_ts) BETWEEN dc.date_valid_from AND COALESCE(dc.date_valid_to, CURRENT_DATE())
LEFT JOIN prom_dwh.DIM_DATETIME dd
  ON DATE(ss.session_start_ts) = dd.full_date;

  CREATE SCHEMA IF NOT EXISTS prom_mart;


  CREATE TABLE IF NOT EXISTS prom_mart.MART_DAILY_SALES AS
SELECT
  fs.date_key_FK,
  fs.product_key_FK,
  fs.location_key_FK,
  SUM(fs.quantity_sold) AS total_quantity,
  SUM(fs.quantity_sold * fs.item_price - fs.discount_amount) AS total_revenue,
  COUNT(DISTINCT fs.order_key_FK) AS total_orders,
  SAFE_DIVIDE(
    SUM(fs.quantity_sold * fs.item_price - fs.discount_amount),
    COUNT(DISTINCT fs.order_key_FK)
  ) AS avg_order_value,
  COUNT(DISTINCT fs.customer_key_FK) AS unique_customers
FROM
  prom_dwh.FACT_SALES fs
GROUP BY
  fs.date_key_FK, fs.product_key_FK, fs.location_key_FK;

CREATE TABLE IF NOT EXISTS prom_mart.MART_CUSTOMER_YEAR AS

SELECT
  dd.year_number,
  fs.customer_key_FK,
  COUNT(DISTINCT fs.order_key_FK) AS total_orders,
  SUM(fs.quantity_sold * fs.item_price - fs.discount_amount) AS total_spend,
  SAFE_DIVIDE(
    SUM(fs.quantity_sold * fs.item_price - fs.discount_amount),
    COUNT(DISTINCT fs.order_key_FK)
  ) AS avg_order_value,
  CAST(NULL AS INT64) AS total_sessions,
  CAST(NULL AS INT64) AS total_product_views
FROM
  prom_dwh.FACT_SALES fs
JOIN prom_dwh.DIM_DATETIME dd
  ON fs.date_key_FK = dd.date_key
GROUP BY
  dd.year_number,
  fs.customer_key_FK;



CREATE TABLE IF NOT EXISTS prom_mart.MART_CUSTOMER_YEAR AS

WITH sales AS (
  SELECT
    dd.year_number,
    fs.customer_key_FK,
    COUNT(DISTINCT fs.order_key_FK) AS total_orders,
    SUM(fs.quantity_sold * fs.item_price - fs.discount_amount) AS total_spend,
    0 AS total_sessions,
    0 AS total_product_views,
    0 AS total_add_to_carts
  FROM
    prom_dwh.FACT_SALES fs
  JOIN prom_dwh.DIM_DATETIME dd
    ON fs.date_key_FK = dd.date_key
  GROUP BY
    dd.year_number,
    fs.customer_key_FK
),
 sessions AS (
  SELECT
    dd.year_number,
    fus.customer_key_FK,
    0 AS total_orders,
    0 AS total_spend,
    COUNT(DISTINCT fus.session_id_NK) AS total_sessions,
    SUM(fus.product_views) AS total_product_views,
    SUM(fus.add_to_carts) AS total_add_to_carts
  FROM
    prom_dwh.FACT_USER_SESSIONS fus
  JOIN prom_dwh.DIM_DATETIME dd
    ON fus.date_key_FK = dd.date_key
  GROUP BY
    dd.year_number,
    fus.customer_key_FK
),
  data_final AS (
  SELECT * FROM sales
  UNION ALL
  SELECT * FROM sessions
)

SELECT
  customer_key_FK,
  SUM(total_orders) AS total_orders,
  SUM(total_spend) AS total_spend,

  SAFE_DIVIDE(
    SUM(total_spend),
    SUM(total_orders)
  ) AS avg_order_value,
  SUM(total_sessions) AS total_sessions,
  SUM(total_product_views) AS total_product_views,
  SUM(total_add_to_carts) AS total_add_to_carts
FROM
  data_final
GROUP BY
  customer_key_FK;
