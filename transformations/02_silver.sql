-- =========================================================
-- 02_silver.sql — SILVER: cleansing, SCD dimensions, fact
-- ---------------------------------------------------------
-- 1. Staging streaming tables: typing + data quality
--    (expectations), processed incrementally from bronze.
-- 2. SCD dimensions with AUTO CDC:
--      dim_customers_scd1  (latest values only)
--      dim_customers_scd2  (address history)
--      dim_products_scd2   (price history)
--    AUTO CDC orders changes by SEQUENCE BY, so a batch that
--    arrives late or is replayed cannot overwrite newer data.
--    This replaces the hand-written MERGE logic and the batch-
--    by-batch replay of the imperative version.
-- 3. fact_orders: one row per order (materialized view).
--
-- Expectation actions used:
--   WARN (default)        keep the row, count the violation
--   ON VIOLATION DROP ROW drop the row, count it
--   ON VIOLATION FAIL UPDATE stop the flow
-- =========================================================


-- ---------------------------------------------------------
-- 1. Staging
-- ---------------------------------------------------------

CREATE OR REFRESH STREAMING TABLE silver.stg_customers (
  CONSTRAINT valid_batch_key   EXPECT (batch_date IS NOT NULL)  ON VIOLATION FAIL UPDATE,
  CONSTRAINT valid_customer_id EXPECT (customer_id IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT valid_name        EXPECT (name IS NOT NULL)        ON VIOLATION DROP ROW,
  CONSTRAINT valid_city        EXPECT (city IS NOT NULL)        ON VIOLATION DROP ROW,
  CONSTRAINT valid_event_date  EXPECT (event_date IS NOT NULL)  ON VIOLATION DROP ROW,
  CONSTRAINT no_rescued_data   EXPECT (_rescued_data IS NULL)
)
COMMENT 'Customers, typed and validated'
TBLPROPERTIES ('quality' = 'silver')
AS SELECT
  CAST(customer_id AS INT)                       AS customer_id,
  name,
  city,
  to_date(event_date)                            AS event_date,
  to_date(concat(nullif(_batch_key, ''), '-01')) AS batch_date,
  _source_file,
  _rescued_data
FROM STREAM(bronze.customers);


CREATE OR REFRESH STREAMING TABLE silver.stg_products (
  CONSTRAINT valid_batch_key  EXPECT (batch_date IS NOT NULL) ON VIOLATION FAIL UPDATE,
  CONSTRAINT valid_product_id EXPECT (product_id IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT positive_price   EXPECT (price > 0)              ON VIOLATION DROP ROW,
  CONSTRAINT no_rescued_data  EXPECT (_rescued_data IS NULL)
)
COMMENT 'Products, typed and validated'
TBLPROPERTIES ('quality' = 'silver')
AS SELECT
  CAST(product_id AS STRING)                     AS product_id,
  product_name,
  category,
  CAST(price AS DOUBLE)                          AS price,
  to_date(launch_date)                           AS launch_date,
  to_date(concat(nullif(_batch_key, ''), '-01')) AS batch_date,
  _source_file,
  _rescued_data
FROM STREAM(bronze.products);


CREATE OR REFRESH STREAMING TABLE silver.stg_orders (
  CONSTRAINT valid_order_id    EXPECT (order_id IS NOT NULL)    ON VIOLATION DROP ROW,
  CONSTRAINT valid_product_id  EXPECT (product_id IS NOT NULL)  ON VIOLATION DROP ROW,
  CONSTRAINT positive_amount   EXPECT (amount > 0)              ON VIOLATION DROP ROW,
  CONSTRAINT valid_customer_id EXPECT (customer_id IS NOT NULL)
)
COMMENT 'Orders, typed and validated (may contain the same order more than once)'
TBLPROPERTIES ('quality' = 'silver')
AS SELECT
  CAST(order_id AS INT)                          AS order_id,
  CAST(customer_id AS INT)                       AS customer_id,
  CAST(product_id AS STRING)                     AS product_id,
  CAST(amount AS DOUBLE)                         AS amount,
  to_date(order_date)                            AS order_date,
  to_date(concat(nullif(_batch_key, ''), '-01')) AS batch_date,
  _source_file
FROM STREAM(bronze.orders);


-- ---------------------------------------------------------
-- 2. SCD dimensions (AUTO CDC)
-- ---------------------------------------------------------

-- SCD Type 1: one row per customer, always the latest values.
CREATE OR REFRESH STREAMING TABLE silver.dim_customers_scd1
CLUSTER BY (customer_id)
COMMENT 'SCD Type 1 customer dimension: latest values, no history'
TBLPROPERTIES ('quality' = 'silver');

CREATE FLOW customers_scd1_flow AS AUTO CDC INTO silver.dim_customers_scd1
FROM STREAM(silver.stg_customers)
KEYS (customer_id)
SEQUENCE BY event_date
COLUMNS customer_id, name, city, event_date
STORED AS SCD TYPE 1;


-- SCD Type 2: a new version only when name or city changes.
-- __START_AT / __END_AT (type of event_date) replace valid_from / valid_to;
-- the current version is the one with __END_AT IS NULL.
CREATE OR REFRESH STREAMING TABLE silver.dim_customers_scd2
CLUSTER BY (customer_id)
COMMENT 'SCD Type 2 customer dimension: address history'
TBLPROPERTIES ('quality' = 'silver');

CREATE FLOW customers_scd2_flow AS AUTO CDC INTO silver.dim_customers_scd2
FROM STREAM(silver.stg_customers)
KEYS (customer_id)
SEQUENCE BY event_date
COLUMNS customer_id, name, city, event_date
STORED AS SCD TYPE 2
TRACK HISTORY ON name, city;


-- SCD Type 2 for products: a new version when name, category or price
-- changes. Sequenced by the batch date taken from the file name, so the
-- 2026-06 price is never overwritten by a replayed 2026-01 file.
CREATE OR REFRESH STREAMING TABLE silver.dim_products_scd2
CLUSTER BY (product_id)
COMMENT 'SCD Type 2 product dimension: price history'
TBLPROPERTIES ('quality' = 'silver');

CREATE FLOW products_scd2_flow AS AUTO CDC INTO silver.dim_products_scd2
FROM STREAM(silver.stg_products)
KEYS (product_id)
SEQUENCE BY batch_date
COLUMNS product_id, product_name, category, price, launch_date
STORED AS SCD TYPE 2
TRACK HISTORY ON product_name, category, price;


-- ---------------------------------------------------------
-- 3. Fact
-- ---------------------------------------------------------

-- An order can arrive in more than one batch: keep the copy from the
-- latest batch. Liquid clustering on the join keys replaces the
-- ZORDER BY of the imperative version.
CREATE OR REFRESH MATERIALIZED VIEW silver.fact_orders
CLUSTER BY (customer_id, product_id)
COMMENT 'Orders fact: one row per order_id'
TBLPROPERTIES ('quality' = 'silver')
AS SELECT order_id, customer_id, product_id, amount, order_date,
          year(order_date) AS year, month(order_date) AS month
FROM (
  SELECT *,
         ROW_NUMBER() OVER (PARTITION BY order_id
                            ORDER BY batch_date DESC, _source_file DESC) AS rn
  FROM silver.stg_orders
)
WHERE rn = 1;
