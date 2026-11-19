-- =========================================================
-- 03_gold.sql — GOLD: business aggregations
-- ---------------------------------------------------------
-- Materialized views: each run keeps them in sync with silver,
-- incrementally when possible (serverless).
--
-- Joins to the dimensions use the BROADCAST hint: dimensions are
-- small, so each executor gets a full copy and the fact is not
-- shuffled.
-- =========================================================

-- Sales per customer city, using each customer's CURRENT address.
CREATE OR REFRESH MATERIALIZED VIEW gold.sales_by_city
COMMENT 'Orders and revenue per customer city (current address)'
TBLPROPERTIES ('quality' = 'gold')
AS SELECT /*+ BROADCAST(c) */
  c.city,
  COUNT(f.order_id) AS total_orders,
  SUM(f.amount)     AS total_amount
FROM silver.fact_orders f
JOIN silver.dim_customers_scd2 c
  ON f.customer_id = c.customer_id
 AND c.__END_AT IS NULL
GROUP BY c.city;


-- Sales per product category, using the current product version
-- (joining every version would count each order once per version).
CREATE OR REFRESH MATERIALIZED VIEW gold.sales_by_category
COMMENT 'Orders, revenue and distinct products per category'
TBLPROPERTIES ('quality' = 'gold')
AS SELECT /*+ BROADCAST(p) */
  p.category,
  COUNT(f.order_id)            AS total_orders,
  SUM(f.amount)                AS total_amount,
  COUNT(DISTINCT f.product_id) AS unique_products
FROM silver.fact_orders f
JOIN silver.dim_products_scd2 p
  ON f.product_id = p.product_id
 AND p.__END_AT IS NULL
GROUP BY p.category;


-- Top 100 customers by amount spent.
CREATE OR REFRESH MATERIALIZED VIEW gold.customer_ranking
COMMENT 'Top 100 customers by total amount spent'
TBLPROPERTIES ('quality' = 'gold')
AS SELECT customer_id, name, city, total_amount, ranking
FROM (
  SELECT /*+ BROADCAST(c) */
    c.customer_id, c.name, c.city,
    SUM(f.amount)                                   AS total_amount,
    ROW_NUMBER() OVER (ORDER BY SUM(f.amount) DESC) AS ranking
  FROM silver.fact_orders f
  JOIN silver.dim_customers_scd2 c
    ON f.customer_id = c.customer_id
   AND c.__END_AT IS NULL
  GROUP BY c.customer_id, c.name, c.city
)
WHERE ranking <= 100;


-- Point-in-time join: each order matched to the product version that
-- was valid ON THE ORDER DATE. A February iPhone order shows the $999
-- list price; a June order shows $899. This is what SCD Type 2 is for.
CREATE OR REFRESH MATERIALIZED VIEW gold.orders_price_at_purchase
COMMENT 'Each order with the product list price valid on the order date'
TBLPROPERTIES ('quality' = 'gold')
AS SELECT /*+ BROADCAST(p) */
  f.order_id,
  f.order_date,
  f.customer_id,
  f.product_id,
  p.product_name,
  p.category,
  p.price  AS list_price_at_order,
  f.amount
FROM silver.fact_orders f
JOIN silver.dim_products_scd2 p
  ON f.product_id = p.product_id
 AND f.order_date >= p.__START_AT
 AND (p.__END_AT IS NULL OR f.order_date < p.__END_AT);
