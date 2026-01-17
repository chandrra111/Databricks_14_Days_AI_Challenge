-- Bronze
CREATE TABLE IF NOT EXISTS ecommerce.bronze.events AS SELECT * FROM workspace.ecommerce.bronze_events;
-- Silver
CREATE TABLE IF NOT EXISTS ecommerce.silver.events AS SELECT * FROM workspace.ecommerce.silver_events;

-- Gold
CREATE TABLE IF NOT EXISTS ecommerce.gold.products AS SELECT * FROM workspace.ecommerce.gold_events;
SHOW VOLUMES IN ecommerce_data;

-- Permissions
GRANT SELECT ON TABLE gold.products TO `analysts`;
GRANT ALL PRIVILEGES ON SCHEMA silver TO `engineers`;

CREATE TABLE ecommerce.gold.products (
  product_id STRING,
  product_name STRING,
  revenue DOUBLE,
  purchases INT,
  conversion_rate DOUBLE
)
USING DELTA;

INSERT INTO ecommerce.gold.products
SELECT
  product_id,
  product_name,
  SUM(price * quantity) AS revenue,
  COUNT_IF(event_type = 'purchase') AS purchases,
  COUNT_IF(event_type = 'purchase') / COUNT(*) AS conversion_rate
FROM ecommerce.silver_events
GROUP BY product_id, product_name;


CREATE OR REPLACE TABLE ecommerce.silver_events AS
SELECT
  event_time,
  event_type,
  product_id,
  category_id,
  category_code,
  brand,
  CAST(price AS DOUBLE) AS price,
  user_id,
  user_session,
  CASE 
    WHEN event_type = 'purchase' THEN 1 
    ELSE 0 
  END AS quantity
FROM ecommerce.bronze_events;

CREATE OR REPLACE TABLE ecommerce.gold.products AS
SELECT
  product_id,
  brand,
  SUM(price * quantity) AS revenue,
  SUM(quantity) AS purchases
FROM ecommerce.silver_events
GROUP BY product_id, brand;

select * from ecommerce.gold.products

-- Controlled view
CREATE OR REPLACE VIEW ecommerce.gold.top_products AS
SELECT
  product_id,
  revenue,
  purchases,
  purchases * 1.0 / total_events AS conversion_rate
FROM (
  SELECT
    product_id,
    SUM(price * quantity) AS revenue,
    SUM(quantity) AS purchases,
    COUNT(*) AS total_events
  FROM ecommerce.silver_events
  GROUP BY product_id
)
WHERE purchases > 10
ORDER BY revenue DESC
LIMIT 100;


select * from ecommerce.gold.top_products



