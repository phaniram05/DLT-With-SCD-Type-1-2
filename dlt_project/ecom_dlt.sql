-- Databricks notebook source
CREATE STREAMING LIVE TABLE sales
AS 
SELECT *, current_timestamp() as ingestion_date 
FROM cloud_files("/Volumes/workspace/dlt-project-schema/raw_files/sales", "csv");

-- COMMAND ----------

CREATE STREAMING TABLE sales_silver(
  CONSTRAINT valid_order_id EXPECT (order_id IS NOT NULL) ON VIOLATION DROP ROW
)
AS
SELECT DISTINCT * FROM STREAM(LIVE.sales)

-- COMMAND ----------

CREATE STREAMING LIVE TABLE customers
AS 
SELECT *, current_timestamp() as ingestion_date 
FROM cloud_files("/Volumes/workspace/dlt-project-schema/raw_files/customers", "csv");

-- COMMAND ----------

CREATE OR REPLACE STREAMING TABLE customers_silver;

APPLY CHANGES INTO LIVE.customers_silver
FROM STREAM(LIVE.customers)
KEYS 
  (customer_id)
APPLY AS DELETE WHEN
  operation = "DELETE"
SEQUENCE BY
  sequenceNum
COLUMNS * EXCEPT
  (operation, sequenceNum, _rescued_data, ingestion_date)
STORED AS
  SCD TYPE 2;

-- COMMAND ----------

create streaming table customers_silver_active as 
SELECT customer_id,customer_name,customer_email,customer_city,customer_state
FROM STREAM(live.customers_silver) where `__END_AT` is null

-- COMMAND ----------

-- Total sales and total discount amount for each customer
-- Just a materialized view
CREATE LIVE TABLE customer_purchases
AS
SELECT c.customer_id,
       c.customer_name,
       c.customer_email,
       COALESCE(SUM(CAST(s.total_amount AS DECIMAL)), 0) AS total_sales,
       COALESCE(SUM(CAST(s.discount_amount AS DECIMAL)), 0) AS total_discount
FROM LIVE.customers_silver_active c
LEFT JOIN LIVE.sales_silver s 
ON c.customer_id = s.customer_id
GROUP BY c.customer_id, c.customer_name, c.customer_email         
ORDER BY total_sales DESC;

-- COMMAND ----------

CREATE STREAMING LIVE TABLE products
AS 
SELECT *, current_timestamp() as ingestion_date 
FROM cloud_files("/Volumes/workspace/dlt-project-schema/raw_files/products", "csv");

-- COMMAND ----------

CREATE OR REPLACE STREAMING TABLE products_silver;

APPLY CHANGES INTO LIVE.products_silver
FROM STREAM(LIVE.products)
KEYS 
  (product_id)
APPLY AS DELETE WHEN
  operation = "DELETE"
SEQUENCE BY
  seqNum
COLUMNS * EXCEPT
  (operation, seqNum, _rescued_data, ingestion_date)
STORED AS
  SCD TYPE 1;

-- COMMAND ----------

-- CREATE OR REFRESH STREAMING TABLE customer_silver;

-- APPLY CHANGES INTO
--   live.customer_silver
-- FROM
--   stream(LIve.customers)
-- KEYS
--   (customer_id)
-- APPLY AS DELETE WHEN
--   operation = "DELETE"
-- SEQUENCE BY
--   sequenceNum
-- COLUMNS * EXCEPT
--   (operation,sequenceNum ,_rescued_data,ingestion_date
-- )
-- STORED AS
--   SCD TYPE 2;

-- COMMAND ----------

-- create streaming table customer_silver_active as 
-- select customer_id,customer_name,customer_email,customer_city,customer_state from STREAM(live.customer_silver) where `__END_AT` is null

-- COMMAND ----------

-- CREATE STREAMING LIVE TABLE products
-- AS SELECT *, current_timestamp() as ingestion_date FROM cloud_files("/mnt/sadmcadls/raw/dlt/products", "csv");

-- COMMAND ----------

-- -- Create and populate the target table.
-- CREATE OR REFRESH STREAMING TABLE product_silver;

-- APPLY CHANGES INTO
--   live.product_silver
-- FROM
--   stream(LIve.products)
-- KEYS
--   (product_id)
-- APPLY AS DELETE WHEN
--   operation = "DELETE"
-- SEQUENCE BY
--   seqNum
-- COLUMNS * EXCEPT
--   (operation,seqNum ,_rescued_data,ingestion_date
-- )
-- STORED AS
--   SCD TYPE 1;

-- COMMAND ----------

-- create live table total_sales_customer as 
-- SELECT 
--     c.customer_id,
--     c.customer_name,
--     round(SUM(s.total_amount)) AS total_sales,
--     SUM(s.discount_amount) AS total_discount
-- FROM LIVE.sales_silver s
-- JOIN LIVE.customer_silver_active c
--     ON s.customer_id = c.customer_id
-- GROUP BY c.customer_id, c.customer_name
-- ORDER BY total_sales DESC;

-- COMMAND ----------

-- create live table total_sales_category as
-- SELECT 
--     p.product_category,
--     round(SUM(s.total_amount)) AS total_sales
-- FROM LIVE.sales_silver s
-- JOIN live.product_silver p
--     ON s.product_id = p.product_id
-- GROUP BY p.product_category
-- ORDER BY total_sales DESC;