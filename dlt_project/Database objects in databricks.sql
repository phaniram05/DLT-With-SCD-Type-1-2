-- Databricks notebook source
-- MAGIC %python
-- MAGIC https://docs.databricks.com/aws/en/database-objects

-- COMMAND ----------

-- MAGIC %md
-- MAGIC - Catalog: The top level container, contains schemas. See What are catalogs in Databricks?.
-- MAGIC - Schema or database: Contains data objects. See What are schemas in Databricks?.
-- MAGIC - Data objects that can be contained in a schema:
-- MAGIC - Volume: a logical volume of non-tabular data in cloud object storage. See What are Unity Catalog volumes?.
-- MAGIC - Table: a collection of data organized by rows and columns. See What is a table?.
-- MAGIC - View: a saved query against one or more tables. See What is a view?.
-- MAGIC - Function: saved logic that returns a scalar value or set of rows. See User-defined functions (UDFs) in Unity Catalog.
-- MAGIC - Model: a machine learning model packaged with MLflow. See Manage model lifecycle in Unity Catalog.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ![](/Volumes/datamaster/bronze/images/object-model-0ed879da6c005615e8a00db9bb10823c.png)

-- COMMAND ----------

use catalog datamaster;

-- COMMAND ----------

create schema if not exists datamaster.bronze; 

-- COMMAND ----------

create table datamaster.bronze.test (id int, name string, age int);
insert into datamaster.bronze.test values (1,'Naval',33)

-- COMMAND ----------

select * from datamaster.bronze.test

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Functions

-- COMMAND ----------

use catalog datamaster;
use schema bronze;
-- creating a table
CREATE OR REPLACE TABLE customer_reviews (
    customer_id INT,
    first_name STRING,
    last_name STRING,
    review STRING
);

INSERT INTO customer_reviews VALUES
(1, 'John', 'Doe', 'Amazing product! I love it.'),
(2, 'Jane', 'Smith', 'Not good, I am disappointed.'),
(3, 'Alice', 'Brown', 'Decent quality but can be better.'),
(4, 'Bob', 'Johnson', 'Fantastic service! Highly recommend.'),
(5, 'Charlie', 'Davis', 'Terrible experience, never buying again.');

-- COMMAND ----------

select * from datamaster.bronze.customer_reviews

-- COMMAND ----------

-- DBTITLE 1,syntax for UDF
create function function_name(para datatype)
returns datatype
return logic

-- COMMAND ----------

-- DBTITLE 1,full name udf
create function datamaster.bronze.full_name_udf(first_name string, last_name string)
returns string
return first_name || ' ' || last_name;

-- COMMAND ----------

-- DBTITLE 1,usage
select customer_id, datamaster.bronze.full_name_udf(first_name, last_name) as full_name, review from datamaster.bronze.customer_reviews

-- COMMAND ----------

-- Create a complex UDF for sentiment analysis
CREATE FUNCTION datamaster.bronze.sentiment_analysis(review STRING)
RETURNS STRING
RETURN 
    CASE 
        WHEN review LIKE '%amazing%' OR review LIKE '%love%' OR review LIKE '%fantastic%' OR review LIKE '%recommend%' THEN 'Positive'
        WHEN review LIKE '%not good%' OR review LIKE '%disappointed%' OR review LIKE '%terrible%' OR review LIKE '%never buying%' THEN 'Negative'
        ELSE 'Neutral'
    END;


-- COMMAND ----------

select customer_id, review, datamaster.bronze.sentiment_analysis(review) as sentiment from datamaster.bronze.customer_reviews

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Views
-- MAGIC - Virtual Tables
-- MAGIC - 1.Standard view: persisted( saved )
-- MAGIC - 2.Temp view/ Global Temp view

-- COMMAND ----------

create schema if not exists datamaster.silver; 
create schema if not exists datamaster.gold;

-- COMMAND ----------

create or replace view datamaster.silver.customer_sentiment as 
select customer_id, review, datamaster.bronze.sentiment_analysis(review) as sentiment from datamaster.bronze.customer_reviews

-- COMMAND ----------

select * from datamaster.silver.customer_sentiment

-- COMMAND ----------

create or replace view datamaster.gold.customer_sentiment as
SELECT 
    datamaster.bronze.sentiment_analysis(review) AS sentiment, 
    COUNT(*) AS count 
FROM datamaster.bronze.customer_reviews
GROUP BY sentiment_analysis(review)

-- COMMAND ----------

select * from datamaster.gold.customer_sentiment