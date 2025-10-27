-- Databricks notebook source
-- MAGIC %md
-- MAGIC Data Ingestion (Table)
-- MAGIC 1. UI (Volume)
-- MAGIC 2. Pyspark Dataframe
-- MAGIC 3. SQL - read_file 

-- COMMAND ----------

/Volumes/datamaster/bronze/raw_csv_files/Spotify_Songs.csv

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df=spark.read.csv("/Volumes/datamaster/bronze/raw_csv_files/Spotify_Songs.csv",header=True,inferSchema=True)
-- MAGIC df.write.saveAsTable("datamaster.bronze.spotify_songs_pyspark")

-- COMMAND ----------

https://docs.databricks.com/aws/en/sql/language-manual/functions/read_files

-- COMMAND ----------

create table datamaster.bronze.spotify_songs_sql as 
(SELECT *,current_timestamp as ingestion_date FROM read_files(
    '/Volumes/datamaster/bronze/raw_csv_files/Spotify_Songs.csv',
    format => 'csv'))