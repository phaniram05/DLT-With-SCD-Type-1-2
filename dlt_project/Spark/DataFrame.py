# Databricks notebook source
data=[(1,'Naval'),(2,'Raj')]
schema="id int, name string"

# COMMAND ----------

# MAGIC %md
# MAGIC Creating a spark dataframe

# COMMAND ----------

df=spark.createDataFrame(data,schema)

# COMMAND ----------

display(df)