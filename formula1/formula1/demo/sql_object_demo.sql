-- Databricks notebook source
CREATE DATABASE IF NOT EXISTS demo;


-- COMMAND ----------

-- MAGIC %run "../includes/configuration"

-- COMMAND ----------

SHOW tables;

-- COMMAND ----------

-- MAGIC %python 
-- MAGIC race_results_df = spark.read.parquet(f"{presentation_folder_path}/race_results")
-- MAGIC race_results_df.write.mode('overwrite').saveAsTable('demo_race_results')

-- COMMAND ----------

-- MAGIC %python 
-- MAGIC race_results_df.write.format('parquet').option('path',f"{presentation_folder_path}/race_results_ext").saveAsTable('demo_race_results_ext')

-- COMMAND ----------

USE demo;
SHOW tables;

-- COMMAND ----------

DESC EXTENDED demo_race_results_ext;

-- COMMAND ----------

SELECT * 
FROM demo_race_results
WHERE race_year = 2020;

-- COMMAND ----------

