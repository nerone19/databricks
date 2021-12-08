# Databricks notebook source
# MAGIC %run "../includes/configuration"

# COMMAND ----------

constructors_df = spark.read.parquet(f"{processed_folder_path}/constructors")\
.withColumnRenamed("name","team")

# COMMAND ----------

races_df = spark.read.parquet(f"{processed_folder_path}/races")\
.withColumnRenamed("name","race_name")\
.withColumnRenamed("race_timestamp","race_date")

# COMMAND ----------

drivers_df = spark.read.parquet(f"{processed_folder_path}/drivers")\
.withColumnRenamed("number","driver_number")\
.withColumnRenamed("name","driver_name")\
.withColumnRenamed("nationality","driver_nationality")

# COMMAND ----------

circuits_df = spark.read.parquet(f"{processed_folder_path}/circuits")\
.withColumnRenamed("location","circuit_location")

# COMMAND ----------

results_df = spark.read.parquet(f"{processed_folder_path}/results")\
.withColumnRenamed("time","race_time")\
.withColumnRenamed("raceId","race_id")

# COMMAND ----------

# MAGIC %md join circuits to races

# COMMAND ----------

race_circuits_df = races_df.join(circuits_df,races_df.circuit_id == circuits_df.circuit_id, "inner") \
.select(races_df.race_id,races_df.race_name,races_df.race_date, circuits_df.circuit_location)

# COMMAND ----------

race_results_df = results_df.join(race_circuits_df, results_df.race_id == race_circuits_df.race_id, "inner") \
.join(drivers_df, results_df.driver_id == drivers_df.driver_id)\
.join(constructors_df, constructors_df.constructor_id == results_df.constructor_id)

# COMMAND ----------

from pyspark.sql.functions import col,current_timestamp,lit
from pyspark.sql.functions import year

# COMMAND ----------

final_df = race_results_df.select("race_date","circuit_location","driver_name","driver_number","driver_nationality","team","grid","fastest_lap","race_time","points","position")\
.withColumn("race_year", year(race_results_df.race_date))\
.withColumn("created_date", current_timestamp())

# COMMAND ----------

display(final_df)

# COMMAND ----------

final_df.write.mode("overwrite").format('parquet').saveAsTable("f1_presentation.race_results")

# COMMAND ----------

