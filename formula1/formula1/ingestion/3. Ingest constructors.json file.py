# Databricks notebook source
# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

dbutils.widgets.text("p_file_date","2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

from pyspark.sql.types import StructType,IntegerType, StringType, DoubleType,StructField
from pyspark.sql.functions import col,current_timestamp,lit

# COMMAND ----------

constructors_schema = "constructorId INT, constructorRef STRING, name STRING, nationality STRING, url STRING"

# COMMAND ----------

constructor_df = spark.read\
.schema(constructors_schema)\
.json(f"{raw_folder_path}{v_file_date}/constructors.json")

# COMMAND ----------

display(constructor_df)

# COMMAND ----------

# MAGIC %md 
# MAGIC Drop unwanted colunms

# COMMAND ----------

constructor_dropped_df = constructor_df.drop(col('url'))

# COMMAND ----------

display(constructor_dropped_df)

# COMMAND ----------

constructor_final_df = constructor_dropped_df.withColumnRenamed("constructorId","constructor_id") \
.withColumnRenamed("constructorRef","constructor_ref") \
.withColumn("ingestion_data", current_timestamp())\
.withColumn("file_date", lit(v_file_date))

# COMMAND ----------

display(constructor_final_df)

# COMMAND ----------

# constructor_final_df.write.mode("overwrite").format('parquet').saveAsTable("f1_processed.constructors")
constructor_final_df.write.mode("overwrite").format('delta').saveAsTable("f1_processed.constructors")

# COMMAND ----------

