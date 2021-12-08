# Databricks notebook source
from pyspark.sql.types import StructType,IntegerType, StringType, DoubleType,StructField,FloatType
from pyspark.sql.functions import col,current_timestamp,lit,concat

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

dbutils.widgets.text("p_file_date","2021-03-28")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

lap_times_schema = StructType(fields= [StructField("raceId",IntegerType(),True),
                                      StructField("driverId",StringType(),True),
                                     StructField("lap",IntegerType(),True),
                                     StructField("position",IntegerType(),True),
                                     StructField("time",StringType(),True),
                                      StructField("milliseconds",IntegerType(),True)
    
])

# COMMAND ----------

lap_times_df = spark.read\
.schema(lap_times_schema)\
.csv(f"{raw_folder_path}{v_file_date}/lap_times")

# COMMAND ----------

display(lap_times_df)

# COMMAND ----------

# MAGIC %md read multi line 

# COMMAND ----------

lap_times_final_df = lap_times_df.withColumnRenamed("driverId","driver_id")\
.withColumnRenamed("raceId","race_id")\
.withColumn("ingestion_date",current_timestamp())

# COMMAND ----------

overwrite_partition(lap_times_final_df, 'f1_processed', 'lap_times','race_id')

# COMMAND ----------

# lap_times_final_df.write.mode("overwrite").format('parquet').saveAsTable("f1_processed.lap_times")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- DROP TABLE f1_processed.lap_times;

# COMMAND ----------

