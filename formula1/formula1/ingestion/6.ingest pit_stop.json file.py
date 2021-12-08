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

pit_stop_schema = StructType(fields= [StructField("raceId",IntegerType(),True),
                                      StructField("driverId",StringType(),True),
                                     StructField("stop",StringType(),True),
                                     StructField("lap",IntegerType(),True),
                                     StructField("type",IntegerType(),True),
                                     StructField("time",StringType(),True),
                                     StructField("duration",StringType(),True),
                                      StructField("milliseconds",IntegerType(),True)
    
])

# COMMAND ----------

pit_stop_df = spark.read\
.schema(pit_stop_schema)\
.option("multiline",True)\
.json(f"{raw_folder_path}{v_file_date}/pit_stops.json")

# COMMAND ----------

display(pit_stop_df)

# COMMAND ----------

# MAGIC %md read multi line 

# COMMAND ----------

pit_stop_with_column_df = pit_stop_df.withColumnRenamed("driverId","driver_id")\
.withColumnRenamed("raceId","race_id")\
.withColumn("ingestion_date",current_timestamp())

# COMMAND ----------

# pit_stop_with_column_df.write.mode("overwrite").format('parquet').saveAsTable("f?1_processed.pit_stops")

# COMMAND ----------

merge_condition = "tgt.race_id = src.race_id AND tgt.driver_id = src.driver_id AND tgt.stop = src.stop AND tgt.race_id = src.race_id"
merge_delta_data(pit_stop_with_column_df, "f1_processed", "pit_stops", processed_folder_path, merge_condition, 'race_id')

# COMMAND ----------

display(pit_stop_with_column_df)

# COMMAND ----------

