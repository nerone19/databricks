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

v_file_date

# COMMAND ----------

drivers_schema = StructType(fields= [StructField("resultId",IntegerType(),True),
                                     StructField("raceId",IntegerType(),True),
                                     StructField("driverId",IntegerType(),True),
                                     StructField("constructorId",IntegerType(),True),
                                     StructField("number",IntegerType(),True),
                                     StructField("grid",IntegerType(),True),
                                     StructField("position",IntegerType(),True),
                                     StructField("positionText",StringType(),True),
                                     StructField("positionOrder",IntegerType(),True),
                                     StructField("points",FloatType(),True),
                                     StructField("laps",IntegerType(),True),
                                     StructField("time",StringType(),True),
                                     StructField("milliseconds",IntegerType(),True),
                                     StructField("fastestLap",IntegerType(),True),
                                     StructField("rank",IntegerType(),True),
                                     StructField("fastestLapTime",StringType(),True),
                                     StructField("fastestLapSpeed",FloatType(),True),
                                     StructField("statusId",StringType(),True)

                                     
                                  
    
    
    
    
])

# COMMAND ----------

results_df = spark.read\
.schema(drivers_schema)\
.json(f"{raw_folder_path}{v_file_date}/results.json")

# COMMAND ----------

results_df.printSchema()

# COMMAND ----------

display(results_df)

# COMMAND ----------

results_with_columns_df = results_df.withColumnRenamed("resultId","results_id")\
.withColumnRenamed("driverId","driver_id")\
.withColumnRenamed("raceId","race_id")\
.withColumnRenamed("constructorId","constructor_id")\
.withColumnRenamed("positionText","position_text")\
.withColumnRenamed("positionOrder","position_order")\
.withColumnRenamed("fastestLapTime","fastest_lap_time")\
.withColumnRenamed("fastestLap","fastest_lap")\
.withColumnRenamed("fastestLapSpeed","fastest_lap_speed")\
.withColumn("ingestion_date",current_timestamp())\
.withColumn("file_date", lit(v_file_date))

# COMMAND ----------

display(results_with_columns_df)

# COMMAND ----------

results_final_df = results_with_columns_df.drop(col('statusId'))

# COMMAND ----------

#INCREMENTAL LOAD(method 1)
# for race_id_list in results_final_df.select("race_id").distinct().collect():
#     if (spark._jsparkSession.catalog().tableExists("f1_processed_results")):
#         spark.sql(f"ALTER TABLE f1_processed.results DROP IF EXISTS PARTITION (race_id = {race_id_list.race_id})")

# COMMAND ----------

# results_final_df.write.mode("append").partitionBy('race_id').format('parquet').saveAsTable("f1_processed.results")

# COMMAND ----------

# MAGIC %md method 2

# COMMAND ----------

# MAGIC %md dinamically create df for partitionBy

# COMMAND ----------

# overwrite_partition(results_final_df, 'f1_processed', 'results','race_id')

# COMMAND ----------

# from delta.tables import DeltaTable

# spark.conf.set("spark.databricks.optimizer.dynamicPartitionPruning", "true")
# if (spark._jsparkSession.catalog().tableExists("f1_processed.results")):
#         #we exploit the nice feature about delta tables, ie merging tables of different times 
#         deltaTable = DeltaTable.forPath(spark, '/mnt/formulta19/processed/results')
#         deltaTable.alias('tgt').merge(
#             results_final_df.alias("src"),
#             "tgt.results_id = src.results_id AND tgt.race_id == src.race_id") \
#             .whenMatchedUpdateAll()\
#             .whenNotMatchedInsertAll()\
#             .execute()
# else: 
#     #table not existing yet
#     results_final_df.write.mode("overwrite").partitionBy('race_id').format('delta').saveAsTable('f1_processed.results')

# COMMAND ----------

merge_condition = "tgt.results_id = src.results_id AND tgt.race_id == src.race_id"
merge_delta_data(results_final_df, "f1_processed", "results", processed_folder_path, merge_condition, 'race_id')

# COMMAND ----------

display(spark.read.parquet("/mnt/formula19/processed/results"))

# COMMAND ----------

# MAGIC %sql 
# MAGIC SELECT race_id, COUNT(1)
# MAGIC FROM f1_processed.results
# MAGIC GROUP BY race_id;

# COMMAND ----------

display(results_final_df)

# COMMAND ----------

dbutils.notebook.exit("Success")