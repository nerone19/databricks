# Databricks notebook source
from pyspark.sql.types import StructType,IntegerType, StringType, DoubleType,StructField,FloatType
from pyspark.sql.functions import col,current_timestamp,lit,concat

# COMMAND ----------

qualifying_schema = StructType(fields= [StructField("qualifyId",IntegerType(),True),
                                      StructField("raceId",IntegerType(),True),
                                     StructField("driverId",IntegerType(),True),
                                     StructField("constructorId",IntegerType(),True),
                                     StructField("number",IntegerType(),True),
                                      StructField("position",IntegerType(),True),
                                       StructField("q1",StringType(),True),
                                       StructField("q2",StringType(),True),
                                       StructField("q3",StringType(),True)
    
])

# COMMAND ----------

qualifying_df = spark.read\
.schema(qualifying_schema)\
.option("multiline",True)\
.json("/mnt/formula19/raw/qualifying")

# COMMAND ----------

display(qualifying_df)

# COMMAND ----------

# MAGIC %md read multi line 

# COMMAND ----------

qualifying_final_df = qualifying_df.withColumnRenamed("qualifyId","qualify_id")\
.withColumnRenamed("raceId","race_id")\
.withColumnRenamed("driverId","driver_id")\
.withColumnRenamed("constructorId","constructor_id")\
.withColumn("ingestion_date",current_timestamp())

# COMMAND ----------

display(qualifying_final_df)

# COMMAND ----------

qualifying_final_df.write.mode("overwrite").format('parquet').saveAsTable("f1_processed.qualifying")

# COMMAND ----------

