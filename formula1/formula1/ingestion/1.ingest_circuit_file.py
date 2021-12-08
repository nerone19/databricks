# Databricks notebook source
# MAGIC %md 
# MAGIC Ingest circuit.csv file

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Step 1 - read csv using pyspark dataframe

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

dbutils.widgets.text("p_data_sources", "")
v_data_source = dbutils.widgets.get("p_data_sources")

# COMMAND ----------

dbutils.widgets.text("p_file_date","2021-03-28")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

from pyspark.sql.types import StructType,IntegerType, StringType, DoubleType,StructField
from pyspark.sql.functions import col,current_timestamp,lit

# COMMAND ----------

circuit_schema =  StructType(fields= [StructField("circuitId", IntegerType(), False),
                                      StructField("circuitRef", StringType(), True),
                                      StructField("name", StringType(), True),
                                      StructField("location", StringType(), True),
                                      StructField("country", StringType(), True),
                                      StructField("lat", DoubleType(), True),
                                      StructField("lng", DoubleType(), True),
                                      StructField("alt", IntegerType(), True),
                                      StructField("url", StringType(), True),
    
    
])

# COMMAND ----------

#we check whther the schema is aligned with the expected one 
circuit_df = spark.read \
.schema(circuit_schema) \
.csv(f"dbfs:{raw_folder_path}{v_file_date}/circuits.csv",header=True)

# COMMAND ----------

display(circuit_df)

# COMMAND ----------

display(dbutils.fs.mounts())

# COMMAND ----------

# MAGIC %fs 
# MAGIC ls /mnt/formula19/raw

# COMMAND ----------

# MAGIC %md 
# MAGIC Select only the require columns

# COMMAND ----------

circuit_selected_df = circuit_df.select("circuitId","circuitRef","name","location","country","lat","lng","alt")

# COMMAND ----------



# COMMAND ----------

circuit_selected_df = circuit_df.select(col("circuitId"),col("circuitRef"),col("name"),col("location"),col("country"),col("lat"),col("lng"),col("alt"))

# COMMAND ----------

circuit_selected_df = circuit_selected_df.withColumnRenamed("circuitId","circuit_id")\
.withColumnRenamed("circuitRef","circuit_ref")\
.withColumnRenamed("lat","latitude")\
.withColumnRenamed("lng","longitude")\
.withColumnRenamed("alt","altitude")\
.withColumn("data_source", lit(v_data_source))\
.withColumn("file_date", lit(v_file_date))


# COMMAND ----------

display(circuit_selected_df)

# COMMAND ----------

circuit_selected_df2 = add_ingestion_date(circuit_selected_df)

# COMMAND ----------

# MAGIC %md write data in datalake as parquet

# COMMAND ----------

# circuit_selected_df.write.mode("overwrite").format('parquet').saveAsTable("f1_processed.circuits")

#case for delta table
circuit_selected_df.write.mode("overwrite").format('delta').saveAsTable("f1_processed.circuits")

# COMMAND ----------

#only for parquet format
# df = spark.read.parquet(f"{processed_folder_path}circuits")

# COMMAND ----------

dbutils.notebook.exit("Success")

# COMMAND ----------

# MAGIC %sql
# MAGIC DESC EXTENDED f1_processed.circuits

# COMMAND ----------

display(circuit_selected_df2)

# COMMAND ----------

