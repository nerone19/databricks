# Databricks notebook source
from pyspark.sql.types import StructType,IntegerType, StringType, DoubleType,StructField
from pyspark.sql.functions import col,current_timestamp,lit,concat

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

dbutils.widgets.text("p_file_date","2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

name_schema = StructType(fields= [StructField("forename",StringType(),True),
                                  StructField("surname",StringType(),True),
                                  
    
    
    
    
])

# COMMAND ----------

drivers_schema = StructType(fields= [StructField("driverId",IntegerType(),True),
                                      StructField("driverRef",StringType(),True),
                                     StructField("number",IntegerType(),True),
                                     StructField("code",StringType(),True),
                                     StructField("name",name_schema),
                                     StructField("nationality",StringType(),True),
                                     StructField("url",StringType(),True),
                                  
    
    
    
    
])

# COMMAND ----------

drivers_df = spark.read\
.schema(drivers_schema)\
.json(f"{raw_folder_path}{v_file_date}/drivers.json")

# COMMAND ----------

drivers_df.printSchema()

# COMMAND ----------

display(drivers_df)

# COMMAND ----------

drivers_with_columns_df = drivers_df.withColumnRenamed("driverId","driver_id")\
.withColumnRenamed("driverRef","driver_ref")\
.withColumn("ingestion_date",current_timestamp())\
.withColumn("name",concat(col("name.forename"), lit(" "), col("name.surname")))\
.withColumn("file_date", lit(v_file_date))         

# COMMAND ----------

display(drivers_with_columns_df)

# COMMAND ----------

drivers_final_df = drivers_with_columns_df.drop(col('url'))

# COMMAND ----------

drivers_final_df.write.mode("overwrite").format('parquet').saveAsTable("f1_processed.drivers")

# COMMAND ----------

