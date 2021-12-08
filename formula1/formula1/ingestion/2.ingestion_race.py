# Databricks notebook source
# MAGIC %md 
# MAGIC Ingest race.csv file

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

dbutils.widgets.text("p_file_date","2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Step 1 - read csv using pyspark dataframe

# COMMAND ----------

from pyspark.sql.types import StructType,IntegerType, StringType, DoubleType,StructField,DateType
from pyspark.sql.functions import col,current_timestamp,lit,to_timestamp,concat

# COMMAND ----------

race_schema =  StructType(fields= [StructField("racetId", IntegerType(), False),
                                      StructField("year", IntegerType(), True),
                                      StructField("round", IntegerType(), True),
                                      StructField("circuitId", IntegerType(), True),
                                      StructField("name", StringType(), True),
                                      StructField("date", DateType(), True),
                                      StructField("time", StringType(), True),
                                      StructField("url", StringType(), True),
    
    
])

# COMMAND ----------

#we check whther the schema is aligned with the expected one 
race_df = spark.read \
.schema(race_schema) \
.csv(f"dbfs:{raw_folder_path}{v_file_date}/races.csv",header=True)

# COMMAND ----------

display(race_df)

# COMMAND ----------

race_with_timestamp_df = race_df.withColumn("ingesion_date", current_timestamp()) \
                                .withColumn("race_timestamp", to_timestamp(concat(col('date'), lit(' '), col('time')), 'yyyy-MM-dd HH:mm:ss'   )) \
                                .withColumn("file_date", lit(v_file_date))

# COMMAND ----------

display(race_with_timestamp_df)

# COMMAND ----------

# MAGIC %md 
# MAGIC Select only the require columns

# COMMAND ----------

race_selected_df = race_with_timestamp_df.select(col("racetId").alias('race_id'),col("year"),col("round"),col("circuitId").alias('circuit_id'),col("name"),col("ingesion_date"),col("race_timestamp"))

# COMMAND ----------

display(race_selected_df)

# COMMAND ----------

#race_selected_df.write.mode('overwrite').format('parquet').saveAsTable("f1_processed.races")
race_selected_df.write.mode("overwrite").format('delta').saveAsTable("f1_processed.races")

# COMMAND ----------

# MAGIC %fs 
# MAGIC ls /mnt/formula19/processed/races

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Step 1 - read csv using pyspark dataframe

# COMMAND ----------

