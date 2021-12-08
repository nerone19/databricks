# Databricks notebook source
# MAGIC %md produce driver standings

# COMMAND ----------

from pyspark.sql.functions import sum as _sum
from pyspark.sql.functions import when,count,col,desc,rank
from pyspark.sql.window import Window

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

race_results_df = spark.read.parquet(f"{presentation_folder_path}/race_results")


# COMMAND ----------

display(race_results_df)

# COMMAND ----------

race_results_df.printSchema()

# COMMAND ----------

race_results_df.na.fill({'points': 0}).show()

# COMMAND ----------

driver_standing_by = race_results_df.groupBy("race_year","driver_name","driver_nationality","team")\
.agg(_sum("points").alias("total_points"),count(when(col("position") ==1, True )).alias("wins") )

# COMMAND ----------

display(driver_standing_by.filter("race_year = 2020"))

# COMMAND ----------


driver_rank_spec = Window.partitionBy("race_year").orderBy(desc("total_points"),desc("wins"))
final_df = driver_standing_by.withColumn("rank", rank().over(driver_rank_spec) )

# COMMAND ----------

display(final_df.filter("race_year = 2020"))

# COMMAND ----------

final_df.write.mode("overwrite").format('parquet').saveAsTable("f1_presentation.drivers_standings")

# COMMAND ----------

