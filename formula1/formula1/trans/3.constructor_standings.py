# Databricks notebook source
# MAGIC %md produce driver standings

# COMMAND ----------

from pyspark.sql.functions import sum as _sum
from pyspark.sql.functions import when,count,col,desc,rank
from pyspark.sql.window import Window

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

constructors_df = spark.read.parquet(f"{presentation_folder_path}/race_results")


# COMMAND ----------

display(constructors_df)

# COMMAND ----------

constructors_df.printSchema()

# COMMAND ----------

constructors_standing_by = constructors_df.groupBy("race_year","team")\
.agg(_sum("points").alias("total_points"),count(when(col("position") ==1, True )).alias("wins") )

# COMMAND ----------

display(constructors_standing_by.filter("race_year = 2020"))

# COMMAND ----------


constructors_rank_spec = Window.partitionBy("race_year").orderBy(desc("total_points"),desc("wins"))
final_df = constructors_standing_by.withColumn("rank", rank().over(constructors_rank_spec) )

# COMMAND ----------

display(final_df.filter("race_year = 2020"))

# COMMAND ----------

final_df.write.mode("overwrite").format('parquet').saveAsTable("f1_presentation.constructor_standings")


# COMMAND ----------

