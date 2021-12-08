# Databricks notebook source
from pyspark.sql.functions import current_timestamp
def add_ingestion_date(input_df):
    output_df = input_df.withColumn("ingestion_date", current_timestamp())
    return output_df

# COMMAND ----------

def re_arrange_partition_column(input_df, partition_column):
    column_list = []

    for column_name in input_df.schema.names:
        if column_name != partition_column:
            column_list.append(column_name)
    column_list.append(partition_column)

    output_df = input_df.select(column_list)
    return output_df

# COMMAND ----------

def overwrite_partition(input_df, db_name, table_name,partition_column):
    output_df = re_arrange_partition_column(input_df,partition_column)
    spark.conf.set("spark.sql.source.partitionOverwriteMode","dynamic")
    if (spark._jsparkSession.catalog().tableExists(f"{db_name}.{table_name}")):
        #table already existing(the column we want to partition by need to be placed as last column)
        output_df.write.mode("overwrite").insertInto(f"{db_name}.{table_name}")
    else: 
        #table not existing yet
        output_df.write.mode("overwrite").partitionBy(partition_column).format('parquet').saveAsTable(f"{db_name}.{table_name}")


# COMMAND ----------

def  merge_delta_data(input_df, db_name, table_name, folder_path, merge_condition, partition_column):
    spark.conf.set("spark.databricks.optimizer.dynamicPartitionPruning", "true")
    if (spark._jsparkSession.catalog().tableExists(f"{db_name}.{table_name}")):
            #we exploit the nice feature about delta tables, ie merging tables of different times 
            deltaTable = DeltaTable.forPath(spark, f"{folder_path}/{table_name}")
            deltaTable.alias('tgt').merge(
                input_df.alias("src"),
                merge_condition) \
                .whenMatchedUpdateAll()\
                .whenNotMatchedInsertAll()\
                .execute()
    else: 
        #table not existing yet
        input_df.write.mode("overwrite").partitionBy(partition_column).format('delta').saveAsTable(f"{db_name}.{table_name}")

# COMMAND ----------

