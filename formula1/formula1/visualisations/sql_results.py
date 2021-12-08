# Databricks notebook source
dbutils.widgets.text("p_file_date","2021-03-28")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

spark.sql(f"""
         CREATE OR REPLACE TEMP VIEW race_result_updated
         AS
         SELECT race.race_year,
                 constructors.name AS team_name,
                 drivers.driver_id,
                 drivers.driver_name AS driver_name,
                 races.race_id,
                 results.position,
                 results.points,
                 i1 - results.position AS calculated_points
             FROM f1.processed.resutls
             JOIN f1_processed.drivers ON (results.driver_id = drivers.driver_id)
             JOIN f1_processed.constructors ON (results.constructor_id = constructors.constructor_id)
             JOIN f1_processed.races ON (results.constructor_id = race.race_id)
         WHERE results.position <= 10
             AND resutls.file_date = '{v_file_date}'
         
 """)

# COMMAND ----------

# MAGIC %sql 
# MAGIC MERGE INTO f1_presentation.calculated_race_results tgt
# MAGIC USING race_result_updated upd
# MAGIC ON (tgt.drvier_id = upd.driver_id AND tgt.race_id = upd.race_id)
# MAGIC WHEN MATCHED THEN 
# MAGIC   UPDATE  SET tgt.position = upd.position,
# MAGIC               tgt.poimts = upd.points,
# MAGIC               tgt.calculated_points = upd.calculated_points,
# MAGIC               tgt.updated_date = current_timestamp
# MAGIC               
# MAGIC WHEN NOT MATCHED 
# MAGIC   THEN INSERT (race_year,team_name,driver_id,driver_name,race_id,potsition, points,calculated_points,created)