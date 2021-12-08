-- Databricks notebook source
USE f1_processed;

-- COMMAND ----------

-- MAGIC %md total race results

-- COMMAND ----------

CREATE TABLE f1_presentation.calculated_race_results
USING parquet
AS
SELECT 
    races.year,
    constructors.name as team_name,
    drivers.name as driver_name,
    results.position,
    results.points,
    11 -  results.position as calculated_points
  FROM f1_processed.results
  JOIN f1_processed.drivers ON (results.driver_id = drivers.driver_id)
  JOIN f1_processed.constructors ON (results.constructor_id = constructors.constructor_id)
  JOIN f1_processed.races ON (results.race_id = races.race_id)
  WHERE results.position <= 10

-- COMMAND ----------

SELECT * FROM f1_presentation.calculated_race_results

-- COMMAND ----------

-- MAGIC %md dominant  drivers

-- COMMAND ----------


SELECT driver_name, 
          COUNT(1) AS total_races,
          SUM(calculated_points) AS total_points,
          AVG(calculated_points) as avg_points 
  FROM f1_presentation.calculated_race_results
GROUP BY driver_name
HAVING count(1) >= 50
ORDER BY total_points DESC

--having refers to ann aggregator in the select statement

-- COMMAND ----------

SELECT team_name, 
          COUNT(1) AS total_races,
          SUM(calculated_points) AS total_points,
          AVG(calculated_points) as avg_points 
  FROM f1_presentation.calculated_race_results
GROUP BY team_name
HAVING count(1) >= 50
ORDER BY total_points DESC

-- COMMAND ----------

SELECT driver_name, 
          COUNT(1) AS total_races,
          SUM(calculated_points) AS total_points,
          AVG(calculated_points) as avg_points,
          RANK() OVER(ORDER BY AVG(calculated_points) DESC) driver_rank
  FROM f1_presentation.calculated_race_results
GROUP BY driver_name
HAVING count(1) >= 50
ORDER BY total_points DESC



-- COMMAND ----------

SELECT year,driver_name, 
          COUNT(1) AS total_races,
          SUM(calculated_points) AS total_points,
          AVG(calculated_points) as avg_points
  FROM f1_presentation.calculated_race_results
GROUP BY driver_name,year
ORDER BY total_points,year DESC

-- COMMAND ----------

