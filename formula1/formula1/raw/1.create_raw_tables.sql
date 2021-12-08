-- Databricks notebook source
CREATE DATABASE IF NOT EXISTS f1_raw;

-- COMMAND ----------

-- MAGIC %md Create circuitd table

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS f1_raw.circuits(

circuitId INT,
circuitRef STRING,
name STRING,
location STRING,
country STRING,
lat DOUBLE,
lng DOUBLE,
alt INT,
url STRING
)
USING csv
OPTIONS (path "/mnt/formula19/raw/circuits.csv",header=true)

-- COMMAND ----------

SELECT * FROM f1_raw.circuits;

-- COMMAND ----------

DROP TABLE f1_raw.circuits;

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS f1_raw.races(
racetId INT,
year INT,
round INT,
name STRING,
circuitId INT,
date STRING,
time STRING,
url STRING
)
USING csv
OPTIONS (path "/mnt/formula19/raw/races.csv",header=true)

-- COMMAND ----------

SELECT * FROM f1_raw.races;

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.drivers;
CREATE TABLE IF NOT EXISTS f1_raw.drivers(
number INT,
driverId INT,
driverRef STRING,
code STRING,
name STRUCT<forename: STRING, surname: STRING>,
nationality STRING,
url STRING,
dob DATE
)
USING json
OPTIONS (path "/mnt/formula19/raw/drivers.json",header=true)



-- COMMAND ----------

SELECT * FROM f1_raw.drivers;

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.constructors;
CREATE TABLE IF NOT EXISTS f1_raw.constructors(
constructorId INT,
constructorRef STRING,
name STRING,
nationality STRING,
url STRING
)
USING json
OPTIONS (path "/mnt/formula19/raw/constructors.json",header=true)


-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.results;
CREATE TABLE IF NOT EXISTS f1_raw.results(
raceId INT,
constructorId INT,
driverId INT,
grid INT,
number INT,
position INT,
positionText STRING,
positionOrder INT,
points FLOAT,
laps INT,
time STRING,
milliseconds INT,
fastestLap INT,
rank INT,
fastestLapTime STRING,
fastestLapSpeed FLOAT,
statusId STRING
)
USING json
OPTIONS (path "/mnt/formula19/raw/results.json",header=true)


-- COMMAND ----------

SELECT * FROM f1_raw.constructors;

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.pit_stops;
CREATE TABLE IF NOT EXISTS f1_raw.pit_stops(
driverId INT,
raceId INT,
stop INT,
lap INT,
time STRING,
milliseconds INT,
duration STRING
)
USING json
OPTIONS (path "/mnt/formula19/raw/pit_stops.json",multiline true)

-- COMMAND ----------

SELECT * FROM f1_raw.pit_stops;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC pit_stop_df = spark.read\
-- MAGIC .option("multiline",True)\
-- MAGIC .json("/mnt/formula19/raw/pit_stops.json")
-- MAGIC pit_stop_df.printSchema()

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.lap_times;
CREATE TABLE IF NOT EXISTS f1_raw.lap_times(
driverId INT,
raceId INT,
lap INT,
milliseconds INT,
time STRING,
position INT
)
USING csv
OPTIONS (path "/mnt/formula19/raw/lap_times")

-- COMMAND ----------

SELECT * FROM f1_raw.lap_times;

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.qualifying;
CREATE TABLE IF NOT EXISTS f1_raw.qualifying(
qualifyId INT,
driverId INT,
raceId INT,
constructorId INT,
number INT,
q1 STRING,
q2 STRING,
q3 STRING,
position INT
)
USING json
OPTIONS (path "/mnt/formula19/raw/qualifying", multiline true)


-- COMMAND ----------

SELECT * FROM f1_raw.qualifying;