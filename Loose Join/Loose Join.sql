-- Databricks notebook source
CREATE DATABASE tab1;
create table tab1.outage as ( select * from default.outage);
create table tab1.weather as ( select * from default.weather);

-- COMMAND ----------

use tab1;

-- COMMAND ----------

DESCRIBE outage;

-- COMMAND ----------

DESCRIBE weather;

-- COMMAND ----------

/*Row counts by both table*/
SELECT
    (SELECT COUNT(*) FROM outage) AS row_count_outage,
    (SELECT COUNT(*) FROM weather) AS row_count_weather;


-- COMMAND ----------

/*Find the number of outages by the different causes and Plot the Results that give the counts */
SELECT CAUSE, COUNT(*) FROM outage
GROUP BY CAUSE;

-- COMMAND ----------

/*Find the number of outages by different cause and facility types*/
SELECT CAUSE, FACILITY_TYPE_DESC, COUNT(*) FROM outage
GROUP BY CAUSE, FACILITY_TYPE_DESC;

-- COMMAND ----------

/*Plot the Max wind gust speed by month*/
SELECT SUBSTRING(TIME_GMT, CHARINDEX('-', TIME_GMT) + 1, CHARINDEX('-', TIME_GMT, CHARINDEX('-', TIME_GMT) + 1) - CHARINDEX('-', TIME_GMT) - 1)  AS extracted_month , MAX(WIND_GUST_MPH) FROM weather
GROUP BY extracted_month;


-- COMMAND ----------

-- MAGIC %md
-- MAGIC Create a Table that maps each outage location to its nearest weather station. This can be used to join the two tables and allow us to examine the impact of weather on outages.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Check how many unique outages location 

-- COMMAND ----------

SELECT count(DISTINCT(CONCAT(CAST(longitude AS STRING), '_', CAST(latitude AS STRING))))
FROM outage
WHERE longitude IS NOT NULL AND latitude IS NOT NULL AND longitude != 0 AND latitude != 0

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC create temporary view for joining

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW outage_loc AS
SELECT
    ROW_NUMBER() OVER (ORDER BY longitude, latitude) AS location_id,
    longitude,
    latitude
FROM outage
WHERE 
  longitude IS NOT NULL AND latitude IS NOT NULL AND longitude != 0 AND latitude != 0
group by
   latitude,
   longitude

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC Check the count of outage locations from created view

-- COMMAND ----------

select count(*) from outage_loc

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Check count of weather station

-- COMMAND ----------

SELECT count(DISTINCT METAR_ID) 
FROM weather


-- COMMAND ----------

-- MAGIC %md 
-- MAGIC Create temporary view of weather station location

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW weather_loc AS
SELECT METAR_ID, longitude, latitude 
FROM (
    SELECT METAR_ID, longitude, latitude,
           ROW_NUMBER() OVER (PARTITION BY METAR_ID ORDER BY longitude DESC) AS long_rank
    FROM weather
) AS RankedData
WHERE long_rank = 1;


-- COMMAND ----------

-- MAGIC %md
-- MAGIC Check count of weather station

-- COMMAND ----------

select count(*) from weather_loc

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC Join the two tables
-- MAGIC

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW distance_table AS
SELECT
    o.location_id AS location_id,
    o.longitude AS o_longitude,
    o.latitude AS o_latitude,
    w.METAR_ID AS METAR_ID,
    w.longitude AS w_longitude,
    w.latitude AS w_latitude,
    SQRT(POW(o.longitude - w.longitude, 2) + POW(o.latitude - w.latitude, 2)) AS euclidean_distance
FROM
    outage_loc o
CROSS JOIN
    weather_loc w;

-- create reference table for nearest weather station
CREATE TABLE nearest_metar AS
SELECT
    location_id,
    o_longitude,
    o_latitude,
    METAR_ID,
    w_longitude,
    w_latitude,
    euclidean_distance
FROM (
    SELECT
        location_id,
        o_longitude,
        o_latitude,
        METAR_ID,
        w_longitude,
        w_latitude,
        euclidean_distance,
        ROW_NUMBER() OVER (PARTITION BY location_id ORDER BY euclidean_distance) AS distance_rank
    FROM
        distance_table
) AS RankedData
WHERE distance_rank = 1;


-- COMMAND ----------

select * from nearest_metar

-- COMMAND ----------

select count(*) from nearest_metar
