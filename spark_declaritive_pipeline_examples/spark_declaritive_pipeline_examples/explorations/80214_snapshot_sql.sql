-- Databricks notebook source
CREATE OR REPLACE TABLE leigh_robertson_demo.silver_noaa.local_snapshot
SELECT * 
FROM leigh_robertson_demo.bronze_noaa.forecasts
WHERE post_code = "80214"
AND cast(startTime AS date) = current_date()


-- COMMAND ----------


