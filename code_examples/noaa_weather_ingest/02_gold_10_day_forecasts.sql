-- Databricks notebook source
-- CREATE MATERIALIZED VIEW leigh_robertson_demo.gold_noaa.ten_day_forecast
-- REFRESH DAILY
-- AS


SELECT 
  forecasts.startTime,
  zip_code.place_name,
  zip_code.state,
  zip_code.post_code,
  forecasts.temperature,
  forecasts.probabilityOfPrecipitation,
  forecasts.windSpeed,
  CAST(forecasts.startTimeUTC AS DATE) AS forecast_date
FROM leigh_robertson_demo.silver_noaa.forecasts_expanded_manual_cdc AS forecasts
INNER JOIN leigh_robertson_demo.bronze_noaa.zip_code AS zip_code
  ON forecasts.post_code = zip_code.post_code
WHERE CAST(forecasts.startTimeUTC AS DATE) BETWEEN 
      current_date() AND 
      DATE_ADD(current_date(), 10)
ORDER BY forecast_date ASC, zip_code.state ASC, zip_code.place_name ASC;

-- COMMAND ----------


