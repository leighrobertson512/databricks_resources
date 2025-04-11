-- Databricks notebook source
"""
Take the following query as a base. Can you help me create an MV within Databricks which would on a given day output two records for each state which would show the city with the lowest temperature and the highest temperature? Do not use any subqueries and instead use CTE's where applicable SELECT zip_code.post_code
,zip_code.place_name
,zip_code.state
,forecasts.temperature
,forecasts.probabilityOfPrecipitation
,forecasts.windSpeed
FROM leigh_robertson_demo.silver_noaa.forecasts_expanded_manual_cdc AS forecasts
INNER JOIN leigh_robertson_demo.bronze_noaa.zip_code AS zip_code
    ON forecasts.post_code = zip_code.post_code
WHERE cast(forecasts.startTimeUTC AS date) = 2025-04-09
ORDER BY forecasts.temperature ASC
"""

-- COMMAND ----------

CREATE MATERIALIZED VIEW leigh_robertson_demo.silver_noaa.state_temperature_extremes
REFRESH DAILY
AS

WITH daily_forecasts AS (
  SELECT 
    f.post_code,
    z.place_name,
    z.state,
    f.temperature,
    f.probabilityOfPrecipitation,
    f.windSpeed,
    f.startTime as localTime,
    CAST(f.startTimeUTC AS date) AS forecast_date
  FROM leigh_robertson_demo.silver_noaa.forecasts_expanded_manual_cdc AS f
  INNER JOIN leigh_robertson_demo.bronze_noaa.zip_code AS z
    ON f.post_code = z.post_code
  WHERE CAST(f.startTimeUTC AS date) >= current_date
),

state_min_temps AS (
  SELECT 
    state,
    forecast_date,
    MIN(temperature) AS min_temperature
  FROM daily_forecasts
  GROUP BY state, forecast_date
),

state_max_temps AS (
  SELECT 
    state,
    forecast_date,
    MAX(temperature) AS max_temperature
  FROM daily_forecasts
  GROUP BY state, forecast_date
),

min_temp_cities AS (
  SELECT 
    df.forecast_date,
    df.state,
    df.place_name,
    df.post_code,
    df.temperature,
    df.probabilityOfPrecipitation,
    df.windSpeed,
    df.localTime,
    'LOWEST' AS temperature_type,
    ROW_NUMBER() OVER (PARTITION BY df.state, df.forecast_date ORDER BY df.temperature ASC) AS rn
  FROM daily_forecasts df
  JOIN state_min_temps smt
    ON df.state = smt.state 
    AND df.forecast_date = smt.forecast_date
    AND df.temperature = smt.min_temperature
),

max_temp_cities AS (
  SELECT 
    df.forecast_date,
    df.state,
    df.place_name,
    df.post_code,
    df.temperature,
    df.probabilityOfPrecipitation,
    df.windSpeed,
    df.localTime,
    'HIGHEST' AS temperature_type,
    ROW_NUMBER() OVER (PARTITION BY df.state, df.forecast_date ORDER BY df.temperature DESC) AS rn
  FROM daily_forecasts df
  JOIN state_max_temps smt
    ON df.state = smt.state 
    AND df.forecast_date = smt.forecast_date
    AND df.temperature = smt.max_temperature
),

union_results AS (
SELECT 
  forecast_date,
  localTime,
  state,
  place_name,
  post_code,
  temperature,
  probabilityOfPrecipitation,
  windSpeed,
  temperature_type
FROM min_temp_cities
WHERE rn = 1

UNION ALL

SELECT 
  forecast_date,
  localTime,
  state,
  place_name,
  post_code,
  temperature,
  probabilityOfPrecipitation,
  windSpeed,
  temperature_type
FROM max_temp_cities
WHERE rn = 1
)
SELECT * 
FROM union_results
ORDER BY forecast_date ASC, state ASC, place_name ASC
