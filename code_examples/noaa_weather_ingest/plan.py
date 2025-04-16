# Databricks notebook source
"""
The goal of this project will to show the following with publicacly available information 
- ELT Ingestion approaches
  - Lakeflow pipelines
  - Custom logic
  - Structured Streaming
- Mangaged tables
  - auto liquid clustering
  - partioning
  - Z ordering 
-Relationships on tables 

To do:
- figure out how to extract query profile from the merge to demonstrate full table scan
- Add expectations so I can make clear the WAP approach works for 
"""

# COMMAND ----------

# MAGIC %sql 
# MAGIC SELECT zip_code.state_abbreviation,
# MAGIC count(*)
# MAGIC FROM leigh_robertson_demo.bronze_noaa.zip_code AS zip_code
# MAGIC GROUP BY zip_code.state_abbreviation
# MAGIC ORDER BY count(*) DESC

# COMMAND ----------

# MAGIC %sql 
# MAGIC SELECT zip_code.place_name,
# MAGIC forecasts.* 
# MAGIC FROM leigh_robertson_demo.bronze_noaa.forecasts AS forecasts
# MAGIC INNER JOIN leigh_robertson_demo.bronze_noaa.zip_code AS zip_code
# MAGIC   ON forecasts.post_code = zip_code.post_code
# MAGIC WHERE zip_code.state_abbreviation = 'RI'

# COMMAND ----------

# MAGIC %sql 
# MAGIC SELECT post_code,
# MAGIC startTime,
# MAGIC count(*)
# MAGIC FROM leigh_robertson_demo.silver_noaa.silver_noaa_forecasts_mv_v1
# MAGIC GROUP BY ALL
# MAGIC ORDER BY count(*) DESC;

# COMMAND ----------

# MAGIC %sql 
# MAGIC DESCRIBE DETAIL leigh_robertson_demo.bronze_noaa.forecasts;

# COMMAND ----------

spark.sql("ALTER TABLE leigh_robertson_demo.bronze_noaa.forecasts SET TBLPROPERTIES (delta.enableChangeDataFeed = true)")

# COMMAND ----------

# MAGIC %sql 
# MAGIC SELECT * 
# MAGIC FROM leigh_robertson_demo.silver_noaa.silver_noaa_forecasts_mv_v1
# MAGIC ORDER BY audit_update_ts DESC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- CREATE MATERIALIZED VIEW leigh_robertson_demo.silver_noaa.state_temperature_extremes
# MAGIC -- REFRESH DAILY
# MAGIC -- AS
# MAGIC
# MAGIC WITH daily_forecasts AS (
# MAGIC   SELECT 
# MAGIC     f.post_code,
# MAGIC     z.place_name,
# MAGIC     z.state,
# MAGIC     f.temperature,
# MAGIC     f.probabilityOfPrecipitation,
# MAGIC     f.windSpeed,
# MAGIC     CAST(f.startTimeUTC AS date) AS forecast_date
# MAGIC   FROM leigh_robertson_demo.silver_noaa.forecasts_expanded_manual_cdc AS f
# MAGIC   INNER JOIN leigh_robertson_demo.bronze_noaa.zip_code AS z
# MAGIC     ON f.post_code = z.post_code
# MAGIC ),
# MAGIC
# MAGIC state_min_temps AS (
# MAGIC   SELECT 
# MAGIC     state,
# MAGIC     forecast_date,
# MAGIC     MIN(temperature) AS min_temperature
# MAGIC   FROM daily_forecasts
# MAGIC   GROUP BY state, forecast_date
# MAGIC ),
# MAGIC
# MAGIC state_max_temps AS (
# MAGIC   SELECT 
# MAGIC     state,
# MAGIC     forecast_date,
# MAGIC     MAX(temperature) AS max_temperature
# MAGIC   FROM daily_forecasts
# MAGIC   GROUP BY state, forecast_date
# MAGIC ),
# MAGIC
# MAGIC min_temp_cities AS (
# MAGIC   SELECT 
# MAGIC     df.forecast_date,
# MAGIC     df.state,
# MAGIC     df.place_name,
# MAGIC     df.post_code,
# MAGIC     df.temperature,
# MAGIC     df.probabilityOfPrecipitation,
# MAGIC     df.windSpeed,
# MAGIC     'LOWEST' AS temperature_type,
# MAGIC     ROW_NUMBER() OVER (PARTITION BY df.state, df.forecast_date ORDER BY df.temperature ASC) AS rn
# MAGIC   FROM daily_forecasts df
# MAGIC   JOIN state_min_temps smt
# MAGIC     ON df.state = smt.state 
# MAGIC     AND df.forecast_date = smt.forecast_date
# MAGIC     AND df.temperature = smt.min_temperature
# MAGIC ),
# MAGIC
# MAGIC max_temp_cities AS (
# MAGIC   SELECT 
# MAGIC     df.forecast_date,
# MAGIC     df.state,
# MAGIC     df.place_name,
# MAGIC     df.post_code,
# MAGIC     df.temperature,
# MAGIC     df.probabilityOfPrecipitation,
# MAGIC     df.windSpeed,
# MAGIC     'HIGHEST' AS temperature_type,
# MAGIC     ROW_NUMBER() OVER (PARTITION BY df.state, df.forecast_date ORDER BY df.temperature DESC) AS rn
# MAGIC   FROM daily_forecasts df
# MAGIC   JOIN state_max_temps smt
# MAGIC     ON df.state = smt.state 
# MAGIC     AND df.forecast_date = smt.forecast_date
# MAGIC     AND df.temperature = smt.max_temperature
# MAGIC ),
# MAGIC
# MAGIC union_results AS (
# MAGIC SELECT 
# MAGIC   forecast_date,
# MAGIC   state,
# MAGIC   place_name,
# MAGIC   post_code,
# MAGIC   temperature,
# MAGIC   probabilityOfPrecipitation,
# MAGIC   windSpeed,
# MAGIC   temperature_type
# MAGIC FROM min_temp_cities
# MAGIC WHERE rn = 1
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 
# MAGIC   forecast_date,
# MAGIC   state,
# MAGIC   place_name,
# MAGIC   post_code,
# MAGIC   temperature,
# MAGIC   probabilityOfPrecipitation,
# MAGIC   windSpeed,
# MAGIC   temperature_type
# MAGIC FROM max_temp_cities
# MAGIC WHERE rn = 1
# MAGIC )
# MAGIC SELECT * 
# MAGIC FROM union_results
# MAGIC ORDER BY forecast_date ASC, state ASC, place_name ASC

# COMMAND ----------



# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC usage_metadata.job_id,
# MAGIC sku_name,
# MAGIC sum(usage_quantity),
# MAGIC count(*)
# MAGIC FROM 
# MAGIC   system.billing.usage
# MAGIC INNER JOIN 
# MAGIC WHERE usage_metadata.job_id IS NOT NULL
# MAGIC AND usage_metadata.job_id = '697624279113020'
# MAGIC GROUP BY ALL 

# COMMAND ----------

# MAGIC %sql 
# MAGIC SELECT * 
# MAGIC FROM system.billing.list_prices
# MAGIC LIMIT 10

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   * EXCEPT (startTime, endTime, dewpoint, probabilityOfPrecipitation, relativeHumidity, audit_update_ts, windSpeed),
# MAGIC   regexp_extract(startTime, '([+-]\\d{2}:\\d{2})$', 1) AS timezoneOffset,
# MAGIC   regexp_replace(startTime, '[+-]\\d{2}:\\d{2}$', '') AS startTime,
# MAGIC   regexp_replace(endTime, '[+-]\\d{2}:\\d{2}$', '') AS endTime,
# MAGIC   CASE 
# MAGIC     WHEN regexp_extract(startTime, '([+-]\\d{2}:\\d{2})$', 1) != '' 
# MAGIC     THEN from_utc_timestamp(
# MAGIC            regexp_replace(startTime, '[+-]\\d{2}:\\d{2}$', ''), 
# MAGIC            regexp_extract(startTime, '([+-]\\d{2}:\\d{2})$', 1)
# MAGIC          )
# MAGIC     ELSE regexp_replace(startTime, '[+-]\\d{2}:\\d{2}$', '')
# MAGIC   END AS startTimeUTC,
# MAGIC   CASE 
# MAGIC     WHEN regexp_extract(startTime, '([+-]\\d{2}:\\d{2})$', 1) != '' 
# MAGIC     THEN from_utc_timestamp(
# MAGIC            regexp_replace(endTime, '[+-]\\d{2}:\\d{2}$', ''), 
# MAGIC            regexp_extract(startTime, '([+-]\\d{2}:\\d{2})$', 1)
# MAGIC          )
# MAGIC     ELSE regexp_replace(endTime, '[+-]\\d{2}:\\d{2}$', '')
# MAGIC   END AS endTimeUTC,
# MAGIC   CAST(regexp_extract(windSpeed, '(\\d+)', 1) AS INT) AS windSpeed,
# MAGIC   dewpoint.value AS dewpoint,
# MAGIC   probabilityOfPrecipitation.value AS probabilityOfPrecipitation,
# MAGIC   relativeHumidity.value AS relativeHumidity,
# MAGIC   CURRENT_TIMESTAMP() AS audit_update_ts
# MAGIC FROM table_changes('leigh_robertson_demo.bronze_noaa.forecasts', '0')

# COMMAND ----------

# MAGIC %sql 
# MAGIC DESCRIBE HISTORY leigh_robertson_demo.bronze_noaa.forecasts;

# COMMAND ----------

# MAGIC %sql 
# MAGIC SELECT count(*)
# MAGIC FROM leigh_robertson_demo.silver_noaa.forecasts_expanded_dlt;

# COMMAND ----------

# MAGIC %sql 
# MAGIC SELECT count(*)
# MAGIC FROM leigh_robertson_demo.silver_noaa.forecasts_ss;

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH base AS (
# MAGIC SELECT 
# MAGIC   forecasts.startTime,
# MAGIC   zip_code.place_name,
# MAGIC   zip_code.state,
# MAGIC   zip_code.post_code,
# MAGIC   forecasts.temperature,
# MAGIC   forecasts.probabilityOfPrecipitation,
# MAGIC   forecasts.windSpeed,
# MAGIC   CAST(forecasts.startTimeUTC AS DATE) AS forecast_date_utc,
# MAGIC   CAST(forecasts.startTime AS DATE) AS forecast_date_local_tz
# MAGIC FROM leigh_robertson_demo.silver_noaa.forecasts_expanded_dlt AS forecasts
# MAGIC INNER JOIN leigh_robertson_demo.bronze_noaa.zip_code AS zip_code
# MAGIC   ON forecasts.post_code = zip_code.post_code
# MAGIC WHERE CAST(forecasts.startTimeUTC AS DATE) BETWEEN 
# MAGIC current_date() AND 
# MAGIC DATE_ADD(current_date(), 10)
# MAGIC AND zip_code.post_code = 80214      
# MAGIC ORDER BY forecast_date_utc ASC, forecasts.startTime ASC
# MAGIC )
# MAGIC SELECT forecast_date_local_tz,
# MAGIC max(temperature) AS max_temperature,
# MAGIC min(temperature) AS min_temperature
# MAGIC FROM base 
# MAGIC GROUP BY ALL

# COMMAND ----------

# MAGIC %sql 
# MAGIC SELECT count(*)
# MAGIC FROM leigh_robertson_demo.bronze_noaa.forecasts

# COMMAND ----------


