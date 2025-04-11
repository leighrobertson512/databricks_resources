-- Databricks notebook source
-- Create streaming view with timezone processing to match your Python implementation
CREATE STREAMING LIVE VIEW bronze_forecasts_preprocessed_sql
OPTIONS (
  readChangeFeed = "true",
  maxBytesPerTrigger = "10g"
)
AS
SELECT
  * EXCEPT (startTime, endTime, dewpoint, probabilityOfPrecipitation, relativeHumidity, audit_update_ts, windSpeed),
  regexp_extract(startTime, '([+-]\\d{2}:\\d{2})$', 1) AS timezoneOffset,
  regexp_replace(startTime, '[+-]\\d{2}:\\d{2}$', '') AS startTime,
  regexp_replace(endTime, '[+-]\\d{2}:\\d{2}$', '') AS endTime,
  CASE 
    WHEN regexp_extract(startTime, '([+-]\\d{2}:\\d{2})$', 1) != '' 
    THEN from_utc_timestamp(
           regexp_replace(startTime, '[+-]\\d{2}:\\d{2}$', ''), 
           regexp_extract(startTime, '([+-]\\d{2}:\\d{2})$', 1)
         )
    ELSE regexp_replace(startTime, '[+-]\\d{2}:\\d{2}$', '')
  END AS startTimeUTC,
  CASE 
    WHEN regexp_extract(startTime, '([+-]\\d{2}:\\d{2})$', 1) != '' 
    THEN from_utc_timestamp(
           regexp_replace(endTime, '[+-]\\d{2}:\\d{2}$', ''), 
           regexp_extract(startTime, '([+-]\\d{2}:\\d{2})$', 1)
         )
    ELSE regexp_replace(endTime, '[+-]\\d{2}:\\d{2}$', '')
  END AS endTimeUTC,
  CAST(regexp_extract(windSpeed, '(\\d+)', 1) AS INT) AS windSpeed,
  dewpoint.value AS dewpoint,
  probabilityOfPrecipitation.value AS probabilityOfPrecipitation,
  relativeHumidity.value AS relativeHumidity,
  CURRENT_TIMESTAMP() AS audit_update_ts
FROM stream(leigh_robertson_demo.bronze_noaa.forecasts);

-- Create target streaming table
CREATE LIVE TABLE silver_noaa_forecasts_sql_v2
COMMENT 'SCD Type 1 managed silver forecasts'
TBLPROPERTIES ('quality' = 'silver');
--AS SELECT * FROM STREAM(live.bronze_forecasts_preprocessed_sql) LIMIT 0;

-- Apply SCD Type 1 changes - matching your Python implementation
APPLY CHANGES INTO silver_noaa_forecasts_sql_v2
FROM stream(LIVE.bronze_forecasts_preprocessed_sql)
KEYS (post_code, startTime)
SEQUENCE BY audit_update_ts
STORED AS SCD TYPE 1;

-- COMMAND ----------

-- DBTITLE 1,working but won't ingest changes
-- -- Create streaming view with timezone processing
-- CREATE STREAMING LIVE VIEW bronze_forecasts_preprocessed_sql
-- AS
-- SELECT
--   * EXCEPT (startTime, endTime, windSpeed, dewpoint, probabilityOfPrecipitation, relativeHumidity, audit_update_ts),
--   REGEXP_REPLACE(startTime, '[+-]\\d{2}:\\d{2}$', '') AS startTime,
--   REGEXP_REPLACE(endTime, '[+-]\\d{2}:\\d{2}$', '') AS endTime,
--   REGEXP_EXTRACT(startTime, '([+-]\\d{2}:\\d{2})$', 1) AS timezoneOffset,
--   CASE 
--     WHEN timezoneOffset != '' 
--     THEN FROM_UTC_TIMESTAMP(
--            REGEXP_REPLACE(startTime, '[+-]\\d{2}:\\d{2}$', ''), 
--            timezoneOffset
--          )
--     ELSE startTime 
--   END AS startTimeUTC,
--   CASE 
--     WHEN timezoneOffset != '' 
--     THEN FROM_UTC_TIMESTAMP(
--            REGEXP_REPLACE(endTime, '[+-]\\d{2}:\\d{2}$', ''), 
--            timezoneOffset
--          )
--     ELSE endTime 
--   END AS endTimeUTC,
--   CAST(REGEXP_EXTRACT(windSpeed, '(\\d+)', 1) AS INT) AS windSpeed,
--   CURRENT_TIMESTAMP() AS audit_update_ts,
--   dewpoint.value AS dewpoint,
--   probabilityOfPrecipitation.value AS probabilityOfPrecipitation,
--   relativeHumidity.value AS relativeHumidity
-- FROM stream(leigh_robertson_demo.bronze_noaa.forecasts);

-- -- Create target streaming table
-- CREATE OR REFRESH STREAMING TABLE silver_noaa_forecasts_sql
-- COMMENT 'SCD Type 1 managed silver forecasts'
-- TBLPROPERTIES ('quality' = 'silver');

-- -- Apply SCD Type 1 changes
-- APPLY CHANGES INTO silver_noaa_forecasts_sql
-- FROM stream(LIVE.bronze_forecasts_preprocessed_sql)
-- KEYS (post_code, startTime)
-- SEQUENCE BY audit_update_ts
-- STORED AS SCD TYPE 1;
