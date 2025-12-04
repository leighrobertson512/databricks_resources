CREATE OR REFRESH MATERIALIZED VIEW leigh_robertson_demo.gold_noaa.forecasts_high_low_ldp
SELECT 
  forecasts.startTime,
  zip_code.place_name,
  zip_code.state,
  zip_code.post_code,
  forecasts.temperature,
  forecasts.probabilityOfPrecipitation,
  forecasts.windSpeed,
  CAST(forecasts.startTimeUTC AS DATE) AS forecast_date_utc,
  CAST(forecasts.startTime AS DATE) AS forecast_date_local_tz,
  current_timestamp() AS audit_update_ts 
FROM leigh_robertson_demo.silver_noaa.forecasts_expanded_ldp AS forecasts
INNER JOIN leigh_robertson_demo.bronze_noaa.zip_code AS zip_code
  ON forecasts.post_code = zip_code.post_code
WHERE CAST(forecasts.startTimeUTC AS DATE) >= current_date() + 10