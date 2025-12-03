-- Please edit the sample below

CREATE OR REFRESH STREAMING TABLE leigh_robertson_demo.silver_noaa.forecasts_expanded_append
(
  CONSTRAINT non_null_time EXPECT (startTime IS NOT NULL AND endTime IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT valid_temperature EXPECT (temperature > -100 AND temperature < 130) ON VIOLATION DROP ROW,
  CONSTRAINT windspeed_positive EXPECT (windSpeed >= 0 OR windSpeed IS NULL) ON VIOLATION DROP ROW,
  CONSTRAINT humidity_percent_range EXPECT (relativeHumidity BETWEEN 0 AND 100 OR relativeHumidity IS NULL) ON VIOLATION DROP ROW,
  CONSTRAINT daytime_boolean EXPECT (isDaytime IN (true, false)) ON VIOLATION DROP ROW
)
AS
SELECT
    post_code,
    number,
    name,
    isDaytime,
    temperature,
    temperatureUnit,
    temperatureTrend,
    windDirection,
    icon,
    shortForecast,
    detailedForecast,
    regexp_extract(startTime, '([+-]\\d{2}:\\d{2})$', 1) AS timezoneOffset,
    regexp_replace(startTime, '[+-]\\d{2}:\\d{2}$', '') AS startTime,
    regexp_replace(endTime, '[+-]\\d{2}:\\d{2}$', '') AS endTime,
    -- Convert time to UTC (SQL CASE/WHEN for conditional logic)
    CASE
        WHEN regexp_extract(startTime, '([+-]\\d{2}:\\d{2})$', 1) != ''
            THEN from_utc_timestamp(
                regexp_replace(startTime, '[+-]\\d{2}:\\d{2}$', ''),
                regexp_extract(startTime, '([+-]\\d{2}:\\d{2})$', 1)
            )
        ELSE regexp_replace(startTime, '[+-]\\d{2}:\\d{2}$', '')
    END AS startTimeUTC,
    CASE
        WHEN regexp_extract(endTime, '([+-]\\d{2}:\\d{2})$', 1) != ''
            THEN from_utc_timestamp(
                regexp_replace(endTime, '[+-]\\d{2}:\\d{2}$', ''),
                regexp_extract(endTime, '([+-]\\d{2}:\\d{2})$', 1)
            )
        ELSE regexp_replace(endTime, '[+-]\\d{2}:\\d{2}$', '')
    END AS endTimeUTC,
    -- Extract numeric windSpeed as int
    CAST(regexp_extract(windSpeed, '(\\d+)', 1) AS INT) AS windSpeed,
    current_timestamp() AS audit_update_ts,
    dewpoint.value AS dewpoint,
    probabilityOfPrecipitation.value AS probabilityOfPrecipitation,
    relativeHumidity.value AS relativeHumidity
FROM STREAM (leigh_robertson_demo.bronze_noaa.forecasts_streaming_demo);
