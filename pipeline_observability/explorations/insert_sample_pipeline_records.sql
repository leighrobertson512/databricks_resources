-- Insert Sample Pipeline Records
-- This script inserts 100 rows of sample data into dlt_observability_summary
-- Dates start from 2024-12-06 and go backwards 100 days
-- Useful for building dashboards with historical sample data

INSERT INTO leigh_robertson_demo.observability.dlt_observability_summary
WITH date_series AS (
  SELECT
    date_sub('2024-12-06', n) AS summary_date,
    n AS days_back
  FROM (
    SELECT explode(sequence(0, 99)) AS n
  )
)
SELECT
  ds.summary_date,
  'noaa_weather_ingestion_sample_pipeline' AS pipeline_name,
  1 AS total_pipelines,
  -- Randomly fail about 5% of the time (using hash for deterministic randomness)
  CASE WHEN ABS(hash(ds.days_back)) % 100 < 5 THEN 1 ELSE 0 END AS pipelines_failed_today,
  -- Streaming backlog varies between 0 and 500 MB
  ROUND((ABS(hash(ds.days_back * 2)) % 10000) / 10000.0 * 500, 2) AS streaming_backlog_mb,
  -- Average runtime varies between 10 and 45 minutes
  ROUND(10 + (ABS(hash(ds.days_back * 3)) % 10000) / 10000.0 * 35, 2) AS avg_runtime_minutes,
  -- Latest runtime similar to average but with some variation
  ROUND(10 + (ABS(hash(ds.days_back * 4)) % 10000) / 10000.0 * 35, 2) AS latest_runtime_minutes,
  -- Retry count varies between 0 and 3
  CAST((ABS(hash(ds.days_back * 5)) % 10000) / 10000.0 * 3 AS INT) AS retry_count_7d,
  -- Longest runtime varies between 15 and 60 minutes
  ROUND(15 + (ABS(hash(ds.days_back * 6)) % 10000) / 10000.0 * 45, 2) AS longest_runtime_minutes,
  -- Most days have autoloader (80% chance)
  CASE WHEN ABS(hash(ds.days_back * 7)) % 100 < 80 THEN 1 ELSE 0 END AS pipelines_with_autoloader,
  -- Tables created varies between 3 and 8
  CAST(3 + (ABS(hash(ds.days_back * 8)) % 10000) / 10000.0 * 5 AS INT) AS tables_created,
  -- Active streaming pipelines (usually 1, sometimes 0)
  CASE WHEN ABS(hash(ds.days_back * 9)) % 100 < 90 THEN 1 ELSE 0 END AS active_streaming_pipelines,
  -- Last failure timestamp - only set if pipeline failed today
  CASE 
    WHEN ABS(hash(ds.days_back)) % 100 < 5 
    THEN cast(ds.summary_date AS TIMESTAMP) + 
         INTERVAL CAST((ABS(hash(ds.days_back * 10)) % 10000) / 10000.0 * 24 AS INT) HOURS +
         INTERVAL CAST((ABS(hash(ds.days_back * 11)) % 10000) / 10000.0 * 60 AS INT) MINUTES
    ELSE NULL 
  END AS last_failure_ts
FROM date_series ds
ORDER BY ds.summary_date DESC;

