CREATE TABLE IF NOT EXISTS leigh_robertson_demo.observability.dlt_observability_summary (
  summary_date DATE,
  pipeline_name STRING,
  total_pipelines INT,
  pipelines_failed_today INT,
  streaming_backlog_mb DOUBLE,
  avg_runtime_minutes DOUBLE,
  retry_count_7d INT,
  longest_runtime_minutes DOUBLE,
  pipelines_with_autoloader INT,
  tables_created INT,
  active_streaming_pipelines INT,
  last_failure_ts TIMESTAMP
);

INSERT OVERWRITE TABLE leigh_robertson_demo.observability.dlt_observability_summary
SELECT
  current_date() AS summary_date,
  pipeline_name,
  COUNT(DISTINCT pipeline_name) OVER () AS total_pipelines,
  SUM(CASE WHEN status = 'FAILED' THEN 1 ELSE 0 END) AS pipelines_failed_today,
  MAX(streaming_backlog) AS streaming_backlog_mb,
  ROUND(AVG(runtime_minutes), 2) AS avg_runtime_minutes,
  SUM(retry_count_7d) AS retry_count_7d,
  MAX(runtime_minutes) AS longest_runtime_minutes,
  SUM(CASE WHEN source_type = 'autoloader' THEN 1 ELSE 0 END) AS pipelines_with_autoloader,
  COUNT(DISTINCT target_table_name) AS tables_created,
  SUM(CASE WHEN is_streaming = true THEN 1 ELSE 0 END) AS active_streaming_pipelines,
  MAX(CASE WHEN status = 'FAILED' THEN event_time ELSE NULL END) AS last_failure_ts
FROM observability.silver.event_log_cleaned
GROUP BY pipeline_name;

CREATE OR REPLACE TABLE leigh_robertson_demo.observability.refresh_dlt_observability_summary AS
SELECT *
FROM observability.gold.dlt_observability_summary
WHERE summary_date = current_date();