INSERT OVERWRITE TABLE observability.gold.dlt_observability_summary
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