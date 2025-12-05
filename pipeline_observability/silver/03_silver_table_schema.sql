CREATE OR REPLACE TABLE leigh_robertson_demo.observability.event_log_cleaned
AS
SELECT
  current_timestamp() AS loaded_at,
  'noaa_weather_ingestion_sample_pipeline' AS pipeline_name,
  event_type,
  timestamp,
  origin.update_id AS update_id,
  origin.org_id as workspace_id,
  origin.cloud,
  details:flow_progress.metrics.backlog_bytes AS backlog_bytes,
  details:flow_progress.data_quality.expectations AS dq_expectations,
  details:user_action.action AS user_action,
  details:autoscale.requested_num_executors AS autoscale_exec,
  details:operation_progress.auto_loader_details.num_files_listed AS auto_loader_file_count
FROM leigh_robertson_demo.observability.event_log_noaa_weather_ingestion_sample_pipeline
CLUSTER BY event_type, pipeline_name