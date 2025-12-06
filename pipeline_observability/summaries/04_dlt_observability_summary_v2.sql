-- DLT Observability Summary - Built from Raw Event Log
-- This file creates the dlt_observability_summary table by properly parsing
-- the Databricks pipeline event log schema as documented at:
-- https://docs.databricks.com/aws/en/ldp/monitor-event-log-schema
--
-- This replaces the previous version which couldn't work with event_log_cleaned
-- because it didn't extract the necessary fields from the raw event log.

CREATE OR REPLACE TABLE leigh_robertson_demo.observability.dlt_observability_summary
AS
WITH 
-- Extract flow progress events to get status, backlog, and runtime metrics
flow_events AS (
  SELECT
    origin.pipeline_id AS pipeline_id,
    origin.pipeline_name AS pipeline_name,
    origin.update_id AS update_id,
    origin.flow_id AS flow_id,
    origin.flow_name AS flow_name,
    timestamp AS event_time,
    details:flow_progress.status AS status,
    details:flow_progress.metrics.backlog_bytes AS backlog_bytes,
    details:flow_progress.metrics.input_rows AS input_rows,
    details:flow_progress.metrics.output_rows AS output_rows,
    details:flow_progress.metrics.data_quality.expectations AS dq_expectations
  FROM leigh_robertson_demo.observability.event_log_noaa_weather_ingestion_sample_pipeline
  WHERE event_type = 'flow_progress'
    AND origin.pipeline_name IS NOT NULL
),

-- Extract update progress events to get overall pipeline state and timing
update_events AS (
  SELECT
    origin.pipeline_id AS pipeline_id,
    origin.pipeline_name AS pipeline_name,
    origin.update_id AS update_id,
    timestamp AS event_time,
    details:update_progress.state AS update_state,
    details:update_progress.metrics.input_rows AS total_input_rows,
    details:update_progress.metrics.output_rows AS total_output_rows
  FROM leigh_robertson_demo.observability.event_log_noaa_weather_ingestion_sample_pipeline
  WHERE event_type = 'update_progress'
    AND origin.pipeline_name IS NOT NULL
),

-- Extract flow definitions to identify streaming vs batch and target tables
flow_definitions AS (
  SELECT DISTINCT
    origin.pipeline_id AS pipeline_id,
    origin.pipeline_name AS pipeline_name,
    origin.flow_id AS flow_id,
    origin.flow_name AS flow_name,
    details:flow_definition.output_dataset AS target_table_name,
    details:flow_definition.is_streaming AS is_streaming
  FROM leigh_robertson_demo.observability.event_log_noaa_weather_ingestion_sample_pipeline
  WHERE event_type = 'flow_definition'
    AND origin.pipeline_name IS NOT NULL
),

-- Extract dataset definitions to identify source types (autoloader, etc.)
dataset_definitions AS (
  SELECT DISTINCT
    origin.pipeline_id AS pipeline_id,
    origin.pipeline_name AS pipeline_name,
    origin.flow_id AS flow_id,
    details:dataset_definition.source_type AS source_type,
    details:dataset_definition.path AS source_path
  FROM leigh_robertson_demo.observability.event_log_noaa_weather_ingestion_sample_pipeline
  WHERE event_type = 'dataset_definition'
    AND origin.pipeline_name IS NOT NULL
),

-- Extract operation progress to get autoloader-specific metrics
operation_progress AS (
  SELECT
    origin.pipeline_id AS pipeline_id,
    origin.pipeline_name AS pipeline_name,
    origin.update_id AS update_id,
    origin.flow_id AS flow_id,
    timestamp AS event_time,
    details:operation_progress.auto_loader_details.num_files_listed AS num_files_listed,
    details:operation_progress.auto_loader_details.num_files_processed AS num_files_processed
  FROM leigh_robertson_demo.observability.event_log_noaa_weather_ingestion_sample_pipeline
  WHERE event_type = 'operation_progress'
    AND details:operation_progress.auto_loader_details IS NOT NULL
    AND origin.pipeline_name IS NOT NULL
),

-- Calculate runtime per update by finding start and end times
update_runtimes AS (
  SELECT
    u1.pipeline_id,
    u1.pipeline_name,
    u1.update_id,
    u1.event_time AS update_start_time,
    COALESCE(
      MIN(CASE WHEN u2.update_state IN ('COMPLETED', 'FAILED', 'CANCELED') THEN u2.event_time END),
      MAX(u2.event_time)
    ) AS update_end_time,
    u2.update_state AS final_state,
    -- Calculate runtime in minutes
    CASE 
      WHEN COALESCE(
        MIN(CASE WHEN u2.update_state IN ('COMPLETED', 'FAILED', 'CANCELED') THEN u2.event_time END),
        MAX(u2.event_time)
      ) IS NOT NULL AND u1.event_time IS NOT NULL
      THEN (unix_timestamp(COALESCE(
        MIN(CASE WHEN u2.update_state IN ('COMPLETED', 'FAILED', 'CANCELED') THEN u2.event_time END),
        MAX(u2.event_time)
      )) - unix_timestamp(u1.event_time)) / 60.0
      ELSE NULL
    END AS runtime_minutes
  FROM update_events u1
  LEFT JOIN update_events u2
    ON u1.pipeline_id = u2.pipeline_id
    AND u1.update_id = u2.update_id
    AND u2.event_time >= u1.event_time
  WHERE u1.update_state = 'RUNNING'
  GROUP BY u1.pipeline_id, u1.pipeline_name, u1.update_id, u1.event_time, u2.update_state
),

-- Identify the latest update per pipeline for latest runtime calculation
latest_updates AS (
  SELECT
    pipeline_id,
    pipeline_name,
    update_id,
    runtime_minutes,
    update_end_time,
    ROW_NUMBER() OVER (
      PARTITION BY pipeline_id, pipeline_name 
      ORDER BY update_end_time DESC NULLS LAST
    ) AS rn
  FROM update_runtimes
  WHERE runtime_minutes IS NOT NULL
),

-- Calculate retry counts (updates that failed and then succeeded)
retry_counts AS (
  SELECT
    pipeline_id,
    pipeline_name,
    COUNT(DISTINCT update_id) AS retry_count_7d
  FROM update_runtimes
  WHERE final_state = 'COMPLETED'
    AND update_start_time >= date_sub(current_date(), 7)
    AND update_id IN (
      -- Find updates that had a previous failure
      SELECT DISTINCT u2.update_id
      FROM update_runtimes u1
      JOIN update_runtimes u2
        ON u1.pipeline_id = u2.pipeline_id
        AND u1.pipeline_name = u2.pipeline_name
        AND u1.final_state = 'FAILED'
        AND u2.final_state = 'COMPLETED'
        AND u2.update_start_time > u1.update_end_time
        AND u2.update_start_time <= date_add(u1.update_end_time, 1)
    )
  GROUP BY pipeline_id, pipeline_name
),

-- Aggregate all metrics per pipeline
-- Use update_events as primary since updates are the fundamental execution unit
-- Flow events provide additional detail but every flow belongs to an update
pipeline_metrics AS (
  SELECT
    u.pipeline_id AS pipeline_id,
    u.pipeline_name AS pipeline_name,
    
    -- Status from flow events (most recent status)
    MAX(f.status) AS status,
    
    -- Streaming backlog (convert bytes to MB)
    MAX(f.backlog_bytes) / 1024.0 / 1024.0 AS streaming_backlog_mb,
    
    -- Runtime calculations from update runtimes
    AVG(ur.runtime_minutes) AS avg_runtime_minutes,
    
    MAX(ur.runtime_minutes) AS longest_runtime_minutes,
    
    -- Latest runtime (most recent update's runtime)
    MAX(lu.runtime_minutes) AS latest_runtime_minutes,
    
    -- Retry count
    COALESCE(rc.retry_count_7d, 0) AS retry_count_7d,
    
    -- Source type detection (autoloader)
    MAX(CASE WHEN dd.source_type = 'autoloader' OR op.num_files_listed IS NOT NULL THEN 1 ELSE 0 END) AS pipelines_with_autoloader,
    
    -- Target tables
    COUNT(DISTINCT fd.target_table_name) AS tables_created,
    
    -- Streaming detection
    MAX(CASE WHEN fd.is_streaming = true THEN 1 ELSE 0 END) AS active_streaming_pipelines,
    
    -- Last failure timestamp
    MAX(CASE WHEN f.status = 'FAILED' THEN f.event_time END) AS last_failure_ts,
    
    -- Most recent event time
    MAX(COALESCE(f.event_time, u.event_time)) AS most_recent_event_time
    
  FROM update_events u
  LEFT JOIN flow_events f
    ON u.pipeline_id = f.pipeline_id
    AND u.pipeline_name = f.pipeline_name
  LEFT JOIN flow_definitions fd
    ON u.pipeline_id = fd.pipeline_id
    AND u.pipeline_name = fd.pipeline_name
  LEFT JOIN dataset_definitions dd
    ON u.pipeline_id = dd.pipeline_id
    AND u.pipeline_name = dd.pipeline_name
  LEFT JOIN operation_progress op
    ON u.pipeline_id = op.pipeline_id
    AND u.pipeline_name = op.pipeline_name
  LEFT JOIN update_runtimes ur
    ON u.pipeline_id = ur.pipeline_id
    AND u.pipeline_name = ur.pipeline_name
  LEFT JOIN latest_updates lu
    ON u.pipeline_id = lu.pipeline_id
    AND u.pipeline_name = lu.pipeline_name
    AND lu.rn = 1
  LEFT JOIN retry_counts rc
    ON u.pipeline_id = rc.pipeline_id
    AND u.pipeline_name = rc.pipeline_name
  WHERE u.pipeline_name IS NOT NULL
  GROUP BY 
    u.pipeline_id,
    u.pipeline_name,
    rc.retry_count_7d
)

-- Final summary table
SELECT
  current_date() AS summary_date,
  pipeline_name,
  COUNT(DISTINCT pipeline_name) AS total_pipelines,
  SUM(CASE WHEN status = 'FAILED' THEN 1 ELSE 0 END) AS pipelines_failed_today,
  MAX(streaming_backlog_mb) AS streaming_backlog_mb,
  ROUND(AVG(avg_runtime_minutes), 2) AS avg_runtime_minutes,
  ROUND(MAX(latest_runtime_minutes), 2) AS latest_runtime_minutes,
  MAX(retry_count_7d) AS retry_count_7d,
  MAX(longest_runtime_minutes) AS longest_runtime_minutes,
  SUM(pipelines_with_autoloader) AS pipelines_with_autoloader,
  SUM(tables_created) AS tables_created,
  SUM(active_streaming_pipelines) AS active_streaming_pipelines,
  MAX(last_failure_ts) AS last_failure_ts
FROM pipeline_metrics
WHERE most_recent_event_time >= date_sub(current_date(), 1)  -- Only include recent activity
GROUP BY pipeline_name;

-- Create refresh table for current date
CREATE OR REPLACE TABLE leigh_robertson_demo.observability.refresh_dlt_observability_summary AS
SELECT *
FROM leigh_robertson_demo.observability.dlt_observability_summary
WHERE summary_date = current_date();

