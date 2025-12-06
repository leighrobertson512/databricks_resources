-- Data Quality Summary - Built from Raw Event Log
-- This file extracts data quality metrics from flow_progress events
-- Based on Databricks event log schema: https://docs.databricks.com/aws/en/ldp/monitor-event-log-schema
--
-- Data quality information is stored in flow_progress events under:
-- details:flow_progress.data_quality.expectations (array of expectation objects)

CREATE OR REPLACE TABLE leigh_robertson_demo.observability.dlt_dq_violations_cleaned AS
WITH 
-- Get flow definitions to map flow_id to output dataset/table
flow_definitions AS (
  SELECT DISTINCT
    origin.pipeline_id AS pipeline_id,
    origin.flow_id AS flow_id,
    origin.dataset_name AS dataset_name,
    details:flow_definition.output_dataset AS output_dataset
  FROM leigh_robertson_demo.observability.event_log_noaa_weather_ingestion_sample_pipeline
  WHERE event_type = 'flow_definition'
    AND origin.pipeline_name IS NOT NULL
),

-- Extract flow_progress events that contain data quality information
-- The expectations array is accessed directly via JSON path
flow_progress_with_dq AS (
  SELECT
    origin.pipeline_id AS pipeline_id,
    origin.pipeline_name AS pipeline_name,
    origin.flow_id AS flow_id,
    origin.update_id AS update_id,
    id AS event_id,
    timestamp AS dq_timestamp,
    date(timestamp) AS dq_date,
    -- Get the expectations array - will be exploded in next CTE
    details:flow_progress.data_quality.expectations AS expectations_array,
    details:flow_progress.metrics.input_rows AS input_rows,
    details:flow_progress.metrics.output_rows AS output_rows
  FROM leigh_robertson_demo.observability.event_log_noaa_weather_ingestion_sample_pipeline
  WHERE event_type = 'flow_progress'
    AND details:flow_progress.data_quality IS NOT NULL
    AND details:flow_progress.data_quality.expectations IS NOT NULL
    AND origin.pipeline_name IS NOT NULL
),

-- Explode the expectations array to get individual expectation records
-- First parse the JSON string into an array, then explode it
expectations_exploded AS (
  SELECT
    fp.pipeline_id,
    fp.pipeline_name,
    fp.flow_id,
    fp.update_id,
    fp.event_id,
    fp.dq_timestamp,
    fp.dq_date,
    COALESCE(fd.output_dataset, fd.dataset_name, CAST(fp.flow_id AS STRING)) AS dataset,
    exp.name AS expectation,
    exp.status AS expectation_status,
    exp.violated_records AS violated_records,
    exp.passed_records AS passed_records,
    exp.failed_records AS failed_records,
    exp.dropped_records AS dropped_records,
    exp.metrics.null_count AS null_count,
    exp.metrics.total_records AS record_count,
    fp.input_rows,
    fp.output_rows
  FROM flow_progress_with_dq fp
  LEFT JOIN flow_definitions fd
    ON fp.pipeline_id = fd.pipeline_id
    AND fp.flow_id = fd.flow_id
  LATERAL VIEW EXPLODE(
    FROM_JSON(
      fp.expectations_array,
      'array<struct<name:string,status:string,violated_records:bigint,passed_records:bigint,failed_records:bigint,dropped_records:bigint,metrics:struct<null_count:bigint,total_records:bigint>>>'
    )
  ) AS exp
  WHERE fp.expectations_array IS NOT NULL
    AND fp.expectations_array != 'null'
    AND length(fp.expectations_array) > 2  -- Ensure it's not empty array "[]"
)

SELECT
  pipeline_id,
  event_id,
  dq_timestamp,
  dq_date,
  dataset,
  expectation,
  expectation_status,
  COALESCE(violated_records, 0) AS violated_records,
  COALESCE(passed_records, 0) AS passed_expectations,
  COALESCE(failed_records, 0) AS failed_expectations,
  COALESCE(dropped_records, 0) AS dropped_records,
  COALESCE(null_count, 0) AS null_count,
  COALESCE(record_count, output_rows, input_rows, 0) AS record_count
FROM expectations_exploded;

-- Create gold layer summary table
CREATE OR REPLACE TABLE leigh_robertson_demo.observability.dlt_dq_summary AS
SELECT
  dq_date,
  dataset,
  COUNT(DISTINCT expectation) AS expectations_defined,
  SUM(violated_records) AS total_violated_records,
  SUM(passed_expectations) AS passed_expectations,
  SUM(failed_expectations) AS failed_expectations,
  SUM(dropped_records) AS dropped_record_count,
  SUM(null_count) AS nulls_total,
  SUM(record_count) AS records_total,
  ROUND(AVG(CASE WHEN record_count > 0 THEN null_count * 100.0 / record_count ELSE NULL END), 2) AS avg_null_ratio
FROM leigh_robertson_demo.observability.dlt_dq_violations_cleaned
GROUP BY dq_date, dataset;