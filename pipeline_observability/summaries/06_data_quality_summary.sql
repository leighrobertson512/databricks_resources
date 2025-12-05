CREATE OR REFRESH STREAMING TABLE observability.silver.dlt_dq_violations_cleaned AS
SELECT 
  pipeline_id,
  id AS event_id,
  timestamp AS dq_timestamp,
  date(timestamp) AS dq_date,
  details:flow_definition.output_dataset AS dataset,
  details:expectation AS expectation,
  details:data_quality.violated_records AS violated_records,
  details:data_quality.passed_records AS passed_expectations,
  details:data_quality.failed_records AS failed_expectations,
  details:data_quality.dropped_records AS dropped_records,
  details:data_quality.metrics:null_count AS null_count,
  details:data_quality.metrics:total_records AS record_count
FROM system.pipelines.event_log
WHERE event_type = 'expectation_evaluation'
  AND details:flow_definition.output_dataset IS NOT NULL;

CREATE OR REFRESH TABLE observability.gold.dlt_dq_summary AS
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
  ROUND(AVG(null_count * 100.0 / NULLIF(record_count, 0)), 2) AS avg_null_ratio
FROM observability.silver.dlt_dq_violations_cleaned
GROUP BY dq_date, dataset;