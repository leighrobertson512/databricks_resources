-- Genie Performance: bottleneck + recommended action per Genie agent / warehouse
-- Source: dashboard "Warehouse & Genie Query Performance" (01f1ce5fa60717a894bf2a26c2a2c7e5), dataset "Genie Performance"
-- Parameters: :start_date, :end_date (YYYY-MM-DD, end inclusive), :genie_space_ids (comma-separated)
--   Blank = last 30 days / now / all Genie agents.

DECLARE OR REPLACE VARIABLE date_range_min TIMESTAMP DEFAULT
  COALESCE(TRY_CAST(NULLIF(TRIM(:start_date), '') AS TIMESTAMP), CAST(CURRENT_DATE() - INTERVAL 30 DAYS AS TIMESTAMP));
DECLARE OR REPLACE VARIABLE date_range_max TIMESTAMP DEFAULT
  COALESCE(TRY_CAST(NULLIF(TRIM(:end_date), '') AS TIMESTAMP) + INTERVAL 1 DAY, CURRENT_TIMESTAMP());
DECLARE OR REPLACE VARIABLE genie_filter ARRAY<STRING> DEFAULT
  CASE
    WHEN TRIM(:genie_space_ids) = '' THEN NULL
    ELSE FILTER(TRANSFORM(SPLIT(:genie_space_ids, ','), x -> TRIM(x)), x -> x <> '')
  END;

WITH base AS (
  SELECT
    query_source.genie_space_id AS genie_space_id,
    compute.warehouse_id AS warehouse_id,
    COUNT(*) AS query_count,
    PERCENTILE_APPROX(total_duration_ms, 0.25) AS p25_total_ms,
    PERCENTILE_APPROX(total_duration_ms, 0.50) AS p50_total_ms,
    PERCENTILE_APPROX(total_duration_ms, 0.75) AS p75_total_ms,
    PERCENTILE_APPROX(total_duration_ms, 0.99) AS p99_total_ms,
    PERCENTILE_APPROX(waiting_for_compute_duration_ms, 0.50) AS p50_wait_compute_ms,
    PERCENTILE_APPROX(waiting_for_compute_duration_ms, 0.99) AS p99_wait_compute_ms,
    PERCENTILE_APPROX(waiting_at_capacity_duration_ms, 0.50) AS p50_wait_capacity_ms,
    PERCENTILE_APPROX(waiting_at_capacity_duration_ms, 0.99) AS p99_wait_capacity_ms,
    PERCENTILE_APPROX(execution_duration_ms, 0.50) AS p50_execution_ms,
    PERCENTILE_APPROX(execution_duration_ms, 0.99) AS p99_execution_ms,
    PERCENTILE_APPROX(compilation_duration_ms, 0.50) AS p50_compilation_ms,
    PERCENTILE_APPROX(compilation_duration_ms, 0.99) AS p99_compilation_ms,
    PERCENTILE_APPROX(total_task_duration_ms, 0.50) AS p50_task_ms,
    PERCENTILE_APPROX(total_task_duration_ms, 0.99) AS p99_task_ms,
    PERCENTILE_APPROX(result_fetch_duration_ms, 0.50) AS p50_fetch_ms,
    PERCENTILE_APPROX(result_fetch_duration_ms, 0.99) AS p99_fetch_ms,
    COALESCE(AVG(waiting_for_compute_duration_ms), 0) AS avg_wait_compute_ms,
    COALESCE(AVG(waiting_at_capacity_duration_ms), 0) AS avg_wait_capacity_ms,
    COALESCE(AVG(compilation_duration_ms), 0) AS avg_compilation_ms,
    COALESCE(AVG(execution_duration_ms), 0) AS avg_execution_ms,
    COALESCE(AVG(result_fetch_duration_ms), 0) AS avg_fetch_ms
  FROM
    system.query.history
  WHERE
    workspace_id = '7474654445014356'
    AND query_source.genie_space_id IS NOT NULL
    AND statement_text NOT LIKE '%__databricks_internal%'
    AND (
      date_range_min IS NULL
      OR start_time >= date_range_min
    )
    AND (
      date_range_max IS NULL
      OR start_time <= date_range_max
    )
    AND (
      genie_filter IS NULL
      OR array_contains(genie_filter, query_source.genie_space_id)
    )
  GROUP BY
    1,
    2
),
tagged AS (
  SELECT
    *,
    CASE
      WHEN
        avg_wait_capacity_ms
          >= GREATEST(avg_wait_compute_ms, avg_compilation_ms, avg_execution_ms, avg_fetch_ms)
      THEN
        'Queueing at capacity'
      WHEN
        avg_wait_compute_ms >= GREATEST(avg_compilation_ms, avg_execution_ms, avg_fetch_ms)
      THEN
        'Compute startup'
      WHEN avg_compilation_ms >= GREATEST(avg_execution_ms, avg_fetch_ms) THEN 'Compilation'
      WHEN avg_execution_ms >= avg_fetch_ms THEN 'Execution'
      ELSE 'Result fetch'
    END AS primary_bottleneck
  FROM
    base
)
SELECT
  *,
  CASE primary_bottleneck
    WHEN
      'Queueing at capacity'
    THEN
      'Raise max clusters (scaling) or give this Genie agent a dedicated warehouse'
    WHEN 'Compute startup' THEN 'Use serverless or lengthen auto-stop so the warehouse stays warm'
    WHEN
      'Compilation'
    THEN
      'Simplify tables/views exposed to Genie (fewer nested views and joins); keep stats current with ANALYZE'
    WHEN
      'Execution'
    THEN
      'Improve pruning (liquid clustering, OPTIMIZE) or size up the warehouse; see pruning tables below'
    ELSE 'Return smaller results: aggregate or LIMIT in Genie instructions and example SQL'
  END AS recommended_action
FROM
  tagged
ORDER BY
  query_count DESC;
