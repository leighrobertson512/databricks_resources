-- Genie KPI Summary: avg/max execution seconds and avg bytes/files pruned %
-- Source: dashboard "Warehouse & Genie Query Performance" (01f1ce5fa60717a894bf2a26c2a2c7e5), dataset "Genie KPI Summary"
--   (query-level dataset wrapped in the same aggregations the dashboard counters use)
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

WITH kpi_base AS (
  SELECT
    statement_id,
    query_source.genie_space_id AS genie_space_id,
    compute.warehouse_id AS warehouse_id,
    execution_duration_ms,
    execution_duration_ms / 1000.0 AS execution_duration_s,
    CAST(pruned_files_bytes AS DOUBLE)
      * 100
      / NULLIF(pruned_files_bytes + read_files_bytes, 0) AS bytes_pruned_pct,
    CAST(pruned_files AS DOUBLE) * 100 / NULLIF(pruned_files + read_files, 0) AS files_pruned_pct
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
)
SELECT
  COUNT(*) AS query_count,
  ROUND(AVG(execution_duration_s), 3) AS avg_execution_s,
  ROUND(MAX(execution_duration_s), 3) AS max_execution_s,
  ROUND(AVG(bytes_pruned_pct), 2) AS avg_bytes_pruned_pct,
  ROUND(AVG(files_pruned_pct), 2) AS avg_files_pruned_pct
FROM
  kpi_base;
