-- Genie Query Volume Trend: daily Genie query count and P50/P99 duration by warehouse
-- Source: dashboard "Warehouse & Genie Query Performance" (01f1ce5fa60717a894bf2a26c2a2c7e5), dataset "Genie Query Volume Trend"
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

SELECT
  DATE_TRUNC('DAY', start_time) AS query_date,
  compute.warehouse_id AS warehouse_id,
  COUNT(*) AS query_count,
  PERCENTILE_APPROX(total_duration_ms, 0.50) AS p50_duration_ms,
  PERCENTILE_APPROX(total_duration_ms, 0.99) AS p99_duration_ms
FROM
  system.query.history
WHERE
  workspace_id = '7474654445014356'
  AND (
    date_range_min IS NULL
    OR start_time >= date_range_min
  )
  AND (
    date_range_max IS NULL
    OR start_time <= date_range_max
  )
  AND query_source.genie_space_id IS NOT NULL
  AND statement_text NOT LIKE '%__databricks_internal%'
  AND (genie_filter IS NULL OR array_contains(genie_filter, query_source.genie_space_id))
GROUP BY
  1,
  2
ORDER BY
  1;
