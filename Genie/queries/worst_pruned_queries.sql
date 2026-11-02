-- Worst Pruned Queries: Genie statements with the lowest file pruning ratio (top 100)
-- Source: dashboard "Warehouse & Genie Query Performance" (01f1ce5fa60717a894bf2a26c2a2c7e5), dataset "Worst Pruned Queries"
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
  statement_id,
  query_source.genie_space_id AS genie_space_id,
  compute.warehouse_id AS warehouse_id,
  LEFT(statement_text, 200) AS statement_snippet,
  total_duration_ms,
  pruned_files,
  read_files,
  pruned_files + read_files AS total_files,
  ROUND(read_files_bytes / NULLIF(read_files, 0) / 1048576, 2) AS avg_file_size_mb,
  ROUND(
    CAST(pruned_files AS DOUBLE) / NULLIF(pruned_files + read_files, 0),
    4
  ) AS file_pruning_ratio,
  read_bytes,
  read_rows,
  pruned_files_bytes,
  read_files_bytes,
  ROUND(
    CAST(pruned_files_bytes AS DOUBLE) / NULLIF(pruned_files_bytes + read_files_bytes, 0),
    4
  ) AS bytes_pruning_ratio,
  start_time
FROM
  system.query.history
WHERE
  workspace_id = '7474654445014356'
  AND query_source.genie_space_id IS NOT NULL
  AND (pruned_files + read_files) > 0
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
ORDER BY
  file_pruning_ratio ASC,
  read_bytes DESC
LIMIT 100;
