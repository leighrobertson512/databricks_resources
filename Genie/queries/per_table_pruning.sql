-- Per-Table Pruning: file layout and pruning effectiveness per table referenced by Genie queries
-- Source: dashboard "Warehouse & Genie Query Performance" (01f1ce5fa60717a894bf2a26c2a2c7e5), dataset "Per-Table Pruning"
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

WITH extracted AS (
  SELECT
    statement_id,
    total_duration_ms,
    pruned_files,
    read_files,
    read_bytes,
    pruned_files_bytes,
    read_files_bytes,
    REPLACE(t.table_ref, '`', '') AS table_name
  FROM
    system.query.history
    LATERAL VIEW
      EXPLODE(
        regexp_extract_all(
          statement_text,
          '(?i)(?:FROM|JOIN)\\s+(`?[a-zA-Z0-9_]+`?\\.`?[a-zA-Z0-9_]+`?\\.`?[a-zA-Z0-9_]+`?)'
        )
      ) t
      AS table_ref
  WHERE
    workspace_id = '7474654445014356'
    AND query_source.genie_space_id IS NOT NULL
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
    AND pruned_files IS NOT NULL
    AND (pruned_files + read_files) > 0
),
agg AS (
  SELECT
    table_name,
    COUNT(DISTINCT statement_id) AS query_count,
    SUM(read_bytes) AS total_read_bytes,
    SUM(read_files) AS total_read_files,
    ROUND(AVG(pruned_files + read_files), 0) AS avg_total_files,
    ROUND(SUM(read_files_bytes) / NULLIF(SUM(read_files), 0) / 1048576, 2) AS avg_file_size_mb,
    AVG(CAST(pruned_files AS DOUBLE) / (pruned_files + read_files)) AS avg_file_pruning_ratio,
    AVG(
      CASE
        WHEN
          (pruned_files_bytes + read_files_bytes) > 0
        THEN
          CAST(pruned_files_bytes AS DOUBLE) / (pruned_files_bytes + read_files_bytes)
      END
    ) AS avg_bytes_pruning_ratio,
    PERCENTILE_APPROX(total_duration_ms, 0.50) AS p50_duration_ms,
    PERCENTILE_APPROX(total_duration_ms, 0.99) AS p99_duration_ms
  FROM
    extracted
  WHERE
    table_name NOT LIKE '%__databricks_internal%'
  GROUP BY
    table_name
)
SELECT
  *,
  CASE
    WHEN
      avg_total_files >= 100
      AND avg_file_size_mb < 32
    THEN
      'Small files: run OPTIMIZE or enable predictive optimization'
    WHEN
      avg_total_files >= 100
      AND avg_file_pruning_ratio < 0.5
    THEN
      'Low pruning: add liquid clustering on columns Genie filters by'
    ELSE 'No action needed'
  END AS recommended_action
FROM
  agg
ORDER BY
  total_read_bytes DESC;
