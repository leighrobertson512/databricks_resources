# Databricks notebook source
# DBTITLE 1,Genie Warehouse Performance analysis
# MAGIC %md
# MAGIC # Genie Warehouse Performance
# MAGIC
# MAGIC Notebook version of the **Warehouse & Genie Query Performance** dashboard (`01f1ce5fa60717a894bf2a26c2a2c7e5`). It shows Genie agent query latency, where the time goes, warehouse configuration, and table pruning/file layout, with recommended actions, without needing the dashboard.
# MAGIC
# MAGIC Data source: `system.query.history` and `system.compute.warehouses`, workspace `7474654445014356`.
# MAGIC
# MAGIC **Parameters** (notebook widgets)
# MAGIC
# MAGIC | Widget | Meaning | Blank means |
# MAGIC |---|---|---|
# MAGIC | `start_date` | Start date, `YYYY-MM-DD` | Last 30 days |
# MAGIC | `end_date` | End date, `YYYY-MM-DD` (whole day included) | Now |
# MAGIC | `genie_space_ids` | Comma-separated Genie space ids | All Genie agents |
# MAGIC
# MAGIC Run the widget and parameter cells first; each analysis cell after that reads the session variables `date_range_min`, `date_range_max`, `genie_filter`.
# MAGIC
# MAGIC ### Metric definitions and actions
# MAGIC
# MAGIC | Metric | Definition | If high, it means | Concrete action |
# MAGIC |---|---|---|---|
# MAGIC | total_duration_ms | End-to-end wall-clock time of the statement | Slow Genie answers | Use the phase metrics below to find the bottleneck |
# MAGIC | waiting_for_compute_duration_ms | Time waiting for the warehouse to start or provision compute | Cold starts | Use serverless; lengthen auto-stop for the Genie warehouse |
# MAGIC | waiting_at_capacity_duration_ms | Time queued because all clusters were at max concurrency | Warehouse is saturated | Raise max clusters, or give Genie a dedicated warehouse |
# MAGIC | compilation_duration_ms | Parsing, analysis, optimization and metadata loading (Delta log, file listing) | Complex views/joins or heavy metadata (often many small files) | Simplify views exposed to Genie, run OPTIMIZE, keep stats current with ANALYZE |
# MAGIC | execution_duration_ms | Wall-clock time spent executing the plan | Large scans or under-sized warehouse | Improve pruning (liquid clustering), size up warehouse |
# MAGIC | total_task_duration_ms | Sum of all task time across all cores; can far exceed wall-clock | Total compute work; high vs execution = heavy scan | Reduce data read via pruning; larger size if parallelism is maxed |
# MAGIC | result_fetch_duration_ms | Time returning results to the client | Large result sets | Aggregate or LIMIT in Genie instructions and example SQL |
# MAGIC | read_files / pruned_files | Files scanned vs files skipped via data skipping | Low pruning = full scans | Cluster on columns Genie filters by |
# MAGIC | total files (pruned + read) | Files in scope of the table snapshot; proxy for table size in files | Many files | Check avg file size |
# MAGIC | read_files_bytes / pruned_files_bytes | Bytes scanned vs bytes skipped | Same as files, weighted by size | Same as above |
# MAGIC | avg file size (MB) | read_files_bytes / read_files | Under ~32 MB with 100+ files = small files problem | OPTIMIZE or enable predictive optimization (target ~128 MB-1 GB) |
# MAGIC
# MAGIC *Per-table figures come from table names parsed from SQL text; multi-table queries attribute query-level metrics to each table.*

# COMMAND ----------

# DBTITLE 1,Create parameter widgets
# MAGIC %sql
# MAGIC -- Blank values = last 30 days / all Genie agents
# MAGIC CREATE WIDGET TEXT start_date DEFAULT '';
# MAGIC CREATE WIDGET TEXT end_date DEFAULT '';
# MAGIC CREATE WIDGET TEXT genie_space_ids DEFAULT '';

# COMMAND ----------

# DBTITLE 1,Resolve parameters into session variables
# MAGIC %sql
# MAGIC -- Equivalents of the dashboard parameters :date_range.min, :date_range.max and :genie_filter
# MAGIC DECLARE OR REPLACE VARIABLE date_range_min TIMESTAMP DEFAULT
# MAGIC   COALESCE(TRY_CAST(NULLIF(TRIM(:start_date), '') AS TIMESTAMP), CAST(CURRENT_DATE() - INTERVAL 30 DAYS AS TIMESTAMP));
# MAGIC
# MAGIC DECLARE OR REPLACE VARIABLE date_range_max TIMESTAMP DEFAULT
# MAGIC   COALESCE(TRY_CAST(NULLIF(TRIM(:end_date), '') AS TIMESTAMP) + INTERVAL 1 DAY, CURRENT_TIMESTAMP());
# MAGIC
# MAGIC DECLARE OR REPLACE VARIABLE genie_filter ARRAY<STRING> DEFAULT
# MAGIC   CASE
# MAGIC     WHEN TRIM(:genie_space_ids) = '' THEN NULL
# MAGIC     ELSE FILTER(TRANSFORM(SPLIT(:genie_space_ids, ','), x -> TRIM(x)), x -> x <> '')
# MAGIC   END;
# MAGIC
# MAGIC SELECT date_range_min, date_range_max, genie_filter;

# COMMAND ----------

# DBTITLE 1,Genie Performance: bottleneck and recommended action per agent/warehouse
# MAGIC %sql
# MAGIC WITH base AS (
# MAGIC   SELECT
# MAGIC     query_source.genie_space_id AS genie_space_id,
# MAGIC     compute.warehouse_id AS warehouse_id,
# MAGIC     COUNT(*) AS query_count,
# MAGIC     PERCENTILE_APPROX(total_duration_ms, 0.25) AS p25_total_ms,
# MAGIC     PERCENTILE_APPROX(total_duration_ms, 0.50) AS p50_total_ms,
# MAGIC     PERCENTILE_APPROX(total_duration_ms, 0.75) AS p75_total_ms,
# MAGIC     PERCENTILE_APPROX(total_duration_ms, 0.99) AS p99_total_ms,
# MAGIC     PERCENTILE_APPROX(waiting_for_compute_duration_ms, 0.50) AS p50_wait_compute_ms,
# MAGIC     PERCENTILE_APPROX(waiting_for_compute_duration_ms, 0.99) AS p99_wait_compute_ms,
# MAGIC     PERCENTILE_APPROX(waiting_at_capacity_duration_ms, 0.50) AS p50_wait_capacity_ms,
# MAGIC     PERCENTILE_APPROX(waiting_at_capacity_duration_ms, 0.99) AS p99_wait_capacity_ms,
# MAGIC     PERCENTILE_APPROX(execution_duration_ms, 0.50) AS p50_execution_ms,
# MAGIC     PERCENTILE_APPROX(execution_duration_ms, 0.99) AS p99_execution_ms,
# MAGIC     PERCENTILE_APPROX(compilation_duration_ms, 0.50) AS p50_compilation_ms,
# MAGIC     PERCENTILE_APPROX(compilation_duration_ms, 0.99) AS p99_compilation_ms,
# MAGIC     PERCENTILE_APPROX(total_task_duration_ms, 0.50) AS p50_task_ms,
# MAGIC     PERCENTILE_APPROX(total_task_duration_ms, 0.99) AS p99_task_ms,
# MAGIC     PERCENTILE_APPROX(result_fetch_duration_ms, 0.50) AS p50_fetch_ms,
# MAGIC     PERCENTILE_APPROX(result_fetch_duration_ms, 0.99) AS p99_fetch_ms,
# MAGIC     COALESCE(AVG(waiting_for_compute_duration_ms), 0) AS avg_wait_compute_ms,
# MAGIC     COALESCE(AVG(waiting_at_capacity_duration_ms), 0) AS avg_wait_capacity_ms,
# MAGIC     COALESCE(AVG(compilation_duration_ms), 0) AS avg_compilation_ms,
# MAGIC     COALESCE(AVG(execution_duration_ms), 0) AS avg_execution_ms,
# MAGIC     COALESCE(AVG(result_fetch_duration_ms), 0) AS avg_fetch_ms
# MAGIC   FROM
# MAGIC     system.query.history
# MAGIC   WHERE
# MAGIC     workspace_id = '7474654445014356'
# MAGIC     AND query_source.genie_space_id IS NOT NULL
# MAGIC     AND statement_text NOT LIKE '%__databricks_internal%'
# MAGIC     AND (
# MAGIC       date_range_min IS NULL
# MAGIC       OR start_time >= date_range_min
# MAGIC     )
# MAGIC     AND (
# MAGIC       date_range_max IS NULL
# MAGIC       OR start_time <= date_range_max
# MAGIC     )
# MAGIC     AND (
# MAGIC       genie_filter IS NULL
# MAGIC       OR array_contains(genie_filter, query_source.genie_space_id)
# MAGIC     )
# MAGIC   GROUP BY
# MAGIC     1,
# MAGIC     2
# MAGIC ),
# MAGIC tagged AS (
# MAGIC   SELECT
# MAGIC     *,
# MAGIC     CASE
# MAGIC       WHEN
# MAGIC         avg_wait_capacity_ms
# MAGIC           >= GREATEST(avg_wait_compute_ms, avg_compilation_ms, avg_execution_ms, avg_fetch_ms)
# MAGIC       THEN
# MAGIC         'Queueing at capacity'
# MAGIC       WHEN
# MAGIC         avg_wait_compute_ms >= GREATEST(avg_compilation_ms, avg_execution_ms, avg_fetch_ms)
# MAGIC       THEN
# MAGIC         'Compute startup'
# MAGIC       WHEN avg_compilation_ms >= GREATEST(avg_execution_ms, avg_fetch_ms) THEN 'Compilation'
# MAGIC       WHEN avg_execution_ms >= avg_fetch_ms THEN 'Execution'
# MAGIC       ELSE 'Result fetch'
# MAGIC     END AS primary_bottleneck
# MAGIC   FROM
# MAGIC     base
# MAGIC )
# MAGIC SELECT
# MAGIC   *,
# MAGIC   CASE primary_bottleneck
# MAGIC     WHEN
# MAGIC       'Queueing at capacity'
# MAGIC     THEN
# MAGIC       'Raise max clusters (scaling) or give this Genie agent a dedicated warehouse'
# MAGIC     WHEN 'Compute startup' THEN 'Use serverless or lengthen auto-stop so the warehouse stays warm'
# MAGIC     WHEN
# MAGIC       'Compilation'
# MAGIC     THEN
# MAGIC       'Simplify tables/views exposed to Genie (fewer nested views and joins); keep stats current with ANALYZE'
# MAGIC     WHEN
# MAGIC       'Execution'
# MAGIC     THEN
# MAGIC       'Improve pruning (liquid clustering, OPTIMIZE) or size up the warehouse; see pruning tables below'
# MAGIC     ELSE 'Return smaller results: aggregate or LIMIT in Genie instructions and example SQL'
# MAGIC   END AS recommended_action
# MAGIC FROM
# MAGIC   tagged
# MAGIC ORDER BY
# MAGIC   query_count DESC

# COMMAND ----------

# DBTITLE 1,Genie KPI Summary: avg/max execution seconds and avg pruning %
# MAGIC %sql
# MAGIC WITH kpi_base AS (
# MAGIC   SELECT
# MAGIC     statement_id,
# MAGIC     query_source.genie_space_id AS genie_space_id,
# MAGIC     compute.warehouse_id AS warehouse_id,
# MAGIC     execution_duration_ms,
# MAGIC     execution_duration_ms / 1000.0 AS execution_duration_s,
# MAGIC     CAST(pruned_files_bytes AS DOUBLE)
# MAGIC       * 100
# MAGIC       / NULLIF(pruned_files_bytes + read_files_bytes, 0) AS bytes_pruned_pct,
# MAGIC     CAST(pruned_files AS DOUBLE) * 100 / NULLIF(pruned_files + read_files, 0) AS files_pruned_pct
# MAGIC   FROM
# MAGIC     system.query.history
# MAGIC   WHERE
# MAGIC     workspace_id = '7474654445014356'
# MAGIC     AND query_source.genie_space_id IS NOT NULL
# MAGIC     AND statement_text NOT LIKE '%__databricks_internal%'
# MAGIC     AND (
# MAGIC       date_range_min IS NULL
# MAGIC       OR start_time >= date_range_min
# MAGIC     )
# MAGIC     AND (
# MAGIC       date_range_max IS NULL
# MAGIC       OR start_time <= date_range_max
# MAGIC     )
# MAGIC     AND (
# MAGIC       genie_filter IS NULL
# MAGIC       OR array_contains(genie_filter, query_source.genie_space_id)
# MAGIC     )
# MAGIC )
# MAGIC SELECT
# MAGIC   COUNT(*) AS query_count,
# MAGIC   ROUND(AVG(execution_duration_s), 3) AS avg_execution_s,
# MAGIC   ROUND(MAX(execution_duration_s), 3) AS max_execution_s,
# MAGIC   ROUND(AVG(bytes_pruned_pct), 2) AS avg_bytes_pruned_pct,
# MAGIC   ROUND(AVG(files_pruned_pct), 2) AS avg_files_pruned_pct
# MAGIC FROM
# MAGIC   kpi_base

# COMMAND ----------

# DBTITLE 1,Genie Query Volume Trend: daily count and P50/P99 duration by warehouse
# MAGIC %sql
# MAGIC SELECT
# MAGIC   DATE_TRUNC('DAY', start_time) AS query_date,
# MAGIC   compute.warehouse_id AS warehouse_id,
# MAGIC   COUNT(*) AS query_count,
# MAGIC   PERCENTILE_APPROX(total_duration_ms, 0.50) AS p50_duration_ms,
# MAGIC   PERCENTILE_APPROX(total_duration_ms, 0.99) AS p99_duration_ms
# MAGIC FROM
# MAGIC   system.query.history
# MAGIC WHERE
# MAGIC   workspace_id = '7474654445014356'
# MAGIC   AND (
# MAGIC     date_range_min IS NULL
# MAGIC     OR start_time >= date_range_min
# MAGIC   )
# MAGIC   AND (
# MAGIC     date_range_max IS NULL
# MAGIC     OR start_time <= date_range_max
# MAGIC   )
# MAGIC   AND query_source.genie_space_id IS NOT NULL
# MAGIC   AND statement_text NOT LIKE '%__databricks_internal%'
# MAGIC   AND (genie_filter IS NULL OR array_contains(genie_filter, query_source.genie_space_id))
# MAGIC GROUP BY
# MAGIC   1,
# MAGIC   2
# MAGIC ORDER BY
# MAGIC   1

# COMMAND ----------

# DBTITLE 1,Per-Table Pruning: file layout and pruning by table
# MAGIC %sql
# MAGIC WITH extracted AS (
# MAGIC   SELECT
# MAGIC     statement_id,
# MAGIC     total_duration_ms,
# MAGIC     pruned_files,
# MAGIC     read_files,
# MAGIC     read_bytes,
# MAGIC     pruned_files_bytes,
# MAGIC     read_files_bytes,
# MAGIC     REPLACE(t.table_ref, '`', '') AS table_name
# MAGIC   FROM
# MAGIC     system.query.history
# MAGIC     LATERAL VIEW
# MAGIC       EXPLODE(
# MAGIC         regexp_extract_all(
# MAGIC           statement_text,
# MAGIC           '(?i)(?:FROM|JOIN)\\s+(`?[a-zA-Z0-9_]+`?\\.`?[a-zA-Z0-9_]+`?\\.`?[a-zA-Z0-9_]+`?)'
# MAGIC         )
# MAGIC       ) t
# MAGIC       AS table_ref
# MAGIC   WHERE
# MAGIC     workspace_id = '7474654445014356'
# MAGIC     AND query_source.genie_space_id IS NOT NULL
# MAGIC     AND (
# MAGIC       date_range_min IS NULL
# MAGIC       OR start_time >= date_range_min
# MAGIC     )
# MAGIC     AND (
# MAGIC       date_range_max IS NULL
# MAGIC       OR start_time <= date_range_max
# MAGIC     )
# MAGIC     AND (
# MAGIC       genie_filter IS NULL
# MAGIC       OR array_contains(genie_filter, query_source.genie_space_id)
# MAGIC     )
# MAGIC     AND pruned_files IS NOT NULL
# MAGIC     AND (pruned_files + read_files) > 0
# MAGIC ),
# MAGIC agg AS (
# MAGIC   SELECT
# MAGIC     table_name,
# MAGIC     COUNT(DISTINCT statement_id) AS query_count,
# MAGIC     SUM(read_bytes) AS total_read_bytes,
# MAGIC     SUM(read_files) AS total_read_files,
# MAGIC     ROUND(AVG(pruned_files + read_files), 0) AS avg_total_files,
# MAGIC     ROUND(SUM(read_files_bytes) / NULLIF(SUM(read_files), 0) / 1048576, 2) AS avg_file_size_mb,
# MAGIC     AVG(CAST(pruned_files AS DOUBLE) / (pruned_files + read_files)) AS avg_file_pruning_ratio,
# MAGIC     AVG(
# MAGIC       CASE
# MAGIC         WHEN
# MAGIC           (pruned_files_bytes + read_files_bytes) > 0
# MAGIC         THEN
# MAGIC           CAST(pruned_files_bytes AS DOUBLE) / (pruned_files_bytes + read_files_bytes)
# MAGIC       END
# MAGIC     ) AS avg_bytes_pruning_ratio,
# MAGIC     PERCENTILE_APPROX(total_duration_ms, 0.50) AS p50_duration_ms,
# MAGIC     PERCENTILE_APPROX(total_duration_ms, 0.99) AS p99_duration_ms
# MAGIC   FROM
# MAGIC     extracted
# MAGIC   WHERE
# MAGIC     table_name NOT LIKE '%__databricks_internal%'
# MAGIC   GROUP BY
# MAGIC     table_name
# MAGIC )
# MAGIC SELECT
# MAGIC   *,
# MAGIC   CASE
# MAGIC     WHEN
# MAGIC       avg_total_files >= 100
# MAGIC       AND avg_file_size_mb < 32
# MAGIC     THEN
# MAGIC       'Small files: run OPTIMIZE or enable predictive optimization'
# MAGIC     WHEN
# MAGIC       avg_total_files >= 100
# MAGIC       AND avg_file_pruning_ratio < 0.5
# MAGIC     THEN
# MAGIC       'Low pruning: add liquid clustering on columns Genie filters by'
# MAGIC     ELSE 'No action needed'
# MAGIC   END AS recommended_action
# MAGIC FROM
# MAGIC   agg
# MAGIC ORDER BY
# MAGIC   total_read_bytes DESC

# COMMAND ----------

# DBTITLE 1,Worst Pruned Queries: lowest file pruning, top 100
# MAGIC %sql
# MAGIC SELECT
# MAGIC   statement_id,
# MAGIC   query_source.genie_space_id AS genie_space_id,
# MAGIC   compute.warehouse_id AS warehouse_id,
# MAGIC   LEFT(statement_text, 200) AS statement_snippet,
# MAGIC   total_duration_ms,
# MAGIC   pruned_files,
# MAGIC   read_files,
# MAGIC   pruned_files + read_files AS total_files,
# MAGIC   ROUND(read_files_bytes / NULLIF(read_files, 0) / 1048576, 2) AS avg_file_size_mb,
# MAGIC   ROUND(
# MAGIC     CAST(pruned_files AS DOUBLE) / NULLIF(pruned_files + read_files, 0),
# MAGIC     4
# MAGIC   ) AS file_pruning_ratio,
# MAGIC   read_bytes,
# MAGIC   read_rows,
# MAGIC   pruned_files_bytes,
# MAGIC   read_files_bytes,
# MAGIC   ROUND(
# MAGIC     CAST(pruned_files_bytes AS DOUBLE) / NULLIF(pruned_files_bytes + read_files_bytes, 0),
# MAGIC     4
# MAGIC   ) AS bytes_pruning_ratio,
# MAGIC   start_time
# MAGIC FROM
# MAGIC   system.query.history
# MAGIC WHERE
# MAGIC   workspace_id = '7474654445014356'
# MAGIC   AND query_source.genie_space_id IS NOT NULL
# MAGIC   AND (pruned_files + read_files) > 0
# MAGIC   AND statement_text NOT LIKE '%__databricks_internal%'
# MAGIC   AND (
# MAGIC     date_range_min IS NULL
# MAGIC     OR start_time >= date_range_min
# MAGIC   )
# MAGIC   AND (
# MAGIC     date_range_max IS NULL
# MAGIC     OR start_time <= date_range_max
# MAGIC   )
# MAGIC   AND (
# MAGIC     genie_filter IS NULL
# MAGIC     OR array_contains(genie_filter, query_source.genie_space_id)
# MAGIC   )
# MAGIC ORDER BY
# MAGIC   file_pruning_ratio ASC,
# MAGIC   read_bytes DESC
# MAGIC LIMIT 100

# COMMAND ----------

# DBTITLE 1,Warehouse Config: latest configuration per warehouse
# MAGIC %sql
# MAGIC SELECT
# MAGIC   warehouse_id,
# MAGIC   warehouse_name,
# MAGIC   warehouse_type,
# MAGIC   warehouse_size,
# MAGIC   warehouse_channel,
# MAGIC   min_clusters,
# MAGIC   max_clusters,
# MAGIC   auto_stop_minutes,
# MAGIC   CASE
# MAGIC     WHEN delete_time IS NOT NULL THEN 'Deleted'
# MAGIC     ELSE 'Active'
# MAGIC   END AS status,
# MAGIC   change_time
# MAGIC FROM
# MAGIC   system.compute.warehouses
# MAGIC WHERE
# MAGIC   workspace_id = '7474654445014356'
# MAGIC QUALIFY
# MAGIC   ROW_NUMBER() OVER (PARTITION BY warehouse_id ORDER BY change_time DESC) = 1

# COMMAND ----------

# DBTITLE 1,Genie Agent Options: Genie space ids seen in last 90 days
# MAGIC %sql
# MAGIC -- Use these ids in the genie_space_ids widget
# MAGIC SELECT DISTINCT
# MAGIC   query_source.genie_space_id AS genie_agent
# MAGIC FROM
# MAGIC   system.query.history
# MAGIC WHERE
# MAGIC   workspace_id = '7474654445014356'
# MAGIC   AND query_source.genie_space_id IS NOT NULL
# MAGIC   AND start_time >= CURRENT_TIMESTAMP() - INTERVAL 90 DAYS
# MAGIC ORDER BY
# MAGIC   genie_agent