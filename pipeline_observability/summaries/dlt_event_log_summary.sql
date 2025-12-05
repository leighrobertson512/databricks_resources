CREATE OR REPLACE TABLE observability.silver.dlt_event_log_summary AS 
WITH pipeline_updates AS (
    -- Filter the cleaned event logs for 'COMPLETED' or 'FAILED' events, 
    -- as these mark the end of a DLT pipeline update run.
    SELECT 
        timestamp,
        origin.update_id AS update_id,
        origin.pipeline_id AS pipeline_id,
        pipeline_name,
        details:cluster_resources.default_cluster_info.cluster_id AS cluster_id, -- Used to derive job_id
        details:cluster_resources.default_cluster_info.cluster_name AS cluster_type, -- Single-node or Multi-node
        details:flow_progress.metrics.output_table AS output_table, -- Example of extracting a table name
        details:cluster_resources.default_cluster_info.idle_minutes AS idle_minutes,
        details:cluster_resources.default_cluster_info.active_minutes AS active_minutes,
        details:cluster_resources.default_cluster_info.job_id AS job_id -- Assumed field for join
    FROM 
        observability.silver.event_log_cleaned 
    WHERE 
        event_type IN ('create_update', 'update_complete', 'update_failed')
)
SELECT 
    pipeline_id,
    pipeline_name,
    -- DLT's cluster ID is often registered as the job_id in billing.usage
    COALESCE(job_id, cluster_id) AS job_id, 
    MAX(output_table) AS output_table,
    MAX(cluster_type) AS cluster_type,
    SUM(idle_minutes) AS idle_minutes,
    SUM(active_minutes) AS active_minutes,
    MAX(timestamp) AS update_time
FROM 
    pipeline_updates
GROUP BY 
    pipeline_id, 
    pipeline_name, 
    COALESCE(job_id, cluster_id), 
    update_id;