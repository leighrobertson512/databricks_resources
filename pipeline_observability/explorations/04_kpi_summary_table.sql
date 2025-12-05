
--Total pipelines monitored 
SELECT COUNT(DISTINCT pipeline_name)
FROM observability.gold.dlt_observability_summary
WHERE summary_date = current_date();

--Pipelines failed today
SELECT SUM(pipelines_failed_today)
FROM observability.gold.dlt_observability_summary
WHERE summary_date = current_date();

--Streaming backlog in MB
SELECT SUM(streaming_backlog_mb)
FROM observability.gold.dlt_observability_summary
WHERE summary_date = current_date();

--Average runtime in minutes
SELECT AVG(avg_runtime_minutes)
FROM observability.gold.dlt_observability_summary
WHERE summary_date = current_date();

--Retry count in the last 7 days        
SELECT SUM(retry_count_7d)
FROM observability.gold.dlt_observability_summary
WHERE summary_date = current_date();

--Longest runtime in minutes
SELECT MAX(longest_runtime_minutes)
FROM observability.gold.dlt_observability_summary
WHERE summary_date = current_date();

--Pipelines with autoloader
SELECT SUM(pipelines_with_autoloader)
FROM observability.gold.dlt_observability_summary   
WHERE summary_date = current_date();

--Tables created
SELECT SUM(tables_created)
FROM observability.gold.dlt_observability_summary
WHERE summary_date = current_date();

--Active streaming pipelines
SELECT SUM(active_streaming_pipelines)
FROM observability.gold.dlt_observability_summary
WHERE summary_date = current_date();

--Last failure timestamp
SELECT MAX(last_failure_ts)
FROM observability.gold.dlt_observability_summary
WHERE summary_date = current_date();