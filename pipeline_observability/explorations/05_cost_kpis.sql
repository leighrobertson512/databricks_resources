SELECT SUM(dbus_7d)
FROM leigh_robertson_demo.observability.cost_observability_summary
WHERE summary_date = current_date();

SELECT ROUND(AVG(avg_cost_per_pipeline), 2)
FROM leigh_robertson_demo.observability.cost_observability_summary
WHERE summary_date = current_date();

SELECT pipeline_name, total_cost_usd
FROM leigh_robertson_demo.observability.cost_observability_summary
WHERE summary_date = current_date()
ORDER BY total_cost_usd DESC
LIMIT 1;

SELECT ROUND(AVG(idle_time_pct), 2)
FROM leigh_robertson_demo.observability.cost_observability_summary
WHERE summary_date = current_date();

SELECT SUM(cost_anomaly_flag)
FROM leigh_robertson_demo.observability.cost_observability_summary
WHERE summary_date = current_date();

SELECT most_expensive_table, MAX(total_cost_usd)
FROM leigh_robertson_demo.observability.cost_observability_summary
WHERE summary_date = current_date()
GROUP BY most_expensive_table
ORDER BY MAX(total_cost_usd) DESC
LIMIT 1;

SELECT ROUND(SUM(cost_forecast_7d), 2)
FROM leigh_robertson_demo.observability.cost_observability_summary
WHERE summary_date = current_date();


SELECT
  ROUND(SUM(job_compute_cost), 2) AS job_cost,
  ROUND(SUM(warehouse_cost), 2) AS warehouse_cost
FROM leigh_robertson_demo.observability.cost_observability_summary
WHERE summary_date = current_date();

SELECT cost_by_cluster, SUM(total_cost_usd) AS total_cost
FROM leigh_robertson_demo.observability.cost_observability_summary
WHERE summary_date = current_date()
GROUP BY cost_by_cluster;

SELECT ROUND(
  (SUM(total_cost_usd) - LAG(SUM(total_cost_usd), 5) OVER ())
  / NULLIF(LAG(SUM(total_cost_usd), 5) OVER (), 0) * 100, 2
) AS cost_trend_pct
FROM leigh_robertson_demo.observability.cost_observability_summary
WHERE summary_date <= current_date()
GROUP BY summary_date
ORDER BY summary_date DESC
LIMIT 1;