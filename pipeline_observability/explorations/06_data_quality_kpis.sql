SELECT COUNT(DISTINCT expectation)
FROM leigh_robertson_demo.observability.dlt_dq_violations_cleaned
WHERE dq_date = current_date();

SELECT ROUND(
  100.0 * SUM(passed_expectations) / NULLIF(SUM(passed_expectations + failed_expectations), 0), 2
)
FROM leigh_robertson_demo.observability.dlt_dq_summary
WHERE dq_date = current_date();

SELECT ROUND(
  100.0 * SUM(total_violated_records) / NULLIF(SUM(total_violated_records + passed_expectations), 0), 2
)
FROM leigh_robertson_demo.observability.dlt_dq_summary
WHERE dq_date = current_date();


SELECT COUNT(DISTINCT dataset)
FROM leigh_robertson_demo.observability.dlt_dq_violations_cleaned
WHERE dq_date = current_date() AND violated_records > 0;

SELECT SUM(dropped_record_count)
FROM leigh_robertson_demo.observability.dlt_dq_summary
WHERE dq_date = current_date();

SELECT
  ROUND(100.0 * (MAX(day_score) - MIN(day_score)) / NULLIF(MIN(day_score), 1), 2) AS trend_percent
FROM (
  SELECT dq_date, 
         ROUND((SUM(passed_expectations) / NULLIF(SUM(passed_expectations + failed_expectations), 0)) * 10, 1) AS day_score
  FROM leigh_robertson_demo.observability.dlt_dq_summary
  WHERE dq_date BETWEEN date_sub(current_date(), 4) AND current_date()
  GROUP BY dq_date
);

SELECT ROUND(AVG(null_count * 100.0 / NULLIF(record_count, 0)), 2)
FROM leigh_robertson_demo.observability.dlt_dq_violations_cleaned
WHERE dq_date = current_date();

SELECT expectation, COUNT(*) AS total_violations
FROM leigh_robertson_demo.observability.dlt_dq_violations_cleaned
WHERE dq_date = current_date()
GROUP BY expectation
ORDER BY total_violations DESC
LIMIT 1;