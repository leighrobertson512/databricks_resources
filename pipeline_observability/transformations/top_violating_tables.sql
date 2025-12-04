CREATE OR REFRESH STREAMING TABLE leigh_robertson_demo.observability.top_violating_tables AS
SELECT 
  table_name,
  COUNT(*) AS violation_count
FROM STREAM(leigh_robertson_demo.observability.pii_violations)
GROUP BY table_name
ORDER BY violation_count DESC
LIMIT 10;