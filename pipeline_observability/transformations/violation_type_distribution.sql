CREATE OR REFRESH STREAMING TABLE leigh_robertson_demo.observability.violation_type_distribution AS
SELECT 
  violation_type,
  COUNT(*) AS count
FROM STREAM(leigh_robertson_demo.observability.pii_violations)
GROUP BY violation_type;