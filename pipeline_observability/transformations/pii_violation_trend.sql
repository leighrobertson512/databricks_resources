CREATE OR REFRESH STREAMING TABLE leigh_robertson_demo.observability.pii_violation_trend AS
SELECT
  current_date() AS snapshot_date,
  violation_type,
  COUNT(*) AS total
FROM STREAM(leigh_robertson_demo.observability.pii_violations)
GROUP BY violation_type;