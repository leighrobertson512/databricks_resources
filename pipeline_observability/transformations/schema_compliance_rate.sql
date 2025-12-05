CREATE OR REFRESH STREAMING TABLE leigh_robertson_demo.observability.schema_compliance_rate AS
SELECT 
  pii.schema_name,
  COUNT(*) AS total_pii_columns,
  COUNT(v.table_name) AS violating_columns,
  ROUND(100 * (1 - COUNT(v.table_name)/COUNT(*)), 2) AS compliance_rate
FROM leigh_robertson_demo.observability.pii_info pii
LEFT JOIN leigh_robertson_demo.observability.pii_violations v
  ON pii.catalog_name = v.catalog_name
  AND pii.schema_name = v.schema_name
  AND pii.table_name = v.table_name
  AND pii.column_name = v.column_name
WHERE pii.is_pii = 'Yes'
GROUP BY pii.schema_name;