CREATE OR REFRESH STREAMING  TABLE leigh_robertson_demo.observability.pii_violations AS
SELECT
  pii.catalog_name,
  pii.schema_name,
  pii.table_name,
  pii.column_name,
  'Public access to PII' AS violation_type
FROM STREAM(leigh_robertson_demo.observability.pii_info) pii
INNER JOIN system.information_schema.table_privileges tp
  ON pii.catalog_name = tp.catalog_name
  AND pii.schema_name = tp.schema_name
  AND pii.table_name = tp.table_name
WHERE pii.is_pii = 'Yes'
  AND lower(tp.grantee) = 'public';
