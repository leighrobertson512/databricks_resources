CREATE OR REFRESH STREAMING TABLE leigh_robertson_demo.observability.pii_kpi_summary AS
SELECT
  COUNT(DISTINCT table_name) AS total_tables_scanned,
  COUNT(DISTINCT CASE WHEN is_pii = 'Yes' THEN table_name END) AS tables_with_pii,
  COUNT(CASE WHEN is_pii = 'Yes' THEN 1 END) AS pii_columns,
  COUNT(DISTINCT schema_name) AS total_schemas,
  COUNT(*) FILTER (WHERE violation_type IS NOT NULL) AS total_violations
FROM STREAM(leigh_robertson_demo.observability.pii_info)
LEFT JOIN STREAM(leigh_robertson_demo.observability.pii_violations) USING (catalog_name, schema_name, table_name, column_name);
