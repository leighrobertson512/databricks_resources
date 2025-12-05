CREATE OR REFRESH STREAMING TABLE leigh_robertson_demo.observability.pii_by_schema AS
SELECT 
  schema_name,
  COUNT(*) AS pii_column_count
FROM pii_info
WHERE is_pii = 'Yes'
GROUP BY schema_name;

CREATE OR REFRESH STREAMING TABLE leigh_robertson_demo.observability.pii_info AS
SELECT *,
  CASE
    WHEN lower(column_name) RLIKE 'email|phone|ssn|dob|address|name|credit|tax|passport'
      OR lower(coalesce(comment, '')) RLIKE 'email|phone|ssn|dob|address|name|credit|tax|passport'
    THEN 'Yes'
    ELSE 'No'
  END AS is_pii
FROM STREAM(leigh_robertson_demo.observability.raw_columns_metadata);


CREATE OR REFRESH STREAMING TABLE leigh_robertson_demo.observability.pii_kpi_summary AS
SELECT
  COUNT(DISTINCT table_name) AS total_tables_scanned,
  COUNT(DISTINCT CASE WHEN is_pii = 'Yes' THEN table_name END) AS tables_with_pii,
  COUNT(CASE WHEN is_pii = 'Yes' THEN 1 END) AS pii_columns,
  COUNT(DISTINCT schema_name) AS total_schemas,
  COUNT(*) FILTER (WHERE violation_type IS NOT NULL) AS total_violations
FROM STREAM(leigh_robertson_demo.observability.pii_info)
LEFT JOIN STREAM(leigh_robertson_demo.observability.pii_violations) USING (catalog_name, schema_name, table_name, column_name);

CREATE OR REFRESH STREAMING TABLE leigh_robertson_demo.observability.pii_type_distribution AS
SELECT 
  CASE
    WHEN lower(column_name) LIKE '%email%' THEN 'email'
    WHEN lower(column_name) LIKE '%phone%' THEN 'phone'
    WHEN lower(column_name) LIKE '%ssn%' THEN 'ssn'
    WHEN lower(column_name) LIKE '%dob%' THEN 'dob'
    WHEN lower(column_name) LIKE '%address%' THEN 'address'
    WHEN lower(column_name) LIKE '%name%' THEN 'name'
    WHEN lower(column_name) LIKE '%credit%' THEN 'credit'
    WHEN lower(column_name) LIKE '%tax%' THEN 'tax'
    WHEN lower(column_name) LIKE '%passport%' THEN 'passport'
    ELSE 'other'
  END AS pii_type,
  COUNT(*) AS count
FROM STREAM(leigh_robertson_demo.observability.pii_info)
WHERE is_pii = 'Yes'
GROUP BY pii_type;

CREATE OR REFRESH STREAMING TABLE leigh_robertson_demo.observability.pii_violation_trend AS
SELECT
  current_date() AS snapshot_date,
  violation_type,
  COUNT(*) AS total
FROM STREAM(leigh_robertson_demo.observability.pii_violations)
GROUP BY violation_type;


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


CREATE OR REFRESH STREAMING TABLE leigh_robertson_demo.observability.schema_compliance_rate AS
SELECT 
  pii.schema_name,
  COUNT(*) AS total_pii_columns,
  COUNT(v.table_name) AS violating_columns,
  ROUND(100 * (1 - COUNT(v.table_name)/COUNT(*)), 2) AS compliance_rate
FROM STREAM(leigh_robertson_demo.observability.pii_info) pii
LEFT JOIN STREAM(leigh_robertson_demo.observability.pii_violations) v
  ON pii.catalog_name = v.catalog_name
  AND pii.schema_name = v.schema_name
  AND pii.table_name = v.table_name
  AND pii.column_name = v.column_name
WHERE pii.is_pii = 'Yes'
GROUP BY pii.schema_name;


CREATE OR REFRESH STREAMING TABLE leigh_robertson_demo.observability.top_violating_tables AS
SELECT 
  table_name,
  COUNT(*) AS violation_count
FROM STREAM(leigh_robertson_demo.observability.pii_violations)
GROUP BY table_name
ORDER BY violation_count DESC;
LIMIT 10;

CREATE OR REFRESH STREAMING TABLE leigh_robertson_demo.observability.violation_type_distribution AS
SELECT 
  violation_type,
  COUNT(*) AS count
FROM STREAM(leigh_robertson_demo.observability.pii_violations)
GROUP BY violation_type;
