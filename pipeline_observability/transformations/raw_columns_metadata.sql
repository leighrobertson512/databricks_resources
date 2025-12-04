CREATE OR REFRESH STREAMING TABLE leigh_robertson_demo.observability.raw_columns_metadata AS
SELECT 
  catalog_name,
  schema_name,
  table_name,
  column_name,
  data_type,
  comment
FROM system.information_schema.columns;
