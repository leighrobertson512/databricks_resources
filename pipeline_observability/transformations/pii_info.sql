CREATE OR REFRESH STREAMING TABLE leigh_robertson_demo.observability.pii_info AS
SELECT *,
  CASE
    WHEN lower(column_name) RLIKE 'email|phone|ssn|dob|address|name|credit|tax|passport'
      OR lower(coalesce(comment, '')) RLIKE 'email|phone|ssn|dob|address|name|credit|tax|passport'
    THEN 'Yes'
    ELSE 'No'
  END AS is_pii
FROM STREAM(leigh_robertson_demo.observability.raw_columns_metadata);
