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