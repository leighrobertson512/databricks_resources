CREATE OR REPLACE TABLE observability.gold.refresh_dlt_observability_summary AS
SELECT *
FROM observability.gold.dlt_observability_summary
WHERE summary_date = current_date();