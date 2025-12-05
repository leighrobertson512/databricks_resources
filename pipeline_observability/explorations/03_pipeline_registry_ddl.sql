CREATE TABLE leigh_robertson_demo.observability.pipeline_registry (
  pipeline_id STRING,
  pipeline_name STRING,
  domain STRING,
  event_log_table STRING,
  log_enabled BOOLEAN,
  owner_email STRING,
  created_at TIMESTAMP
);

-- Example INSERT statement with placeholders for all values
-- INSERT INTO leigh_robertson_demo.observability.pipeline_registry (
--   pipeline_id,
--   pipeline_name,
--   domain,
--   event_log_table,
--   log_enabled,
--   owner_email,
--   created_at
-- ) VALUES (
--   '7f2feb6f-2e37-4a00-9c6e-87c1c48186a4',
--   'weather_ingestion_sample_pipeline',
--   'noaa',
--   'leigh_robertson_demo.observability.event_log_noaa_weather_ingestion_sample_pipeline',
--   TRUE,
--   'leigh.robertson@databricks.com',
--   current_timestamp()
-- );

SELECT * 
FROM leigh_robertson_demo.observability.pipeline_registry
LIMIT 5;