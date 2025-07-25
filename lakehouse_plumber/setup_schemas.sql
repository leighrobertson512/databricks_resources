-- Setup script to create required schemas for the weather data pipeline
-- Run this in a Databricks SQL notebook before creating the DLT pipeline

-- Create silver schema if it doesn't exist
CREATE SCHEMA IF NOT EXISTS leigh_robertson_demo.silver_noaa
COMMENT 'Silver layer for cleaned weather data';

-- Create gold schema if it doesn't exist  
CREATE SCHEMA IF NOT EXISTS leigh_robertson_demo.gold_noaa
COMMENT 'Gold layer for aggregated weather analytics';

-- Verify schemas were created
SHOW SCHEMAS IN leigh_robertson_demo;

-- Check that bronze source table exists
DESCRIBE TABLE leigh_robertson_demo.bronze_noaa.forecasts_streaming_demo;

-- Show sample data from bronze table
SELECT 
    post_code,
    temperature,
    shortForecast,
    audit_update_ts
FROM leigh_robertson_demo.bronze_noaa.forecasts_streaming_demo 
LIMIT 10; 