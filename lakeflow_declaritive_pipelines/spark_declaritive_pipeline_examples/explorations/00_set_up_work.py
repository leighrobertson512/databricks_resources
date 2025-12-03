# Databricks notebook source
#define these variables up front
catalog = 'leigh_robertson_demo'
bronze_schema = 'bronze_noaa'
silver_schema = 'silver_noaa'

#table_specific variables
zip_code_table_name = 'zip_code'
forecast_table_name = 'forecasts'
forecasts_expanded = 'forecasts_expanded'

# COMMAND ----------

#this will build the DDL 
zip_code_ddl = f"""
CREATE CATALOG IF NOT EXISTS {catalog};
CREATE SCHEMA IF NOT EXISTS {catalog}.{bronze_schema};
CREATE TABLE IF NOT EXISTS {catalog}.{bronze_schema}.{zip_code_table_name} (
    post_code STRING PRIMARY KEY,
    country STRING,
    country_abbreviation STRING,
    latitude DOUBLE,
    longitude DOUBLE,
    place_name STRING,
    state STRING,
    state_abbreviation STRING,
    audit_update_ts TIMESTAMP
)
CLUSTER BY AUTO;
"""
#spark.sql(zip_code_ddl)


# COMMAND ----------

forecasts_ddl = f"""
CREATE TABLE IF NOT EXISTS {catalog}.{bronze_schema}.{forecast_table_name} (
    post_code STRING NOT NULL,
    number BIGINT,
    name STRING,
    startTime STRING NOT NULL,
    endTime STRING,
    isDaytime BOOLEAN,
    temperature BIGINT,
    temperatureUnit STRING,
    temperatureTrend STRING,
    probabilityOfPrecipitation STRUCT<unitCode: STRING, value: BIGINT>,
    dewpoint STRUCT<unitCode: STRING, value: DOUBLE>,
    relativeHumidity STRUCT<unitCode: STRING, value: BIGINT>,
    windSpeed STRING,
    windDirection STRING,
    icon STRING,
    shortForecast STRING,
    detailedForecast STRING,
    audit_update_ts TIMESTAMP
)
CLUSTER BY AUTO;
"""
#spark.sql(zip_code_ddl)
forecasts_pk_sql = f"ALTER TABLE {catalog}.{bronze_schema}.{forecast_table_name} ADD CONSTRAINT forecasts_pk PRIMARY KEY (post_code, startTime);"
forecasts_fk_sql = f"ALTER TABLE {catalog}.{bronze_schema}.{forecast_table_name} ADD CONSTRAINT forecasts_fk FOREIGN KEY (post_code) REFERENCES leigh_robertson_demo.bronze_noaa.zip_code(post_code);"
#spark.sql(forecasts_pk_sql)
#spark.sql(forecasts_fk_sql)

# COMMAND ----------

silver_table_ddl = f"""
CREATE SCHEMA IF NOT EXISTS {catalog}.;
CREATE TABLE IF NOT EXISTS  {catalog}.{silver_schema}.{forecasts_expanded} (
    post_code STRING NOT NULL,
    number LONG,
    name STRING,
    startTime STRING NOT NULL,
    endTime STRING,
    isDaytime BOOLEAN,
    temperature LONG,
    temperatureUnit STRING,
    temperatureTrend STRING,
    probabilityOfPrecipitation LONG,
    dewpoint DOUBLE,
    relativeHumidity LONG,
    windSpeed INTEGER,
    windDirection STRING,
    icon STRING,
    shortForecast STRING,
    detailedForecast STRING,
    audit_update_ts TIMESTAMP,
    timezoneOffset STRING,
    startTimeUTC STRING,
    endTimeUTC STRING
)
CLUSTER BY AUTO;
"""
#spark.sql(silver_table_ddl)
forecasts_silver_pk_sql = f"ALTER TABLE {catalog}.{silver_schema}.{forecasts_expanded} ADD CONSTRAINT forecasts_pk PRIMARY KEY (post_code, startTime);"
forecasts_silver_fk_sql = f"ALTER TABLE {catalog}.{silver_schema}.{forecasts_expanded} ADD CONSTRAINT forecasts_fk FOREIGN KEY (post_code) REFERENCES leigh_robertson_demo.bronze_noaa.zip_code(post_code);"
spark.sql(forecasts_silver_pk_sql)
spark.sql(forecasts_silver_fk_sql)

# COMMAND ----------


