# Databricks notebook source
"""
Centralized configuration variables for NOAA weather data pipeline.
Update these variables once to configure the entire pipeline.
"""

# COMMAND ----------

# Catalog and Schema Configuration
catalog = 'leigh_robertson_demo'
bronze_schema = 'bronze_noaa'
silver_schema = 'silver_noaa'

# COMMAND ----------

# Table Name Configuration (base names)
zip_code_table_name_base = 'zip_code'
forecast_table_name_base = 'forecasts'
forecasts_expanded_base = 'forecasts_expanded'

# COMMAND ----------

# Full Table Names (constructed from catalog, schema, and table names)
zip_code_table_name = f"{catalog}.{bronze_schema}.{zip_code_table_name_base}"
forecast_table_name = f"{catalog}.{bronze_schema}.{forecast_table_name_base}"
forecasts_expanded_table_name = f"{catalog}.{silver_schema}.{forecasts_expanded_base}"

# COMMAND ----------

# Zip Code Loading Configuration
start_zip = 82556
end_zip = 99950

# COMMAND ----------

# Weather Forecast Configuration
default_state = 'CO'
default_country_code = 'US'

# COMMAND ----------

# Databricks Workspace Configuration
# Update this with your workspace host URL
workspace_host = "https://<your-workspace-host>"

# COMMAND ----------

