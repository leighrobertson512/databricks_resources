# Databricks notebook source
# MAGIC %md 
# MAGIC From a design standpoint, you can either have each config object represent or put it all into one. I have a preference for config at the schema level but you can experiment and decide what works best for if

# COMMAND ----------

table_json = {
  "forecasts": {
    "source_database": "leigh_robertson_demo",
    "source_schema": "bronze_noaa",
    "source_incremental_column": "audit_update_ts",
    "target_catalog": "leigh_robertson_demo",
    "target_schema": "silver_noaa",
    "target_table": "forecasts_expanded_manual_cdc",
    "target_merge_columns": ["post_code", "startTime"],
    "target_incremental_column": "audit_update_ts",
    
  }
}

# COMMAND ----------


