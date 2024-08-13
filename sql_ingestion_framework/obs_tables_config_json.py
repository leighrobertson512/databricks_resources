# Databricks notebook source
# MAGIC %md 
# MAGIC From a design standpoint, you can either have each config object represent or put it all into one. I have a preference for config at the schema level but you can experiment and decide what works best for if

# COMMAND ----------

table_json = {
  "customer_fact": {
    "source_database": "raw_ingestion_data__dev",
    "source_schema": "lf_demo",
    "source_incremental_column": "audit_update_ts",
    "target_catalog": "raw_ingestion_data__dev",
    "target_schema": "lf_demo_target",
    "target_table": "customer_fact_target_leigh",
    "target_merge_columns": ["c_custkey"],
    "target_incremental_column": "audit_update_ts",
    
  },
  "source_table_1": {
    "source_database": "source_db_2",
    "source_schema": "source_schema_2",
    "source_table": "source_table_2",
    "source_incremental_column": "audit_update_ts",
    "target_catalog": "target_catalog",
    "target_schema": "target_schema",
    "target_table": "target_table_2",
    "target_merge_column": "c_custkey",
    "target_incremental_column": "audit_update_ts",
  }
}

# COMMAND ----------


