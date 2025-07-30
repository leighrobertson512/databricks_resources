# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE OR REPLACE MATERIALIZED VIEW leigh_robertson_demo.observability.tables_columns_genie
# MAGIC SCHEDULE EVERY 4 HOURS
# MAGIC TBLPROPERTIES ('pipelines.autoOptimize.zOrderCols'= 'table_catalog,table_name, table_schema, column_name')
# MAGIC COMMENT 'All tables and columns in a single table'
# MAGIC AS 
# MAGIC SELECT tables.table_catalog,
# MAGIC tables.table_schema,
# MAGIC tables.table_name,
# MAGIC tables.table_type,
# MAGIC tables.table_owner,
# MAGIC tables.comment AS table_comment,
# MAGIC tables.created,
# MAGIC tables.created_by,
# MAGIC tables.last_altered,
# MAGIC tables.last_altered_by,
# MAGIC columns.column_name,
# MAGIC columns.data_type,
# MAGIC columns.comment AS column_comment
# MAGIC FROM system.information_schema.tables AS tables
# MAGIC INNER JOIN system.information_schema.columns AS columns
# MAGIC ON tables.table_name = columns.table_name
# MAGIC AND tables.table_schema = columns.table_schema
# MAGIC AND tables.table_catalog = columns.table_catalog
# MAGIC WHERE
# MAGIC   tables.table_catalog != '__databricks_internal'
