# Databricks notebook source
def add_tag_to_table(table_name, tag, value):
    alter_table_sql = f"ALTER TABLE {table_name} SET TBLPROPERTIES ('{tag}' = '{value}')"
    spark.sql(alter_table_sql)

# Example usage
table_name = "leigh_robertson_demo.bronze_noaa.zip_code"
tag = "RemoveAfter"
value = "2025-09-01"
add_tag_to_table(table_name, tag, value)

# COMMAND ----------

def add_tag_to_schema(schema_name, tag, value):
    alter_schema_sql = f"ALTER SCHEMA {schema_name} SET DBPROPERTIES ('{tag}' = '{value}')"
    spark.sql(alter_schema_sql)

# Example usage
schema_name = "leigh_robertson_demo.bronze_noaa"
tag = "RemoveAfter"
value = "2025-09-01"
add_tag_to_schema(schema_name, tag, value)

# COMMAND ----------

def add_tag_to_catalog(catalog_name, tag, value):
    alter_catalog_sql = f"ALTER CATALOG {catalog_name} SET '{tag}' = '{value}'"
    spark.sql(alter_catalog_sql)

# Example usage
catalog_name = "leigh_robertson_demo"
tag = "RemoveAfter"
value = "2025-09-01"
add_tag_to_catalog(catalog_name, tag, value)

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER CATALOG leigh_robertson_demo SET TAGS ("RemoveAfter"='2025-09-01') 
