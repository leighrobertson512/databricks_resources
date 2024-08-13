# Databricks notebook source
# MAGIC %md
# MAGIC This notebook showcases a simple way to load all data from a SQL server database an efficient fashion

# COMMAND ----------

"""
Job Widgets:
- date_cutoff: use if you need to manually replay data from some point forward
- incremental: whether it should be a full or partial load
- source_table: name of the table to load
""""

# COMMAND ----------

# MAGIC %run ../utilities/utils_file

# COMMAND ----------

# MAGIC %md
# MAGIC Note: I have designed this so that each source schema has it's JSON object. This is a choice and you could put everything into object if desired. Each key in the JSON will represent a table and the values underneath are the config options

# COMMAND ----------

# MAGIC %run ../obs/obs_tables_config_json

# COMMAND ----------

from datetime import date, timedelta, datetime
from pyspark.sql.functions import col

# COMMAND ----------

# DBTITLE 1,Define variables for source and target
#define widgets
dbutils.widgets.text('source_table', 'customer_fact')
dbutils.widgets.text('incremental', 'True')
dbutils.widgets.text('date_cutoff', '')

#get source values
source_table = dbutils.widgets.get('source_table')
source_database = table_json[source_table]['source_database']
source_schema = table_json[source_table]['source_schema']
source_incremental_column = table_json[source_table]['source_incremental_column']
source = source_database.lower() + '.' + source_schema.lower() + '.' + source_table.lower()

# #get target variables
target_catalog = table_json[source_table]['target_catalog']
target_schema = table_json[source_table]['target_schema']
target_table = table_json[source_table]['target_table']
#I suggest optimizing on the merge columns as this will make the incremental load faster
optimize_columns = table_json[source_table]['target_merge_columns']
target_merge_columns = table_json[source_table]['target_merge_columns']
target_incremental_column = table_json[source_table]['target_incremental_column']
target = target_catalog.lower() + '.' + target_schema.lower() + '.' + target_table.lower()

# #define run variables
#date_column = dbutils.widgets.get('date_column')
date_cutoff = dbutils.widgets.get('date_cutoff')
incremental = dbutils.widgets.get('incremental')


# #define merge variable which can be used in the case a schema change has occured
incremental_load = boolean_value = eval(incremental)

# COMMAND ----------

# DBTITLE 1,Pull the source data 
source_df = get_source_data_demo(source_database, source_schema, source_table)

#If this step is taking too long then optimization should be made on the SQL side
if len(date_cutoff) != 0:
    #if merge is true we will merge the dataframe otherwise we will end up doing a full rewrite
    source_df = source_df.filter(col(f'{source_incremental_column}') >= date_cutoff)
elif incremental_load:
    target_df = spark.table(target)
    max_date = target_df.agg({f"{target_incremental_column}": "max"}).collect()[0][0]
    # Convert the result to a Python datetime object (optional)
    #max_date = max_date.date()
    source_df = source_df.filter(col(f'{source_incremental_column}') >= max_date)


# COMMAND ----------

source_df.display()

# COMMAND ----------

separator = ', '
target_merge_columns_clean = separator.join(target_merge_columns)
match_columns, update_columns, insert_columns = generate_match_insert_columns(target_merge_columns_clean, source_df)
# Generate the dynamic MERGE statement
merge_statement = dynamic_merge_sql(target, match_columns, update_columns, insert_columns)



# Print or execute the generated SQL statement as needed
#print(merge_statement)
if incremental_load == False:
    (source_df.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(target))
else:
    source_df.createOrReplaceTempView('updates')
    spark.sql(merge_statement)


# COMMAND ----------

separator = ', '
optimize_columns_clean = separator.join(optimize_columns)
spark_sql = (f'OPTIMIZE {target} ZORDER BY ({optimize_columns_clean})')
#print(spark_sql)
spark.sql(spark_sql)

# COMMAND ----------

#as an organization, you should determine what the vacuum period should be and create a common so that everyone uses the same value here
spark.sql(f"VACUUM {target_catalog}.{target_schema}.{target_table} RETAIN 160 HOURS")

# COMMAND ----------

# DBTITLE 1,Drop Table (I recommend doing it this if you want to drop and not recover a table)
# Use this also to drop tables and not have to worry about files not being deleted
# spark.conf.set('spark.databricks.delta.retentionDurationCheck.enabled', False)
# spark.sql(f'TRUNCATE TABLE {target_catalog}.{target_schema}.{target_table}')
# spark.sql(f"VACUUM {target_catalog}.{target_schema}.{target_table} RETAIN 0 HOURS")
# spark.sql(f"DROP TABLE {target_catalog}.{target_schema}.{target_table}")
