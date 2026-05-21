# Databricks notebook source
# MAGIC %pip install noaa_sdk

# COMMAND ----------

#define these variables up front
catalog = 'serverless_stable_phngd8_catalog'
bronze_schema = 'bronze_noaa'
silver_schema = 'silver_noaa'

#table_specific variables
zip_code_table_name = 'zip_code'
forecast_table_name = 'forecasts'
forecasts_expanded = 'forecasts_expanded'

# COMMAND ----------

from noaa_sdk import NOAA
import pandas as pd
from pyspark.sql.functions import lit, current_timestamp

# COMMAND ----------

# MAGIC %run ./sql_ingestion_framework/utils_file

# COMMAND ----------

dbutils.widgets.text('state', 'CO')

# COMMAND ----------

def get_and_load_forecasts(postal_code, country_code):
    n = NOAA()
    #postal_code = '80214'
    #country_code = 'US'
    forecasts = n.get_forecasts(postal_code, country_code)

    df = pd.DataFrame(forecasts)
    df_spark = spark.createDataFrame(df).withColumn('post_code', lit(postal_code)).withColumn('audit_update_ts', lit(current_timestamp()))
    df_spark.createOrReplaceTempView('forecasts')

    table_name = f'{catalog}.{bronze_schema}.{forecast_table_name}'
    match_columns, update_columns, insert_columns = generate_match_insert_columns("post_code, startTime", df_spark)
    merge_sql = dynamic_merge_sql(table_name, "updates", match_columns, update_columns, insert_columns)
    df_spark.createOrReplaceTempView("updates")
    spark.sql(merge_sql)
    print(f'loaded forecasts for {postal_code} and country {country_code}')

# COMMAND ----------

state = dbutils.widgets.get('state')
state_forecasts = f"""
SELECT distinct post_code
FROM {catalog}.{bronze_schema}.{zip_code_table_name}
WHERE state_abbreviation = '{state}'
"""
df = spark.sql(state_forecasts)
for row in df.collect():
    postal_code = row['post_code']
    try:
        get_and_load_forecasts(postal_code, 'US')
    except Exception as e:
        print(f'Failed to load forecasts for {postal_code}: {e}')

# COMMAND ----------

spark.sql(f"OPTIMIZE {catalog}.{bronze_schema}.{forecast_table_name};")
spark.sql(f"VACUUM {catalog}.{bronze_schema}.{forecast_table_name};") 

# COMMAND ----------


