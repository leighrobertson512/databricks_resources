# Databricks notebook source
# MAGIC %pip install noaa_sdk

# COMMAND ----------

from noaa_sdk import NOAA
import pandas as pd
from pyspark.sql.functions import lit, current_timestamp

# COMMAND ----------

# MAGIC %run ./sql_ingestion_framework/utils_file

# COMMAND ----------

dbutils.widgets.text('state', 'NY')

# COMMAND ----------

# from noaa_sdk import NOAA

# # Initialize NOAA object
# n = NOAA()

# # Fetch weather forecast for a specific location (latitude and longitude)
# lat, lon = 40.7314, -73.8656
# forecast = n.points_forecast(lat, lon, type='forecastGridData')

# # Print the forecast
# for item in forecast:
#     print(item)

# # # Alternatively, fetch forecast by postal code and country code
# # postal_code = '11365'
# # country_code = 'US'
# # forecasts = n.get_forecasts(postal_code, country_code)
# # for forecast in forecasts:
# #     print(forecast)


# COMMAND ----------

def get_and_load_forecasts(postal_code, country_code):
    n = NOAA()
    #postal_code = '80214'
    #country_code = 'US'
    forecasts = n.get_forecasts(postal_code, country_code)

    df = pd.DataFrame(forecasts)
    df_spark = spark.createDataFrame(df).withColumn('post_code', lit(postal_code)).withColumn('audit_update_ts', lit(current_timestamp()))
    df_spark.createOrReplaceTempView('forecasts')

    table_name = 'leigh_robertson_demo.bronze_noaa.forecasts'
    match_columns, update_columns, insert_columns = generate_match_insert_columns("post_code, startTime", df_spark)
    merge_sql = dynamic_merge_sql(table_name, "updates", match_columns, update_columns, insert_columns)
    df_spark.createOrReplaceTempView("updates")
    spark.sql(merge_sql)
    print(f'loaded forecasts for {postal_code} and country {country_code}')

# COMMAND ----------

state = dbutils.widgets.get('state')
state_forecasts = f"""
SELECT distinct post_code
FROM leigh_robertson_demo.bronze_noaa.zip_code
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

# MAGIC %sql 
# MAGIC OPTIMIZE leigh_robertson_demo.bronze_noaa.forecasts;
# MAGIC VACUUM leigh_robertson_demo.bronze_noaa.forecasts;

# COMMAND ----------

# MAGIC %sql 
# MAGIC DESCRIBE HISTORY leigh_robertson_demo.bronze_noaa.forecasts;

# COMMAND ----------


