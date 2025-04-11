# Databricks notebook source
#https://adb-984752964297111.11.azuredatabricks.net/editor/notebooks/1242762840020712?o=984752964297111#command/1242762840020713

# COMMAND ----------

# DBTITLE 1,python_example_working
import dlt
from pyspark.sql.functions import *


@dlt.view
@dlt.expect_or_drop("valid_wind_speed", "windSpeed >= 0")
def bronze_forecasts_preprocessed():
    return (
        #dlt.read_stream
        spark.readStream
        .option("readChangeFeed", "true")
        .option("maxBytesPerTrigger", "10g")
        .table('leigh_robertson_demo.bronze_noaa.forecasts')
        .withColumn("timezoneOffset", regexp_extract(col("startTime"), r"([+-]\d{2}:\d{2})$", 1))
        .withColumn("startTime", regexp_replace(col("startTime"), r"[+-]\d{2}:\d{2}$", ""))
        .withColumn("endTime", regexp_replace(col("endTime"), r"[+-]\d{2}:\d{2}$", ""))
        .withColumn("startTimeUTC", 
                    when(col("timezoneOffset") != "", 
                         expr("from_utc_timestamp(startTime, timezoneOffset)"))
                    .otherwise(col("startTime")))
        .withColumn("endTimeUTC", 
                    when(col("timezoneOffset") != "", 
                         expr("from_utc_timestamp(endTime, timezoneOffset)"))
                    .otherwise(col("endTime")))
        .withColumn("windSpeed", regexp_extract(col("windSpeed"), "(\\d+)", 1).cast("int"))
        .withColumn("audit_update_ts", current_timestamp())
        .withColumn("dewpoint", col("dewpoint.value"))
        .withColumn("probabilityOfPrecipitation", col("probabilityOfPrecipitation.value"))
        .withColumn("relativeHumidity", col("relativeHumidity.value"))
    )
    #return df

dlt.create_streaming_table(
    name="forecasts_expanded_dlt",
    comment="SCD Type 1 managed silver forecasts",
    table_properties={"quality": "silver"},
    # constraints={
    #     "valid_wind_speed": "windSpeed BETWEEN 0 AND 200",
    #     "valid_timestamps": "startTimeUTC <= endTimeUTC"
    # }
)

dlt.apply_changes(
    target = "forecasts_expanded_dlt",
    source = "bronze_forecasts_preprocessed",
    keys = ["post_code", "startTime"],  # Replace with your actual key
    sequence_by = col("audit_update_ts"),
    #apply_as_deletes = expr("operation = 'DELETE'"),
    #except_column_list = ["operation"],
    stored_as_scd_type = 1,
)
