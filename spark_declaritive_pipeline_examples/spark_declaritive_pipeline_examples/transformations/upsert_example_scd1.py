#this file shows how to handle 

from pyspark import pipelines as dp
from pyspark.sql.functions import *
from utilities import append_audit_columns



@dp.temporary_view
@dp.expect("valid_wind_speed", "windSpeed >= 0")
def bronze_forecasts_preprocessed():
    return (
        #dlt.read_stream
        spark.readStream
        .option("readChangeFeed", "true")
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
        .transform(append_audit_columns.add_audit_columns)
        .withColumn("dewpoint", col("dewpoint.value"))
        .withColumn("probabilityOfPrecipitation", col("probabilityOfPrecipitation.value"))
        .withColumn("relativeHumidity", col("relativeHumidity.value"))
        .drop("_change_type")
        .drop("_commit_version")
        .drop("_commit_timestamp")
    )
    #return df

dp.create_streaming_table(
    name="forecasts_expanded_ldp",
    comment="SCD Type 1 managed silver forecasts",
    table_properties={"quality": "silver"},
    # constraints={
    #     "valid_wind_speed": "windSpeed BETWEEN 0 AND 200",
    #     "valid_timestamps": "startTimeUTC <= endTimeUTC"
    # }
)

dp.create_auto_cdc_flow(
    target = "forecasts_expanded_ldp",
    source = "bronze_forecasts_preprocessed",
    keys = ["post_code", "startTime"],  # Replace with your actual key
    sequence_by = col("audit_update_ts"),
    #apply_as_deletes = expr("operation = 'DELETE'"),
    #except_column_list = ["operation"],
    stored_as_scd_type = 1,
)