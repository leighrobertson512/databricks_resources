import dlt
from pyspark.sql.functions import *


@dlt.view
@dlt.expect_or_drop("valid_wind_speed", "windSpeed >= 0")
def bronze_forecasts_preprocessed():
    return (
        #dlt.read_stream
        spark.readStream
        .option("readChangeFeed", "true")
        #.option("maxBytesPerTrigger", "10g")
        .table('leigh_robertson_demo.bronze_noaa.forecasts_streaming_demo_upsert')
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
        .drop("_change_type")
        .drop("_commit_version")
        .drop("_commit_timestamp")
    )
    #return df

dlt.create_streaming_table(
    name="forecasts_expanded_ldp",
    comment="SCD Type 1 managed silver forecasts",
    table_properties={"quality": "silver"},
    # constraints={
    #     "valid_wind_speed": "windSpeed BETWEEN 0 AND 200",
    #     "valid_timestamps": "startTimeUTC <= endTimeUTC"
    # }
)

dlt.apply_changes(
    target = "forecasts_expanded_ldp",
    source = "bronze_forecasts_preprocessed",
    keys = ["post_code", "startTime"],  # Replace with your actual key
    sequence_by = col("audit_update_ts"),
    #apply_as_deletes = expr("operation = 'DELETE'"),
    #except_column_list = ["operation"],
    stored_as_scd_type = 1,
)