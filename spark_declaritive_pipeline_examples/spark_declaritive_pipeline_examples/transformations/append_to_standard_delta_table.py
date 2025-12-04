from pyspark import pipelines as dp
from pyspark.sql.functions import *

dp.create_sink("forecasts_expanded_delta_sink", "delta", {"tableName": "leigh_robertson_demo.silver_noaa.forecasts_expanded_delta_sink"})



#@append_flow(name = "flow", target = "my_sink")
@dp.append_flow(name = "forecasts_expanded_delta_sink_flow", target="forecasts_expanded_delta_sink")
def flowFunc():
    return (
        spark.readStream
        .table('leigh_robertson_demo.bronze_noaa.forecasts_streaming_demo')
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